/** Web-of-trust introduction and repository discovery. */
import { Effect } from "effect";

import type { Oid } from "../git/Store.ts";
import { isRepoId, type RepoId } from "../trust/Genesis.ts";
import { canonicalUrl } from "../trust/KnownRepos.ts";
import { isPrincipalId, type PrincipalId } from "../trust/Principal.ts";
import { pathsTo, type Projection, type TrustPath } from "./Projection.ts";

export interface RepositoryEvidence {
  readonly repo: RepoId;
  readonly url: string;
  readonly role: "origin" | "mirror" | "fork";
  readonly forkOf: RepoId | null;
  readonly lineage: string | null;
  readonly inbox: string | null;
  readonly author: PrincipalId;
  readonly commit: Oid;
  readonly paths: ReadonlyArray<TrustPath>;
}

const normal = (value: string): string | null => {
  try {
    const url = new URL(value);
    const path = url.pathname
      .replace(/\/+$/, "")
      .replace(/\.git$/, "")
      .replace(/\/+$/, "");
    return `${url.protocol}//${url.host}${path}`;
  } catch {
    return null;
  }
};

/** Every reachable repository attestation, one row per URL. */
export const repositories = (projection: Projection): ReadonlyArray<RepositoryEvidence> => {
  const found: RepositoryEvidence[] = [];
  for (const statement of projection.active) {
    if (statement.payload.type !== "social.attest.repo") continue;
    if (!isPrincipalId(statement.payload.author) || !isRepoId(statement.payload.repo)) continue;
    if (statement.payload.forkOf !== null && !isRepoId(statement.payload.forkOf)) continue;
    const paths = pathsTo(projection, statement.payload.author, "introduce.repo");
    if (paths.length === 0) continue;
    for (const url of statement.payload.urls) {
      const canonical = normal(url);
      if (canonical === null) continue;
      found.push({
        repo: statement.payload.repo,
        url: canonical,
        role: statement.payload.role,
        forkOf: statement.payload.forkOf,
        lineage: statement.payload.lineage,
        inbox: statement.payload.inbox,
        author: statement.payload.author,
        commit: statement.commit,
        paths,
      });
    }
  }
  return found.sort((left, right) =>
    left.repo !== right.repo
      ? left.repo < right.repo
        ? -1
        : 1
      : left.url !== right.url
        ? left.url < right.url
          ? -1
          : 1
        : left.commit < right.commit
          ? -1
          : 1,
  );
};

/**
 * Independent evidence across different attesters.
 *
 * The attester is a dependency here, unlike the common subject of several
 * paths in `Projection.confidence`: Bob's own attestation and Carol's word
 * reached only through Bob are not two independent introductions.
 */
const independentEvidence = (
  evidence: ReadonlyArray<RepositoryEvidence>,
): ReadonlyArray<{ readonly evidence: RepositoryEvidence; readonly path: TrustPath }> => {
  const candidates: Array<{ readonly evidence: RepositoryEvidence; readonly path: TrustPath }> = [];
  const seen = new Set<string>();
  for (const item of evidence) {
    for (const path of item.paths) {
      const key = `${item.author}\u0000${path.principals.join("\u0000")}`;
      if (seen.has(key)) continue;
      seen.add(key);
      candidates.push({ evidence: item, path });
    }
  }
  candidates.sort((left, right) =>
    left.path.principals.length !== right.path.principals.length
      ? left.path.principals.length - right.path.principals.length
      : left.path.principals.join("") < right.path.principals.join("")
        ? -1
        : 1,
  );

  const dependencies = new Set<PrincipalId>();
  const authors = new Set<PrincipalId>();
  const selected: Array<{ readonly evidence: RepositoryEvidence; readonly path: TrustPath }> = [];
  for (const candidate of candidates) {
    if (authors.has(candidate.evidence.author)) continue;
    const depends = candidate.path.principals.slice(1);
    if (depends.some((principal) => dependencies.has(principal))) continue;
    selected.push(candidate);
    authors.add(candidate.evidence.author);
    for (const principal of depends) dependencies.add(principal);
  }
  return selected;
};

export interface Claim {
  readonly repo: RepoId;
  readonly paths: number;
  readonly attesters: ReadonlyArray<PrincipalId>;
}

export type Decision =
  | { readonly kind: "tofu"; readonly repoId: RepoId }
  | {
      readonly kind: "introduced";
      readonly repoId: RepoId;
      readonly paths: number;
      readonly attesters: ReadonlyArray<PrincipalId>;
    }
  | { readonly kind: "split"; readonly presented: RepoId; readonly claims: ReadonlyArray<Claim> };

/** Decide a first sighting against the verifier's own rooted web. */
export const decide = Effect.fn("social.Introduce.decide")(function* (input: {
  readonly projection: Projection;
  readonly url: string;
  readonly presented: RepoId;
  readonly minPaths?: number;
}) {
  const wanted = yield* canonicalUrl(input.url);
  const matching = repositories(input.projection).filter((evidence) => evidence.url === wanted);
  const grouped = new Map<RepoId, RepositoryEvidence[]>();
  for (const evidence of matching) {
    grouped.set(evidence.repo, [...(grouped.get(evidence.repo) ?? []), evidence]);
  }

  const claims: Claim[] = [];
  for (const [repo, evidence] of grouped) {
    const independent = independentEvidence(evidence);
    if (independent.length === 0) continue;
    claims.push({
      repo,
      paths: independent.length,
      attesters: independent.map(({ evidence }) => evidence.author),
    });
  }
  claims.sort((left, right) => (left.repo < right.repo ? -1 : 1));

  const claim = claims[0];
  if (claims.length > 1 || (claim !== undefined && claim.repo !== input.presented)) {
    return { kind: "split", presented: input.presented, claims } as const;
  }
  const minPaths = Math.max(1, input.minPaths ?? 1);
  if (claim?.repo === input.presented && claim.paths >= minPaths) {
    return {
      kind: "introduced",
      repoId: input.presented,
      paths: claim.paths,
      attesters: claim.attesters,
    } as const;
  }
  return { kind: "tofu", repoId: input.presented } as const;
});
