/** A verifier-rooted, deterministic projection over locally held social logs. */
import { Context, Effect, Layer } from "effect";

import type { Oid } from "../git/Store.ts";
import { isPrincipalId, principalOf, type PrincipalId } from "../trust/Principal.ts";
import type { VerifiedLog, VerifiedStatement } from "./Log.ts";
import { SOCIAL_SCOPES, type SocialScope } from "./Statement.ts";

const MAX_DEPTH = 16;
const MAX_PATHS = 4096;
const MAX_CHECKPOINTS = 32;
const CLOCK_SKEW_MS = 300_000;
const scopes = new Set<string>(SOCIAL_SCOPES);

const isSocialScope = (scope: string): scope is SocialScope => scopes.has(scope);

export interface TrustPath {
  /** Root first, reached subject last. */
  readonly principals: ReadonlyArray<PrincipalId>;
  /** The vouch statements whose edges make this path. */
  readonly statements: ReadonlyArray<Oid>;
  /** Intersection of every edge's scope. */
  readonly scopes: ReadonlySet<SocialScope>;
  /** How many further vouch edges the narrowest edge still allows. */
  readonly remainingDepth: number;
}

export interface ProjectionRejected {
  readonly commit: Oid;
  readonly reason: string;
}

export interface SocialCheckpoint {
  readonly principal: PrincipalId;
  readonly commit: Oid;
  readonly at: Date;
  readonly frontier: ReadonlyArray<string>;
}

export interface Projection {
  readonly roots: ReadonlyArray<PrincipalId>;
  readonly logs: ReadonlyArray<VerifiedLog>;
  readonly active: ReadonlyArray<VerifiedStatement>;
  readonly paths: ReadonlyMap<PrincipalId, ReadonlyArray<TrustPath>>;
  readonly petnames: ReadonlyMap<PrincipalId, ReadonlyMap<PrincipalId, string>>;
  /** Bounded newest-first checkpoint evidence, grouped by the log that made it. */
  readonly checkpoints: ReadonlyMap<PrincipalId, ReadonlyArray<SocialCheckpoint>>;
  readonly rejected: ReadonlyArray<ProjectionRejected>;
  readonly at: Date;
  readonly truncated: boolean;
}

/** Verified social logs already synchronized for one policy decision. */
export class SocialWeb extends Context.Service<
  SocialWeb,
  { readonly logs: Effect.Effect<ReadonlyArray<VerifiedLog>> }
>()("social/Projection/SocialWeb") {}

export const socialWebInMemory = (logs: ReadonlyArray<VerifiedLog>): Layer.Layer<SocialWeb> =>
  Layer.succeed(SocialWeb)({ logs: Effect.succeed(logs) });

/**
 * Current statements in one log.
 *
 * Revoke edges point backwards. Walking newest-to-oldest therefore settles a
 * revoke before its target and naturally handles revoking a revoke: an
 * inactive revoke never withdraws its own target.
 */
interface ActiveStatements {
  readonly active: ReadonlyArray<VerifiedStatement>;
  readonly rejected: ReadonlyArray<ProjectionRejected>;
}

const activeOf = (log: VerifiedLog): ActiveStatements => {
  const positions = new Map<string, number>();
  const statements = new Map<string, VerifiedStatement>();
  for (const [index, statement] of log.statements.entries()) {
    if (!positions.has(statement.payload.id)) {
      positions.set(statement.payload.id, index);
      statements.set(statement.payload.id, statement);
    }
  }

  const reaches = (from: Oid, target: Oid): boolean => {
    if (from === target) return true;
    if (log.parents === undefined) return true;
    const seen = new Set<Oid>();
    const pending = [...(log.parents.get(from) ?? [])];
    while (pending.length > 0) {
      const commit = pending.pop();
      if (commit === undefined || seen.has(commit)) continue;
      if (commit === target) return true;
      seen.add(commit);
      pending.push(...(log.parents.get(commit) ?? []));
    }
    return false;
  };

  const withdrawn = new Set<string>();
  const superseded = new Set<string>();
  const active: VerifiedStatement[] = [];
  const rejected: ProjectionRejected[] = [];
  for (let index = log.statements.length - 1; index >= 0; index--) {
    const statement = log.statements[index];
    if (statement === undefined) continue;
    if (withdrawn.has(statement.payload.id)) continue;

    // These are replaceable *views* expressed by append-only records. A
    // revoked newest value reveals the value before it; otherwise only the
    // newest value for the same subject participates in the current fold.
    const replacementKey =
      statement.payload.type === "social.mirrors"
        ? `mirrors\u0000${statement.payload.repo}`
        : statement.payload.type === "social.follow"
          ? `follow\u0000${statement.payload.subject}`
          : null;
    if (replacementKey !== null) {
      if (superseded.has(replacementKey)) continue;
      superseded.add(replacementKey);
    }
    active.push(statement);

    if (statement.payload.type !== "social.revoke") continue;
    const target = positions.get(statement.payload.target);
    const targetStatement = statements.get(statement.payload.target);
    if (
      target === undefined ||
      target >= index ||
      targetStatement === undefined ||
      !reaches(statement.commit, targetStatement.commit)
    ) {
      active.pop();
      rejected.push({
        commit: statement.commit,
        reason: `revoke target ${statement.payload.target} is not an earlier statement in this log`,
      });
      continue;
    }
    withdrawn.add(statement.payload.target);
  }
  active.reverse();
  return { active, rejected };
};

interface VouchEdge {
  readonly from: PrincipalId;
  readonly to: PrincipalId;
  readonly scope: ReadonlySet<SocialScope>;
  readonly depth: number;
  readonly commit: Oid;
}

const intersect = (
  left: ReadonlySet<SocialScope>,
  right: ReadonlySet<SocialScope>,
): ReadonlySet<SocialScope> => {
  const found = new Set<SocialScope>();
  for (const scope of left) if (right.has(scope)) found.add(scope);
  return found;
};

const pathKey = (path: TrustPath): string =>
  `${path.principals.join("\u0000")}\u0001${[...path.scopes].sort().join(",")}`;

export const project = (input: {
  readonly roots: ReadonlyArray<PrincipalId>;
  readonly logs: ReadonlyArray<VerifiedLog>;
  readonly at?: Date;
  readonly maxDepth?: number;
}): Projection => {
  const at = input.at ?? new Date();
  const rejected: ProjectionRejected[] = [];
  const active: VerifiedStatement[] = [];
  for (const log of input.logs) {
    const current = activeOf(log);
    active.push(...current.active);
    rejected.push(...current.rejected);
  }

  const edges = new Map<PrincipalId, VouchEdge[]>();
  const petnames = new Map<PrincipalId, Map<PrincipalId, string>>();
  const checkpointCandidates = new Map<PrincipalId, SocialCheckpoint[]>();
  for (const statement of active) {
    if (!isPrincipalId(statement.payload.author)) continue;
    if (statement.payload.type === "social.checkpoint") {
      const held = checkpointCandidates.get(statement.payload.author) ?? [];
      held.push({
        principal: statement.payload.author,
        commit: statement.commit,
        at: new Date(statement.payload.issuedAt),
        frontier: statement.payload.frontier,
      });
      checkpointCandidates.set(statement.payload.author, held);
      continue;
    }
    if (statement.payload.type === "social.follow") {
      const subject = principalOf(statement.payload.subject);
      if (subject === null) continue;
      const names = petnames.get(statement.payload.author) ?? new Map<PrincipalId, string>();
      names.set(subject, statement.payload.petname);
      petnames.set(statement.payload.author, names);
      continue;
    }
    if (statement.payload.type !== "social.vouch") continue;
    if (
      statement.payload.expiresAt !== null &&
      Date.parse(statement.payload.expiresAt) <= at.getTime()
    ) {
      continue;
    }
    const subject = principalOf(statement.payload.subject);
    if (subject === null) continue;
    const edge: VouchEdge = {
      from: statement.payload.author,
      to: subject,
      scope: new Set(statement.payload.scope.filter(isSocialScope)),
      depth: statement.payload.depth,
      commit: statement.commit,
    };
    edges.set(edge.from, [...(edges.get(edge.from) ?? []), edge]);
  }
  for (const outgoing of edges.values()) {
    outgoing.sort((left, right) =>
      left.to !== right.to ? (left.to < right.to ? -1 : 1) : left.commit < right.commit ? -1 : 1,
    );
  }
  const checkpoints = new Map<PrincipalId, ReadonlyArray<SocialCheckpoint>>();
  for (const [principal, candidates] of checkpointCandidates) {
    checkpoints.set(
      principal,
      candidates
        .sort((left, right) =>
          left.at.getTime() !== right.at.getTime()
            ? right.at.getTime() - left.at.getTime()
            : left.commit < right.commit
              ? -1
              : 1,
        )
        .slice(0, MAX_CHECKPOINTS),
    );
  }

  const found = new Map<PrincipalId, TrustPath[]>();
  const pending: TrustPath[] = [];
  const allScopes = new Set<SocialScope>(SOCIAL_SCOPES);
  for (const root of [...new Set(input.roots)].sort()) {
    const path: TrustPath = {
      principals: [root],
      statements: [],
      scopes: allScopes,
      remainingDepth: Number.POSITIVE_INFINITY,
    };
    found.set(root, [...(found.get(root) ?? []), path]);
    pending.push(path);
  }

  const seen = new Set(pending.map(pathKey));
  const maxDepth = Math.max(0, Math.min(input.maxDepth ?? MAX_DEPTH, MAX_DEPTH));
  let total = pending.length;
  let truncated = false;
  while (pending.length > 0) {
    const path = pending.shift();
    if (path === undefined) break;
    const hops = path.principals.length - 1;
    if (hops >= maxDepth) continue;
    // A root is an explicit anchor. Every later principal may extend the web
    // only when the path that reached it retained `vouch` scope and depth.
    if (hops > 0 && (!path.scopes.has("vouch") || path.remainingDepth <= 0)) continue;

    const current = path.principals.at(-1);
    if (current === undefined) continue;
    for (const edge of edges.get(current) ?? []) {
      if (path.principals.includes(edge.to)) continue;
      const effective = intersect(path.scopes, edge.scope);
      if (effective.size === 0) continue;

      const remaining =
        hops === 0 ? edge.depth : Math.min(Math.max(0, path.remainingDepth - 1), edge.depth);
      const next: TrustPath = {
        principals: [...path.principals, edge.to],
        statements: [...path.statements, edge.commit],
        scopes: effective,
        remainingDepth: remaining,
      };
      const key = pathKey(next);
      if (seen.has(key)) continue;
      seen.add(key);
      found.set(edge.to, [...(found.get(edge.to) ?? []), next]);
      pending.push(next);
      total++;
      if (total >= MAX_PATHS) {
        truncated = true;
        pending.length = 0;
        break;
      }
    }
  }

  return {
    roots: [...new Set(input.roots)],
    logs: input.logs,
    active,
    paths: found,
    petnames,
    checkpoints,
    rejected,
    at,
    truncated,
  };
};

export type Freshness = { readonly ok: true } | { readonly ok: false; readonly reason: string };

/**
 * Bound stale social views without letting unrelated locally held logs veto a
 * decision. Only principals whose vouches occur on a reachable path need a
 * checkpoint; explicit roots need no graph statement to be trusted.
 */
export const fresh = (
  projection: Projection,
  maxAgeMs: number,
  now: Date = new Date(),
): Freshness => {
  const relevant = new Set<PrincipalId>();
  for (const paths of projection.paths.values()) {
    for (const path of paths) {
      for (const principal of path.principals.slice(0, -1)) relevant.add(principal);
    }
  }

  for (const principal of [...relevant].sort()) {
    const checkpoints = projection.checkpoints.get(principal) ?? [];
    let future = 0;
    let credible: SocialCheckpoint | null = null;
    for (const checkpoint of checkpoints) {
      if (checkpoint.at.getTime() - now.getTime() > CLOCK_SKEW_MS) {
        future++;
        continue;
      }
      credible = checkpoint;
      break;
    }
    if (credible === null) {
      return {
        ok: false,
        reason:
          future === 0
            ? `${principal}'s social log has no checkpoint`
            : `${principal}'s social checkpoints are dated in the future`,
      };
    }
    const age = now.getTime() - credible.at.getTime();
    if (age > maxAgeMs) {
      return {
        ok: false,
        reason: `${principal}'s social checkpoint is ${Math.floor(age / 1000)}s old`,
      };
    }
  }
  return { ok: true };
};

/** Every distinct principal route retaining one scope, before independence. */
export const pathsTo = (
  projection: Projection,
  subject: PrincipalId,
  scope: SocialScope,
  maxDepth = MAX_DEPTH,
): ReadonlyArray<TrustPath> => {
  const distinct = new Map<string, TrustPath>();
  for (const path of projection.paths.get(subject) ?? []) {
    if (!path.scopes.has(scope) || path.principals.length - 1 > maxDepth) continue;
    // Different statements over the same principal route are corroboration by
    // nobody new, so they are one path for confidence purposes.
    const key = path.principals.join("\u0000");
    const held = distinct.get(key);
    if (held === undefined || path.statements.join("") < held.statements.join("")) {
      distinct.set(key, path);
    }
  }
  return [...distinct.values()].sort((left, right) =>
    left.principals.length !== right.principals.length
      ? left.principals.length - right.principals.length
      : left.principals.join("") < right.principals.join("")
        ? -1
        : 1,
  );
};

/**
 * A conservative maximal set of paths sharing no intermediate principal.
 * Shortest deterministic routes are selected first; under-counting refuses a
 * decision, while over-counting would create trust that is not there.
 */
export const independentPaths = (paths: ReadonlyArray<TrustPath>): ReadonlyArray<TrustPath> => {
  const selected: TrustPath[] = [];
  const intermediates = new Set<PrincipalId>();
  for (const path of paths) {
    const inside = path.principals.slice(1, -1);
    if (inside.some((principal) => intermediates.has(principal))) continue;
    selected.push(path);
    for (const principal of inside) intermediates.add(principal);
  }
  return selected;
};

export const confidence = (
  projection: Projection,
  subject: PrincipalId,
  scope: SocialScope,
  maxDepth = MAX_DEPTH,
): number => independentPaths(pathsTo(projection, subject, scope, maxDepth)).length;
