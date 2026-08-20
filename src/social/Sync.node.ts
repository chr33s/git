/** Bounded follow-driven synchronization for identity repositories on Node. */
import * as path from "node:path";

import { Effect, Layer, Result } from "effect";

import { fetchRepository } from "../client/Fetch.ts";
import { Invalid } from "../git/Error.ts";
import { stores } from "../git/Node.ts";
import * as Refspec from "../git/Refspec.ts";
import * as GitRepository from "../git/Repository.ts";
import { ObjectStore, RefStore } from "../git/Store.ts";
import { reconcile } from "../server/Replication.ts";
import { identityAt } from "../cli/remote-id.node.ts";
import { GENESIS_REF, readGenesis } from "../trust/Genesis.ts";
import type { KnownRepo } from "../trust/KnownRepos.ts";
import { isPrincipalId, principalId, principalOf, type PrincipalId } from "../trust/Principal.ts";
import { encodePrincipal } from "./Encode.ts";
import * as Introduce from "./Introduce.ts";
import type { Projection } from "./Projection.ts";
import { identityRepositoryAt } from "./Web.node.ts";

const localRepository = (directory: string) =>
  GitRepository.layer.pipe(
    Layer.provide(GitRepository.hooksNoop),
    Layer.provideMerge(stores(directory)),
  );

/** Active follow targets published by the current traversal frontier. */
export const followedBy = (
  projection: Projection,
  authors: ReadonlySet<PrincipalId>,
): ReadonlyArray<PrincipalId> => {
  const followed = new Set<PrincipalId>();
  for (const statement of projection.active) {
    if (
      statement.payload.type !== "social.follow" ||
      !isPrincipalId(statement.payload.author) ||
      !authors.has(statement.payload.author)
    ) {
      continue;
    }
    const subject = principalOf(statement.payload.subject);
    if (subject !== null) followed.add(subject);
  }
  return [...followed].sort();
};

/** Subject-published mirrors win over static pins and third-party attestations. */
export const locationOf = (
  projection: Projection,
  subject: PrincipalId,
  known: ReadonlyArray<KnownRepo>,
): string | null => {
  const self = projection.active
    .filter(
      (statement) =>
        statement.payload.type === "social.mirrors" &&
        statement.payload.author === subject &&
        statement.payload.repo === "self",
    )
    .sort((left, right) =>
      left.payload.issuedAt !== right.payload.issuedAt
        ? right.payload.issuedAt.localeCompare(left.payload.issuedAt)
        : left.commit < right.commit
          ? -1
          : 1,
    )[0];
  if (self?.payload.type === "social.mirrors") {
    const preferred =
      self.payload.urls.find((location) => location.mode === "write") ?? self.payload.urls[0];
    if (preferred !== undefined) return preferred.url;
  }

  const pinned = known.find((entry) => entry.repoId === subject);
  if (pinned !== undefined) return pinned.url;

  return Introduce.repositories(projection).find((entry) => entry.repo === subject)?.url ?? null;
};

export interface SyncOutcome {
  readonly principal: PrincipalId;
  readonly url: string;
  readonly directory: string;
  readonly fetched: ReadonlyArray<string>;
  readonly joined: ReadonlyArray<string>;
}

/** Verify the remote pin before allowing it to mutate a held identity clone. */
export const syncIdentity = Effect.fn("social.Sync.syncIdentity")(function* (input: {
  readonly root: string;
  readonly principal: PrincipalId;
  readonly url: string;
  readonly token?: string | undefined;
}) {
  const presented = yield* Effect.tryPromise({
    try: () => identityAt(input.url),
    catch: (cause) =>
      new Invalid({
        field: "identity",
        reason: `could not verify ${input.url}: ${cause instanceof Error ? cause.message : String(cause)}`,
      }),
  });
  if (principalId(presented) !== input.principal) {
    return yield* new Invalid({
      field: "identity",
      reason: `${input.url} presented ${presented}, expected ${input.principal}`,
    });
  }

  const existing = yield* identityRepositoryAt(input.root, input.principal);
  const encoded = encodePrincipal({ id: input.principal });
  if (Result.isFailure(encoded)) return yield* encoded.failure;
  const directory = path.join(input.root, existing ?? encoded.success);

  // `social sync` itself runs inside the caller's identity repository. A
  // nested provide merges with that ambient context and lets the ambient
  // Repository win, so the followed clone must run in a fresh runtime.
  const state = yield* Effect.tryPromise({
    try: () =>
      Effect.runPromise(
        Effect.gen(function* () {
          const target = { objects: yield* ObjectStore, refs: yield* RefStore };
          const fetched: string[] = [];
          const joined: string[] = [];
          for (const spec of [
            { force: false, source: GENESIS_REF, destination: GENESIS_REF },
            { force: false, source: Refspec.TRUST_LOG, destination: Refspec.TRUST_LOG },
            { force: false, source: Refspec.SOCIAL_LOG, destination: Refspec.SOCIAL_LOG },
          ]) {
            const result = yield* fetchRepository({
              url: input.url,
              token: input.token,
              stores: target,
              refspecs: [spec],
            });
            fetched.push(...result.refs.map((update) => update.name));
            for (const rejected of result.rejected) {
              if (rejected.name === GENESIS_REF) continue;
              const divergence = yield* reconcile(rejected.name, rejected.oid);
              if (divergence.joined !== null) joined.push(rejected.name);
            }
          }

          const stored = yield* readGenesis();
          if (stored === null || principalId(stored.genesis.repoId) !== input.principal) {
            return yield* new Invalid({
              field: "identity",
              reason: `${directory} does not hold the verified identity after synchronization`,
            });
          }
          return { fetched, joined };
        }).pipe(Effect.provide(localRepository(directory))),
      ),
    catch: (cause) =>
      new Invalid({
        field: "sync",
        reason: cause instanceof Error ? cause.message : String(cause),
      }),
  });

  return {
    principal: input.principal,
    url: input.url,
    directory,
    fetched: state.fetched,
    joined: state.joined,
  } satisfies SyncOutcome;
});
