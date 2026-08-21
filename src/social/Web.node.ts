/** Verified social logs held as sibling repositories by a Node client. */
import * as fs from "node:fs/promises";
import * as path from "node:path";

import { Effect, Layer } from "effect";

import { stores } from "../git/Node.ts";
import * as GitRepository from "../git/Repository.ts";
import { readGenesis } from "../trust/Genesis.ts";
import { principalId, type PrincipalId } from "../trust/Principal.ts";
import { project as projectTrust } from "../trust/Projection.ts";
import * as SocialLog from "./Log.ts";
import { SocialWeb } from "./Projection.ts";

const repositoryAt = (root: string, name: string) =>
  GitRepository.layer.pipe(
    Layer.provide(GitRepository.hooksNoop),
    Layer.provide(stores(path.join(root, name))),
  );

const repositoryNames = (root: string) =>
  Effect.promise(() =>
    fs
      .readdir(root, { withFileTypes: true })
      .then((entries) =>
        entries
          .filter((entry) => entry.isDirectory())
          .map((entry) => entry.name)
          .sort()
          .slice(0, 4096),
      )
      .catch(() => []),
  );

/**
 * A sibling's genesis, on its own runtime.
 *
 * Nested `Effect.provide` merges with the caller's `Repository`, so the
 * ambient one would win and every sibling would look like the caller. A
 * fresh runtime is the isolation boundary; this is not inside an Effect
 * generator for that reason.
 */
const genesisMatches = (root: string, name: string, wanted: PrincipalId) =>
  Effect.runPromise(
    readGenesis().pipe(
      Effect.map((stored) => stored !== null && principalId(stored.genesis.repoId) === wanted),
      Effect.provide(repositoryAt(root, name)),
      Effect.orElseSucceed(() => false),
    ),
  );

/** Existing sibling directory holding exactly the requested identity. */
export const identityRepositoryAt = Effect.fn("social.Web.identityRepositoryAt")(function* (
  root: string,
  wanted: PrincipalId,
) {
  for (const name of yield* repositoryNames(root)) {
    if (yield* Effect.promise(() => genesisMatches(root, name, wanted))) return name;
  }
  return null;
});

const readSiblingLog = (root: string, name: string) =>
  Effect.runPromise(
    Effect.gen(function* () {
      const stored = yield* readGenesis();
      if (stored === null) return null;
      const trust = yield* projectTrust(stored.genesis);
      return yield* SocialLog.verified(stored.genesis, trust);
    }).pipe(
      Effect.provide(repositoryAt(root, name)),
      Effect.orElseSucceed(() => null),
    ),
  );

/** Malformed or non-repository siblings are absence, never a broken graph. */
export const verifiedLogsAt = Effect.fn("social.Web.verifiedLogsAt")(function* (root: string) {
  const logs: SocialLog.VerifiedLog[] = [];
  for (const name of yield* repositoryNames(root)) {
    const log = yield* Effect.promise(() => readSiblingLog(root, name));
    if (log !== null) logs.push(log);
  }
  return logs;
});

export const localSocialWeb = (root: string): Layer.Layer<SocialWeb> =>
  Layer.succeed(SocialWeb)({ logs: verifiedLogsAt(root) });
