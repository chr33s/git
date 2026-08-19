/**
 * Helpers every CLI command group leans on.
 *
 * The shared flags, layer wiring and revision resolution live here rather
 * than in `main.ts` because `main.ts` imports the command modules — a helper
 * they need has to sit below both to avoid a cycle.
 */
import * as fs from "node:fs";
import * as path from "node:path";

import { Effect, Layer, Result } from "effect";
import { Argument, Flag } from "effect/unstable/cli";

import {
  formatPublicKey,
  parsePrivateKey,
  parsePublicKey,
  type PrivateKey,
} from "../crypto/SshSignature.ts";
import { Invalid } from "../git/Error.ts";
import type { Signature } from "../git/Format.ts";
import { stores } from "../git/Node.ts";
import * as GitRepository from "../git/Repository.ts";
import { Repository } from "../git/Repository.ts";
import { isOid } from "../git/Store.ts";

export const rootFlag = Flag.string("root").pipe(
  Flag.withDefault("."),
  Flag.withDescription("Directory holding one bare repository per subdirectory"),
);

export const repoArgument = Argument.string("repo");

/**
 * Who a CLI commit is by. `git` reads `user.name`/`user.email` from its own
 * config; there is no such file here, so the environment is the one place a
 * caller can say, and the fallback is honest rather than a fake identity.
 */
export const cliSignature = (): Signature => ({
  name: process.env["GIT_AUTHOR_NAME"] ?? process.env["USER"] ?? "chr33s-git",
  email: process.env["GIT_AUTHOR_EMAIL"] ?? "chr33s-git@localhost",
  at: new Date(),
  offset: -new Date().getTimezoneOffset(),
});

/**
 * A revision as a user types it. `git` accepts `main` for
 * `refs/heads/main`, and a CLI that demanded the full name for every
 * argument would be the only thing here that did — so the disambiguation
 * lives at this layer, in git's own order, rather than in the ref store.
 */
export const resolveRev = (repository: Repository["Service"], rev: string) =>
  Effect.gen(function* () {
    if (isOid(rev)) return rev;
    for (const candidate of [rev, `refs/heads/${rev}`, `refs/tags/${rev}`]) {
      const found = yield* repository.resolve(candidate);
      if (found !== null) return found;
    }
    return null;
  });

/** The full ref name a branch argument means, for commands that write one. */
export const refNameOf = (rev: string) => (rev.startsWith("refs/") ? rev : `refs/heads/${rev}`);

/** `resolveRev` where not finding it is the end of the command. */
export const mustResolve = (repository: Repository["Service"], rev: string) =>
  Effect.gen(function* () {
    const oid = yield* resolveRev(repository, rev);
    if (oid === null) {
      return yield* new Invalid({ field: "ref", reason: `unknown revision '${rev}'` });
    }
    return oid;
  });

export const withRepo = <A, E>(
  root: string,
  repo: string,
  effect: Effect.Effect<A, E, Repository>,
) =>
  effect.pipe(
    Effect.provide(
      GitRepository.layer.pipe(
        Layer.provide(GitRepository.hooksNoop),
        Layer.provide(stores(path.join(root, repo))),
      ),
    ),
  );

/**
 * An SSH private key from disk.
 *
 * Read here rather than passed as a value so that no command takes key
 * material as an argument: a private key on a command line is a private key
 * in a shell history. The failure is deliberately specific — "no such file"
 * and "this key is encrypted" send a user to different places.
 */
export const readPrivateKey = (location: string): Effect.Effect<PrivateKey, Invalid> =>
  Effect.gen(function* () {
    const contents = yield* Effect.try({
      try: () => fs.readFileSync(location, "utf8"),
      catch: () => new Invalid({ field: "key", reason: `cannot read ${location}` }),
    });
    const parsed = parsePrivateKey(contents);
    return Result.isFailure(parsed)
      ? yield* new Invalid({ field: "key", reason: `${location}: ${parsed.failure.reason}` })
      : parsed.success;
  });

/** The public half of a key, as an `authorized_keys` line. */
export const readPublicKey = (location: string): Effect.Effect<string, Invalid> =>
  Effect.gen(function* () {
    const contents = yield* Effect.try({
      try: () => fs.readFileSync(location, "utf8").trim(),
      catch: () => new Invalid({ field: "key", reason: `cannot read ${location}` }),
    });
    const parsed = parsePublicKey(contents);
    return Result.isFailure(parsed)
      ? yield* new Invalid({ field: "key", reason: `${location}: ${parsed.failure.reason}` })
      : formatPublicKey(parsed.success);
  });
