/**
 * Helpers every CLI command group leans on.
 *
 * The shared flags, layer wiring and revision resolution live here rather
 * than in `main.ts` because `main.ts` imports the command modules — a helper
 * they need has to sit below both to avoid a cycle.
 */
import * as fs from "node:fs";
import * as path from "node:path";

import { Config, Effect, Layer, Result } from "effect";
import { Argument, Flag } from "effect/unstable/cli";

import {
  formatPublicKey,
  parsePrivateKey,
  parsePublicKey,
  type PrivateKey,
  type PublicKey,
} from "../crypto/SshSignature.ts";
import { Invalid } from "../git/Error.ts";
import type { Signature } from "../git/Format.ts";
import { stores } from "../git/Node.ts";
import * as GitRepository from "../git/Repository.ts";
import { Repository } from "../git/Repository.ts";
import { IndexStore, WorkTree } from "../git/Work.ts";
import { workspace } from "../git/Work.node.ts";
import * as AfterPush from "../server/AfterPush.node.ts";
import { isOid } from "../git/Store.ts";
import { readGenesis } from "../trust/Genesis.ts";
import { GitInvocation } from "./GitCompat.ts";
import { discoverRepository } from "./GitCompat.node.ts";

export const rootFlag = Flag.string("root").pipe(
  Flag.withDefault("."),
  Flag.withDescription("Directory holding one bare repository per subdirectory"),
);

export const repoArgument = Argument.string("repo");

/**
 * Who a CLI commit is by. `git` reads `user.name`/`user.email` from its own
 * config; there is no such file here, so `Config` is the one place a caller
 * can say, and the fallback is honest rather than a fake identity.
 */
export const cliSignature = Effect.fn("cli.signature")(function* () {
  const name = yield* Config.string("GIT_AUTHOR_NAME").pipe(
    Config.orElse(() => Config.string("USER")),
    Config.withDefault("chr33s-git"),
  );
  const email = yield* Config.string("GIT_AUTHOR_EMAIL").pipe(
    Config.withDefault("chr33s-git@localhost"),
  );
  return {
    name,
    email,
    at: new Date(),
    offset: -new Date().getTimezoneOffset(),
  } satisfies Signature;
});

/**
 * A revision as a user types it. `git` accepts `main` for
 * `refs/heads/main`, and a CLI that demanded the full name for every
 * argument would be the only thing here that did — so the disambiguation
 * lives at this layer, in git's own order, rather than in the ref store.
 */
export const resolveRev = Effect.fn("cli.resolveRev")(function* (
  repository: Repository["Service"],
  rev: string,
) {
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
export const mustResolve = Effect.fn("cli.mustResolve")(function* (
  repository: Repository["Service"],
  rev: string,
) {
  const oid = yield* resolveRev(repository, rev);
  if (oid === null) {
    return yield* new Invalid({ field: "ref", reason: `unknown revision '${rev}'` });
  }
  return oid;
});

export interface RepoOptions {
  /**
   * Whether landing a ref tells whoever the repository says to tell.
   *
   * On by default, and safe as a default because the chain is derived from the
   * repository rather than configured here: a clone with no `webhooks.json` and
   * no `remotes.json` has nothing to deliver and nothing to forward, so the
   * cost is a list of nothing. Off is for a caller that holds the configuration
   * and means to move a ref without acting on it.
   *
   * Only `Repository.receive` runs the chain — `setRef`, `commit` and `merge`
   * move a ref and tell nobody — so today this reaches exactly the verbs that
   * land through it.
   */
  readonly notify?: boolean;
}

/**
 * The genesis of a repository that has one, or the refusal that says how.
 *
 * One guard, because every verb that touches identity, trust, the hub or the
 * social log has to ask this first and they had each grown their own — four
 * copies telling the same user two different things about the same repository,
 * one naming the command that fixes it and one not. What a caller does with
 * the answer still differs; asking is what does not.
 */
export const mustBeEnabled = Effect.fn("cli.mustBeEnabled")(function* (repo: string) {
  const stored = yield* readGenesis();
  if (stored === null) {
    return yield* new Invalid({
      field: "repo",
      reason: `${repo} has no genesis; run \`git+ hub init ${repo} --key <key>\` first`,
    });
  }
  return stored;
});

export const withRepo = <A, E>(
  root: string,
  repo: string,
  effect: Effect.Effect<A, E, Repository | GitRepository.Hooks>,
  options?: RepoOptions,
): Effect.Effect<A, E> => {
  // `provideMerge` for the hooks, so the verb can reach them too. A verb that
  // writes a ref some other way than through `receive` — every hub append is an
  // `Event.appendTo`, and that is a `setRef` — is the only thing that can say
  // what it wrote, and saying so is the difference between a mirror that is
  // behind and one that is wrong.
  const under = (hooks: Layer.Layer<GitRepository.Hooks>) =>
    effect.pipe(
      Effect.provide(
        GitRepository.layer.pipe(
          Layer.provideMerge(hooks),
          Layer.provide(stores(path.join(root, repo))),
        ),
      ),
    );
  if (options?.notify === false) return under(GitRepository.hooksNoop);

  // Started in the hook and waited for here, which is the whole difference from
  // the server's chain. There, a hook's work is detached to outlive an HTTP
  // response. Here the process exits when the verb is done, so a detached fork
  // dies with it — and awaiting it *inside* the hook is worse than either: the
  // chain runs between the ref moving and everything the caller does next, so a
  // slow receiver would hold that window open with the ref already swapped.
  const deliveries = AfterPush.deliveries();
  // Held and sent once, because what a verb did is one thing that happened. A
  // landing moves the branch through `receive` and then appends the records
  // that say what it holds, and announced separately those are separate pushes
  // to every mirror — separately lost, so one can arrive without the other and
  // leave the mirror wrong rather than behind.
  const collected = AfterPush.collected(
    AfterPush.chain({ root, repo, background: deliveries.background }),
  );
  return under(collected.layer).pipe(
    // On every exit, not only success: a landing the verb then fails after is
    // still a landing, and this process is the only one that can announce it or
    // wait for it.
    Effect.ensuring(collected.flush.pipe(Effect.andThen(deliveries.settle))),
  );
};

/**
 * An SSH private key from disk.
 *
 * Read here rather than passed as a value so that no command takes key
 * material as an argument: a private key on a command line is a private key
 * in a shell history. The failure is deliberately specific — "no such file"
 * and "this key is encrypted" send a user to different places.
 */
export const readPrivateKey = Effect.fn("cli.readPrivateKey")(function* (
  location: string,
): Effect.fn.Return<PrivateKey, Invalid> {
  const contents = yield* Effect.try({
    try: () => fs.readFileSync(location, "utf8"),
    catch: () => new Invalid({ field: "key", reason: `cannot read ${location}` }),
  });
  const parsed = parsePrivateKey(contents);
  return Result.isFailure(parsed)
    ? yield* new Invalid({ field: "key", reason: `${location}: ${parsed.failure.reason}` })
    : parsed.success;
});

/**
 * The public half of a key, from whichever half the caller has on disk.
 *
 * A private key carries its own public half — `hub init` leans on the same
 * thing — and the `.pub` beside it often does not exist on the machine an
 * agent runs on, because what a secret store injects is the private key alone.
 * A command that only reads what it needs to *identify* a key should not make
 * the caller produce a file to be identified by.
 */
export const readAnyPublicKey = Effect.fn("cli.readAnyPublicKey")(function* (
  location: string,
): Effect.fn.Return<PublicKey, Invalid> {
  const contents = yield* Effect.try({
    try: () => fs.readFileSync(location, "utf8"),
    catch: () => new Invalid({ field: "key", reason: `cannot read ${location}` }),
  });
  const secret = parsePrivateKey(contents);
  if (Result.isSuccess(secret)) return secret.success.publicKey;

  // Reported against the public parse, not the private one: a caller who
  // passed a public key wants to hear what is wrong with it as a public key,
  // and one who passed a mangled private key learns the file is neither.
  const parsed = parsePublicKey(contents.trim());
  return Result.isFailure(parsed)
    ? yield* new Invalid({ field: "key", reason: `${location}: ${parsed.failure.reason}` })
    : parsed.success;
});

/** The public half of a key, as an `authorized_keys` line. */
export const readPublicKey = Effect.fn("cli.readPublicKey")(function* (location: string) {
  const contents = yield* Effect.try({
    try: () => fs.readFileSync(location, "utf8").trim(),
    catch: () => new Invalid({ field: "key", reason: `cannot read ${location}` }),
  });
  const parsed = parsePublicKey(contents);
  return Result.isFailure(parsed)
    ? yield* new Invalid({ field: "key", reason: `${location}: ${parsed.failure.reason}` })
    : formatPublicKey(parsed.success);
});

/**
 * A checkout, rather than one of the bare repositories under `--root`.
 *
 * The working-tree commands are the only ones that need files on disk, so
 * they take `--work` — a directory whose repository is `.git` inside it,
 * which is the layout `git` itself uses and the reason the two can be
 * pointed at the same directory.
 */
export const workFlag = Flag.string("work").pipe(
  Flag.optional,
  Flag.withDescription("Explicit checkout selector for extension commands"),
);

export const withWork = <A, E>(
  work: { readonly _tag: "None" } | { readonly _tag: "Some"; readonly value: string },
  effect: Effect.Effect<A, E, Repository | WorkTree | IndexStore>,
) =>
  Effect.gen(function* () {
    const invocation = yield* GitInvocation;
    if (invocation.bare) {
      return yield* new Invalid({
        field: "bare",
        reason: "this command requires a work tree",
      });
    }
    const selected =
      invocation.workTree !== undefined || work._tag === "None"
        ? invocation
        : { ...invocation, workTree: path.resolve(invocation.cwd, work.value) };
    const found = yield* discoverRepository(selected);
    if (found === null || found.workTree === null) {
      return yield* new Invalid({
        field: "repository",
        reason: "not a Git work tree",
      });
    }
    return yield* effect.pipe(
      Effect.provide(
        GitRepository.layer.pipe(
          Layer.provide(GitRepository.hooksNoop),
          Layer.provide(stores(found.gitDir)),
          Layer.provideMerge(workspace(found.workTree)),
        ),
      ),
    );
  });

/**
 * The repository a repo-scoped audit command should act on.
 *
 * The current checkout by default, an explicit bare repository under `--root`
 * when one is named (docs/telemetry.md §17, docs/context-pack.md §13). Two
 * paths rather than one because the two callers are genuinely different: a
 * person standing in a project has a `.git` to find, and a server
 * administering a directory of bare repositories has a name and no checkout.
 *
 * Hooks are noop on the discovery path. Nothing an audit verb does lands a ref
 * through `receive`, which is the only thing the chain runs for, so building
 * the delivery machinery here would be paying for a notification nobody sends.
 */
export const withDiscovered = <A, E>(
  root: string,
  repo: string,
  effect: Effect.Effect<A, E, Repository | GitRepository.Hooks>,
): Effect.Effect<A, E | Invalid, GitInvocation> => {
  if (repo !== "") return withRepo(root, repo, effect, { notify: false });

  return Effect.gen(function* () {
    const found = yield* discoverRepository(yield* GitInvocation);
    if (found === null) {
      return yield* new Invalid({
        field: "repository",
        reason: "not a Git repository; name one with --repo, or run this inside a checkout",
      });
    }
    return yield* effect.pipe(
      Effect.provide(
        GitRepository.layer.pipe(
          Layer.provideMerge(GitRepository.hooksNoop),
          Layer.provide(stores(found.gitDir)),
        ),
      ),
    );
  });
};

/** Selects a bare repository under `--root`; empty means the current checkout. */
export const repoFlag = Flag.string("repo").pipe(
  Flag.withDefault(""),
  Flag.withDescription("A bare repository under --root, instead of the current checkout"),
);
