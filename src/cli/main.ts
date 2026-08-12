#!/usr/bin/env node
/**
 * CLI.
 *
 * `effect/unstable/cli` owns parsing, flags, help and exit codes; each
 * handler calls the same `Repository` service, host, client and auth code
 * the server runs — the CLI is not another implementation of anything.
 *
 *   chr33s-git init my-repo                      # bare repository under --root
 *   chr33s-git refs my-repo · log my-repo        # inspect it
 *   chr33s-git clone http://host/repo my-copy    # bare clone over smart HTTP
 *   chr33s-git serve --port 8080 --secret s3…    # the node host, optionally authed
 *   chr33s-git token my-repo --secret s3… -s write
 *
 * The failure channel reaches `main`, so exit codes come from the error
 * type: a bad ref is a diagnostic and exit 1, an interrupt is 130, an
 * unexpected defect prints a `Cause` with the fiber trace.
 */
import { createWriteStream } from "node:fs";
import * as path from "node:path";
import { Readable } from "node:stream";
import { pipeline } from "node:stream/promises";

import { NodeRuntime, NodeServices } from "@effect/platform-node";
import { Config, Console, Effect, Layer, Stream } from "effect";
import { Argument, Command, Flag } from "effect/unstable/cli";

import { fetchRepository } from "../client/Fetch.ts";
import { push } from "../client/Push.ts";
import * as Checkout from "../git/Checkout.ts";
import { next as bisectNext } from "../git/Bisect.ts";
import { isBinary, unified } from "../git/Diff.ts";
import { forPath as pathHistory } from "../git/History.ts";
import { Invalid } from "../git/Error.ts";
import type { Signature } from "../git/Format.ts";
import { stores } from "../git/Node.ts";
import { cherryPick, rebase } from "../git/Rebase.ts";
import * as GitRepository from "../git/Repository.ts";
import { Repository } from "../git/Repository.ts";
import { isOid, ObjectStore, type Oid, RefStore } from "../git/Store.ts";
import { IndexStore, WorkTree } from "../git/Work.ts";
import { workspace } from "../git/Work.node.ts";
import { serve } from "../host/Node.ts";
import * as Archive from "../server/Archive.ts";
import { hmacMint, hmacVerify, type Scope } from "../server/Auth.ts";

const rootFlag = Flag.string("root").pipe(
  Flag.withDefault("."),
  Flag.withDescription("Directory holding one bare repository per subdirectory"),
);

const repoArgument = Argument.string("repo");

/** One repository's stores as raw instances, for code that needs them directly. */
const openStores = (directory: string) =>
  Effect.gen(function* () {
    return { objects: yield* ObjectStore, refs: yield* RefStore };
  }).pipe(Effect.provide(stores(directory)));

/**
 * Who a CLI commit is by. `git` reads `user.name`/`user.email` from its own
 * config; there is no such file here, so the environment is the one place a
 * caller can say, and the fallback is honest rather than a fake identity.
 */
const cliSignature = (): Signature => ({
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
const resolveRev = (repository: Repository["Service"], rev: string) =>
  Effect.gen(function* () {
    if (isOid(rev)) return rev;
    for (const candidate of [rev, `refs/heads/${rev}`, `refs/tags/${rev}`]) {
      const found = yield* repository.resolve(candidate);
      if (found !== null) return found;
    }
    return null;
  });

/** The full ref name a branch argument means, for commands that write one. */
const refNameOf = (rev: string) => (rev.startsWith("refs/") ? rev : `refs/heads/${rev}`);

/** `resolveRev` where not finding it is the end of the command. */
const mustResolve = (repository: Repository["Service"], rev: string) =>
  Effect.gen(function* () {
    const oid = yield* resolveRev(repository, rev);
    if (oid === null) {
      return yield* new Invalid({ field: "ref", reason: `unknown revision '${rev}'` });
    }
    return oid;
  });

/** The tree a revision names: a ref, an oid, a tag that peels, or a tree. */
const treeOf = (repository: Repository["Service"], rev: string) =>
  Effect.flatMap(mustResolve(repository, rev), (oid) => GitRepository.treeAt(repository, oid));

/**
 * A checkout, rather than one of the bare repositories under `--root`.
 *
 * The working-tree commands are the only ones that need files on disk, so
 * they take `--work` — a directory whose repository is `.git` inside it,
 * which is the layout `git` itself uses and the reason the two can be
 * pointed at the same directory.
 */
const workFlag = Flag.string("work").pipe(
  Flag.withDefault("."),
  Flag.withDescription("A checkout: a work tree whose repository is .git inside it"),
);

const withWork = <A, E>(
  work: string,
  effect: Effect.Effect<A, E, Repository | WorkTree | IndexStore>,
) =>
  effect.pipe(
    Effect.provide(
      GitRepository.layer.pipe(
        Layer.provide(GitRepository.hooksNoop),
        Layer.provide(stores(path.join(work, ".git"))),
        Layer.provideMerge(workspace(work)),
      ),
    ),
  );

const withRepo = <A, E>(root: string, repo: string, effect: Effect.Effect<A, E, Repository>) =>
  effect.pipe(
    Effect.provide(
      GitRepository.layer.pipe(
        Layer.provide(GitRepository.hooksNoop),
        Layer.provide(stores(path.join(root, repo))),
      ),
    ),
  );

const init = Command.make(
  "init",
  {
    root: rootFlag,
    branch: Flag.string("branch").pipe(Flag.withDefault("main"), Flag.withAlias("b")),
    repo: repoArgument,
  },
  ({ branch, repo, root }) =>
    Effect.gen(function* () {
      const directory = path.join(root, repo);
      const { refs } = yield* openStores(directory);
      yield* refs.setHead(`refs/heads/${branch}`);
      yield* Console.log(`Initialized empty repository in ${directory}`);
    }),
);

const refs = Command.make("refs", { root: rootFlag, repo: repoArgument }, ({ repo, root }) =>
  withRepo(
    root,
    repo,
    Effect.gen(function* () {
      const repository = yield* Repository;
      for (const [name, oid] of yield* repository.refs) {
        yield* Console.log(`${oid}\t${name}`);
      }
    }),
  ),
);

const log = Command.make(
  "log",
  {
    root: rootFlag,
    limit: Flag.integer("max-count").pipe(Flag.withDefault(20), Flag.withAlias("n")),
    ref: Flag.string("ref").pipe(Flag.withDefault("HEAD")),
    repo: repoArgument,
  },
  ({ limit, ref, repo, root }) =>
    withRepo(
      root,
      repo,
      Effect.gen(function* () {
        const repository = yield* Repository;
        const head = yield* resolveRev(repository, ref);
        if (head === null) {
          return yield* new Invalid({ field: "ref", reason: `unknown ref '${ref}'` });
        }
        // Streamed: the first commit prints before the walk finishes.
        yield* repository
          .log(head, { limit })
          .pipe(
            Stream.runForEach((entry) =>
              Console.log(`${entry.oid} ${entry.message.split("\n")[0]}`),
            ),
          );
      }),
    ),
);

const clone = Command.make(
  "clone",
  {
    root: rootFlag,
    branch: Flag.string("branch").pipe(Flag.withDefault(""), Flag.withAlias("b")),
    token: Flag.string("token").pipe(
      Flag.withDefault(""),
      Flag.withDescription("Access token for a server that requires one"),
    ),
    url: Argument.string("url"),
    name: Argument.string("name"),
  },
  ({ branch, name, root, token: accessToken, url }) =>
    Effect.gen(function* () {
      const directory = path.join(root, name);
      const target = yield* openStores(directory);
      const result = yield* fetchRepository({
        url,
        branch: branch === "" ? undefined : branch,
        token: accessToken === "" ? undefined : accessToken,
        stores: target,
      });
      if (result.defaultBranch !== undefined) {
        yield* target.refs.setHead(`refs/heads/${result.defaultBranch}`);
      }
      yield* Console.log(`Cloned ${result.refs.length} ref(s) from ${url} into ${directory}`);
    }),
);

const token = Command.make(
  "token",
  {
    secret: Flag.string("secret").pipe(
      Flag.withDescription("The server's GIT_AUTH_SECRET"),
      Flag.withFallbackConfig(Config.string("GIT_AUTH_SECRET")),
    ),
    scope: Flag.choice("scope", ["read", "write"]).pipe(
      Flag.withDefault("read"),
      Flag.withAlias("s"),
    ),
    ttl: Flag.integer("ttl").pipe(
      Flag.withDefault(3600),
      Flag.withDescription("Seconds until the token expires"),
    ),
    repo: repoArgument,
  },
  ({ repo, scope, secret, ttl }) =>
    Effect.gen(function* () {
      const minted = yield* hmacMint(secret, repo, scope as Scope, ttl);
      yield* Console.log(minted);
    }),
);

const serveCommand = Command.make(
  "serve",
  {
    root: rootFlag,
    port: Flag.integer("port").pipe(Flag.withDefault(8080), Flag.withAlias("p")),
    hostname: Flag.string("hostname").pipe(Flag.withDefault("127.0.0.1")),
    secret: Flag.string("secret").pipe(
      Flag.withDefault(""),
      Flag.withDescription("Require hmac tokens signed with this secret; empty serves open"),
    ),
  },
  ({ hostname, port, root, secret }) => {
    // Built out here, not inside the generator: `serve` wants a promise-
    // returning callback, and running an Effect inside an Effect would
    // discard the surrounding services.
    const verify =
      secret === ""
        ? {}
        : {
            verify: (repo: string, credential: string | null) =>
              Effect.runPromise(hmacVerify(secret, repo, credential)),
          };

    return Effect.gen(function* () {
      const server = yield* Effect.promise(() => serve({ root, port, hostname, ...verify }));
      yield* Console.log(
        `git smart-HTTP server on ${server.url}, repositories under ${root}/` +
          (secret === "" ? " (open access)" : " (token required)"),
      );
      return yield* Effect.never;
    });
  },
);

const branch = Command.make(
  "branch",
  {
    root: rootFlag,
    delete: Flag.string("delete").pipe(Flag.withDefault(""), Flag.withAlias("d")),
    base: Flag.string("base").pipe(Flag.withDefault("HEAD")),
    name: Flag.string("create").pipe(Flag.withDefault(""), Flag.withAlias("c")),
    repo: repoArgument,
  },
  ({ base, delete: remove, name, repo, root }) =>
    withRepo(
      root,
      repo,
      Effect.gen(function* () {
        const repository = yield* Repository;

        if (remove !== "") {
          const deleted = yield* repository.deleteRef(`refs/heads/${remove}`);
          return yield* Console.log(
            deleted ? `Deleted branch ${remove}` : `No such branch: ${remove}`,
          );
        }
        if (name !== "") {
          const oid = yield* repository.branch({ name, base });
          return yield* Console.log(`${oid}\trefs/heads/${name}`);
        }

        const head = yield* repository.head;
        for (const [ref, oid] of yield* repository.refs) {
          if (!ref.startsWith("refs/heads/")) continue;
          yield* Console.log(`${ref === head ? "*" : " "} ${oid}\t${ref.slice(11)}`);
        }
      }),
    ),
);

const tag = Command.make(
  "tag",
  {
    root: rootFlag,
    message: Flag.string("message").pipe(
      Flag.withDefault(""),
      Flag.withAlias("m"),
      Flag.withDescription("Makes it an annotated tag rather than a bare ref"),
    ),
    delete: Flag.string("delete").pipe(Flag.withDefault(""), Flag.withAlias("d")),
    name: Flag.string("name").pipe(Flag.withDefault("")),
    target: Flag.string("target").pipe(Flag.withDefault("HEAD")),
    force: Flag.boolean("force").pipe(Flag.withAlias("f")),
    repo: repoArgument,
  },
  ({ delete: remove, force, message, name, repo, root, target }) =>
    withRepo(
      root,
      repo,
      Effect.gen(function* () {
        const repository = yield* Repository;

        if (remove !== "") {
          const deleted = yield* repository.deleteTag(remove);
          return yield* Console.log(deleted ? `Deleted tag ${remove}` : `No such tag: ${remove}`);
        }
        if (name !== "") {
          const created = yield* repository.tag({
            name,
            target,
            force,
            ...(message === "" ? {} : { message: `${message}\n`, tagger: cliSignature() }),
          });
          return yield* Console.log(`${created.oid}\t${created.ref}`);
        }

        for (const [ref, oid] of yield* repository.refs) {
          if (ref.startsWith("refs/tags/")) yield* Console.log(`${oid}\t${ref.slice(10)}`);
        }
      }),
    ),
);

const show = Command.make(
  "show",
  { root: rootFlag, repo: repoArgument, rev: Argument.string("rev") },
  ({ repo, rev, root }) =>
    withRepo(
      root,
      repo,
      Effect.gen(function* () {
        const repository = yield* Repository;
        const oid = yield* resolveRev(repository, rev);
        if (oid === null) {
          return yield* new Invalid({ field: "rev", reason: `unknown revision '${rev}'` });
        }

        const object = yield* repository.readObject(oid);
        yield* Console.log(`${oid} ${object.type} ${object.data.length}`);
        // A blob or a commit is text worth printing; a tree is a listing.
        if (object.type === "tree") {
          for (const entry of yield* repository.readTree(oid)) {
            yield* Console.log(`${entry.mode} ${entry.oid}\t${entry.name}`);
          }
          return;
        }
        yield* Console.log(new TextDecoder().decode(object.data));
      }),
    ),
);

const files = Command.make(
  "files",
  {
    root: rootFlag,
    ref: Flag.string("ref").pipe(Flag.withDefault("HEAD")),
    repo: repoArgument,
  },
  ({ ref, repo, root }) =>
    withRepo(
      root,
      repo,
      Effect.gen(function* () {
        const repository = yield* Repository;
        for (const file of yield* repository.listFiles(yield* treeOf(repository, ref))) {
          yield* Console.log(`${file.mode} ${file.oid}\t${file.path}`);
        }
      }),
    ),
);

const diff = Command.make(
  "diff",
  {
    root: rootFlag,
    repo: repoArgument,
    from: Argument.string("from"),
    to: Argument.string("to"),
  },
  ({ from, repo, root, to }) =>
    withRepo(
      root,
      repo,
      Effect.gen(function* () {
        const repository = yield* Repository;
        const before = new Map(
          (yield* repository.listFiles(yield* treeOf(repository, from))).map((file) => [
            file.path,
            file,
          ]),
        );
        const after = new Map(
          (yield* repository.listFiles(yield* treeOf(repository, to))).map((file) => [
            file.path,
            file,
          ]),
        );

        for (const path of [...new Set([...before.keys(), ...after.keys()])].sort()) {
          const old = before.get(path);
          const now = after.get(path);
          if (old?.oid === now?.oid) continue;

          const read = (oid: Oid | undefined) =>
            oid === undefined ? Effect.succeed(new Uint8Array(0)) : repository.readBlob(oid);
          const oldBytes = yield* read(old?.oid);
          const newBytes = yield* read(now?.oid);

          if (isBinary(oldBytes) || isBinary(newBytes)) {
            yield* Console.log(`Binary files a/${path} and b/${path} differ`);
            continue;
          }
          const decoder = new TextDecoder();
          yield* Console.log(
            unified(decoder.decode(oldBytes), decoder.decode(newBytes), {
              beforeName: path,
              afterName: path,
            }).trimEnd(),
          );
        }
      }),
    ),
);

const merge = Command.make(
  "merge",
  {
    root: rootFlag,
    into: Flag.string("into").pipe(
      Flag.withDefault(""),
      Flag.withDescription("Move this ref on success; omit to compute and stop"),
    ),
    strategy: Flag.choice("strategy", ["recursive", "ours", "theirs"]).pipe(
      Flag.withDefault("recursive"),
      Flag.withAlias("s"),
    ),
    repo: repoArgument,
    ours: Argument.string("ours"),
    theirs: Argument.string("theirs"),
  },
  ({ into, ours, repo, root, strategy, theirs }) =>
    withRepo(
      root,
      repo,
      Effect.gen(function* () {
        const repository = yield* Repository;
        const outcome = yield* repository.merge({
          ours,
          theirs,
          author: cliSignature(),
          strategy: strategy as "recursive" | "ours" | "theirs",
          ...(into === "" ? {} : { into }),
        });

        yield* Console.log(`${outcome.kind}${outcome.commit === null ? "" : ` ${outcome.commit}`}`);
        for (const conflict of outcome.conflicts) {
          yield* Console.error(`CONFLICT (${conflict.reason}): ${conflict.path}`);
        }
        // A conflict is a failed merge as far as a shell is concerned, and
        // the exit code is the only thing a script reads.
        if (outcome.kind === "conflicted") {
          return yield* new Invalid({
            field: "merge",
            reason: `${outcome.conflicts.length} conflict(s)`,
          });
        }
      }),
    ),
);

const grep = Command.make(
  "grep",
  {
    root: rootFlag,
    ref: Flag.string("ref").pipe(Flag.withDefault("HEAD")),
    ignoreCase: Flag.boolean("ignore-case").pipe(Flag.withAlias("i")),
    repo: repoArgument,
    pattern: Argument.string("pattern"),
  },
  ({ ignoreCase, pattern, ref, repo, root }) =>
    withRepo(
      root,
      repo,
      Effect.gen(function* () {
        const repository = yield* Repository;
        const expression = new RegExp(pattern, ignoreCase ? "i" : "");

        for (const file of yield* repository.listFiles(yield* treeOf(repository, ref))) {
          const data = yield* repository.readBlob(file.oid);
          if (isBinary(data)) continue;
          const lines = new TextDecoder().decode(data).split("\n");
          for (let index = 0; index < lines.length; index++) {
            const text = lines[index]!;
            if (expression.test(text)) yield* Console.log(`${file.path}:${index + 1}:${text}`);
          }
        }
      }),
    ),
);

const fsck = Command.make("fsck", { root: rootFlag, repo: repoArgument }, ({ repo, root }) =>
  withRepo(
    root,
    repo,
    Effect.gen(function* () {
      const repository = yield* Repository;
      const report = yield* repository.fsck;

      for (const problem of report.problems) {
        yield* Console.error(`${problem.oid}: ${problem.problem}`);
      }
      for (const dangling of report.danglingRefs) {
        yield* Console.error(`${dangling.ref}: points at missing ${dangling.oid}`);
      }
      yield* Console.log(`checked ${report.checked} object(s)`);

      const bad = report.problems.length + report.danglingRefs.length;
      if (bad > 0) return yield* new Invalid({ field: "fsck", reason: `${bad} problem(s)` });
    }),
  ),
);

const gc = Command.make(
  "gc",
  {
    root: rootFlag,
    dryRun: Flag.boolean("dry-run").pipe(Flag.withAlias("n")),
    repack: Flag.boolean("repack").pipe(
      Flag.withDescription("Write what survives into one pack and drop the loose objects"),
    ),
    repo: repoArgument,
  },
  ({ dryRun, repack, repo, root }) =>
    withRepo(
      root,
      repo,
      Effect.gen(function* () {
        const repository = yield* Repository;
        const report = yield* repository.gc({ dryRun, repack });
        yield* Console.log(
          `${dryRun ? "would remove" : "removed"} ${report.removed.length} of ${report.scanned} object(s), ${report.reachable} reachable`,
        );
        if (report.packed !== undefined) {
          yield* Console.log(
            `packed ${report.packed.objects} object(s) into ${report.packed.name}`,
          );
        }
      }),
    ),
);

const pushCommand = Command.make(
  "push",
  {
    root: rootFlag,
    token: Flag.string("token").pipe(Flag.withDefault("")),
    force: Flag.boolean("force").pipe(Flag.withAlias("f")),
    atomic: Flag.boolean("atomic"),
    delete: Flag.boolean("delete").pipe(
      Flag.withAlias("d"),
      Flag.withDescription("Remove the ref on the server instead of updating it"),
    ),
    repo: repoArgument,
    url: Argument.string("url"),
    ref: Argument.string("ref"),
  },
  ({ atomic, delete: remove, force, ref, repo, root, token: accessToken, url }) =>
    withRepo(
      root,
      repo,
      Effect.gen(function* () {
        // Local and remote name the same branch: pushing `main` somewhere
        // else is what an explicit `refs/…:refs/…` spelling would be for,
        // and this CLI does not pretend to offer one.
        const remote = refNameOf(ref);
        const results = yield* push({
          url,
          refs: [{ local: remote, remote, delete: remove }],
          force,
          atomic,
          ...(accessToken === "" ? {} : { token: accessToken }),
        });

        for (const result of results) {
          yield* Console.log(
            `${result.ok ? "ok" : "ng"} ${result.ref}${result.reason === undefined ? "" : ` (${result.reason})`}`,
          );
        }
        // The exit code is what a script reads, so a rejected ref is a
        // failure even though the request itself succeeded.
        if (results.some((result) => !result.ok)) {
          return yield* new Invalid({ field: "push", reason: "some refs were rejected" });
        }
      }),
    ),
);

const archiveCommand = Command.make(
  "archive",
  {
    root: rootFlag,
    ref: Flag.string("ref").pipe(Flag.withDefault("HEAD")),
    prefix: Flag.string("prefix").pipe(Flag.withDefault("")),
    output: Flag.string("output").pipe(
      Flag.withAlias("o"),
      Flag.withDescription("Where to write it; the extension picks the format"),
    ),
    repo: repoArgument,
  },
  ({ output, prefix, ref, repo, root }) =>
    withRepo(
      root,
      repo,
      Effect.gen(function* () {
        const format = Archive.formatOf(output);
        if (format === null) {
          return yield* new Invalid({
            field: "output",
            reason: `cannot tell the format from '${output}' (want .tar, .tar.gz, .tgz or .zip)`,
          });
        }

        const repository = yield* Repository;
        const stream = yield* Archive.archive({
          tree: yield* treeOf(repository, ref),
          format,
          ...(prefix === "" ? {} : { prefix }),
        });

        // Streamed to disk rather than collected: the whole point of the
        // archive being a Stream is that a big repository never lands in
        // memory, and buffering it here would throw that away.
        yield* Effect.promise(() =>
          pipeline(Readable.from(Stream.toAsyncIterable(stream)), createWriteStream(output)),
        );
        yield* Console.log(`Wrote ${output}`);
      }),
    ),
);

/**
 * One bisect step, stateless.
 *
 * `git bisect` keeps a session in `.git`; this takes the known state as
 * arguments and prints the next commit to test. A shell loop is then the
 * whole session, and the same call works over HTTP where there is nothing to
 * keep a session in.
 */
const bisectCommand = Command.make(
  "bisect",
  {
    root: rootFlag,
    repo: repoArgument,
    bad: Flag.string("bad").pipe(Flag.withDescription("A revision known to have the problem")),
    good: Flag.string("good").pipe(
      Flag.atLeast(1),
      Flag.withDescription("A revision known not to; repeat for more than one"),
    ),
  },
  ({ bad, good, repo, root }) =>
    withRepo(
      root,
      repo,
      Effect.gen(function* () {
        const repository = yield* Repository;
        const step = yield* bisectNext({
          bad: yield* mustResolve(repository, bad),
          good: yield* Effect.forEach(good, (rev) => mustResolve(repository, rev)),
        });

        if (step.kind === "found") {
          yield* Console.log(`${step.commit} is the first bad commit`);
          return;
        }
        yield* Console.log(
          `${step.commit}\t${step.remaining} revision(s) left, roughly ${step.steps} step(s)`,
        );
      }),
    ),
);

const history = Command.make(
  "history",
  {
    root: rootFlag,
    repo: repoArgument,
    ref: Flag.string("ref").pipe(Flag.withDefault("HEAD")),
    limit: Flag.integer("max-count").pipe(Flag.withDefault(20), Flag.withAlias("n")),
    file: Argument.string("path"),
  },
  ({ file, limit, ref, repo, root }) =>
    withRepo(
      root,
      repo,
      Effect.gen(function* () {
        const repository = yield* Repository;
        // Streamed like `log`: the first commit prints before the walk ends.
        yield* pathHistory(yield* mustResolve(repository, ref), file, { limit }).pipe(
          Stream.runForEach((change) =>
            Console.log(
              `${change.oid} ${change.blob === null ? "(deleted)" : change.blob.slice(0, 7)} ${
                change.message.split("\n")[0]
              }`,
            ),
          ),
        );
      }),
    ),
);

const reset = Command.make(
  "reset",
  {
    root: rootFlag,
    repo: repoArgument,
    ref: Argument.string("ref"),
    to: Argument.string("to"),
    expected: Flag.string("expected").pipe(
      Flag.optional,
      Flag.withDescription("Refuse unless the ref is currently this oid"),
    ),
  },
  ({ expected, ref, repo, root, to }) =>
    withRepo(
      root,
      repo,
      Effect.gen(function* () {
        const repository = yield* Repository;
        const moved = yield* repository.setRef({
          name: refNameOf(ref),
          to: yield* mustResolve(repository, to),
          ...(expected._tag === "Some"
            ? { expected: expected.value === "" ? null : (expected.value as Oid) }
            : {}),
        });
        yield* Console.log(`${moved.ref} ${moved.previous ?? "(new)"} -> ${moved.oid}`);
      }),
    ),
);

/** Both replay commands print the same ledger, so they share the printer. */
const reportReplay = (outcome: {
  readonly kind: string;
  readonly head: Oid | null;
  readonly commits: ReadonlyArray<{
    readonly original: Oid;
    readonly replayed: Oid | null;
    readonly conflicts: ReadonlyArray<{ readonly path: string; readonly reason: string }>;
  }>;
}) =>
  Effect.gen(function* () {
    for (const entry of outcome.commits) {
      for (const conflict of entry.conflicts) {
        yield* Console.log(`CONFLICT (${conflict.reason}): ${conflict.path}`);
      }
      // A replay of `null` is a commit that produced nothing: already present
      // on the target, or one whose change was empty once applied.
      yield* Console.log(
        entry.replayed === null
          ? `skipped ${entry.original}`
          : `${entry.original} -> ${entry.replayed}`,
      );
    }
    yield* Console.log(`${outcome.kind}${outcome.head === null ? "" : ` at ${outcome.head}`}`);
  });

const cherryPickCommand = Command.make(
  "cherry-pick",
  {
    root: rootFlag,
    repo: repoArgument,
    commit: Argument.string("commit"),
    onto: Flag.string("onto").pipe(Flag.withDescription("The commit or branch to replay onto")),
    into: Flag.string("into").pipe(
      Flag.optional,
      Flag.withDescription("Ref to move on success; absent computes the replay and stops"),
    ),
  },
  ({ commit, into, onto, repo, root }) =>
    withRepo(
      root,
      repo,
      Effect.gen(function* () {
        const repository = yield* Repository;
        const outcome = yield* cherryPick({
          commit: yield* mustResolve(repository, commit),
          onto: yield* mustResolve(repository, onto),
          author: cliSignature(),
          ...(into._tag === "Some" ? { into: refNameOf(into.value) } : {}),
        });
        yield* reportReplay(outcome);
      }),
    ),
);

const rebaseCommand = Command.make(
  "rebase",
  {
    root: rootFlag,
    repo: repoArgument,
    branch: Argument.string("branch"),
    onto: Flag.string("onto").pipe(Flag.withDescription("The commit or branch to replay onto")),
    into: Flag.string("into").pipe(
      Flag.optional,
      Flag.withDescription("Ref to move on success; absent computes the replay and stops"),
    ),
  },
  ({ branch, into, onto, repo, root }) =>
    withRepo(
      root,
      repo,
      Effect.gen(function* () {
        const repository = yield* Repository;
        const outcome = yield* rebase({
          branch: yield* mustResolve(repository, branch),
          onto: yield* mustResolve(repository, onto),
          // Defaulting `--into` to the branch is what makes `rebase main`
          // move `main`, which is the only thing a caller ever means by it.
          into: refNameOf(into._tag === "Some" ? into.value : branch),
        });
        yield* reportReplay(outcome);
      }),
    ),
);

/**
 * `git status --porcelain`, deliberately.
 *
 * Two columns — index against HEAD, then disk against the index — is the
 * format every script that reads git's status already parses, so it is the
 * one worth emitting rather than a prettier private one.
 */
const statusCommand = Command.make("status", { work: workFlag }, ({ work }) =>
  withWork(
    work,
    Effect.gen(function* () {
      const current = yield* Checkout.status();
      const letter = { added: "A", modified: "M", deleted: "D" } as const;

      const staged = new Map(current.staged.map((entry) => [entry.path, letter[entry.change]]));
      const unstaged = new Map(current.unstaged.map((entry) => [entry.path, letter[entry.change]]));

      yield* Console.log(`## ${current.branch.replace(/^refs\/heads\//, "")}`);
      for (const path of [...new Set([...staged.keys(), ...unstaged.keys()])].sort()) {
        yield* Console.log(`${staged.get(path) ?? " "}${unstaged.get(path) ?? " "} ${path}`);
      }
      for (const path of current.untracked) yield* Console.log(`?? ${path}`);
    }),
  ),
);

const addCommand = Command.make(
  "add",
  { work: workFlag, paths: Argument.string("paths").pipe(Argument.variadic({ min: 1 })) },
  ({ paths, work }) =>
    withWork(
      work,
      Effect.gen(function* () {
        for (const staged of yield* Checkout.add(paths)) yield* Console.log(staged);
      }),
    ),
);

const rm = Command.make(
  "rm",
  {
    work: workFlag,
    cached: Flag.boolean("cached").pipe(
      Flag.withDescription("Unstage only, and leave the file on disk"),
    ),
    paths: Argument.string("paths").pipe(Argument.variadic({ min: 1 })),
  },
  ({ cached, paths, work }) =>
    withWork(
      work,
      Effect.gen(function* () {
        for (const removed of yield* Checkout.remove(paths, { cached }))
          yield* Console.log(removed);
      }),
    ),
);

const mv = Command.make(
  "mv",
  { work: workFlag, from: Argument.string("from"), to: Argument.string("to") },
  ({ from, to, work }) =>
    withWork(
      work,
      Effect.gen(function* () {
        const moved = yield* Checkout.move(from, to);
        yield* Console.log(`${moved.from} -> ${moved.to}`);
      }),
    ),
);

const restore = Command.make(
  "restore",
  {
    work: workFlag,
    staged: Flag.boolean("staged").pipe(
      Flag.withDescription("Restore the index rather than the work tree"),
    ),
    source: Flag.string("source").pipe(
      Flag.optional,
      Flag.withDescription("Take content from this revision instead of the index"),
    ),
    paths: Argument.string("paths").pipe(Argument.variadic({ min: 1 })),
  },
  ({ paths, source, staged, work }) =>
    withWork(
      work,
      Effect.gen(function* () {
        const restored = yield* Checkout.restore(paths, {
          staged,
          worktree: !staged,
          ...(source._tag === "Some" ? { source: source.value } : {}),
        });
        for (const path of restored) yield* Console.log(path);
      }),
    ),
);

const switchCommand = Command.make(
  "switch",
  {
    work: workFlag,
    create: Flag.boolean("create").pipe(Flag.withDescription("Branch from HEAD first")),
    force: Flag.boolean("force").pipe(
      Flag.withDescription("Overwrite unstaged changes instead of refusing"),
    ),
    branch: Argument.string("branch"),
  },
  ({ branch, create, force, work }) =>
    withWork(
      work,
      Effect.gen(function* () {
        const result = yield* Checkout.checkout(branch, { create, force });
        yield* Console.log(`${result.ref} ${result.oid} (${result.files} file(s))`);
      }),
    ),
);

const commitCommand = Command.make(
  "commit",
  {
    work: workFlag,
    message: Flag.string("message").pipe(Flag.withDescription("Commit message")),
  },
  ({ message, work }) =>
    withWork(
      work,
      Effect.gen(function* () {
        const made = yield* Checkout.commit({ message, author: cliSignature() });
        yield* Console.log(`${made.oid} ${made.files} file(s)`);
      }),
    ),
);

const git = Command.make("chr33s-git").pipe(
  // Descriptions live here rather than beside each definition so `--help`
  // can be read as one list and checked for gaps in one place.
  Command.withSubcommands([
    addCommand.pipe(Command.withDescription("Stage paths as they are on disk")),
    archiveCommand.pipe(Command.withDescription("Write a tree as a tar, tar.gz or zip archive")),
    bisectCommand.pipe(
      Command.withDescription("Name the next commit to test between a good and a bad one"),
    ),
    branch.pipe(Command.withDescription("List, create or delete branches")),
    cherryPickCommand.pipe(Command.withDescription("Replay one commit onto another")),
    clone.pipe(Command.withDescription("Clone a repository over smart HTTP")),
    commitCommand.pipe(Command.withDescription("Commit what is staged")),
    diff.pipe(Command.withDescription("Unified diff between two revisions")),
    files.pipe(Command.withDescription("List the files a revision's tree holds")),
    fsck.pipe(Command.withDescription("Check every object and ref for damage")),
    gc.pipe(Command.withDescription("Drop unreachable objects, optionally repacking")),
    grep.pipe(Command.withDescription("Search a revision's file contents")),
    history.pipe(Command.withDescription("Commits that changed one path")),
    init.pipe(Command.withDescription("Create an empty bare repository")),
    log.pipe(Command.withDescription("Commit history, newest first")),
    merge.pipe(Command.withDescription("Three-way merge two revisions")),
    mv.pipe(Command.withDescription("Move a tracked path, staging both halves")),
    pushCommand.pipe(Command.withDescription("Push refs to a remote over smart HTTP")),
    rebaseCommand.pipe(Command.withDescription("Replay a branch's commits onto another")),
    refs.pipe(Command.withDescription("Every ref and the object it points at")),
    reset.pipe(Command.withDescription("Move a ref, optionally compare-and-swap")),
    restore.pipe(Command.withDescription("Restore a path from the index or a commit")),
    rm.pipe(Command.withDescription("Unstage a path, and delete it unless --cached")),
    serveCommand.pipe(
      Command.withDescription("Run the node host over a directory of repositories"),
    ),
    show.pipe(Command.withDescription("Show one object: a commit, tree, tag or blob")),
    statusCommand.pipe(Command.withDescription("Working-tree status in git's porcelain format")),
    switchCommand.pipe(
      Command.withDescription("Check out a branch, replacing index and work tree"),
    ),
    tag.pipe(Command.withDescription("List, create or delete tags")),
    token.pipe(Command.withDescription("Mint or verify a scoped access token")),
  ]),
);

const main = Command.runWith(git, { version: "0.0.0" });

/**
 * Domain failures leave as one readable line — `Schema.TaggedError` carries
 * its detail in fields, not `message`, and a stack trace helps nobody at a
 * shell prompt.
 */
const rendered = (error: unknown): unknown => {
  if (typeof error === "object" && error !== null && "_tag" in error) {
    const { _tag, ...fields } = error as Record<string, unknown>;
    const detail = typeof fields["reason"] === "string" ? fields["reason"] : JSON.stringify(fields);
    return new Error(`${String(_tag)}: ${detail}`);
  }
  return error;
};

if (import.meta.main) {
  NodeRuntime.runMain(
    main(process.argv.slice(2)).pipe(Effect.mapError(rendered), Effect.provide(NodeServices.layer)),
  );
}

export { git, main };
