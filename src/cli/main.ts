#!/usr/bin/env node
/**
 * CLI.
 *
 * `effect/unstable/cli` owns parsing, flags, help and exit codes; each
 * handler calls the same `Repository` service, host, client and auth code
 * the server runs — the CLI is not another implementation of anything.
 *
 *   git+ init my-repo                      # bare repository under --root
 *   git+ refs my-repo · log my-repo        # inspect it
 *   git+ clone http://host/repo my-copy    # bare clone over smart HTTP
 *   git+ serve --port 8080                 # the node host
 *   git+ credential my-repo --key ~/.ssh/id_ed25519
 *
 * The failure channel reaches `main`, so exit codes come from the error
 * type: a bad ref is a diagnostic and exit 1, an interrupt is 130, an
 * unexpected defect prints a `Cause` with the fiber trace.
 */
import { createWriteStream } from "node:fs";
import * as path from "node:path";
import { Readable } from "node:stream";
import { pipeline } from "node:stream/promises";

// Deep imports, not the package barrel: the index re-exports the whole
// platform (undici, ws, msgpackr, ioredis), which defeats tree-shaking and
// costs the bundled CLI ~1 MiB and ~20 ms of startup for modules no command
// reaches.
import * as NodeRuntime from "@effect/platform-node/NodeRuntime";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { Console, Effect, Logger, Predicate, Stream } from "effect";
import { Argument, Command, Flag } from "effect/unstable/cli";

import * as Client from "../client/Client.ts";
import { fetchRepository } from "../client/Fetch.ts";
import { push } from "../client/Push.ts";
import { isBinary, unified } from "../git/Diff.ts";
import { forPath as pathHistory } from "../git/History.ts";
import { Invalid } from "../git/Error.ts";
import { isGitlink } from "../git/Format.ts";
import { stores } from "../git/Node.ts";
import * as GitRepository from "../git/Repository.ts";
import { Repository } from "../git/Repository.ts";
import { isOid, ObjectStore, type Oid, RefStore } from "../git/Store.ts";
import * as Redaction from "../hub/Redaction.ts";
import * as Archive from "../server/Archive.ts";
import { mintDelegation } from "../server/Auth.ts";
import { readGenesis } from "../trust/Genesis.ts";
import { GitInvocation } from "./GitCompat.ts";
import { parseInvocation, runCoreCompatibility } from "./GitCompat.node.ts";
import { contextCommand } from "./context.ts";
import { hubCommand } from "./hub.ts";
import { idCommand } from "./id.ts";
import * as replay from "./replay.ts";
import {
  cliSignature,
  mustResolve,
  refNameOf,
  readPrivateKey,
  repoArgument,
  resolveRev,
  rootFlag,
  withRepo,
} from "./shared.ts";
import { sessionCommand } from "./session.ts";
import { serveCommand } from "./serve.ts";
import { fetchCommand, pullCommand } from "./transport.ts";
import { queueCommand } from "./queue.ts";
import { socialCommand } from "./social.ts";
import { taskCommand } from "./task.ts";
import { prCommand } from "./pr.ts";
import { wakeCommand } from "./wake.ts";
import * as work from "./work.ts";
import { version } from "./version.ts";

/** One repository's stores as raw instances, for code that needs them directly. */
const openStores = (directory: string) =>
  Effect.gen(function* () {
    return { objects: yield* ObjectStore, refs: yield* RefStore };
  }).pipe(Effect.provide(stores(directory)));

/** The tree a revision names: a ref, an oid, a tag that peels, or a tree. */
const treeOf = (repository: Repository["Service"], rev: string) =>
  Effect.flatMap(mustResolve(repository, rev), (oid) => GitRepository.treeAt(repository, oid));

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
      // Refused rather than applied, and said out loud: a branch that stops
      // tracking silently is a divergence nobody sees until they compare tips.
      for (const ref of result.rejected) {
        yield* Console.log(`! rejected ${ref.name} (non-fast-forward)`);
      }
    }),
);

/**
 * A short-lived credential stock `git` can present.
 *
 * This is the replacement for `token`, and the difference is where the
 * authority comes from: the old command needed the *server's* secret, so
 * anyone who could mint a token could mint anyone's token. This one is signed
 * by the holder's own SSH key and verifies against the repository's membership
 * graph, so it can never carry more than the person running it already had.
 */
const credential = Command.make(
  "credential",
  {
    root: rootFlag,
    key: Flag.string("key").pipe(Flag.withDescription("Path to the SSH private key to sign with")),
    capability: Flag.string("capability").pipe(
      Flag.withDefault("repo.read"),
      Flag.withDescription("Capability to scope the credential to (repeatable as a,b)"),
      Flag.withAlias("c"),
    ),
    ttl: Flag.integer("ttl").pipe(
      Flag.withDefault(3600),
      Flag.withDescription("Seconds until the credential expires"),
    ),
    repo: repoArgument,
  },
  ({ capability, key, repo, root, ttl }) =>
    Effect.gen(function* () {
      const signer = yield* readPrivateKey(key);
      const minted = yield* withRepo(
        root,
        repo,
        Effect.gen(function* () {
          const stored = yield* readGenesis();
          if (stored === null) {
            return yield* new Invalid({
              field: "repo",
              reason: `${repo} is not hub-enabled; run \`hub init\` first`,
            });
          }
          return yield* mintDelegation({
            key: signer,
            repo: stored.genesis.repoId,
            capabilities: capability.split(",").map((value) => value.trim()),
            ttlSeconds: ttl,
          });
        }),
      );
      yield* Console.log(minted);
    }),
);

/**
 * Everything git wrote before it stopped writing.
 *
 * The helper protocol ends its request with a blank line, but git also closes
 * the stream, so reading to end-of-input is both simpler and correct — and a
 * helper that waits for a blank line it has already been given hangs the push
 * that called it.
 */
const readStdin = Effect.promise(
  () =>
    new Promise<string>((resolve) => {
      let text = "";
      process.stdin.setEncoding("utf8");
      process.stdin.on("data", (chunk: string) => {
        text += chunk;
      });
      process.stdin.on("end", () => {
        resolve(text);
      });
      // A stream that errors — or one already closed by the caller — never
      // ends, and a promise nothing settles is a helper that never exits and a
      // `git push` that hangs behind it. Resolved with what did arrive, so the
      // caller refuses on what it can see rather than on a timeout.
      process.stdin.on("error", () => {
        resolve(text);
      });
      if (process.stdin.destroyed) resolve(text);
    }),
);

/**
 * The same credential, in the shape stock `git` asks for one.
 *
 * git does not take a password on a command line: it runs a helper and speaks
 * a line protocol at it — `key=value` lines on stdin, a blank line, and the
 * answer the same way on stdout. Configured as
 *
 *   git config credential.useHttpPath true
 *   git config credential.helper '!git+ credential-helper --key ~/.ssh/id_ed25519 --root .'
 *
 * git appends the operation, so `get` arrives as an argument and a push picks
 * the credential up with nothing else to remember.
 *
 * `useHttpPath` is not a nicety. A credential here is scoped to one
 * repository — the RepoID is inside the signed bytes — and git's default is to
 * identify a credential by protocol and host alone, so without it the helper
 * is never told *which* repository is being pushed to. `--repo` names it
 * instead where a helper line is per repository; with neither, the refusal
 * says so rather than failing obscurely.
 *
 * `store` and `erase` succeed and do nothing, which is not laziness: there is
 * nothing to store. The credential is minted from the key on every ask, it
 * expires by itself, and it is not revocable except by revoking the member —
 * so a cache would only be a copy that outlives the question it answered.
 * Exiting non-zero there would make git report a failure for a push that
 * worked.
 *
 * The repository comes from `--repo` when given and otherwise from the
 * `path` git supplies, which is the last segment of the URL being pushed to:
 * one helper line then serves every repository on a host.
 *
 * The `host` git supplies is bound into the credential. git asks whichever
 * helper matches for whatever URL it is about to fetch from or push to, so
 * without that a redirect, a stale remote or a typo'd hostname was enough to
 * have this hand a live credential for the local repository to somebody
 * else's server — one that authenticates at the real host, since a repository
 * and its replicas share a `RepoID`. Bound, it is spendable only where git
 * said it was going.
 */
const credentialHelper = Command.make(
  "credential-helper",
  {
    root: rootFlag,
    key: Flag.string("key").pipe(Flag.withDescription("Path to the SSH private key to sign with")),
    capability: Flag.string("capability").pipe(
      Flag.withDefault("repo.read,source.push"),
      Flag.withDescription("Capabilities to scope the credential to (repeatable as a,b)"),
      Flag.withAlias("c"),
    ),
    ttl: Flag.integer("ttl").pipe(
      Flag.withDefault(3600),
      Flag.withDescription("Seconds until the credential expires"),
    ),
    repo: Flag.string("repo").pipe(
      Flag.withDefault(""),
      Flag.withDescription("Repository to mint for; defaults to the path git asks about"),
    ),
    operation: Argument.string("operation"),
  },
  ({ capability, key, operation, repo, root, ttl }) =>
    Effect.gen(function* () {
      if (operation !== "get") return;

      const asked = yield* readStdin;
      const fields = new Map<string, string>();
      for (const line of asked.split("\n")) {
        const cut = line.indexOf("=");
        if (cut > 0) fields.set(line.slice(0, cut), line.slice(cut + 1).trim());
      }

      // git gives the path without a leading slash, and a repository here is
      // one directory under the root — spelled the way the *server* reads it,
      // trailing `.git` and all. Taken verbatim, a push to `host/repo.git`
      // looked for a directory called `repo.git` and reported the repository as
      // not hub-enabled, while the same push to `host/repo` worked.
      const supplied =
        (fields.get("path") ?? "")
          .split("/")
          .filter((part) => part !== "")
          .at(-1) ?? "";
      const wanted = repo === "" ? supplied : repo;
      // Only the trailing `.git` is a suffix; `my.git.repo` keeps its name.
      const named = wanted.endsWith(".git") ? wanted.slice(0, -4) : wanted;
      if (named === "") {
        return yield* new Invalid({
          field: "repo",
          reason:
            "no repository to mint for: pass --repo, or set credential.useHttpPath=true so git says which repository it is asking about",
        });
      }

      // What git is about to talk to, and therefore the only host this
      // credential may be spent at. git supplies the port here only when it is
      // not the scheme's default, which is the same rule `new URL().host`
      // follows on the server side, so the two spellings agree.
      const audience = fields.get("host")?.toLowerCase() ?? "";
      if (audience === "") {
        return yield* new Invalid({
          field: "host",
          reason: "git named no host to bind the credential to",
        });
      }

      const signer = yield* readPrivateKey(key);
      const minted = yield* withRepo(
        root,
        named,
        Effect.gen(function* () {
          const stored = yield* readGenesis();
          if (stored === null) {
            return yield* new Invalid({
              field: "repo",
              reason: `${named} is not hub-enabled; run \`hub init\` first`,
            });
          }
          return yield* mintDelegation({
            key: signer,
            repo: stored.genesis.repoId,
            capabilities: capability.split(",").map((value) => value.trim()),
            audience,
            ttlSeconds: ttl,
          });
        }),
      );

      // The username is not read — the credential is the whole claim — but git
      // asks for one, and a helper that answers only a password makes it
      // prompt for the name it was trying to avoid asking for.
      yield* Console.log(`username=chr33s-git\npassword=${minted}`);
    }),
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
    force: Flag.boolean("force").pipe(Flag.withDefault(false), Flag.withAlias("f")),
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
          // A message makes it an annotated tag object; without one the tag
          // is nothing but a ref.
          const input = { name, target, force };
          const created = yield* message === ""
            ? repository.tag(input)
            : repository.tag({
                ...input,
                message: `${message}\n`,
                tagger: yield* cliSignature(),
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
          // A gitlink names a commit in another repository: nothing to read,
          // and nothing a text diff could say about it.
          if (isGitlink(old?.mode ?? "") || isGitlink(now?.mode ?? "")) {
            yield* Console.log(`Submodule ${path} ${old?.oid ?? "0".repeat(7)}..${now?.oid ?? ""}`);
            continue;
          }

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
        const input = { ours, theirs, author: yield* cliSignature(), strategy };
        const outcome = yield* into === ""
          ? repository.merge(input)
          : repository.merge({ ...input, into });

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
    ignoreCase: Flag.boolean("ignore-case").pipe(Flag.withDefault(false), Flag.withAlias("i")),
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
          if (isGitlink(file.mode)) continue;
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
    dryRun: Flag.boolean("dry-run").pipe(Flag.withDefault(false), Flag.withAlias("n")),
    repack: Flag.boolean("repack").pipe(
      Flag.withDefault(false),
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
        // A tombstoned payload is still named by its own event's tree, so
        // reachability protects it unless it is excluded here. Without this,
        // `hub redact` never removes anything a pack already holds.
        //
        // A dry run pays for the set too. It exists to say what the real run
        // would do, and skipping it to save a trust fold made it say something
        // else: a tombstoned payload reported as reachable, "would remove 0",
        // and the same command without `--dry-run` removing it.
        const exclude = yield* Redaction.excluded();
        const report = yield* repository.gc({ dryRun, repack, exclude });
        yield* Console.log(
          `${dryRun ? "would remove" : "removed"} ${report.removed.length} of ${report.scanned} object(s), ${report.reachable} reachable`,
        );
        if (report.retained.length > 0) {
          yield* Console.log(
            `${report.retained.length} unreachable object(s) are inside a pack; run with --repack to collect them`,
          );
        }
        if (report.packed !== undefined) {
          yield* Console.log(
            `packed ${report.packed.objects} object(s) into ${report.packed.name}`,
          );
        }
        // `--repack` that quietly did nothing is the confusing outcome this
        // reports its way out of: the run refused to pack, and said why.
        if (report.repackSkipped !== undefined) {
          yield* Console.log(`did not repack: ${report.repackSkipped}`);
        }
      }),
    ),
);

const pushCommand = Command.make(
  "push",
  {
    root: rootFlag,
    token: Flag.string("token").pipe(Flag.withDefault("")),
    force: Flag.boolean("force").pipe(Flag.withDefault(false), Flag.withAlias("f")),
    atomic: Flag.boolean("atomic").pipe(Flag.withDefault(false)),
    delete: Flag.boolean("delete").pipe(
      Flag.withDefault(false),
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
        const request = { url, refs: [{ local: remote, remote, delete: remove }], force, atomic };
        const results = yield* accessToken === ""
          ? push(request)
          : push({ ...request, token: accessToken });

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
        const tree = yield* treeOf(repository, ref);
        const stream = yield* prefix === ""
          ? Archive.archive({ tree, format })
          : Archive.archive({ tree, format, prefix });

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
        const input = { name: refNameOf(ref), to: yield* mustResolve(repository, to) };
        // An empty `--expected` claims the ref does not exist yet; anything
        // else must spell the oid the ref currently holds.
        const previous = expected._tag === "Some" && expected.value !== "" ? expected.value : null;
        if (previous !== null && !isOid(previous)) {
          return yield* new Invalid({
            field: "expected",
            reason: `'${previous}' is not an object id`,
          });
        }
        const moved = yield* expected._tag === "None"
          ? repository.setRef(input)
          : repository.setRef({ ...input, expected: previous });
        yield* Console.log(`${moved.ref} ${moved.previous ?? "(new)"} -> ${moved.oid}`);
      }),
    ),
);

const reflogCommand = Command.make(
  "reflog",
  { root: rootFlag, repo: repoArgument, ref: Argument.string("ref") },
  ({ ref, repo, root }) =>
    withRepo(
      root,
      repo,
      Effect.gen(function* () {
        const repository = yield* Repository;
        const entries = yield* repository.reflog(
          ref.startsWith("refs/") || ref === "HEAD" ? ref : `refs/heads/${ref}`,
        );
        for (const entry of entries) {
          yield* Console.log(
            `${entry.from ?? "0".repeat(40)} ${entry.to ?? "0".repeat(40)} ${entry.at.toISOString()} ${entry.message}`,
          );
        }
      }),
    ),
);

/** A server flag pair every registry verb shares. */
const serverFlags = {
  server: Flag.string("server").pipe(
    Flag.withDescription("The git+ server's base URL, e.g. https://git.example.com"),
  ),
  token: Flag.string("token").pipe(Flag.withDefault("")),
};

const clientFor = (server: string, token: string) =>
  token === "" ? Client.remote(server) : Client.remote(server, { token });

/**
 * The server's remote registry, administered from here.
 *
 * These verbs manage state that lives on the *server* — stored remotes, their
 * credentials and standing sync instructions — so they speak the JSON API
 * through the client derived from its declaration, exactly as the browser
 * does. Nothing here touches a local repository.
 */
const remoteAdd = Command.make(
  "add",
  {
    ...serverFlags,
    credential: Flag.string("credential").pipe(Flag.withDefault("")),
    repo: repoArgument,
    name: Argument.string("name"),
    url: Argument.string("url"),
  },
  ({ credential, name, repo, server, token, url }) =>
    Effect.gen(function* () {
      const client = yield* clientFor(server, token);
      const added = yield* client.remotes.remoteAdd({
        params: { repo },
        payload: credential === "" ? { name, url } : { name, url, credential },
      });
      yield* Console.log(JSON.stringify(added, null, 2));
    }).pipe(Effect.scoped),
);

const remoteList = Command.make(
  "list",
  { ...serverFlags, repo: repoArgument },
  ({ repo, server, token }) =>
    Effect.gen(function* () {
      const client = yield* clientFor(server, token);
      const found = yield* client.remotes.remoteList({ params: { repo } });
      yield* Console.log(JSON.stringify(found.remotes, null, 2));
    }).pipe(Effect.scoped),
);

const remoteRemove = Command.make(
  "rm",
  { ...serverFlags, repo: repoArgument, name: Argument.string("name") },
  ({ name, repo, server, token }) =>
    Effect.gen(function* () {
      const client = yield* clientFor(server, token);
      yield* client.remotes.remoteRemove({ params: { repo, name } });
    }).pipe(Effect.scoped),
);

const serverRemoteCommand = Command.make("remote", {}, () =>
  Console.log("git+ server remote <add|list|rm> --server <url> — see --help"),
).pipe(
  Command.withSubcommands([
    remoteAdd.pipe(Command.withDescription("Register a remote on the server")),
    remoteList.pipe(Command.withDescription("The server's stored remotes, secrets redacted")),
    remoteRemove.pipe(Command.withDescription("Forget a stored remote")),
  ]),
);

const serverCommand = Command.make("server", {}, () =>
  Console.log("git+ server <remote> — see --help"),
).pipe(
  Command.withSubcommands([
    serverRemoteCommand.pipe(
      Command.withDescription("Administer a server's stored remotes over its JSON API"),
    ),
  ]),
);

const webhookAdd = Command.make(
  "add",
  {
    ...serverFlags,
    secret: Flag.string("secret").pipe(Flag.withDescription("Signs each delivery")),
    repo: repoArgument,
    url: Argument.string("url"),
  },
  ({ repo, secret, server, token, url }) =>
    Effect.gen(function* () {
      const client = yield* clientFor(server, token);
      const added = yield* client.repo.webhookAdd({ params: { repo }, payload: { url, secret } });
      yield* Console.log(JSON.stringify(added, null, 2));
    }).pipe(Effect.scoped),
);

const webhookList = Command.make(
  "list",
  { ...serverFlags, repo: repoArgument },
  ({ repo, server, token }) =>
    Effect.gen(function* () {
      const client = yield* clientFor(server, token);
      const found = yield* client.repo.webhookList({ params: { repo } });
      yield* Console.log(JSON.stringify(found.webhooks, null, 2));
    }).pipe(Effect.scoped),
);

const webhookRemove = Command.make(
  "rm",
  { ...serverFlags, repo: repoArgument, id: Argument.string("id") },
  ({ id, repo, server, token }) =>
    Effect.gen(function* () {
      const client = yield* clientFor(server, token);
      yield* client.repo.webhookRemove({ params: { repo, id } });
    }).pipe(Effect.scoped),
);

const webhookCommand = Command.make("webhook", {}, () =>
  Console.log("git+ webhook <add|list|rm> --server <url> — see --help"),
).pipe(
  Command.withSubcommands([
    webhookAdd.pipe(Command.withDescription("Register a webhook; the secret never comes back")),
    webhookList.pipe(Command.withDescription("The server's webhooks, secrets redacted")),
    webhookRemove.pipe(Command.withDescription("Remove a webhook")),
  ]),
);

const git = Command.make("git+").pipe(
  // Descriptions live here rather than beside each definition so `--help`
  // can be read as one list and checked for gaps in one place.
  Command.withSubcommands([
    work.addCommand.pipe(Command.withDescription("Stage paths as they are on disk")),
    archiveCommand.pipe(Command.withDescription("Write a tree as a tar, tar.gz or zip archive")),
    replay.bisectCommand.pipe(
      Command.withDescription("Name the next commit to test between a good and a bad one"),
    ),
    branch.pipe(Command.withDescription("List, create or delete branches")),
    replay.cherryPickCommand.pipe(Command.withDescription("Replay one commit onto another")),
    clone.pipe(Command.withDescription("Clone a repository over smart HTTP")),
    work.commitCommand.pipe(Command.withDescription("Commit what is staged")),
    contextCommand.pipe(Command.withDescription("Git-native context packs, renders and exposures")),
    diff.pipe(Command.withDescription("Unified diff between two revisions")),
    files.pipe(Command.withDescription("List the files a revision's tree holds")),
    fetchCommand.pipe(
      Command.withDescription("Fetch a remote's branches and tags into a cloned repository"),
    ),
    fsck.pipe(Command.withDescription("Check every object and ref for damage")),
    gc.pipe(Command.withDescription("Drop unreachable objects, optionally repacking")),
    grep.pipe(Command.withDescription("Search a revision's file contents")),
    hubCommand.pipe(Command.withDescription("Repository identity, membership and trust")),
    idCommand.pipe(Command.withDescription("Stable principal identity and device rotation")),
    history.pipe(Command.withDescription("Commits that changed one path")),
    init.pipe(Command.withDescription("Create an empty bare repository")),
    log.pipe(Command.withDescription("Commit history, newest first")),
    merge.pipe(Command.withDescription("Three-way merge two revisions")),
    work.mv.pipe(Command.withDescription("Move a tracked path, staging both halves")),
    prCommand.pipe(Command.withDescription("Pull requests: open, review, discuss, check, merge")),
    pullCommand.pipe(Command.withDescription("Fast-forward one branch from a remote")),
    pushCommand.pipe(Command.withDescription("Push refs to a remote over smart HTTP")),
    replay.rebaseCommand.pipe(Command.withDescription("Replay a branch's commits onto another")),
    reflogCommand.pipe(Command.withDescription("Where a ref has been: every move, newest first")),
    refs.pipe(Command.withDescription("Every ref and the object it points at")),
    serverCommand.pipe(Command.withDescription("Server JSON-API administration extensions")),
    reset.pipe(Command.withDescription("Move a ref, optionally compare-and-swap")),
    work.restore.pipe(Command.withDescription("Restore a path from the index or a commit")),
    work.rm.pipe(Command.withDescription("Unstage a path, and delete it unless --cached")),
    serveCommand.pipe(
      Command.withDescription("Run the node host over a directory of repositories"),
    ),
    show.pipe(Command.withDescription("Show one object: a commit, tree, tag or blob")),
    work.statusCommand.pipe(
      Command.withDescription("Working-tree status in git's porcelain format"),
    ),
    work.switchCommand.pipe(
      Command.withDescription("Check out a branch, replacing index and work tree"),
    ),
    sessionCommand.pipe(
      Command.withDescription("Record what an agent was told, and what came of it"),
    ),
    socialCommand.pipe(Command.withDescription("Social graph: follows, vouches and discovery")),
    tag.pipe(Command.withDescription("List, create or delete tags")),
    taskCommand.pipe(Command.withDescription("What needs doing, and who is on it")),
    queueCommand.pipe(Command.withDescription("Land approved pull requests as one tested batch")),
    wakeCommand,
    webhookCommand.pipe(
      Command.withDescription("Administer a server's webhooks over its JSON API"),
    ),
    credential.pipe(Command.withDescription("Mint a short-lived credential stock git can present")),
    credentialHelper.pipe(Command.withDescription("Answer git's credential helper protocol")),
  ]),
);

const main = Command.runWith(git, { version });

/**
 * Domain failures leave as one readable line — `Schema.TaggedError` carries
 * its detail in fields, not `message`, and a stack trace helps nobody at a
 * shell prompt.
 */
const rendered = <E>(error: E): E | Error => {
  if (Predicate.hasProperty(error, "_tag")) {
    const { _tag, ...fields } = error;
    const detail =
      Predicate.hasProperty(fields, "reason") && Predicate.isString(fields.reason)
        ? fields.reason
        : JSON.stringify(fields);
    return new Error(`${String(_tag)}: ${detail}`);
  }
  return error;
};

/**
 * Parse Git's global invocation before command-local Effect CLI parsing.
 *
 * `-C` has to change the process directory before file-backed command layers
 * are built; `--git-dir` and `--work-tree` travel as `GitInvocation` so work
 * commands bind the same stores Git selected rather than a private `--work`.
 */
const run = (argv: ReadonlyArray<string> = process.argv.slice(2)) => {
  const parsed = parseInvocation({
    argv,
    cwd: process.cwd(),
    environment: process.env,
  });
  if (parsed._tag === "InvalidInvocation") {
    process.stderr.write(`git+: ${parsed.message}\n`);
    process.exitCode = 129;
    return;
  }
  try {
    process.chdir(parsed.invocation.cwd);
  } catch {
    process.stderr.write(`git+: cannot change to '${parsed.invocation.cwd}'\n`);
    process.exitCode = 128;
    return;
  }
  if (runCoreCompatibility(parsed.invocation)) return;
  NodeRuntime.runMain(
    main(parsed.invocation.argv).pipe(
      Effect.mapError(rendered),
      Effect.provideService(GitInvocation, parsed.invocation),
      // Diagnostics on stderr, because stdout is a result. Several verbs print
      // JSON there and are read by something that parses it, and the default
      // logger writes to stdout — so one warning from anything the verb touched
      // (a webhook that would not answer, a mirror that refused) landed in front
      // of the JSON and the reader got a syntax error instead of a result.
      Effect.provideService(Logger.LogToStderr, true),
      Effect.provide(NodeServices.layer),
    ),
  );
};

if (import.meta.main) run();

export { git, main, run };
