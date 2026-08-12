#!/usr/bin/env node
/**
 * CLI — phase 6.
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
import * as path from "node:path";

import { NodeRuntime, NodeServices } from "@effect/platform-node";
import { Config, Console, Effect, Layer, Stream } from "effect";
import { Argument, Command, Flag } from "effect/unstable/cli";

import { fetchRepository } from "../client/Fetch.ts";
import { Invalid } from "../git/Error.ts";
import { stores } from "../git/Node.ts";
import * as GitRepository from "../git/Repository.ts";
import { Repository } from "../git/Repository.ts";
import { ObjectStore, RefStore } from "../git/Store.ts";
import { serve } from "../host/Node.ts";
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
        const head = yield* repository.resolve(ref);
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
  ({ hostname, port, root, secret }) =>
    Effect.gen(function* () {
      const server = yield* Effect.promise(() =>
        serve({
          root,
          port,
          hostname,
          ...(secret === ""
            ? {}
            : {
                verify: (repo: string, credential: string | null) =>
                  Effect.runPromise(hmacVerify(secret, repo, credential)),
              }),
        }),
      );
      yield* Console.log(
        `git smart-HTTP server on ${server.url}, repositories under ${root}/` +
          (secret === "" ? " (open access)" : " (token required)"),
      );
      yield* Effect.never;
    }),
);

const git = Command.make("chr33s-git").pipe(
  Command.withSubcommands([clone, init, log, refs, serveCommand, token]),
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
