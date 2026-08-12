#!/usr/bin/env node
/**
 * CLI.
 *
 * Today `src/cli.ts` is 1,991 lines, and most of it is not git: it is a
 * hand-written argv parser, per-command flag validation, usage text, help
 * output, and `console.log` formatting, repeated 21 times.
 *
 * Sketch: `effect/unstable/cli` owns parsing, flags, help, completions and
 * exit codes. What is left is one handler per command calling the same
 * `Repository` service the server uses — the CLI stops being a third
 * implementation of anything.
 */
import { Effect, Layer, Stream } from "effect";
import { Argument, Command, Flag } from "effect/unstable/cli";
import { NodeRuntime, NodeServices } from "@effect/platform-node";
import { memory, node } from "../adapters/Local.ts";
import * as GitRepository from "../git/Repository.ts";
import { Repository } from "../git/Repository.ts";

const commit = Command.make(
  "commit",
  {
    message: Flag.string("message").pipe(Flag.withAlias("m")),
    all: Flag.boolean("all").pipe(Flag.withAlias("a")),
  },
  ({ message }) =>
    Effect.gen(function* () {
      const repository = yield* Repository;
      const oid = yield* repository.commit({
        branch: "main",
        tree: yield* stageWorkTree(),
        message,
        author: yield* signature,
      });
      yield* Effect.log(`[main ${oid.slice(0, 7)}] ${message}`);
    }),
);

const log = Command.make(
  "log",
  { limit: Flag.integer("max-count").pipe(Flag.withDefault(20)) },
  ({ limit }) =>
    Effect.gen(function* () {
      const repository = yield* Repository;
      const head = yield* resolveHead;
      yield* repository
        .log(head, { limit })
        .pipe(
          // Streamed to stdout: `git log` on a large repo prints the first
          // commit immediately instead of collecting the walk first
          // (`cli.ts` buffers today).
          Stream.runForEach((entry) => Effect.log(`${entry.oid} ${entry.message.split("\n")[0]}`)),
        );
    }),
);

const clone = Command.make("clone", { url: Argument.string("url") }, ({ url }) =>
  Effect.log(`cloning ${url}`),
);

const git = Command.make("git").pipe(Command.withSubcommands([clone, commit, log]));

/**
 * The failure channel reaches `main`, so exit codes come from the error type:
 * `RefConflict` is 1 with a diagnostic, an interrupt is 130, an unexpected
 * defect prints a `Cause` with the fiber trace. `cli.ts` today catches
 * everything into `console.error` + `process.exit(1)`.
 */
const main = Command.runWith(git, { version: "0.0.0" });

/**
 * The whole local stack in one expression: node stores under the domain, the
 * platform services the CLI framework needs on the side. Swapping `node(...)`
 * for `memory` is what makes a CLI test a unit test.
 */
const CliLive = Layer.mergeAll(
  GitRepository.layer.pipe(
    Layer.provide(Layer.mergeAll(node(process.cwd()), memory, hooks)),
    Layer.provide(NodeServices.layer),
  ),
  NodeServices.layer,
);

NodeRuntime.runMain(main(process.argv).pipe(Effect.provide(CliLive)));

declare const hooks: Layer.Layer<import("../git/Repository.ts").Hooks>;

declare const stageWorkTree: () => Effect.Effect<import("../git/Store.ts").Oid>;
declare const signature: Effect.Effect<import("../git/Format.ts").Signature>;
declare const resolveHead: Effect.Effect<import("../git/Store.ts").Oid>;
