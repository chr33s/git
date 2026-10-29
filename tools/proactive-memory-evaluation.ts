#!/usr/bin/env node
/** Prepare/review local historical comparisons; model execution remains operator-owned. */
import * as path from "node:path";
import { Console, Effect, Layer, Schema } from "effect";
import { Invalid } from "../src/git/Error.ts";
import * as Node from "../src/git/Node.ts";
import * as Repository from "../src/git/Repository.ts";
import * as Journal from "../src/knowledge/Journal.node.ts";
import * as Evaluation from "../src/knowledge/Evaluation.ts";
import * as Host from "../src/knowledge/Evaluation.node.ts";

const read = <S extends Schema.Top>(location: string, schema: S) =>
  Effect.gen(function* () {
    const value = yield* Journal.read(path.resolve(location), schema);
    if (value === null)
      return yield* new Invalid({ field: "evaluation", reason: `missing input: ${location}` });
    return value;
  });

const program = Effect.gen(function* () {
  const [action, ...args] = process.argv.slice(2);
  if (action === "--help") {
    yield* Console.log(
      "prepare <git-directory> <corpus.json> <configuration.json> <new-output-directory>\ncontexts <output-directory> <contexts.json>\nreport <output-directory>\nPreparation and reporting never invoke a model.",
    );
    return;
  }
  if (action === "prepare" && args.length === 4) {
    const [gitDirectory, corpusPath, configurationPath, output] = args;
    if (
      gitDirectory === undefined ||
      corpusPath === undefined ||
      configurationPath === undefined ||
      output === undefined
    )
      return;
    const corpus = yield* read(corpusPath, Evaluation.Corpus);
    const configuration = yield* Host.readConfiguration(path.resolve(configurationPath));
    const prepared = yield* Host.prepare({
      directory: path.resolve(output),
      corpus,
      configuration,
    }).pipe(
      Effect.provide(
        Repository.layer.pipe(
          Layer.provide(Repository.hooksNoop),
          Layer.provide(Node.stores(path.resolve(gitDirectory))),
        ),
      ),
    );
    yield* Console.log(JSON.stringify(prepared));
    return;
  }
  if (action === "contexts" && args.length === 2) {
    const [directory, contextsPath] = args;
    if (directory === undefined || contextsPath === undefined) return;
    yield* Console.log(
      JSON.stringify(
        yield* Host.contexts(path.resolve(directory), yield* read(contextsPath, Host.Contexts)),
      ),
    );
    return;
  }
  if (action === "report" && args.length === 1) {
    const directory = args[0];
    if (directory === undefined) return;
    const summary = yield* Host.report(path.resolve(directory));
    yield* Console.log(JSON.stringify(summary, null, 2));
    process.exitCode = summary.state === "partial" ? 2 : 0;
    return;
  }
  return yield* new Invalid({
    field: "arguments",
    reason:
      "usage: node tools/proactive-memory-evaluation.ts prepare <git-directory> <corpus.json> <configuration.json> <new-output-directory> | contexts <output-directory> <contexts.json> | report <output-directory>",
  });
});

await Effect.runPromise(
  program.pipe(
    Effect.catch((error) =>
      Console.error(`evaluation: ${Schema.is(Invalid)(error) ? error.reason : error._tag}`).pipe(
        Effect.tap(() =>
          Effect.sync(() => {
            process.exitCode = 1;
          }),
        ),
      ),
    ),
  ),
);
