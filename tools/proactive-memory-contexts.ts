#!/usr/bin/env node
/** Generate condition contexts using real runtime code and isolated synthetic replay repositories. */
import * as fs from "node:fs";
import * as path from "node:path";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { pathToFileURL } from "node:url";
import { Effect, Layer, Schema } from "effect";
import { Invalid } from "../src/git/Error.ts";
import * as Node from "../src/git/Node.ts";
import * as GitRepository from "../src/git/Repository.ts";
import { Repository } from "../src/git/Repository.ts";
import { unqualify } from "../src/git/Oid.ts";
import { fingerprint } from "../src/crypto/SshSignature.ts";
import * as Trust from "../src/trust/Projection.ts";
import * as Certificate from "../src/trust/Certificate.ts";
import * as Log from "../src/trust/Log.ts";
import * as Session from "../src/hub/Session.ts";
import * as Memory from "../src/hub/Memory.ts";
import * as Journal from "../src/knowledge/Journal.node.ts";
import * as Host from "../src/knowledge/Maintenance.node.ts";
import * as Evaluation from "../src/knowledge/Evaluation.ts";
import * as Replay from "../src/testing/HistoricalMemory.ts";

const read = <S extends Schema.Top>(location: string, schema: S) =>
  Effect.gen(function* () {
    const value = yield* Journal.read(location, schema);
    if (value === null)
      return yield* new Invalid({ field: "evaluation", reason: `missing ${location}` });
    return value;
  });
const stores = (directory: string) =>
  GitRepository.layer.pipe(
    Layer.provide(GitRepository.hooksNoop),
    Layer.provide(Node.stores(directory)),
  );
const baseline = Effect.fn("evaluation.baseline")(function* (
  runtime: string,
  directory: string,
  mode: "seed" | "read",
) {
  const url = (name: string) => pathToFileURL(path.join(runtime, name)).href;
  const script = `import {Effect,Layer} from ${JSON.stringify(url("node_modules/effect/dist/index.js"))};
    import * as Node from ${JSON.stringify(url("src/git/Node.ts"))};
    import * as GitRepository from ${JSON.stringify(url("src/git/Repository.ts"))};
    import * as Memory from ${JSON.stringify(url("src/hub/Memory.ts"))};
    const value=await Effect.runPromise(Effect.gen(function*(){
      const started=performance.now();
      let text='';
      if(process.argv[2]==='seed') {const held=yield* Memory.distill();text=Memory.render(held.entries,held.sessions);yield* Memory.write(text);}
      else text=(yield* Memory.read())??'';
      return {text,latencyMs:performance.now()-started};
    }).pipe(Effect.provide(GitRepository.layer.pipe(Layer.provide(GitRepository.hooksNoop),Layer.provide(Node.stores(process.argv[1]))))));
    process.stdout.write(JSON.stringify(value));`;
  const started = performance.now();
  const result = yield* Effect.tryPromise({
    try: () =>
      promisify(execFile)(
        process.execPath,
        ["--input-type=module", "-e", script, directory, mode],
        { maxBuffer: 1048576, timeout: 30000 },
      ),
    catch: () =>
      new Invalid({ field: "evaluation.baseline", reason: "pinned baseline subprocess failed" }),
  });
  const parsed = yield* Schema.decodeEffect(
    Schema.fromJsonString(Schema.Struct({ text: Schema.String, latencyMs: Schema.Finite })),
  )(result.stdout);
  return {
    text: parsed.text,
    latencyMs: parsed.latencyMs,
    subprocessMs: performance.now() - started,
  };
});

const program = Effect.gen(function* () {
  const [gitDirectory, corpusPath, lessonsPath, baselineDirectory, destination] =
    process.argv.slice(2);
  if (
    gitDirectory === undefined ||
    corpusPath === undefined ||
    lessonsPath === undefined ||
    baselineDirectory === undefined ||
    destination === undefined
  )
    return yield* new Invalid({
      field: "arguments",
      reason:
        "usage: node tools/proactive-memory-contexts.ts <git-directory> <corpus.json> <lessons.json> <pinned-baseline-runtime> <new-output-directory>",
    });
  const corpus = yield* read(path.resolve(corpusPath), Evaluation.Corpus);
  const lessons = yield* read(
    path.resolve(lessonsPath),
    Schema.Struct({
      version: Schema.Literal(1),
      lessons: Schema.Record(Schema.String, Schema.String),
    }),
  );
  const source = yield* Repository.pipe(Effect.provide(stores(path.resolve(gitDirectory))));
  const directory = path.resolve(destination);
  yield* Effect.try({
    try: () => fs.mkdirSync(directory, { mode: 0o700 }),
    catch: () =>
      new Invalid({
        field: "evaluation",
        reason: "output directory must be new and have an existing parent",
      }),
  });
  const entries: Array<Schema.Json> = [];
  for (const task of corpus.cases) {
    const lesson = lessons.lessons[task.id];
    if (lesson === undefined)
      return yield* new Invalid({
        field: "evaluation",
        reason: `no preregistered lesson for ${task.id}`,
      });
    const files = Effect.fn("evaluation.files")(function* (revision: string) {
      const oid = unqualify(`sha1:${revision}`);
      if (oid === null)
        return yield* new Invalid({ field: "evaluation", reason: "invalid source revision" });
      const tree = (yield* source.readCommit(oid)).tree;
      return yield* Effect.forEach(task.paths, (name) =>
        Effect.gen(function* () {
          const entry = yield* source.findPath(tree, name);
          if (entry === null || entry.mode !== "100644")
            return yield* new Invalid({
              field: "evaluation",
              reason: `missing regular source: ${name}`,
            });
          return { path: name, content: yield* source.readBlob(entry.oid) };
        }),
      );
    });
    const before = yield* files(task.base);
    const after = yield* files(task.referenceFix);
    for (const variant of task.variants) {
      if (!/^[A-Za-z0-9-]+$/u.test(task.id) || !/^[A-Za-z0-9-]+$/u.test(variant))
        return yield* new Invalid({ field: "evaluation", reason: "unsafe fixture name" });
      const repositoryDirectory = path.join(directory, `${task.id}-${variant}.git`);
      yield* Effect.gen(function* () {
        const repository = yield* Repository;
        const held = yield* Replay.fixture({
          caseId: task.id,
          historicalBase: task.base,
          historicalFix: task.referenceFix,
          before,
          after,
          lesson,
        });
        const evaluatedAt = new Date().toISOString();
        const curationStarted = performance.now();
        const curation = yield* Replay.curate(held, evaluatedAt);
        const curationMs = performance.now() - curationStarted;
        const initial = yield* Memory.derive({
          sourceRef: "refs/heads/main",
          repo: held.genesis.repoId,
          trust: yield* Trust.project(held.genesis),
          evaluationTime: new Date(evaluatedAt),
        });
        const initialRaw = yield* baseline(
          path.resolve(baselineDirectory),
          repositoryDirectory,
          "seed",
        );
        let invalidatedAt: number | null = null;
        if (variant === "support-tombstoned-before-start") {
          const output = (yield* Session.entries(held.session)).events.find(
            (entry) => entry.commit === held.output,
          );
          if (output === undefined)
            return yield* new Invalid({
              field: "evaluation",
              reason: "fixture output disappeared",
            });
          yield* Session.redact({
            repo: held.genesis.repoId,
            session: held.session,
            target: output.payload.id,
            key: held.root,
            reason: "controlled benchmark tombstone",
          });
          invalidatedAt = performance.now();
        }
        if (variant === "support-compromised-before-start") {
          yield* Log.issue(
            Certificate.revoke({
              repo: held.genesis.repoId,
              subject: yield* fingerprint(held.agent.publicKey),
              reason: "compromised",
              compromisedAt: new Date(0),
              id: Log.newId(),
            }),
            [held.root],
          );
          invalidatedAt = performance.now();
        }
        if (variant === "dependency-changed-after-briefing") {
          const head = yield* repository.resolve("refs/heads/main");
          if (head === null)
            return yield* new Invalid({
              field: "evaluation",
              reason: "fixture source disappeared",
            });
          const current = yield* repository.readCommit(head);
          const packageFile = after.find((file) => file.path === "package.json");
          if (packageFile === undefined)
            return yield* new Invalid({
              field: "evaluation",
              reason: "missing dependency fixture",
            });
          const tree = yield* repository.writeFiles({
            base: current.tree,
            changes: [
              {
                path: "package.json",
                mode: "100644",
                content: new TextEncoder().encode(
                  `${new TextDecoder().decode(packageFile.content)}\n`,
                ),
              },
            ],
          });
          yield* repository.commit({
            branch: "refs/heads/main",
            tree,
            message: "Controlled declared-dependency drift\n",
            author: {
              name: "Fixture operator",
              email: "fixture@example.invalid",
              at: new Date(),
              offset: 0,
            },
          });
          invalidatedAt = performance.now();
        }
        const maintenanceStarted = performance.now();
        const maintained = yield* Host.run({
          directory: repositoryDirectory,
          sourceRef: "refs/heads/main",
          evaluatedAt: new Date().toISOString(),
        });
        const maintenanceMs = performance.now() - maintenanceStarted;
        const view = yield* repository.readCommit(held.accepted);
        const currentHead = yield* repository.resolve("refs/heads/main");
        if (currentHead === null)
          return yield* new Invalid({
            field: "evaluation",
            reason: "fixture source disappeared",
          });
        // A separate committed source view retains current dependency bytes while excluding curation files.
        const currentTree = (yield* repository.readCommit(currentHead)).tree;
        const dependency = yield* repository.findPath(currentTree, "package.json");
        const tree =
          dependency === null
            ? view.tree
            : yield* repository.writeFiles({
                base: view.tree,
                changes: [
                  {
                    path: "package.json",
                    mode: "100644",
                    content: yield* repository.readBlob(dependency.oid),
                  },
                ],
              });
        const commit = yield* repository.commitTree({
          tree,
          parents: [held.accepted],
          message: "Integrated-only controlled source view\n",
          author: {
            name: "Fixture operator",
            email: "fixture@example.invalid",
            at: new Date(0),
            offset: 0,
          },
        });
        yield* repository.setRef({ name: "refs/heads/integrated-control", to: commit });

        for (const condition of Evaluation.conditions) {
          const started = performance.now();
          const raw =
            condition === "current-main"
              ? yield* baseline(path.resolve(baselineDirectory), repositoryDirectory, "read")
              : null;
          const built =
            condition === "current-main"
              ? null
              : yield* Memory.derive({
                  sourceRef:
                    condition === "integrated-recall"
                      ? "refs/heads/integrated-control"
                      : "refs/heads/main",
                  repo: held.genesis.repoId,
                  trust: yield* Trust.project(held.genesis),
                  evaluationTime: new Date(),
                });
          const text = raw?.text ?? built?.text ?? "";
          const projection = built;
          const containsLesson = text.includes(lesson);
          entries.push({
            caseId: task.id,
            variant,
            condition,
            text,
            provenance: {
              synthetic: true,
              historicalBase: task.base,
              historicalFix: task.referenceFix,
              repo: held.genesis.repoId,
              originalOutput: `sha1:${held.output}`,
              initialFingerprint: initial.projectionFingerprint,
              manifest: projection?.manifest ?? null,
              baseline: corpus.baselineRuntime,
            },
            maintenance: condition === "recall-with-reviewed-curation" ? maintained.job : null,
            timings: {
              startupLatencyMs: raw?.latencyMs ?? performance.now() - started,
              baselineSubprocessMs: raw?.subprocessMs ?? null,
              maintenanceRuntimeMs:
                condition === "recall-with-reviewed-curation" ? maintenanceMs + curationMs : 0,
              correctionLatencyMs:
                invalidatedAt === null
                  ? null
                  : variant === "dependency-changed-after-briefing"
                    ? condition === "recall-with-reviewed-curation" &&
                      !projection?.entries.some((entry) => entry.source?.path === curation.path)
                      ? performance.now() - invalidatedAt
                      : null
                    : containsLesson
                      ? null
                      : performance.now() - invalidatedAt,
            },
            observed: {
              containsLesson,
              curatedConceptVisible:
                projection?.entries.some((entry) => entry.source?.path === curation.path) ?? false,
              conflicts: 0,
              incompleteJobs:
                condition === "recall-with-reviewed-curation" && maintained.exitCode !== 0 ? 1 : 0,
            },
            fixtureReview: {
              reviewer: "benchmark preparer",
              basis:
                "preregistered interpretation of the retained historical fix; synthetic replay, not human-preference evidence",
            },
            initialRawBytes: new TextEncoder().encode(initialRaw.text).length,
          });
        }
        yield* Journal.write(path.join(directory, "contexts.partial.json"), {
          version: 1,
          entries,
        });
        console.log(`prepared ${task.id}/${variant}`);
      }).pipe(Effect.provide(stores(repositoryDirectory)));
    }
  }
  yield* Journal.write(path.join(directory, "contexts.json"), { version: 1, entries });
});

await Effect.runPromise(
  program.pipe(
    Effect.catch((error) =>
      Effect.sync(() => {
        console.error(
          `evaluation-contexts: ${Schema.is(Invalid)(error) ? error.reason : error._tag}`,
        );
        process.exitCode = 1;
      }),
    ),
  ),
);
