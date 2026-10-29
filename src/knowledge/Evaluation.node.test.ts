import assert from "node:assert/strict";
import * as fs from "node:fs/promises";
import * as path from "node:path";
import * as os from "node:os";
import { it } from "@effect/vitest";
import { Effect, Layer } from "effect";
import * as GitRepository from "../git/Repository.ts";
import { Repository } from "../git/Repository.ts";
import { stores } from "../git/Memory.ts";
import { sha256 } from "../context/Render.ts";
import * as Evaluation from "./Evaluation.ts";
import * as Host from "./Evaluation.node.ts";
import * as Journal from "./Journal.node.ts";

const world = GitRepository.layer.pipe(
  Layer.provide(GitRepository.hooksNoop),
  Layer.provide(stores),
);

it.live(
  "freezes selected source packets and contexts, then rejects changed retained model bytes",
  () =>
    Effect.gen(function* () {
      const directory = yield* Effect.promise(() =>
        fs.mkdtemp(path.join(os.tmpdir(), "memory-evaluation-")),
      );
      yield* Effect.addFinalizer(() =>
        Effect.promise(() => fs.rm(directory, { recursive: true, force: true })),
      );
      const repository = yield* Repository;
      const tree = yield* repository.writeFiles({
        changes: [
          {
            path: "src/example.ts",
            mode: "100644",
            content: new TextEncoder().encode("export const value = 1;\n"),
          },
        ],
      });
      const base = yield* repository.commitTree({
        tree,
        parents: [],
        message: "historical fixture\n",
        author: { name: "Fixture", email: "fixture@example.com", at: new Date(0), offset: 0 },
      });
      const digest = `sha256:${"b".repeat(64)}`;
      const corpus: Evaluation.Corpus = {
        version: 1,
        id: "fixture-only",
        baselineRuntime: base,
        replayKind: "retrospective-known-fixes",
        metrics: ["inputTokens"],
        cases: [
          {
            id: "one",
            category: "fixture",
            base,
            referenceFix: base,
            paths: ["src/example.ts"],
            prompt: "Diagnose the task.",
            acceptance: ["Withheld criterion"],
            variants: ["normal"],
          },
        ],
      };
      const configuration: Evaluation.Configuration = {
        version: 1,
        runner: { id: "fixture", version: "1", model: "no-model-invoked", settings: {} },
        budget: {
          wallTimeMs: 100,
          inputTokens: 100,
          outputTokens: 100,
          toolCalls: 1,
          corrections: 0,
        },
        repetitions: 1,
        standingInstructionsDigest: digest,
        runtimeDigests: {
          "current-main": base,
          "integrated-recall": digest,
          "recall-with-reviewed-curation": digest,
        },
      };
      const output = path.join(directory, "comparison");
      const prepared = yield* Host.prepare({ directory: output, corpus, configuration });
      assert.equal(prepared.modelInvoked, false);
      assert.equal(prepared.slots, 3);
      const plan = yield* Journal.read(path.join(output, "plan.json"), Evaluation.Plan);
      assert.ok(plan);
      for (const slot of plan.slots) {
        const packet = yield* Effect.promise(() =>
          fs.readFile(path.join(output, `${slot.id.slice(7)}.packet.json`), "utf8"),
        );
        assert.ok(packet.includes("export const value = 1;"));
        assert.ok(!packet.includes("Withheld criterion"));
        assert.ok(!packet.includes("referenceFix"));
      }
      assert.equal((yield* Host.report(output)).missing.length, 3);
      const contexts = {
        version: 1 as const,
        plan: plan.id,
        entries: plan.slots.map((slot) => ({
          slot: slot.id,
          text: "Reference fixture data.",
          provenance: { fixture: true },
          maintenance: null,
        })),
      };
      const bound = yield* Host.contexts(output, contexts);
      assert.equal((yield* Host.contexts(output, contexts).pipe(Effect.flip))._tag, "Invalid");
      for (const input of bound.inputs) {
        const text = "Fixture output; no model was invoked.\n";
        const sample: Evaluation.Sample = {
          version: 1,
          plan: plan.id,
          slot: input.slot,
          attempt: 1,
          inputDigest: input.digest,
          outputDigest: yield* sha256(new TextEncoder().encode(text)),
          state: "completed",
          diagnostic: "Test fixture only",
          metrics: { inputTokens: { state: "unavailable", reason: "No model was invoked." } },
        };
        yield* Effect.promise(() =>
          fs.writeFile(path.join(output, `${input.slot.slice(7)}.output.txt`), text),
        );
        yield* Journal.write(path.join(output, `${input.slot.slice(7)}.sample.json`), sample);
      }
      assert.equal((yield* Host.report(output)).state, "recorded");
      const first = bound.inputs[0];
      assert.ok(first);
      yield* Effect.promise(() =>
        fs.writeFile(path.join(output, `${first.slot.slice(7)}.output.txt`), "changed"),
      );
      const rejected = yield* Host.report(output).pipe(Effect.flip);
      assert.equal(rejected._tag, "Invalid");
      assert.equal(
        (yield* Host.prepare({ directory: output, corpus, configuration }).pipe(Effect.flip))._tag,
        "StorageFailure",
      );
    }).pipe(Effect.scoped, Effect.provide(world)),
);

it.live("reporting refuses unsupported retained artifact versions", () =>
  Effect.gen(function* () {
    const directory = yield* Effect.promise(() =>
      fs.mkdtemp(path.join(os.tmpdir(), "memory-evaluation-version-")),
    );
    yield* Effect.addFinalizer(() =>
      Effect.promise(() => fs.rm(directory, { recursive: true, force: true })),
    );
    yield* Journal.write(path.join(directory, "corpus.json"), { version: 2 });
    const failed = yield* Host.report(directory).pipe(Effect.flip);
    assert.equal(failed._tag, "Invalid");
    if (failed._tag === "Invalid") assert.match(failed.reason, /unsupported artifact/u);
  }).pipe(Effect.scoped),
);
