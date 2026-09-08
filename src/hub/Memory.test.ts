import assert from "node:assert/strict";
import { describe, it } from "@effect/vitest";
import { Effect, Layer } from "effect";

import { generate, type PrivateKey } from "../crypto/SshSignature.ts";
import { stores } from "../git/Memory.ts";
import * as GitRepository from "../git/Repository.ts";
import * as Memory from "./Memory.ts";
import * as Session from "./Session.ts";

const repository = GitRepository.layer.pipe(
  Layer.provide(GitRepository.hooksNoop),
  Layer.provide(stores),
);

const note = Effect.fn("test.memoryNote")(function* (key: PrivateKey, text: string) {
  const { session } = yield* Session.open({
    repo: "memory-test",
    agent: { kind: "test", model: "", harness: "" },
    prompt: "record a lesson",
    key,
  });
  yield* Session.produced({ repo: "memory-test", session, key, note: text });
});

describe("repository memory budget", () => {
  it.effect("keeps a recently repeated lesson when an older session resumes", () =>
    Effect.gen(function* () {
      const key = yield* generate("memory@example.com");
      const older = "00000000-0000-7000-8000-000000000001";
      const newer = "00000000-0000-7000-8000-000000000002";
      for (const session of [older, newer]) {
        yield* Session.open({
          repo: "memory-test",
          session,
          key,
          agent: { kind: "test", model: "", harness: "" },
          prompt: "record a lesson",
        });
      }
      const recent = "recent lesson. ".repeat(650);
      const stale = "stale lesson. ".repeat(650);
      const observations = [
        { session: older, note: recent },
        { session: newer, note: stale },
        { session: older, note: recent },
      ];
      for (const [index, observation] of observations.entries()) {
        yield* Session.issue(
          {
            ...(yield* Session.context("memory-test", observation.session)),
            id: `00000000-0000-7000-8000-00000000001${index}`,
            type: "session.produced",
            note: observation.note,
            commits: [],
            refs: [],
            pulls: [],
            usage: null,
          },
          key,
        );
      }
      const distilled = yield* Memory.distill();
      assert.equal(distilled.entries.length, 1, "only one lesson fits");
      assert.ok(distilled.entries[0]?.text === recent.trim(), "keep the recently repeated lesson");
      assert.equal(
        distilled.entries[0]?.observations,
        1,
        "repeating in one session is one observation",
      );
      assert.deepEqual(distilled.entries[0]?.cites, [older]);
    }).pipe(Effect.provide(repository)),
  );

  it.effect("keeps useful notes when a higher-ranked note cannot fit", () =>
    Effect.gen(function* () {
      const key = yield* generate("memory@example.com");
      const oversized = "gotcha: " + "a long lesson with ordinary words. ".repeat(600);
      yield* note(key, oversized);
      yield* note(key, oversized);
      yield* note(key, "convention: run the formatter before committing");

      const distilled = yield* Memory.distill();
      assert.deepEqual(
        distilled.entries.map((entry) => entry.text),
        ["run the formatter before committing"],
      );
      assert.equal(distilled.dropped, 1);
    }).pipe(Effect.provide(repository)),
  );

  it.effect("applies the byte budget to multibyte notes", () =>
    Effect.gen(function* () {
      const key = yield* generate("memory@example.com");
      yield* note(key, "convention: " + "文".repeat(3000));
      yield* note(key, "gotcha: " + "字".repeat(3000));

      const distilled = yield* Memory.distill();
      const bytes = new TextEncoder().encode(Memory.render(distilled.entries, distilled.sessions));
      assert.ok(bytes.length <= Memory.MAX_MEMORY, `memory used ${bytes.length} bytes`);
      assert.equal(distilled.entries.length, 1);
      assert.equal(distilled.dropped, 1);
    }).pipe(Effect.provide(repository)),
  );
});
