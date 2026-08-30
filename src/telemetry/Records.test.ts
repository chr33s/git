/**
 * The durable shapes, and the rules a schema cannot state.
 *
 * The round-trip here is deliberately maximal for the reason
 * `context/Pack.test.ts` says: the encoder fixes field order with one global
 * property list, so a field missing from that list is dropped in silence — a
 * record that verifies as a record making fewer claims than its author made.
 * Only a value carrying every field catches it.
 */
import assert from "node:assert/strict";
import { describe, it } from "@effect/vitest";

import { Effect, Layer } from "effect";

import { generate } from "../crypto/SshSignature.ts";
import { stores } from "../git/Memory.ts";
import * as GitRepository from "../git/Repository.ts";
import { Repository } from "../git/Repository.ts";
import { qualify } from "../git/Oid.ts";
import * as Trace from "../hub/Trace.ts";
import * as Records from "./Records.ts";

const REPO = "SHA256:test";
const SESSION = "0192f000-0000-7000-8000-000000000000";
const OID = `sha1:${"a".repeat(40)}`;

const world = GitRepository.layer.pipe(
  Layer.provide(GitRepository.hooksNoop),
  Layer.provideMerge(stores),
);

const scenario = <A, E>(effect: Effect.Effect<A, E, Repository>): Promise<A> =>
  Effect.runPromise(effect.pipe(Effect.provide(world)));

const envelope = {
  version: 1,
  repo: REPO,
  session: SESSION,
  id: "0192f000-0000-7000-8000-00000000000a",
  issuedAt: "2026-08-30T00:00:00.000Z",
  trustHead: null,
} as const;

const roundTrip = (payload: Records.Payload) =>
  scenario(
    Effect.gen(function* () {
      const bytes = Records.encode(payload);
      const decoded = yield* Records.decode(bytes);
      assert.deepEqual(decoded, payload);
      // The bytes are what a signature covers, so re-encoding what was just
      // decoded has to reproduce them exactly.
      assert.deepEqual([...Records.encode(decoded)], [...bytes]);
      return decoded;
    }),
  );

describe("trace records", () => {
  it.effect("round-trips every field an invocation record can carry", () =>
    Effect.promise(() =>
      roundTrip({
        type: Records.INVOCATION,
        ...envelope,
        exposure: OID,
        capture: {
          transport: "otel",
          stage: "sdk-export",
          traceId: "4bf92f3577b34da6a3ce929d0e0e4736",
          spanId: "00f067aa0ba902b7",
          semconv: { profile: "open-telemetry/semantic-conventions-genai", revision: "1.37.0" },
        },
        operation: { name: "chat" },
        model: { provider: "anthropic", requested: "model-x", response: "model-x-20260815" },
        usage: {
          source: "provider",
          inputTokens: 118_420,
          outputTokens: 4281,
          cacheReadInputTokens: 90_210,
          cacheWriteInputTokens: 12,
          reasoningOutputTokens: 900,
        },
        outcome: { status: "ok", errorType: "timeout" },
        response: { finishReasons: ["stop", "length"] },
        context: {
          renderBytes: 483_921,
          compacted: false,
          contextWindowTokens: 200_000,
          contextWindowSource: "model-catalog",
          effectiveInputLimitTokens: 180_000,
          effectiveInputLimitSource: "harness-config",
        },
        agent: { id: "a1", name: "coder", version: "3" },
        conversation: { externalId: "conv_42" },
        attempts: [
          { index: 1, status: "error", errorType: "timeout" },
          { index: 2, status: "ok" },
        ],
      }),
    ),
  );

  it.effect("round-trips every field the other kinds can carry", () =>
    Effect.promise(async () => {
      await roundTrip({
        type: Records.TOOL,
        ...envelope,
        invocation: OID,
        capture: { transport: "hook", stage: "hook" },
        tool: { name: "read_file", callId: "call_7", kind: "function", description: "reads" },
        outcome: { status: "error", errorType: "not_found" },
        result: { bytes: 4096, digest: "sha256:beef", truncated: true },
        mutation: { paths: 2, beforeTree: OID, afterTree: OID },
      });
      await roundTrip({
        type: Records.WORKSPACE,
        ...envelope,
        beforeTree: OID,
        afterTree: OID,
        operation: OID,
      });
      await roundTrip({
        type: Records.COMPACTION,
        ...envelope,
        evidence: "observed",
        strategy: "drop-oldest",
        reason: "input pressure",
        beforeTokens: 180_000,
        afterTokens: 40_000,
      });
      await roundTrip({
        type: Records.HEALTH,
        ...envelope,
        source: "otel",
        stage: "local-collector",
        sampling: "none",
        transformed: false,
        dropped: 0,
        reasons: ["export queue overflow"],
      });
    }),
  );

  it.effect("omits what the runtime did not report rather than inventing zeros", () =>
    Effect.promise(async () => {
      const decoded = await roundTrip({
        type: Records.INVOCATION,
        ...envelope,
        exposure: null,
        capture: null,
      });
      // §8: a provider that reported no token count and one that reported zero
      // are different facts, and only absence can say the first.
      assert.equal(decoded.type === Records.INVOCATION ? decoded.usage : "wrong", undefined);
      const document = JSON.parse(new TextDecoder().decode(Records.encode(decoded)));
      assert.equal("usage" in document, false);
      assert.equal("model" in document, false);
      assert.equal("attempts" in document, false);
    }),
  );

  it.effect("refuses an estimate that does not name its estimator", () =>
    Effect.promise(async () => {
      // §7.2: estimated usage must be labeled estimated and should carry an
      // estimator identifier — an unattributed estimate is indistinguishable
      // from a measurement once it is in a table.
      const refused = await scenario(
        Effect.result(
          Records.check({
            type: Records.INVOCATION,
            ...envelope,
            exposure: null,
            capture: null,
            usage: { source: "estimated", inputTokens: 100 },
          }),
        ),
      );
      assert.equal(refused._tag, "Failure");

      const accepted = await scenario(
        Effect.result(
          Records.check({
            type: Records.INVOCATION,
            ...envelope,
            exposure: null,
            capture: null,
            usage: { source: "estimated", estimator: "tiktoken/o200k", inputTokens: 100 },
          }),
        ),
      );
      assert.equal(accepted._tag, "Success");
    }),
  );

  it.effect("refuses a join by anything that is not a record oid", () =>
    Effect.promise(async () => {
      // §3: canonical cross-record references are qualified Git commit OIDs.
      // A trace id here would make the join something a provider can rename.
      const refused = await scenario(
        Effect.result(
          Records.check({
            type: Records.INVOCATION,
            ...envelope,
            exposure: "4bf92f3577b34da6a3ce929d0e0e4736",
            capture: null,
          }),
        ),
      );
      assert.equal(refused._tag, "Failure");
    }),
  );

  it.effect("appends to the trace ref, chaining onto what is there", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const key = yield* generate("runner@example.com");

          const base = yield* Records.context(REPO, SESSION);
          const first = yield* Records.record(
            { type: Records.INVOCATION, ...base, exposure: null, capture: null },
            key,
          );
          const second = yield* Records.record(
            {
              ...(yield* Records.context(REPO, SESSION)),
              type: Records.HEALTH,
              source: "otel",
              sampling: "none",
              transformed: false,
              dropped: 0,
            },
            key,
          );

          assert.equal(first.oid, qualify(first.commit));
          assert.equal(yield* repository.resolve(Trace.refOf(SESSION)), second.commit);

          const walked = yield* Records.entries(SESSION);
          assert.deepEqual(
            walked.records.map((entry) => entry.payload.type),
            [Records.INVOCATION, Records.HEALTH],
          );
          assert.deepEqual(walked.unreadable, []);
        }),
      ),
    ),
  );
});
