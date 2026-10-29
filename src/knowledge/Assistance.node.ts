/** Bounded optional model calls, durable interpretation reuse, and explicit local publication. */
import * as fs from "node:fs/promises";
import * as path from "node:path";
import { Effect, Schema } from "effect";
import { Invalid, StorageFailure } from "../git/Error.ts";
import { parsePrivateKey } from "../crypto/SshSignature.ts";
import * as Input from "../hub/MemoryInput.ts";
import * as Journal from "./Journal.node.ts";
import * as Assistance from "./Assistance.ts";
import * as Proposal from "./Proposal.ts";
import * as Publication from "./Publication.ts";
import * as Publisher from "./Publisher.ts";
import * as Runner from "./Runner.node.ts";

const Checked = Schema.Struct({
  evidence: Schema.Struct({
    concepts: Schema.Array(
      Schema.Struct({
        path: Schema.String,
        blob: Schema.String,
        temporal: Schema.Json,
        citations: Schema.Array(Schema.Json),
        eligibility: Schema.Json,
      }),
    ),
  }),
});
const Cache = Schema.Struct({
  version: Schema.Literal(1),
  fingerprint: Schema.String,
  runner: Proposal.Label,
  output: Proposal.Output,
});

export const assess = Effect.fn("knowledge.Assistance.assess")(function* (input: {
  readonly directory: string;
  readonly evidence: Proposal.Evidence;
  readonly configuration: Runner.Configuration | null;
  readonly roots: ReadonlyArray<string>;
  readonly maximumBytes: number;
  readonly timeoutMs: number;
  readonly previous: Assistance.State;
  readonly checkpoint: (state: Assistance.State) => Effect.Effect<void, StorageFailure>;
}) {
  const configured = input.configuration;
  if (configured === null)
    return {
      version: 1,
      state: "not-configured",
      runner: null,
      fingerprint: null,
      output: null,
      diagnostic: null,
    } satisfies Assistance.State;
  const concepts = yield* Effect.forEach(input.evidence.concepts, (raw) =>
    Schema.decodeUnknownEffect(Checked)(raw).pipe(
      Effect.mapError(
        () => new Invalid({ field: "assistance", reason: "invalid deterministic check evidence" }),
      ),
    ),
  );
  const fingerprint = yield* Input.digest({
    runner: configured,
    source: input.evidence.manifest.view,
    trust: input.evidence.manifest.trustHead,
    policy: input.evidence.manifest.policyHead,
    concepts,
    outcomes: input.evidence.outcomes.map((outcome) => ({
      key: outcome.key,
      head: outcome.evidenceHead,
      eligible: outcome.eligible,
      accepted: outcome.acceptedHead,
      merge: outcome.mergeCommit,
      sources: outcome.sources,
      records: outcome.records,
      coverage: outcome.coverage,
    })),
    roots: input.roots,
  });
  const label = { id: configured.id, version: configured.version };
  const cachedPath = path.join(
    Journal.directory(input.directory),
    "models",
    `${fingerprint.slice(7)}.json`,
  );
  const cached = yield* Journal.read(cachedPath, Cache);
  const retained =
    input.previous.state === "complete" && input.previous.fingerprint === fingerprint
      ? input.previous.output
      : cached?.fingerprint === fingerprint
        ? cached.output
        : null;
  if (retained !== null) {
    yield* Proposal.validate({
      evidence: input.evidence,
      output: retained,
      roots: input.roots,
      maximumBytes: input.maximumBytes,
    });
    return {
      version: 1,
      state: "complete",
      runner: label,
      fingerprint,
      output: retained,
      diagnostic: null,
    } satisfies Assistance.State;
  }
  const pending: Assistance.State = {
    version: 1,
    state: "pending",
    runner: label,
    fingerprint,
    output: null,
    diagnostic: null,
  };
  yield* input.checkpoint(pending);
  const generated = yield* Runner.invoke({
    configuration: { ...configured, timeoutMs: Math.min(configured.timeoutMs, input.timeoutMs) },
    evidence: input.evidence,
    maximumBytes: input.maximumBytes,
  }).pipe(
    Effect.flatMap((output) =>
      Proposal.validate({
        evidence: input.evidence,
        output,
        roots: input.roots,
        maximumBytes: input.maximumBytes,
      }).pipe(Effect.as(output)),
    ),
    Effect.map((output): Assistance.State => ({ ...pending, state: "complete", output })),
    Effect.orElseSucceed(
      () =>
        ({
          ...pending,
          state: "partial",
          diagnostic:
            "configured assistance failed, timed out, or returned an invalid draft; deterministic evidence is retained",
        }) satisfies Assistance.State,
    ),
  );
  yield* input.checkpoint(generated);
  if (generated.output !== null)
    yield* Journal.write(cachedPath, {
      version: 1,
      fingerprint,
      runner: label,
      output: generated.output,
    });
  return generated;
});

export const propose = Effect.fn("knowledge.Assistance.propose")(function* (input: {
  readonly directory: string;
  readonly evidence: Proposal.Evidence;
  readonly output: Proposal.Output;
  readonly runner: Proposal.Label;
  readonly roots: ReadonlyArray<string>;
  readonly maximumBytes: number;
  readonly keyPath: string | null;
  readonly actors: ReadonlyArray<import("./Outcomes.ts").Actor>;
  readonly checkpoint: (candidate: string) => Effect.Effect<void, StorageFailure>;
}) {
  const prepared = yield* Publisher.prepare(input);
  if (prepared === null) return { state: "no-proposal" } as const;
  const keyPath = input.keyPath;
  if (keyPath === null)
    return yield* new Invalid({
      field: "publisher",
      reason: "--propose with draft changes requires an operator-configured publisher.key",
    });
  const location = path.join(
    Journal.directory(input.directory),
    "proposals",
    `${prepared.id}.json`,
  );
  const state = (yield* Journal.read(location, Publication.State)) ?? prepared;
  if (state.id !== prepared.id)
    return yield* new Invalid({
      field: "proposal",
      reason: "publication journal identity mismatch",
    });
  yield* Journal.write(location, state);
  yield* input.checkpoint(state.id);
  const pem = yield* Effect.tryPromise({
    try: () => fs.readFile(keyPath, "utf8"),
    catch: (cause) => new StorageFailure({ operation: "publisher.key", path: keyPath, cause }),
  });
  const key = yield* Effect.fromResult(parsePrivateKey(pem));
  return yield* Publisher.publish({
    state,
    key,
    actors: input.actors,
    roots: input.roots,
    maximumBytes: input.maximumBytes,
    checkpoint: (next) => Journal.write(location, next),
  });
});
