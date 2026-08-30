/**
 * `git+ trace record` — the integration surface, not a human workflow.
 *
 * docs/telemetry.md §16: raw trace writing exists for harnesses without
 * suitable native OpenTelemetry. The product-level read path is `git+ session
 * show --audit`, and there is deliberately no `trace show` — a person asking
 * "what happened in this run?" wants the joined Invocation, not the two signed
 * records it was assembled from.
 *
 * Two input shapes, and the difference matters. `--event` takes an already
 * normalized Git+ record, which is what a hook writes; `--otel` takes a GenAI
 * span, which is what an exporter has, and runs it through `Semconv.ts` first.
 * Native OTel ingestion uses the same writer in-process rather than one shell
 * command per span — this command exists so that a harness which can only
 * shell out is not thereby excluded.
 */
import * as fs from "node:fs";

import { Console, Effect, Predicate } from "effect";
import { Command, Flag } from "effect/unstable/cli";

import { Invalid } from "../git/Error.ts";
import * as Records from "../telemetry/Records.ts";
import * as Semconv from "../telemetry/Semconv.ts";
import { readGenesis } from "../trust/Genesis.ts";
import { readPrivateKey, repoFlag, rootFlag, withDiscovered } from "./shared.ts";

/**
 * The repository's own identity, which every record is bound to.
 *
 * Refused rather than defaulted, for the reason `session.ts` refuses: a record
 * names the repository inside its signed bytes so it cannot be replayed into
 * another one, and a repository with no genesis has no identity to name.
 */
const identityOf = Effect.fn("trace.identityOf")(function* () {
  const stored = yield* readGenesis();
  if (stored === null) {
    return yield* new Invalid({
      field: "repo",
      reason: "this repository has no genesis; run `git+ hub init` first",
    });
  }
  return stored.genesis.repoId;
});

const readFile = Effect.fn("trace.readFile")(function* (location: string) {
  const contents = yield* Effect.try({
    try: () => fs.readFileSync(location),
    catch: () => new Invalid({ field: "event", reason: `cannot read ${location}` }),
  });
  return new Uint8Array(contents);
});

const decoder = new TextDecoder();

/**
 * A normalized payload from a file the caller wrote.
 *
 * The envelope is this command's to fill in, not the caller's: `repo`,
 * `session`, `id`, `issuedAt` and `trustHead` are what bind the record to this
 * repository, and a harness allowed to supply them could write a record
 * claiming to belong to a session — or a repository — it has nothing to do
 * with. So the file carries the *facts* and the recorder carries the binding,
 * which is exactly the division §16 describes.
 */
const normalized = Effect.fn("trace.normalized")(function* (input: {
  readonly bytes: Uint8Array;
  readonly repo: string;
  readonly session: string;
}) {
  const json: unknown = yield* Effect.try({
    try: () => JSON.parse(decoder.decode(input.bytes)),
    catch: () => new Invalid({ field: "event", reason: "the event is not valid JSON" }),
  });
  if (!Predicate.isReadonlyObject(json)) {
    return yield* new Invalid({ field: "event", reason: "the event must be a JSON object" });
  }

  const base = yield* Records.context(input.repo, input.session);
  // The caller's fields first, the envelope last, so nothing supplied can
  // overwrite the binding.
  return yield* Records.decode(new TextEncoder().encode(JSON.stringify({ ...json, ...base })));
});

const record = Command.make(
  "record",
  {
    root: rootFlag,
    repo: repoFlag,
    key: Flag.string("key").pipe(
      Flag.withDescription("Path to the SSH private key the record is signed with"),
    ),
    session: Flag.string("session").pipe(Flag.withDescription("The session this record traces")),
    event: Flag.string("event").pipe(Flag.withDescription("A file holding the normalized event")),
    otel: Flag.boolean("otel").pipe(
      Flag.withDefault(false),
      Flag.withDescription("The file holds a GenAI span to normalize rather than a Git+ event"),
    ),
    exposure: Flag.string("exposure").pipe(
      Flag.withDefault(""),
      Flag.withDescription("The Context Exposure this invocation was made against, by record OID"),
    ),
    stage: Flag.string("stage").pipe(
      Flag.withDefault(""),
      Flag.withDescription("Where the signal was captured: sdk-export, local-collector, hook, …"),
    ),
    revision: Flag.string("semconv-revision").pipe(
      Flag.withDefault(""),
      Flag.withDescription(
        "The upstream semconv revision this span declared, when it declared one",
      ),
    ),
  },
  ({ event, exposure, key, otel, repo, revision, root, session, stage }) =>
    Effect.gen(function* () {
      const signer = yield* readPrivateKey(key);
      const bytes = yield* readFile(event);

      const written = yield* withDiscovered(
        root,
        repo,
        Effect.gen(function* () {
          const identity = yield* identityOf();
          if (!otel) {
            const payload = yield* normalized({ bytes, repo: identity, session });
            return yield* Records.record(payload, signer);
          }

          const span = yield* Semconv.decode(bytes);
          const normalizedSpan = Semconv.normalize(span, {
            stage: stage === "" ? undefined : stage,
            revision: revision === "" ? undefined : revision,
            exposure: exposure === "" ? null : exposure,
          });

          const base = yield* Records.context(identity, session);
          if (normalizedSpan.kind === "inference") {
            return yield* Records.record(
              { type: Records.INVOCATION, ...base, ...normalizedSpan.fields },
              signer,
            );
          }
          if (normalizedSpan.kind === "tool") {
            return yield* Records.record(
              { type: Records.TOOL, ...base, ...normalizedSpan.fields },
              signer,
            );
          }
          // A retrieval span is selector diagnostics and never evidence that
          // anything crossed the invocation boundary (§7.6); an unsupported
          // one is somebody else's span. Neither becomes a record, and saying
          // so is more useful than writing an empty one.
          return yield* new Invalid({
            field: "event",
            reason:
              normalizedSpan.kind === "retrieval"
                ? `a ${normalizedSpan.diagnostics.operation} span is retrieval diagnostics; Context Exposure is what records repository context`
                : `'${normalizedSpan.operation ?? "unnamed"}' is not an operation this version records`,
          });
        }),
      );

      // The qualified OID alone: a later record references this one by it, and
      // a hook capturing stdout should not have to parse prose.
      yield* Console.log(written.oid);
    }),
);

export const traceCommand = Command.make("trace", {}, () =>
  Console.log("Usage: git+ trace record --session <id> --key <key> --event <file>"),
).pipe(
  Command.withSubcommands([
    record.pipe(Command.withDescription("Sign one normalized trace record and append it")),
  ]),
);
