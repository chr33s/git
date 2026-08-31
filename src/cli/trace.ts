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
import { qualify } from "../git/Oid.ts";
import * as Redaction from "../hub/Redaction.ts";
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
    invocation: Flag.string("invocation").pipe(
      Flag.withDefault(""),
      Flag.withDescription("The invocation a tool span belongs beneath, by record OID"),
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
  ({ event, exposure, invocation, key, otel, repo, revision, root, session, stage }) =>
    Effect.gen(function* () {
      // Refused rather than dropped. These four say how to *normalize* a span,
      // so on the `--event` path — where the caller has already normalized —
      // there is nothing for them to do, and silently ignoring a flag that
      // names the record's join is how a record ends up joined to nothing.
      const spanOnly = { exposure, invocation, stage, "semconv-revision": revision };
      const ignored = Object.entries(spanOnly)
        .filter(([, value]) => value !== "")
        .map(([name]) => `--${name}`);
      if (!otel && ignored.length > 0) {
        return yield* new Invalid({
          field: "event",
          reason: `${ignored.join(", ")} ${ignored.length === 1 ? "describes" : "describe"} how to read a span; pass --otel, or put the field in the event itself`,
        });
      }

      // Held to the vocabulary `Records.STAGES` declares. `Capture.stage` is a
      // bare string so an older reader can still read a stage it does not
      // know, which also means a typo lands in a signed, immutable record on
      // an append-only ref and every later reader sees a stage that is not one.
      if (stage !== "" && !Records.STAGES.some((known) => known === stage)) {
        return yield* new Invalid({
          field: "stage",
          reason: `'${stage}' is not a capture stage; one of ${Records.STAGES.join(", ")}`,
        });
      }

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
          // The other half of the "refused rather than dropped" rule. Each of
          // these two applies to exactly one span kind: `--exposure` to an
          // inference span, `--invocation` to a tool span. Passed with the
          // wrong kind they were read by nothing, and the record was written
          // joined to nothing — the same failure, on the other path.
          const operation = Semconv.operationOf(span);
          // One normalization, read twice. Called again below with the real
          // options, the check here ran against a throwaway `Capture` built
          // with no stage and no revision — two results for one span, free to
          // disagree the day the options reach the classification.
          const normalizedSpan = Semconv.normalize(span, {
            stage: stage === "" ? undefined : stage,
            revision: revision === "" ? undefined : revision,
            exposure: exposure === "" ? null : exposure,
            invocation: invocation === "" ? null : invocation,
          });
          const kind = normalizedSpan.kind;
          // `inference` and `unsupported` both take `an`, and both were
          // reachable here.
          const named = `${/^[aeiou]/.test(kind) ? "an" : "a"} ${kind} span`;
          if (exposure !== "" && kind !== "inference") {
            return yield* new Invalid({
              field: "exposure",
              reason: `--exposure names the context an inference made use of; '${operation ?? "this span"}' is ${named}`,
            });
          }
          if (invocation !== "" && kind !== "tool") {
            return yield* new Invalid({
              field: "invocation",
              reason: `--invocation names the call a tool ran under; '${operation ?? "this span"}' is ${named}`,
            });
          }

          // The envelope last, exactly as the `--event` path spreads it. No
          // normalizer field is named `repo` or `session` today, so the two
          // orders agree — but they are two orders for one security-relevant
          // binding, and the day a field is added the wrong one wins silently.
          const base = yield* Records.context(identity, session);
          if (normalizedSpan.kind === "inference") {
            return yield* Records.record(
              { type: Records.INVOCATION, ...normalizedSpan.fields, ...base },
              signer,
            );
          }
          if (normalizedSpan.kind === "tool") {
            return yield* Records.record(
              { type: Records.TOOL, ...normalizedSpan.fields, ...base },
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

/**
 * `git+ trace redact` — the way back out of a leaked prompt.
 *
 * The trace is where a retained render holds the task string verbatim and the
 * exact bytes of every exposed file, so `session redact` alone left the text
 * it was asked to remove readable one ref over. Nothing is deleted here: the
 * tombstone is what replicates, and the bytes go at the next `gc`.
 */
const redact = Command.make(
  "redact",
  {
    root: rootFlag,
    repo: repoFlag,
    key: Flag.string("key").pipe(
      Flag.withDescription("Path to the SSH private key the tombstone is signed with"),
    ),
    session: Flag.string("session").pipe(Flag.withDescription("The session holding the record")),
    target: Flag.string("target").pipe(Flag.withDescription("The record's own id")),
    reason: Flag.string("reason").pipe(Flag.withDescription("Why it is being removed")),
  },
  ({ key, reason, repo, root, session, target }) =>
    Effect.gen(function* () {
      const signer = yield* readPrivateKey(key);
      const done = yield* withDiscovered(
        root,
        repo,
        Effect.gen(function* () {
          const written = yield* Records.redact({
            repo: yield* identityOf(),
            session,
            target,
            reason,
            key: signer,
          });
          // Asked after the tombstone is on the ref, because that is what
          // decides the answer. A Pack and a ContextRender are deterministic,
          // so a second exposure of the same view and task names the same
          // objects — and `gc` will not delete an object a live record still
          // needs. The removal is then partial, and it used to say nothing:
          // this printed an oid, `gc` reported a count, and the verbatim task
          // string and every exposed file byte stayed readable at
          // `<record>^{tree}:context/render.bin` and clonable off the ref.
          return { written, ...(yield* Redaction.withheld(written.targetCommit)) } as const;
        }),
      );
      yield* Console.log(done.written.oid);
      if (done.blobs.length > 0) {
        // Two reasons an object stays, and they need different answers. A live
        // record naming it is one, and redacting that record is what finishes
        // the job — so they are named, since an operator told "partly removed"
        // with no way to find the rest has been told the bad news and nothing
        // else. A ref this host cannot walk is the other: `excluded` withholds
        // the shared `context/` objects wholesale then, and there is no holder
        // to name, so the message said "held by 0 other live record(s)" and
        // stopped.
        if (done.holders.length === 0) {
          yield* Console.error(
            `! ${done.blobs.length} object(s) this record names will not be collected; a record ref this host cannot walk may name them`,
          );
        } else {
          yield* Console.error(
            `! ${done.blobs.length} object(s) this record names are still held by ${done.holders.length} other live record(s) and will not be collected`,
          );
          for (const holder of done.holders) yield* Console.error(`  ${qualify(holder)}`);
        }
      }
    }),
);

export const traceCommand = Command.make("trace", {}, () =>
  Console.log("Usage: git+ trace record --session <id> --key <key> --event <file>"),
).pipe(
  Command.withSubcommands([
    record.pipe(Command.withDescription("Sign one normalized trace record and append it")),
    redact.pipe(
      Command.withDescription("Remove one trace record's content, signed and replicated"),
    ),
  ]),
);
