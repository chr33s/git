/** A local audit of startup-memory bytes, plus invalidation at subsequent prompt boundaries. */
import { Effect, Schema } from "effect";
import * as Memory from "../hub/Memory.ts";
import * as Input from "../hub/MemoryInput.ts";
import * as Render from "../context/Render.ts";
import * as Journal from "../knowledge/Journal.node.ts";
import { compareUtf8 } from "../text.ts";

const Exposure = Schema.Struct({
  manifest: Input.InputManifest,
  manifestDigest: Schema.String,
  projectionFingerprint: Schema.String,
  renderDigest: Schema.String,
  records: Schema.Array(Schema.String),
  concepts: Schema.Array(Schema.String),
});
const Artifact = Schema.Struct({
  version: Schema.Literal(2),
  exposure: Schema.NullOr(Exposure),
  session: Schema.String,
  scope: Schema.Literal("startup-memory-only"),
  delivered: Schema.Boolean,
  manifest: Input.InputManifest,
  manifestDigest: Schema.String,
  projectionFingerprint: Schema.String,
  renderDigest: Schema.String,
  text: Schema.String,
  records: Schema.Array(Schema.String),
  concepts: Schema.Array(Schema.String),
});

/** Old bytes are never used as the next briefing, even when the artifact matches. */
export const record = Effect.fn("cli.Briefing.record")(function* (input: {
  readonly location: string;
  readonly session: string;
  readonly built: Memory.Derived;
  readonly delivered: boolean;
}) {
  const previous = yield* Journal.read(input.location, Artifact);
  const selectedRecords = [...new Set(input.built.entries.flatMap((entry) => entry.records))].sort(
    compareUtf8,
  );
  const selectedConcepts = input.built.entries
    .flatMap((entry) =>
      entry.source === undefined ? [] : [`${entry.source.path} ${entry.source.blob}`],
    )
    .sort(compareUtf8);
  const same = previous !== null && previous.session === input.session;
  const availableRecords = new Set(input.built.support.records);
  const availableConcepts = new Set(input.built.support.concepts);
  const invalidated =
    same &&
    (previous.records.some((record) => !availableRecords.has(record)) ||
      previous.concepts.some((concept) => !availableConcepts.has(concept)));
  const records = [
    ...new Set([
      ...(same ? previous.records.filter((record) => availableRecords.has(record)) : []),
      ...(input.delivered ? selectedRecords : []),
    ]),
  ].sort(compareUtf8);
  const concepts = [
    ...new Set([
      ...(same ? previous.concepts.filter((concept) => availableConcepts.has(concept)) : []),
      ...(input.delivered ? selectedConcepts : []),
    ]),
  ].sort(compareUtf8);
  const render = yield* Render.commit([
    {
      placement: "claude-code/user-prompt-submit",
      mediaType: "text/markdown; charset=utf-8",
      body: new TextEncoder().encode(input.built.text),
    },
  ]);
  yield* Journal.write(input.location, {
    version: 2,
    exposure:
      same && previous.exposure !== null
        ? previous.exposure
        : input.delivered
          ? {
              manifest: input.built.manifest,
              manifestDigest: input.built.manifestDigest,
              projectionFingerprint: input.built.projectionFingerprint,
              renderDigest: render.digest,
              records: selectedRecords,
              concepts: selectedConcepts,
            }
          : null,
    session: input.session,
    scope: "startup-memory-only",
    delivered: input.delivered,
    manifest: input.built.manifest,
    manifestDigest: input.built.manifestDigest,
    projectionFingerprint: input.built.projectionFingerprint,
    renderDigest: render.digest,
    text: input.built.text,
    records,
    concepts,
  });
  return { invalidated };
});
