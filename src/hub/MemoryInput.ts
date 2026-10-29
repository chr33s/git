/** Inspectable derivation inputs. Ref enumeration is explicit, not an atomic snapshot. */
import { Predicate, Schema } from "effect";
import { View } from "../context/Pack.ts";
import { sha256 } from "../context/Render.ts";
import { compareUtf8 } from "../text.ts";

export const Head = Schema.Struct({ ref: Schema.String, head: Schema.String });
export const InputManifest = Schema.Struct({
  version: Schema.Literal(1),
  repository: Schema.NullOr(Schema.String),
  view: Schema.NullOr(View),
  sourceRef: Schema.NullOr(Schema.String),
  sourceHead: Schema.NullOr(Schema.String),
  sessionHeads: Schema.Array(Head),
  noteHeads: Schema.Array(Head),
  outcomeHeads: Schema.Array(Head),
  proposalHeads: Schema.Array(Head),
  genesis: Schema.NullOr(Schema.String),
  trustHead: Schema.NullOr(Schema.String),
  policyHead: Schema.NullOr(Schema.String),
  evaluatedAt: Schema.String,
  versions: Schema.Struct({
    parser: Schema.String,
    checker: Schema.String,
    projection: Schema.String,
    selection: Schema.String,
  }),
  configurationDigest: Schema.String,
  limits: Schema.Struct({ sessions: Schema.Finite, concepts: Schema.Finite, bytes: Schema.Finite }),
  expectedMemoryHead: Schema.NullOr(Schema.String),
  coverage: Schema.Struct({
    complete: Schema.Boolean,
    omittedSessions: Schema.Finite,
    continuation: Schema.Array(Head),
  }),
});
export interface InputManifest extends Schema.Schema.Type<typeof InputManifest> {}

/** JSON keys use byte order, including nested objects; array order is significant. */
export const canonical = (value: Schema.Json): string => {
  if (
    Predicate.isNull(value) ||
    Predicate.isString(value) ||
    Predicate.isNumber(value) ||
    Predicate.isBoolean(value)
  )
    return JSON.stringify(value);
  if (Array.isArray(value))
    return `[${value.map((item: Schema.Json) => canonical(item)).join(",")}]`;
  return `{${Object.entries<Schema.Json>(value)
    .sort(([left], [right]) => compareUtf8(left, right))
    .map(([key, item]) => `${JSON.stringify(key)}:${canonical(item)}`)
    .join(",")}}`;
};
export const digest = (value: Schema.Json) => sha256(new TextEncoder().encode(canonical(value)));
