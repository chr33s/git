/**
 * What a rejected promise carries, read back after Effect has wrapped it.
 *
 * `Effect.tryPromise` does not hand the thrown value through: it wraps it in
 * an `UnknownError` whose own `message` is the literal "An error occurred in
 * Effect.tryPromise" and whose original is on `cause`. A `catch` in the Effect
 * therefore sees a wrapper, not an `ApiError` — which is why anything that
 * needs the status or the server's own sentence unwraps here first.
 *
 * The unwrap walks `cause` rather than stopping at the first object that
 * happens to parse: every field below is optional, so the wrapper itself
 * matches the shape and would otherwise be answered with its own placeholder
 * message. A frame only counts when it carries something worth reading.
 */
import { Schema } from "effect";

const Fields = Schema.Struct({
  status: Schema.optional(Schema.Finite),
  message: Schema.optional(Schema.String),
  tag: Schema.optional(Schema.String),
  unreachable: Schema.optional(Schema.Boolean),
});
export type Thrown = typeof Fields.Type;

const parse = Schema.decodeUnknownOption(Fields);

/** The placeholder Effect puts on its own wrapper; never worth showing. */
const WRAPPER = "An error occurred in Effect.tryPromise";

const useful = (held: Thrown): boolean =>
  held.status !== undefined ||
  held.tag !== undefined ||
  held.unreachable !== undefined ||
  (held.message !== undefined && held.message !== "" && held.message !== WRAPPER);

/** One frame of the chain, as a value: what it says, and what wrapped it. */
const Frame = Schema.Struct({ cause: Schema.optional(Schema.Unknown) });
const parseFrame = Schema.decodeUnknownOption(Frame);

/** The innermost frame that says something, or nothing at all. */
export const thrownOf = (cause: unknown): Thrown => {
  let at: unknown = cause;
  // Bounded: a cause chain is short, and a cycle would otherwise spin here.
  for (let depth = 0; depth < 8; depth++) {
    const held = parse(at);
    if (held._tag === "Some" && useful(held.value)) return held.value;
    const frame = parseFrame(at);
    if (frame._tag === "None" || frame.value.cause === undefined) return {};
    at = frame.value.cause;
  }
  return {};
};

/** Whether the repository turned this reader away, rather than being absent. */
export const refused = (cause: unknown): boolean => {
  const status = thrownOf(cause).status;
  return status === 401 || status === 403;
};

/** Whether the repository simply does not have what was asked for. */
export const absent = (cause: unknown): boolean => {
  const held = thrownOf(cause);
  return held.unreachable === true || held.status === 404 || held.tag === "ObjectNotFound";
};

/** What went wrong, in the words the reader is shown. */
export const reasonOf = (cause: unknown, fallback = "the git+ API is not reachable"): string => {
  const message = thrownOf(cause).message;
  return message === undefined || message === "" ? fallback : message;
};
