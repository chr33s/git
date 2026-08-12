/**
 * Error model.
 *
 * Schema-backed tagged errors. Every failure is in the `E` channel of
 * the effect that can produce it, so the compiler — not a code review — decides
 * whether a handler is exhaustive. Because they are `Schema` classes they also
 * serialize over the wire unchanged, which is what lets the JSON API and the
 * browser client share one error union.
 *
 * The status code is a schema annotation (`httpApiStatus`, the same one
 * `HttpApiError` uses) rather than a field: `HttpApi` reads it when it encodes
 * the failure, so `worker.ts`'s catch block and `server.api.ts`'s per-handler
 * mapping both become nothing at all.
 */
import { Schema } from "effect";

export class ObjectNotFound extends Schema.TaggedError<ObjectNotFound>()(
  "ObjectNotFound",
  { oid: Schema.String },
  { httpApiStatus: 404 },
) {}

export class RefConflict extends Schema.TaggedError<RefConflict>()(
  "RefConflict",
  {
    ref: Schema.String,
    expected: Schema.NullOr(Schema.String),
    actual: Schema.NullOr(Schema.String),
  },
  { httpApiStatus: 409 },
) {}

export class PackCorrupt extends Schema.TaggedError<PackCorrupt>()(
  "PackCorrupt",
  {
    reason: Schema.String,
    offset: Schema.optional(Schema.Number),
  },
  { httpApiStatus: 422 },
) {}

export class StorageFailure extends Schema.TaggedError<StorageFailure>()(
  "StorageFailure",
  {
    operation: Schema.String,
    path: Schema.String,
    cause: Schema.optional(Schema.Unknown),
  },
  { httpApiStatus: 500 },
) {}

export class Invalid extends Schema.TaggedError<Invalid>()(
  "Invalid",
  {
    field: Schema.String,
    reason: Schema.String,
  },
  { httpApiStatus: 400 },
) {}

/** Hooks reject a push; carried as a value, not an exception. */
export class HookRejected extends Schema.TaggedError<HookRejected>()(
  "HookRejected",
  {
    hook: Schema.Literals(["pre-receive", "update", "post-receive"]),
    ref: Schema.optional(Schema.String),
    message: Schema.String,
  },
  { httpApiStatus: 403 },
) {}

export type GitError =
  | ObjectNotFound
  | RefConflict
  | PackCorrupt
  | StorageFailure
  | Invalid
  | HookRejected;

export const GitError = Schema.Union([
  ObjectNotFound,
  RefConflict,
  PackCorrupt,
  StorageFailure,
  Invalid,
  HookRejected,
]);
