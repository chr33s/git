/**
 * Typed failures.
 *
 * Schema-backed, so the same class is the server's failure, its wire
 * representation, and the type a client matches on. The HTTP status lives as an
 * `httpApiStatus` annotation — the same one `HttpApiError` uses — so response
 * mapping is not a hand-written switch.
 *
 * See the readme's "Errors" convention.
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
    offset: Schema.optional(Schema.Finite),
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

/** A generated bundle failed verification before it could be advertised. */
export class BundleCorrupt extends Schema.TaggedError<BundleCorrupt>()(
  "BundleCorrupt",
  { reason: Schema.String },
  { httpApiStatus: 422 },
) {}

export class OperationNotFound extends Schema.TaggedError<OperationNotFound>()(
  "OperationNotFound",
  { id: Schema.String },
  { httpApiStatus: 404 },
) {}

/** A hook rejected a push; carried as a value, not thrown. */
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
  | HookRejected
  | BundleCorrupt
  | OperationNotFound;

export const GitError = Schema.Union([
  ObjectNotFound,
  RefConflict,
  PackCorrupt,
  StorageFailure,
  Invalid,
  HookRejected,
  BundleCorrupt,
  OperationNotFound,
]);

/** Status for a failure, for the paths that are not `HttpApi` (smart-HTTP). */
export const statusOf = (error: GitError): number => {
  switch (error._tag) {
    case "Invalid":
      return 400;
    case "HookRejected":
      return 403;
    case "ObjectNotFound":
    case "OperationNotFound":
      return 404;
    case "RefConflict":
      return 409;
    case "PackCorrupt":
    case "BundleCorrupt":
      return 422;
    case "StorageFailure":
      return 500;
  }
};
