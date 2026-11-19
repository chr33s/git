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

export class ObjectNotFound extends Schema.TaggedError<
  ObjectNotFound,
  { readonly brand: unique symbol }
>()("ObjectNotFound", { oid: Schema.String }, { httpApiStatus: 404 }) {}

export class RefConflict extends Schema.TaggedError<
  RefConflict,
  { readonly brand: unique symbol }
>()(
  "RefConflict",
  {
    ref: Schema.String,
    expected: Schema.NullOr(Schema.String),
    actual: Schema.NullOr(Schema.String),
  },
  { httpApiStatus: 409 },
) {}

export class PackCorrupt extends Schema.TaggedError<
  PackCorrupt,
  { readonly brand: unique symbol }
>()(
  "PackCorrupt",
  {
    reason: Schema.String,
    offset: Schema.optional(Schema.Finite),
  },
  { httpApiStatus: 422 },
) {}

export class StorageFailure extends Schema.TaggedError<
  StorageFailure,
  { readonly brand: unique symbol }
>()(
  "StorageFailure",
  {
    operation: Schema.String,
    path: Schema.String,
    cause: Schema.optional(Schema.Unknown),
  },
  { httpApiStatus: 500 },
) {}

/** Headers may already be sent; response-stream expiry cannot replace them. */
export class ResponseTimeout extends Schema.TaggedError<
  ResponseTimeout,
  { readonly brand: unique symbol }
>()("ResponseTimeout", { kind: Schema.Literals(["idle", "absolute"]) }, { httpApiStatus: 504 }) {
  override get message() {
    return `response ${this.kind} deadline exceeded`;
  }
}

export class LfsQuotaExceeded extends Schema.TaggedError<
  LfsQuotaExceeded,
  { readonly brand: unique symbol }
>()(
  "LfsQuotaExceeded",
  { resource: Schema.Literals(["bytes", "objects"]) },
  { httpApiStatus: 507 },
) {}

export class LfsQuotaBusy extends Schema.TaggedError<
  LfsQuotaBusy,
  { readonly brand: unique symbol }
>()("LfsQuotaBusy", {}, { httpApiStatus: 503 }) {}

/** A source anchor could not be inspected by the selected resolver. */
export class AnchorResolutionFailure extends Schema.TaggedError<
  AnchorResolutionFailure,
  { readonly brand: unique symbol }
>()(
  "AnchorResolutionFailure",
  {
    operation: Schema.String,
    path: Schema.String,
    reason: Schema.String,
  },
  { httpApiStatus: 422 },
) {}

export class Invalid extends Schema.TaggedError<Invalid, { readonly brand: unique symbol }>()(
  "Invalid",
  {
    field: Schema.String,
    reason: Schema.String,
  },
  { httpApiStatus: 400 },
) {}

/** An aggregate request budget was exceeded before further work was admitted. */
export class RequestLimitExceeded extends Schema.TaggedError<
  RequestLimitExceeded,
  { readonly brand: unique symbol }
>()(
  "RequestLimitExceeded",
  { budget: Schema.String, limit: Schema.Finite },
  { httpApiStatus: 413 },
) {}

/** A hook rejected a push; carried as a value, not thrown. */
export class HookRejected extends Schema.TaggedError<
  HookRejected,
  { readonly brand: unique symbol }
>()(
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
  | AnchorResolutionFailure
  | Invalid
  | RequestLimitExceeded
  | HookRejected;

export const GitError = Schema.Union([
  ObjectNotFound,
  RefConflict,
  PackCorrupt,
  StorageFailure,
  AnchorResolutionFailure,
  Invalid,
  RequestLimitExceeded,
  HookRejected,
]);

/** Status for a failure, for the paths that are not `HttpApi` (smart-HTTP). */
export const statusOf = (error: GitError): number => {
  switch (error._tag) {
    case "Invalid":
      return 400;
    case "HookRejected":
      return 403;
    case "ObjectNotFound":
      return 404;
    case "RefConflict":
      return 409;
    case "RequestLimitExceeded":
      return 413;
    case "PackCorrupt":
      return 422;
    case "StorageFailure":
      return 500;
    case "AnchorResolutionFailure":
      return 422;
  }
};
