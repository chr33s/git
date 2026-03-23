export class GitError extends Error {
  code: string;

  constructor(message: string, code: string, options?: ErrorOptions) {
    super(message, options);
    this.name = "GitError";
    this.code = code;
  }
}

export class ObjectNotFoundError extends GitError {
  oid: string;

  constructor(oid: string, options?: ErrorOptions) {
    super(`Object ${oid} not found`, "object_not_found", options);
    this.name = "ObjectNotFoundError";
    this.oid = oid;
  }
}

export class RefConflictError extends GitError {
  ref: string;

  constructor(ref: string, message?: string, options?: ErrorOptions) {
    super(message || `Ref conflict: ${ref}`, "ref_conflict", options);
    this.name = "RefConflictError";
    this.ref = ref;
  }
}

export class PackCorruptError extends GitError {
  constructor(message: string, options?: ErrorOptions) {
    super(message, "pack_corrupt", options);
    this.name = "PackCorruptError";
  }
}

export class StorageError extends GitError {
  constructor(message: string, options?: ErrorOptions) {
    super(message, "storage_error", options);
    this.name = "StorageError";
  }
}

export class ValidationError extends GitError {
  constructor(message: string, options?: ErrorOptions) {
    super(message, "validation_error", options);
    this.name = "ValidationError";
  }
}
