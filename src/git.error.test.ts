import * as assert from "node:assert/strict";
import { describe, it } from "node:test";

import {
  GitError,
  ObjectNotFoundError,
  PackCorruptError,
  RefConflictError,
  StorageError,
  ValidationError,
} from "./git.error.ts";
import { GitObjectStore } from "./git.object.ts";
import { GitRefStore } from "./git.ref.ts";
import { MemoryStorage } from "./git.storage.ts";

void describe("GitError hierarchy", () => {
  void it("GitError has name, code, and message", () => {
    const error = new GitError("something broke", "test_code");
    assert.equal(error.name, "GitError");
    assert.equal(error.code, "test_code");
    assert.equal(error.message, "something broke");
    assert.ok(error instanceof Error);
    assert.ok(error instanceof GitError);
  });

  void it("GitError supports cause option", () => {
    const cause = new Error("root cause");
    const error = new GitError("wrapper", "test_code", { cause });
    assert.equal(error.cause, cause);
  });

  void it("ObjectNotFoundError has oid and correct code", () => {
    const oid = "a".repeat(40);
    const error = new ObjectNotFoundError(oid);
    assert.equal(error.name, "ObjectNotFoundError");
    assert.equal(error.code, "object_not_found");
    assert.equal(error.oid, oid);
    assert.equal(error.message, `Object ${oid} not found`);
    assert.ok(error instanceof GitError);
    assert.ok(error instanceof ObjectNotFoundError);
  });

  void it("RefConflictError has ref and correct code", () => {
    const error = new RefConflictError("refs/heads/main", "non-fast-forward");
    assert.equal(error.name, "RefConflictError");
    assert.equal(error.code, "ref_conflict");
    assert.equal(error.ref, "refs/heads/main");
    assert.equal(error.message, "non-fast-forward");
    assert.ok(error instanceof GitError);
  });

  void it("RefConflictError uses default message when none provided", () => {
    const error = new RefConflictError("refs/heads/main");
    assert.equal(error.message, "Ref conflict: refs/heads/main");
  });

  void it("PackCorruptError has correct code", () => {
    const error = new PackCorruptError("checksum mismatch");
    assert.equal(error.name, "PackCorruptError");
    assert.equal(error.code, "pack_corrupt");
    assert.equal(error.message, "checksum mismatch");
    assert.ok(error instanceof GitError);
  });

  void it("StorageError has correct code", () => {
    const error = new StorageError("File not found: .git/HEAD");
    assert.equal(error.name, "StorageError");
    assert.equal(error.code, "storage_error");
    assert.ok(error instanceof GitError);
  });

  void it("ValidationError has correct code", () => {
    const error = new ValidationError("Invalid ref name");
    assert.equal(error.name, "ValidationError");
    assert.equal(error.code, "validation_error");
    assert.ok(error instanceof GitError);
  });

  void it("all error types are catchable as GitError", () => {
    const errors = [
      new ObjectNotFoundError("a".repeat(40)),
      new RefConflictError("refs/heads/main"),
      new PackCorruptError("bad data"),
      new StorageError("disk full"),
      new ValidationError("bad input"),
    ];

    for (const error of errors) {
      assert.ok(error instanceof GitError, `${error.name} should be instanceof GitError`);
      assert.ok(typeof error.code === "string", `${error.name} should have a code`);
    }
  });
});

void describe("Error propagation", () => {
  void it("ObjectNotFoundError is thrown for missing objects", async () => {
    const storage = new MemoryStorage();
    await storage.init("test-repo");
    const objectStore = new GitObjectStore(storage);
    await objectStore.init();

    await assert.rejects(
      () => objectStore.readObject("a".repeat(40)),
      (error: Error) => {
        assert.ok(error instanceof ObjectNotFoundError);
        assert.equal(error.code, "object_not_found");
        return true;
      },
    );
  });

  void it("ValidationError is thrown for invalid OID", async () => {
    const storage = new MemoryStorage();
    await storage.init("test-repo");
    const objectStore = new GitObjectStore(storage);
    await objectStore.init();

    await assert.rejects(
      () => objectStore.readObject("not-a-valid-oid"),
      (error: Error) => {
        assert.ok(error instanceof ValidationError);
        assert.equal(error.code, "validation_error");
        return true;
      },
    );
  });

  void it("ValidationError is thrown for invalid ref names", async () => {
    const storage = new MemoryStorage();
    await storage.init("test-repo");
    const refStore = new GitRefStore(storage);
    await refStore.init();

    // writeRef wraps validation errors as RefConflictError
    await assert.rejects(
      () => refStore.writeRef("refs/heads/bad..name", "a".repeat(40)),
      (error: Error) => {
        assert.ok(error instanceof RefConflictError);
        assert.match(error.message, /cannot contain '\.\.'/);
        return true;
      },
    );
  });

  void it("ValidationError is thrown for invalid OID on ref write", async () => {
    const storage = new MemoryStorage();
    await storage.init("test-repo");
    const refStore = new GitRefStore(storage);
    await refStore.init();

    await assert.rejects(
      () => refStore.writeRef("refs/heads/main", "not-valid"),
      (error: Error) => {
        assert.ok(error instanceof ValidationError);
        return true;
      },
    );
  });

  void it("StorageError is thrown for missing files", async () => {
    const storage = new MemoryStorage();
    await storage.init("test-repo");

    await assert.rejects(
      () => storage.readFile(".git/nonexistent"),
      (error: Error) => {
        assert.ok(error instanceof StorageError);
        assert.equal(error.code, "storage_error");
        return true;
      },
    );
  });

  void it("ValidationError is thrown for path traversal", async () => {
    const storage = new MemoryStorage();
    await storage.init("test-repo");

    await assert.rejects(
      () => storage.readFile("../etc/passwd"),
      (error: Error) => {
        assert.ok(error instanceof ValidationError);
        return true;
      },
    );
  });
});
