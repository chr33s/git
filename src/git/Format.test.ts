import assert from "node:assert/strict";
import { describe, it } from "@effect/vitest";

import { Effect, Result } from "effect";

import {
  bytesToHex,
  decodeObject,
  EMPTY_TREE_OID,
  encodeCommit,
  encodeObject,
  encodeTree,
  hashObject,
  hexToBytes,
  parseCommit,
  parseTree,
  type Signature,
  type TreeEntry,
} from "./Format.ts";
import type { Oid } from "./Store.ts";

const alice: Signature = {
  name: "Alice",
  email: "alice@example.com",
  at: new Date(1_700_000_000_000),
  offset: 60,
};

/**
 * Narrowing helpers that throw rather than leaning on `assert.ok`'s `asserts`
 * signature — the repo typechecks with oxlint, not tsc, and a test should not
 * depend on which one is looking at it.
 */
const expectSuccess = <A, E>(result: Result.Result<A, E>): A => {
  if (Result.isFailure(result)) {
    throw new Error(`expected success, got failure: ${JSON.stringify(result.failure)}`);
  }
  return result.success;
};

const expectFailure = <A, E>(result: Result.Result<A, E>): E => {
  if (Result.isSuccess(result)) {
    throw new Error(`expected failure, got success: ${JSON.stringify(result.success)}`);
  }
  return result.failure;
};

const oid = (hex: string) => hex as Oid;
const blob = oid("0".repeat(39) + "1");
const tree = oid("0".repeat(39) + "2");

describe("Format", () => {
  it("hashes the empty tree to git's well-known oid", async () => {
    const hashed = await Effect.runPromise(hashObject({ type: "tree", data: new Uint8Array() }));
    assert.equal(hashed, EMPTY_TREE_OID);
  });

  it("hashes a blob the way git does", async () => {
    // `printf 'hello\n' | git hash-object --stdin`
    const hashed = await Effect.runPromise(
      hashObject({ type: "blob", data: new TextEncoder().encode("hello\n") }),
    );
    assert.equal(hashed, "ce013625030ba8dba906f756967f9e9ca394464a");
  });

  it("round-trips object framing", () => {
    const framed = encodeObject({ type: "blob", data: new TextEncoder().encode("hi") });
    assert.equal(new TextDecoder().decode(framed), "blob 2\0hi");

    const decoded = expectSuccess(decodeObject(framed));
    assert.equal(decoded.type, "blob");
    assert.equal(new TextDecoder().decode(decoded.data), "hi");
  });

  it("rejects framing whose length disagrees with the body", () => {
    const bad = new TextEncoder().encode("blob 99\0hi");
    assert.equal(expectFailure(decodeObject(bad))._tag, "Invalid");
  });

  it("round-trips a commit, timezone included", () => {
    const encoded = encodeCommit({
      tree,
      parents: [blob],
      author: alice,
      committer: alice,
      message: "first\n\nbody",
    });

    const parsed = expectSuccess(parseCommit(encoded));
    assert.deepEqual(parsed.parents, [blob]);
    assert.equal(parsed.tree, tree);
    assert.equal(parsed.message, "first\n\nbody");
    assert.equal(parsed.author.name, "Alice");
    assert.equal(parsed.author.email, "alice@example.com");
    assert.equal(parsed.author.offset, 60);
    assert.equal(parsed.author.at.getTime(), alice.at.getTime());
  });

  it("parses a negative timezone offset", () => {
    const west: Signature = { ...alice, offset: -450 };
    const parsed = expectSuccess(
      parseCommit(encodeCommit({ tree, parents: [], author: west, committer: west, message: "x" })),
    );
    assert.equal(parsed.author.offset, -450);
  });

  it("fails a commit with no tree instead of defaulting", () => {
    assert.equal(
      expectFailure(parseCommit(new TextEncoder().encode("parent abc\n\nmessage"))).field,
      "tree",
    );
  });

  it("sorts tree entries the way git does, directories as 'name/'", () => {
    const entries: TreeEntry[] = [
      { mode: "100644", name: "b.txt", oid: blob },
      { mode: "40000", name: "a", oid: tree },
      { mode: "100644", name: "a.txt", oid: blob },
    ];

    const parsed = expectSuccess(parseTree(encodeTree(entries)));
    assert.deepEqual(
      parsed.map((entry) => entry.name),
      ["a.txt", "a", "b.txt"],
    );
  });

  it("round-trips tree entry oids through binary", () => {
    const parsed = expectSuccess(parseTree(encodeTree([{ mode: "100644", name: "f", oid: blob }])));
    assert.equal(parsed[0]?.oid, blob);
  });

  it("fails a truncated tree", () => {
    const truncated = encodeTree([{ mode: "100644", name: "f", oid: blob }]).slice(0, 12);
    assert.equal(expectFailure(parseTree(truncated))._tag, "Invalid");
  });

  it("round-trips hex", () => {
    assert.equal(bytesToHex(hexToBytes(blob)), blob);
  });
});
