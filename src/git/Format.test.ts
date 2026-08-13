import assert from "node:assert/strict";
import { describe, it } from "@effect/vitest";

import { Effect, Result } from "effect";

import {
  bytesToHex,
  decodeObject,
  EMPTY_TREE_OID,
  encodeCommit,
  encodeObject,
  encodeReflogLine,
  encodeTree,
  hashObject,
  hexToBytes,
  parseCommit,
  parseReflogLine,
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

  it("sorts tree entries by their UTF-8 bytes, not by UTF-16 code unit", () => {
    // U+FFFF encodes to EF BF BF and U+1F600 to F0 9F 98 80, so git puts the
    // emoji last. In UTF-16 the emoji is a surrogate pair starting D83D, which
    // compares *below* FFFF — an order git reads as a corrupt tree.
    const entries: ReadonlyArray<TreeEntry> = [
      { mode: "100644", name: "\u{1F600}.txt", oid: blob },
      { mode: "100644", name: "￿.txt", oid: blob },
    ];

    const parsed = expectSuccess(parseTree(encodeTree(entries)));
    assert.deepEqual(
      parsed.map((entry) => entry.name),
      ["￿.txt", "\u{1F600}.txt"],
    );
  });

  it("writes back a tree entry name that is not valid UTF-8", () => {
    // git names are bytes: a repository can hold `café.txt` written in
    // Latin-1. Decoding replaces those bytes with U+FFFD, and re-encoding the
    // replacement would rename the file on the next commit that rewrites the
    // tree — silently, and unrecoverably from the new tree.
    const latin1 = Uint8Array.from([0x63, 0x61, 0x66, 0xe9, 0x2e, 0x74, 0x78, 0x74]);
    // Built by hand so the name really is those bytes.
    const prefix = new TextEncoder().encode("100644 ");
    const raw = new Uint8Array(prefix.length + latin1.length + 1 + 20);
    raw.set(prefix);
    raw.set(latin1, prefix.length);
    raw[prefix.length + latin1.length] = 0;
    raw.set(hexToBytes(blob), prefix.length + latin1.length + 1);

    const parsed = expectSuccess(parseTree(raw));
    assert.deepEqual(encodeTree(parsed), raw);
  });

  it("round-trips a reflog line, and reads the one git writes", () => {
    const at = new Date(1_700_000_000_000);
    const line = encodeReflogLine({ from: null, to: blob, at, message: "commit: one" });
    const parsed = parseReflogLine(line.trimEnd());

    assert.equal(parsed?.from, null);
    assert.equal(parsed?.to, blob);
    assert.equal(parsed?.at.getTime(), at.getTime());
    assert.equal(parsed?.message, "commit: one");

    // git's own spelling of the same thing, which a `Node` repository can hold
    // because `git` wrote to it: the identity sits where an older build here
    // put an ISO timestamp, so the time has to be read from the end.
    const fromGit = parseReflogLine(
      `${"0".repeat(40)} ${blob} Alice <alice@example.com> 1700000000 +0100\tcommit: one`,
    );
    assert.equal(fromGit?.at.getTime(), 1_700_000_000_000);
    assert.equal(fromGit?.to, blob);
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
