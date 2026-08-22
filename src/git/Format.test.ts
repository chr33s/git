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
  encodeTag,
  parseCommit,
  parseReflogLine,
  parseTag,
  parseTree,
  type Signature,
  type TreeEntry,
} from "./Format.ts";
import { isOid, type Oid } from "./Store.ts";

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

const oid = (hex: string): Oid => {
  if (!isOid(hex)) throw new Error(`not a valid oid: '${hex}'`);
  return hex;
};
const blob = oid("0".repeat(39) + "1");
const tree = oid("0".repeat(39) + "2");

describe("Format", () => {
  it.effect("hashes the empty tree to git's well-known oid", () =>
    Effect.promise(async () => {
      const hashed = await Effect.runPromise(hashObject({ type: "tree", data: new Uint8Array() }));
      assert.equal(hashed, EMPTY_TREE_OID);
    }),
  );

  it.effect("hashes a blob the way git does", () =>
    Effect.promise(async () => {
      // `printf 'hello\n' | git hash-object --stdin`
      const hashed = await Effect.runPromise(
        hashObject({ type: "blob", data: new TextEncoder().encode("hello\n") }),
      );
      assert.equal(hashed, "ce013625030ba8dba906f756967f9e9ca394464a");
    }),
  );

  it.effect("round-trips object framing", () =>
    Effect.sync(() => {
      const framed = encodeObject({ type: "blob", data: new TextEncoder().encode("hi") });
      assert.equal(new TextDecoder().decode(framed), "blob 2\0hi");

      const decoded = expectSuccess(decodeObject(framed));
      assert.equal(decoded.type, "blob");
      assert.equal(new TextDecoder().decode(decoded.data), "hi");
    }),
  );

  it.effect("rejects framing whose length disagrees with the body", () =>
    Effect.sync(() => {
      const bad = new TextEncoder().encode("blob 99\0hi");
      assert.equal(expectFailure(decodeObject(bad))._tag, "Invalid");
    }),
  );

  it.effect("round-trips a commit, timezone included", () =>
    Effect.sync(() => {
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
    }),
  );

  it.effect("parses a negative timezone offset", () =>
    Effect.sync(() => {
      const west: Signature = { ...alice, offset: -450 };
      const parsed = expectSuccess(
        parseCommit(
          encodeCommit({ tree, parents: [], author: west, committer: west, message: "x" }),
        ),
      );
      assert.equal(parsed.author.offset, -450);
    }),
  );

  it.effect("fails a commit with no tree instead of defaulting", () =>
    Effect.sync(() => {
      assert.equal(
        expectFailure(parseCommit(new TextEncoder().encode("parent abc\n\nmessage"))).field,
        "tree",
      );
    }),
  );

  it.effect("sorts tree entries the way git does, directories as 'name/'", () =>
    Effect.sync(() => {
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
    }),
  );

  it.effect("sorts tree entries by their UTF-8 bytes, not by UTF-16 code unit", () =>
    Effect.sync(() => {
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
    }),
  );

  it.effect("writes back a tree entry name that is not valid UTF-8", () =>
    Effect.sync(() => {
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
    }),
  );

  it.effect("round-trips a reflog line, and reads the one git writes", () =>
    Effect.sync(() => {
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
    }),
  );

  it.effect("round-trips tree entry oids through binary", () =>
    Effect.sync(() => {
      const parsed = expectSuccess(
        parseTree(encodeTree([{ mode: "100644", name: "f", oid: blob }])),
      );
      assert.equal(parsed[0]?.oid, blob);
    }),
  );

  it.effect("fails a truncated tree", () =>
    Effect.sync(() => {
      const truncated = encodeTree([{ mode: "100644", name: "f", oid: blob }]).slice(0, 12);
      assert.equal(expectFailure(parseTree(truncated))._tag, "Invalid");
    }),
  );

  it.effect("round-trips hex", () =>
    Effect.sync(() => {
      assert.equal(bytesToHex(hexToBytes(blob)), blob);
    }),
  );
});

describe("commit objects are bytes", () => {
  // Every case here is a real object git writes and this codec used to
  // rewrite: it decoded to a string, dropped what it had no field for, and
  // encoded the remains. What came back was a different object under a
  // different id — silently, in the middle of a rebase.
  const roundTrips = (label: string, data: Uint8Array) => {
    const parsed = expectSuccess(parseCommit(data));
    assert.deepEqual(
      [...encodeCommit(parsed)],
      [...data],
      `${label}: encode(parse(bytes)) must be the bytes`,
    );
    return parsed;
  };

  const utf8 = new TextEncoder();
  /** An object's bytes, written the way it is stored: ASCII lines and raw bytes. */
  const bytes = (...parts: ReadonlyArray<string | Uint8Array>): Uint8Array => {
    const chunks = parts.map((part) => (part instanceof Uint8Array ? part : utf8.encode(part)));
    const out = new Uint8Array(chunks.reduce((sum, chunk) => sum + chunk.length, 0));
    let at = 0;
    for (const chunk of chunks) {
      out.set(chunk, at);
      at += chunk.length;
    }
    return out;
  };

  it.effect("round-trips an ordinary commit", () =>
    Effect.sync(() => {
      roundTrips(
        "plain",
        bytes(
          `tree ${EMPTY_TREE_OID}\n`,
          `parent ${blob}\n`,
          "author Alice <alice@example.com> 1700000000 +0100\n",
          "committer Alice <alice@example.com> 1700000000 +0100\n",
          "\n",
          "a message\n",
        ),
      );
    }),
  );

  it.effect("keeps a message whose bytes are not UTF-8", () =>
    Effect.sync(() => {
      // `café` in Latin-1: one byte, 0xe9, which is not a UTF-8 sequence. A
      // decode turns it into U+FFFD and an encode writes three bytes back, so
      // the message a replay copied was not the message its author wrote.
      const latin1 = Uint8Array.from([0x63, 0x61, 0x66, 0xe9, 0x0a]);
      const parsed = roundTrips(
        "latin-1 message",
        bytes(
          `tree ${EMPTY_TREE_OID}\n`,
          "author Alice <alice@example.com> 1700000000 +0000\n",
          "committer Alice <alice@example.com> 1700000000 +0000\n",
          "\n",
          latin1,
        ),
      );
      assert.notEqual(parsed.raw, undefined, "the stored bytes have to ride along");
      assert.deepEqual([...(parsed.raw ?? [])], [...latin1]);
    }),
  );

  it.effect("keeps an author line whose bytes are not UTF-8", () =>
    Effect.sync(() => {
      const line = bytes(
        "author ",
        Uint8Array.from([0x4a, 0xf8, 0x72, 0x6e]),
        " <j@example.com> 1700000000 +0000\n",
      );
      const parsed = roundTrips(
        "latin-1 author",
        bytes(
          `tree ${EMPTY_TREE_OID}\n`,
          line,
          "committer Alice <alice@example.com> 1700000000 +0000\n",
          "\n",
          "fine\n",
        ),
      );
      assert.notEqual(parsed.author.raw, undefined);
      // The committer decoded cleanly, so it carries nothing extra.
      assert.equal(parsed.committer.raw, undefined);
    }),
  );

  it.effect("keeps the headers it does not understand, signature included", () =>
    Effect.sync(() => {
      // A signed commit's `gpgsig` is one header spanning many lines, each
      // continuation beginning with a space. Dropped on re-encode, a rebase
      // stripped every signature it touched and said nothing.
      const parsed = roundTrips(
        "signed",
        bytes(
          `tree ${EMPTY_TREE_OID}\n`,
          "author Alice <alice@example.com> 1700000000 +0000\n",
          "committer Alice <alice@example.com> 1700000000 +0000\n",
          "encoding ISO-8859-1\n",
          "gpgsig -----BEGIN PGP SIGNATURE-----\n",
          " \n",
          " iQEcBAABCgAGBQJb...\n",
          " -----END PGP SIGNATURE-----\n",
          "\n",
          "signed work\n",
        ),
      );

      assert.deepEqual(
        (parsed.headers ?? []).map((header) => header.name),
        ["encoding", "gpgsig"],
      );
      // And it is the *whole* header: the continuation lines are part of it.
      const signature = new TextDecoder().decode((parsed.headers ?? [])[1]?.raw);
      assert.match(signature, /BEGIN PGP SIGNATURE/);
      assert.match(signature, /END PGP SIGNATURE/);
    }),
  );

  it.effect("round-trips a commit with no message at all", () =>
    Effect.sync(() => {
      roundTrips(
        "empty message",
        bytes(
          `tree ${EMPTY_TREE_OID}\n`,
          "author Alice <alice@example.com> 1700000000 +0000\n",
          "committer Alice <alice@example.com> 1700000000 +0000\n",
          "\n",
        ),
      );
    }),
  );

  it.effect("round-trips an annotated tag, whose signature lives in its message", () =>
    Effect.sync(() => {
      const data = bytes(
        `object ${blob}\n`,
        "type commit\n",
        "tag v1.0\n",
        "tagger Alice <alice@example.com> 1700000000 +0000\n",
        "\n",
        // A Latin-1 byte in the notes, and an armoured signature under it: what
        // a decode ruins and what a maintainer would never notice until the tag
        // stopped verifying.
        Uint8Array.from([0x72, 0x65, 0x6c, 0x65, 0x61, 0x73, 0x65, 0x20, 0xe9, 0x0a]),
        "-----BEGIN PGP SIGNATURE-----\n",
        "iQEcBAABCgAGBQJb...\n",
        "-----END PGP SIGNATURE-----\n",
      );
      const parsed = expectSuccess(parseTag(data));
      assert.deepEqual([...encodeTag(parsed)], [...data]);
    }),
  );
});
