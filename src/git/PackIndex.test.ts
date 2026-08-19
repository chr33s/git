import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { describe, it } from "@effect/vitest";

import { Result } from "effect";

import { hasGit } from "../testing/Git.ts";
import { bytesToHex, hexToBytes } from "./Format.ts";
import {
  buildPackIndex,
  crc32,
  findInPackIndex,
  type PackIndexEntry,
  parsePackIndex,
} from "./PackIndex.ts";
import type { Oid } from "./Store.ts";

const encoder = new TextEncoder();

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

/** Deterministic but well-spread oids, so the fanout buckets are not all one. */
// SAFETY: a hex-encoded SHA-1 digest is exactly the 40 lowercase hex
// characters an oid is.
const oidOf = (seed: string): Oid => createHash("sha1").update(seed).digest("hex") as Oid;

const sample = (count: number): PackIndexEntry[] =>
  Array.from({ length: count }, (_, index) => ({
    oid: oidOf(`object-${index}`),
    offset: 12 + index * 137,
    crc32: crc32(encoder.encode(`object-${index}`)),
  }));

const packChecksum = Uint8Array.from({ length: 20 }, (_, index) => index * 7);

const readU32 = (bytes: Uint8Array, at: number): number =>
  new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength).getUint32(at);

describe("PackIndex", () => {
  it("round-trips entries, sorted by oid", () => {
    const entries = sample(64);
    const parsed = expectSuccess(parsePackIndex(buildPackIndex(entries, packChecksum)));

    const expected = [...entries].sort((left, right) => (left.oid < right.oid ? -1 : 1));
    assert.deepEqual(parsed, expected);
    for (let index = 1; index < parsed.length; index++) {
      assert.ok(parsed[index - 1]!.oid < parsed[index]!.oid);
    }
  });

  it("writes a cumulative fanout consistent with the oid table", () => {
    const entries = sample(200);
    const bytes = buildPackIndex(entries, packChecksum);
    const oids = expectSuccess(parsePackIndex(bytes)).map((entry) => entry.oid);

    let previous = 0;
    for (let bucket = 0; bucket < 256; bucket++) {
      const fanout = readU32(bytes, 8 + bucket * 4);
      assert.ok(fanout >= previous, `fanout[${bucket}] went backwards`);
      // fanout[i] is "how many oids start with a byte <= i".
      assert.equal(fanout, oids.filter((oid) => hexToBytes(oid)[0]! <= bucket).length);
      previous = fanout;
    }
    assert.equal(readU32(bytes, 8 + 255 * 4), entries.length);
  });

  it("finds every entry and reports absent oids as null", () => {
    const entries = sample(300);
    const bytes = buildPackIndex(entries, packChecksum);

    for (const entry of entries) {
      assert.deepEqual(expectSuccess(findInPackIndex(bytes, entry.oid)), entry);
    }
    assert.equal(expectSuccess(findInPackIndex(bytes, oidOf("absent"))), null);
    // SAFETY: forty '0's are a well-formed oid at the very bottom of the table.
    assert.equal(expectSuccess(findInPackIndex(bytes, "0".repeat(40) as Oid)), null);
    // SAFETY: forty 'f's are a well-formed oid at the very top of the table.
    assert.equal(expectSuccess(findInPackIndex(bytes, "f".repeat(40) as Oid)), null);
  });

  it("stores offsets past 2^31 in the large-offset table", () => {
    const entries: PackIndexEntry[] = [
      { oid: oidOf("small"), offset: 12, crc32: 0 },
      { oid: oidOf("boundary"), offset: 2 ** 31, crc32: 1 },
      { oid: oidOf("huge"), offset: 2 ** 33 + 4321, crc32: 2 },
    ];
    const bytes = buildPackIndex(entries, packChecksum);
    const parsed = expectSuccess(parsePackIndex(bytes));

    for (const entry of entries) {
      assert.deepEqual(expectSuccess(findInPackIndex(bytes, entry.oid)), entry);
    }

    // Two entries escape into the 8-byte table, so the file carries 16 extra bytes.
    const offsetsAt = 8 + 256 * 4 + 24 * parsed.length;
    const escaped = parsed.filter(
      (_, index) => (readU32(bytes, offsetsAt + index * 4) & 0x8000_0000) !== 0,
    );
    assert.equal(escaped.length, 2);
    assert.equal(bytes.length, 8 + 256 * 4 + 28 * 3 + 16 + 40);
  });

  it("rejects a corrupt trailer, bad magic and an unknown version", () => {
    const bytes = buildPackIndex(sample(8), packChecksum);

    const flipped = bytes.slice();
    flipped[flipped.length - 25]! ^= 0xff; // inside the recorded pack checksum
    assert.match(expectFailure(parsePackIndex(flipped)).reason, /checksum mismatch/);
    assert.match(
      expectFailure(findInPackIndex(flipped, oidOf("object-0"))).reason,
      /checksum mismatch/,
    );

    const truncatedHash = bytes.slice();
    truncatedHash[truncatedHash.length - 1]! ^= 0x01;
    assert.match(expectFailure(parsePackIndex(truncatedHash)).reason, /checksum mismatch/);

    const badMagic = bytes.slice();
    badMagic[0] = 0x00;
    assert.match(expectFailure(parsePackIndex(badMagic)).reason, /bad magic/);

    const badVersion = bytes.slice();
    badVersion[7] = 3;
    assert.match(expectFailure(parsePackIndex(badVersion)).reason, /unsupported index version 3/);

    assert.match(expectFailure(parsePackIndex(new Uint8Array(16))).reason, /truncated/);
  });

  it("computes crc32 against known vectors", () => {
    assert.equal(crc32(new Uint8Array()), 0);
    assert.equal(crc32(encoder.encode("123456789")), 0xcbf4_3926);
    assert.equal(crc32(encoder.encode("a")), 0xe8b7_be43);
    assert.equal(crc32(encoder.encode("The quick brown fox jumps over the lazy dog")), 0x414f_a339);
    // Unsigned, not the negative int32 a naive implementation leaks.
    assert.ok(crc32(encoder.encode("hello\n")) > 0);
  });
});

/**
 * Interop: git's own `.idx` parses here, and says the same thing `verify-pack`
 * does. Self-consistency between `buildPackIndex` and `parsePackIndex` would
 * survive a shared misreading of the format; this would not.
 */
describe.skipIf(!hasGit)("PackIndex interop with git", () => {
  const git = (cwd: string, ...args: string[]): string =>
    execFileSync("git", args, { cwd, encoding: "utf8" });

  const repack = async (): Promise<{ root: string; idx: Uint8Array; idxPath: string }> => {
    const root = await fs.mkdtemp(path.join(os.tmpdir(), "packindex-interop-"));
    git(root, "init", "--quiet", ".");
    git(root, "config", "user.name", "Alice");
    git(root, "config", "user.email", "alice@example.com");

    for (const [index, name] of ["a.txt", "b.txt", "c.txt"].entries()) {
      await fs.writeFile(path.join(root, name), `${name}\n${"payload ".repeat(index * 40)}\n`);
      git(root, "add", name);
      git(root, "commit", "--quiet", "-m", `add ${name}`);
    }
    // -a -d: one pack holding everything, loose objects removed.
    git(root, "repack", "-a", "-d");

    const packDir = path.join(root, ".git", "objects", "pack");
    const names = (await fs.readdir(packDir)).filter((name) => name.endsWith(".idx"));
    assert.equal(names.length, 1, `expected one .idx, got ${names.join(", ")}`);
    const idxPath = path.join(packDir, names[0]!);
    return { root, idx: new Uint8Array(await fs.readFile(idxPath)), idxPath };
  };

  it("parses a git-written .idx and agrees with git verify-pack", async () => {
    const { root, idx, idxPath } = await repack();
    try {
      const entries = expectSuccess(parsePackIndex(idx));

      // `<oid> <type> <size> <size-in-pack> <offset>` (delta rows add depth/base).
      const reported = new Map<string, number>();
      for (const line of git(root, "verify-pack", "-v", idxPath).split("\n")) {
        const match = line.match(/^([0-9a-f]{40}) \w+ +\d+ +\d+ +(\d+)/);
        if (match !== null) reported.set(match[1]!, Number(match[2]!));
      }
      assert.ok(reported.size >= 9, `expected the repo's objects, got ${reported.size}`);

      assert.deepEqual(
        entries.map((entry) => entry.oid).sort(),
        [...reported.keys()].sort(),
        "index oids differ from verify-pack",
      );

      for (const [oid, offset] of reported) {
        // SAFETY: only lines matching the 40-hex capture group above populate
        // `reported`, so every key is a well-formed oid.
        const found = expectSuccess(findInPackIndex(idx, oid as Oid));
        assert.notEqual(found, null, `findInPackIndex missed ${oid}`);
        assert.equal(found!.offset, offset, `wrong offset for ${oid}`);
      }

      // The trailer's first 20 bytes name the pack the index belongs to.
      const packName = path
        .basename(idxPath)
        .replace(/\.idx$/, "")
        .replace(/^pack-/, "");
      assert.equal(bytesToHex(idx.subarray(idx.length - 40, idx.length - 20)), packName);
    } finally {
      await fs.rm(root, { force: true, recursive: true });
    }
  });

  it("rebuilds git's .idx byte for byte from its entries", async () => {
    const { root, idx } = await repack();
    try {
      const entries = expectSuccess(parsePackIndex(idx));
      const rebuilt = buildPackIndex(entries, idx.subarray(idx.length - 40, idx.length - 20));
      assert.deepEqual(rebuilt, idx);
    } finally {
      await fs.rm(root, { force: true, recursive: true });
    }
  });
});
