/**
 * The index is a handover format, so the tests that matter are the two
 * directions of handover: `git ls-files` reading an index we encoded, and
 * `decodeIndex` reading one `git add` wrote. The round-trip tests only prove
 * self-consistency; these prove the bytes are right.
 *
 * Skipped when `git` is not on PATH, like `Node.interop.test.ts`.
 */
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { describe, it } from "@effect/vitest";

import { Result } from "effect";

import {
  addEntry,
  decodeIndex,
  encodeIndex,
  findEntry,
  type IndexEntry,
  removeEntry,
} from "./Index.ts";
import type { Oid } from "./Store.ts";

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

const oid = (seed: number) => seed.toString(16).padStart(40, "0") as Oid;

const entry = (overrides: Partial<IndexEntry> & { readonly path: string }): IndexEntry => ({
  oid: oid(1),
  mode: 0o100644,
  size: 6,
  mtimeSeconds: 1_700_000_000,
  mtimeNanos: 123_456_789,
  ctimeSeconds: 1_699_999_999,
  ctimeNanos: 987_654_321,
  device: 66_310,
  inode: 4_242_424,
  uid: 1000,
  gid: 1000,
  stage: 0,
  assumeValid: false,
  ...overrides,
});

const hasGit = (() => {
  try {
    execFileSync("git", ["--version"], { stdio: "ignore" });
    return true;
  } catch {
    return false;
  }
})();

describe("Index", () => {
  it("round-trips entries through the DIRC format", () => {
    const entries = [
      entry({ path: "a.txt", oid: oid(0xaa), size: 6 }),
      entry({ path: "src/main.ts", oid: oid(0xbb), mode: 0o100755, size: 4096 }),
      entry({ path: "link", oid: oid(0xcc), mode: 0o120000, size: 11, assumeValid: true }),
    ];

    const decoded = expectSuccess(decodeIndex(encodeIndex(entries)));
    assert.deepEqual(
      [...decoded].sort((a, b) => (a.path < b.path ? -1 : 1)),
      [...entries].sort((a, b) => (a.path < b.path ? -1 : 1)),
    );
  });

  it("writes a valid header and a 20-byte trailer", () => {
    const bytes = encodeIndex([entry({ path: "a.txt" })]);
    assert.equal(new TextDecoder().decode(bytes.subarray(0, 4)), "DIRC");
    const view = new DataView(bytes.buffer);
    assert.equal(view.getUint32(4), 2);
    assert.equal(view.getUint32(8), 1);
    // 12 header + 72 entry (62 + 5 path + 1 NUL, rounded up to 8) + 20 trailer.
    assert.equal(bytes.length, 12 + 72 + 20);
  });

  it("pads every entry to a multiple of eight, at every boundary", () => {
    // 63 + len rounded up to 8: the lengths either side of each boundary are
    // where an off-by-one in the padding shows up.
    for (const length of [1, 2, 7, 8, 9, 10, 16, 17, 25, 33, 64, 65, 128, 255]) {
      const name = "p".repeat(length);
      const bytes = encodeIndex([entry({ path: name })]);
      const size = bytes.length - 12 - 20;
      assert.equal(size % 8, 0, `entry for a ${length}-byte path is not 8-aligned`);
      assert.equal(size, Math.ceil((62 + length + 1) / 8) * 8);

      const decoded = expectSuccess(decodeIndex(bytes));
      assert.equal(decoded[0]?.path, name);
    }
  });

  it("keeps paths of many lengths distinct in one index", () => {
    const entries = [1, 5, 8, 9, 17, 40, 63, 100].map((length) =>
      entry({ path: "x".repeat(length), oid: oid(length) }),
    );
    const decoded = expectSuccess(decodeIndex(encodeIndex(entries)));
    assert.deepEqual(
      decoded.map((found) => found.path).sort(),
      entries.map((found) => found.path).sort(),
    );
  });

  it("round-trips merge stages and sorts by path then stage", () => {
    const entries = [
      entry({ path: "conflict.txt", oid: oid(3), stage: 3 }),
      entry({ path: "conflict.txt", oid: oid(1), stage: 1 }),
      entry({ path: "conflict.txt", oid: oid(2), stage: 2 }),
      entry({ path: "a.txt", oid: oid(9) }),
    ];

    const decoded = expectSuccess(decodeIndex(encodeIndex(entries)));
    assert.deepEqual(
      decoded.map((found) => [found.path, found.stage]),
      [
        ["a.txt", 0],
        ["conflict.txt", 1],
        ["conflict.txt", 2],
        ["conflict.txt", 3],
      ],
    );
  });

  it("preserves the assume-valid bit without disturbing the stage", () => {
    const entries = [entry({ path: "a.txt", stage: 2, assumeValid: true })];
    const decoded = expectSuccess(decodeIndex(encodeIndex(entries)));
    assert.equal(decoded[0]?.assumeValid, true);
    assert.equal(decoded[0]?.stage, 2);
  });

  it("fails when the trailer checksum does not match", () => {
    const bytes = encodeIndex([entry({ path: "a.txt" }), entry({ path: "b.txt" })]);
    const corrupt = bytes.slice();
    corrupt[40] = corrupt[40]! ^ 0xff;

    const failure = expectFailure(decodeIndex(corrupt));
    assert.equal(failure._tag, "Invalid");
    assert.match(failure.reason, /checksum/);
  });

  it("fails on a bad signature and an unsupported version", () => {
    const bytes = encodeIndex([entry({ path: "a.txt" })]);

    const badMagic = bytes.slice();
    badMagic[0] = 0x44 ^ 0x20;
    assert.match(expectFailure(decodeIndex(badMagic)).reason, /signature/);

    const badVersion = bytes.slice();
    new DataView(badVersion.buffer).setUint32(4, 4);
    assert.match(expectFailure(decodeIndex(badVersion)).reason, /version 4/);

    assert.match(expectFailure(decodeIndex(new Uint8Array(8))).reason, /truncated/);
  });

  it("adds, replaces, removes and finds entries", () => {
    const first = addEntry([], entry({ path: "b.txt", oid: oid(1) }));
    const second = addEntry(first, entry({ path: "a.txt", oid: oid(2) }));
    assert.deepEqual(
      second.map((found) => found.path),
      ["a.txt", "b.txt"],
    );

    const replaced = addEntry(second, entry({ path: "b.txt", oid: oid(3) }));
    assert.equal(replaced.length, 2);
    assert.equal(findEntry(replaced, "b.txt")?.oid, oid(3));

    // A different stage is a different entry, not a replacement.
    const staged = addEntry(replaced, entry({ path: "b.txt", oid: oid(4), stage: 2 }));
    assert.equal(staged.length, 3);

    // ...and removing drops every stage of the path.
    assert.deepEqual(
      removeEntry(staged, "b.txt").map((found) => found.path),
      ["a.txt"],
    );
    assert.equal(findEntry(removeEntry(staged, "b.txt"), "b.txt"), undefined);
  });
});

describe.skipIf(!hasGit)("Index interop with git", () => {
  const git = (cwd: string, ...args: string[]) =>
    execFileSync("git", args, { cwd, encoding: "utf8" }).trim();

  const init = async () => {
    const root = await fs.mkdtemp(path.join(os.tmpdir(), "git-index-"));
    // index.version pins what `git add` writes; this codec only speaks v2.
    git(root, "-c", "init.defaultBranch=main", "init", "-q", ".");
    git(root, "config", "index.version", "2");
    return root;
  };

  it("writes an index git can read", async () => {
    const root = await init();
    try {
      // The oids have to exist as real blobs, or ls-files is reading a promise
      // git cannot check.
      const files = [
        { path: "a.txt", content: "hello\n", mode: 0o100644 },
        { path: "src/deeply/nested/module.ts", content: "export {};\n", mode: 0o100644 },
        { path: "run.sh", content: "#!/bin/sh\n", mode: 0o100755 },
        { path: `${"n".repeat(120)}.txt`, content: "long\n", mode: 0o100644 },
      ];
      const entries = files.map((file) =>
        entry({
          path: file.path,
          mode: file.mode,
          size: file.content.length,
          oid: execFileSync("git", ["hash-object", "-w", "--stdin"], {
            cwd: root,
            encoding: "utf8",
            input: file.content,
          }).trim() as Oid,
        }),
      );

      await fs.writeFile(path.join(root, ".git", "index"), encodeIndex(entries));

      const listed = git(root, "ls-files", "--stage")
        .split("\n")
        .map((line) => {
          const [meta = "", filePath = ""] = line.split("\t");
          const [mode = "", found = "", stage = ""] = meta.split(" ");
          return { mode, oid: found, stage, path: filePath };
        });

      assert.equal(listed.length, entries.length);
      for (const expected of entries) {
        const found = listed.find((line) => line.path === expected.path);
        assert.ok(found !== undefined, `git did not list ${expected.path}`);
        assert.equal(found.oid, expected.oid);
        assert.equal(found.mode, expected.mode.toString(8));
        assert.equal(found.stage, "0");
      }

      // write-tree consumes the whole index — modes, oids and all — and refuses
      // an index whose trailer or padding is wrong.
      assert.match(git(root, "write-tree"), /^[0-9a-f]{40}$/);
    } finally {
      await fs.rm(root, { force: true, recursive: true });
    }
  });

  it("reads an index git wrote", async () => {
    const root = await init();
    try {
      await fs.mkdir(path.join(root, "lib"), { recursive: true });
      await fs.writeFile(path.join(root, "a.txt"), "hello\n");
      await fs.writeFile(path.join(root, "lib", "util.ts"), "export const one = 1;\n");
      git(root, "add", "a.txt", "lib/util.ts");

      const bytes = await fs.readFile(path.join(root, ".git", "index"));
      const decoded = expectSuccess(decodeIndex(new Uint8Array(bytes)));

      const listed = git(root, "ls-files", "--stage")
        .split("\n")
        .map((line) => {
          const [meta = "", filePath = ""] = line.split("\t");
          const [mode = "", found = ""] = meta.split(" ");
          return { mode, oid: found, path: filePath };
        });

      assert.deepEqual(
        decoded.map((found) => ({
          mode: found.mode.toString(8),
          oid: found.oid as string,
          path: found.path,
        })),
        listed,
      );
      assert.equal(findEntry(decoded, "a.txt")?.size, 6);
      // git fills the stat cache in; a zeroed mtime would mean we misread it.
      assert.ok((findEntry(decoded, "a.txt")?.mtimeSeconds ?? 0) > 1_600_000_000);
    } finally {
      await fs.rm(root, { force: true, recursive: true });
    }
  });

  it("re-encodes git's index to the same bytes", async () => {
    const root = await init();
    try {
      await fs.writeFile(path.join(root, "a.txt"), "hello\n");
      await fs.writeFile(path.join(root, "beta.md"), "# beta\n");
      git(root, "add", ".");

      const bytes = new Uint8Array(await fs.readFile(path.join(root, ".git", "index")));
      const decoded = expectSuccess(decodeIndex(bytes));
      // git appends a TREE extension, so compare only the part we write.
      const ours = encodeIndex(decoded);
      assert.deepEqual(
        [...ours.subarray(0, ours.length - 20)],
        [...bytes.subarray(0, ours.length - 20)],
      );
    } finally {
      await fs.rm(root, { force: true, recursive: true });
    }
  });
});
