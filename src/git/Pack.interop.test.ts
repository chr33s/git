/**
 * Interop: packs cross the wire between this code and the real `git`.
 *
 * `Pack.test.ts` proves the codec is self-consistent. This proves it speaks
 * git's dialect — packs produced by `git repack` (ofs-delta and ref-delta
 * flavours both) unpack to exactly the objects git says are reachable, and a
 * pack produced here survives `git index-pack --strict`.
 *
 * Skipped when `git` is not on PATH.
 */
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { describe, it } from "@effect/vitest";

import { Effect, Stream } from "effect";

import { stores } from "./Memory.ts";
import { hasGit } from "../testing/Git.ts";
import { pack, unpack } from "./Pack.ts";
import { ObjectStore, type Oid } from "./Store.ts";

const git = (cwd: string, ...args: string[]): string =>
  execFileSync("git", args, { cwd, encoding: "utf8" });

const chunked = (bytes: Uint8Array, size: number): Uint8Array[] => {
  const chunks: Uint8Array[] = [];
  for (let at = 0; at < bytes.length; at += size) chunks.push(bytes.subarray(at, at + size));
  return chunks;
};

const concat = (parts: ReadonlyArray<Uint8Array>): Uint8Array => {
  const out = new Uint8Array(parts.reduce((sum, part) => sum + part.length, 0));
  let offset = 0;
  for (const part of parts) {
    out.set(part, offset);
    offset += part.length;
  }
  return out;
};

describe.skipIf(!hasGit)("Pack interop with git", () => {
  /**
   * A repository with two large, nearly identical blobs — prime delta
   * material, so `repack -f` reliably produces delta objects and the delta
   * path is actually exercised.
   */
  const build = async (deltaBaseOffset: boolean) => {
    const root = await fs.mkdtemp(path.join(os.tmpdir(), "git-pack-interop-"));
    git(root, "init", "-q", "-b", "main");
    git(root, "config", "user.name", "Test");
    git(root, "config", "user.email", "test@example.com");

    const lines = Array.from({ length: 200 }, (_, index) => `line ${index}: content`);
    await fs.writeFile(path.join(root, "big.txt"), lines.join("\n"));
    git(root, "add", ".");
    git(root, "commit", "-q", "-m", "first");

    lines[100] = "line 100: changed";
    await fs.writeFile(path.join(root, "big.txt"), lines.join("\n"));
    git(root, "commit", "-q", "-am", "second");

    git(
      root,
      "-c",
      `repack.usedeltabaseoffset=${deltaBaseOffset}`,
      "repack",
      "-adf",
      "--window=50",
      "--depth=50",
    );

    const packDir = path.join(root, ".git", "objects", "pack");
    const packName = (await fs.readdir(packDir)).find((name) => name.endsWith(".pack"));
    assert.ok(packName, "repack produced a pack");

    // The pack must contain deltas, or this test proves nothing.
    const verify = git(root, "verify-pack", "-v", path.join(packDir, packName));
    assert.match(verify, /chain length = 1: \d+ object/);

    const reachable = git(root, "rev-list", "--objects", "--all")
      .trim()
      .split("\n")
      .map((line) => line.split(" ")[0]!)
      .sort((a, b) => a.localeCompare(b));

    return {
      root,
      packBytes: new Uint8Array(await fs.readFile(path.join(packDir, packName))),
      reachable,
    };
  };

  for (const deltaBaseOffset of [true, false]) {
    const flavour = deltaBaseOffset ? "ofs-delta" : "ref-delta";
    it(`unpacks a git-produced ${flavour} pack to the reachable set`, async () => {
      const { root, packBytes, reachable } = await build(deltaBaseOffset);

      const { oids, headCommit } = await Effect.runPromise(
        Effect.gen(function* () {
          const store = yield* ObjectStore;
          const oids = yield* unpack(Stream.fromIterable(chunked(packBytes, 1013)));
          const head = git(root, "rev-parse", "HEAD").trim() as Oid;
          return { oids, headCommit: yield* store.read(head) };
        }).pipe(Effect.provide(stores)),
      );

      assert.deepEqual(
        [...oids].sort((a, b) => a.localeCompare(b)),
        reachable,
      );

      // Byte-for-byte agreement on the head commit, delta-resolved or not.
      const expected = git(root, "cat-file", "commit", "HEAD");
      assert.equal(new TextDecoder().decode(headCommit.data), expected);

      await fs.rm(root, { recursive: true, force: true });
    });
  }

  it("produces a pack that git index-pack --strict accepts", async () => {
    const { root, packBytes } = await build(true);

    const bytes = await Effect.runPromise(
      Effect.gen(function* () {
        const oids = yield* unpack(Stream.fromIterable([packBytes]));
        const chunks = yield* Stream.runCollect(pack(oids));
        return concat([...chunks]);
      }).pipe(Effect.provide(stores)),
    );

    const out = path.join(root, "ours.pack");
    await fs.writeFile(out, bytes);
    execFileSync("git", ["index-pack", "--strict", out], { cwd: root, stdio: "ignore" });

    await fs.rm(root, { recursive: true, force: true });
  });

  it("produces a deltified pack git accepts, with real deltas, smaller than the full one", async () => {
    const { root, packBytes } = await build(true);

    const { deltified, full } = await Effect.runPromise(
      Effect.gen(function* () {
        const oids = yield* unpack(Stream.fromIterable([packBytes]));
        const collect = (options?: Parameters<typeof pack>[1]) =>
          Stream.runCollect(pack(oids, options)).pipe(Effect.map((chunks) => concat([...chunks])));
        return { deltified: yield* collect({ deltify: {} }), full: yield* collect() };
      }).pipe(Effect.provide(stores)),
    );

    assert.ok(
      deltified.length < full.length,
      `deltified pack (${deltified.length}) should undercut the full one (${full.length})`,
    );

    const out = path.join(root, "ours-deltified.pack");
    await fs.writeFile(out, deltified);
    execFileSync("git", ["index-pack", "--strict", out], { cwd: root, stdio: "ignore" });

    // The pack must contain deltas git resolves, or deltify proved nothing.
    const verify = git(root, "verify-pack", "-v", path.join(root, "ours-deltified.idx"));
    assert.match(verify, /chain length = 1: \d+ object/);

    await fs.rm(root, { recursive: true, force: true });
  });
});
