/**
 * Random access into a real packfile.
 *
 * The pack under test is one `git repack` produced, not one we wrote: a
 * reader that only understands its own writer's output is worth nothing for
 * storage, and git's packs are the ones with delta chains in them.
 */
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { afterAll, beforeAll, describe, it } from "@effect/vitest";

import { gitIn, hasGit } from "../testing/Git.ts";
import { deflateSync } from "node:zlib";
import { type ByteSource, InflateError, inflate as portableInflate } from "./Inflate.ts";
import { inflate as zlibInflate } from "./Inflate.zlib.ts";
import { parsePackIndex } from "./PackIndex.ts";
import { bufferSource, type PackSource, readAt } from "./PackFile.ts";
import { Effect, Result } from "effect";
import type { Oid } from "./Store.ts";

const git = (cwd: string, ...args: string[]) => gitIn(cwd)(...args);

/** Nothing to resolve: a pack git wrote is self-contained. */
const noBases = () => Promise.resolve(null);

describe.skipIf(!hasGit)("PackFile", () => {
  let root: string;
  let packBytes: Uint8Array;
  let indexBytes: Uint8Array;

  beforeAll(async () => {
    root = await fs.mkdtemp(path.join(os.tmpdir(), "packfile-"));
    git(root, "init", "-q", "-b", "main", ".");

    // Several revisions of one growing file, so git has a reason to store
    // some of them as deltas rather than as full objects.
    for (let revision = 0; revision < 12; revision++) {
      const lines = Array.from({ length: 200 + revision * 40 }, (_, line) =>
        line === revision ? `changed at revision ${revision}` : `line ${line}`,
      );
      await fs.writeFile(path.join(root, "big.txt"), `${lines.join("\n")}\n`);
      await fs.writeFile(path.join(root, `file-${revision}.txt`), `content ${revision}\n`);
      git(root, "add", ".");
      git(root, "commit", "-q", "-m", `revision ${revision}`);
    }

    git(root, "repack", "-a", "-d", "-f");

    const packDirectory = path.join(root, ".git", "objects", "pack");
    const names = await fs.readdir(packDirectory);
    const idx = names.find((name) => name.endsWith(".idx"))!;
    indexBytes = new Uint8Array(await fs.readFile(path.join(packDirectory, idx)));
    packBytes = new Uint8Array(
      await fs.readFile(path.join(packDirectory, idx.replace(/\.idx$/, ".pack"))),
    );
  });

  afterAll(async () => {
    await fs.rm(root, { recursive: true, force: true });
  });

  it.effect("reads every object in a pack git wrote, and agrees with git on all of them", () =>
    Effect.promise(async () => {
      const parsed = parsePackIndex(indexBytes);
      assert.ok(Result.isSuccess(parsed));
      const entries = parsed.success;
      assert.ok(entries.length > 30, `expected a substantial pack, got ${entries.length} objects`);

      const source = bufferSource(packBytes);
      const decoder = new TextDecoder();

      for (const entry of entries) {
        const object = await readAt(source, entry.offset, noBases);

        // git's own view of the same object: type, size and content.
        const type = git(root, "cat-file", "-t", entry.oid).trim();
        const size = Number(git(root, "cat-file", "-s", entry.oid).trim());
        assert.equal(object.type, type, `type of ${entry.oid}`);
        assert.equal(object.data.length, size, `size of ${entry.oid}`);

        if (type === "blob" || type === "commit") {
          const expected = execFileSync("git", ["cat-file", type, entry.oid], {
            cwd: root,
            maxBuffer: 64 * 1024 * 1024,
          });
          assert.equal(decoder.decode(object.data), decoder.decode(new Uint8Array(expected)));
        }
      }
    }),
  );

  it.effect("resolves the delta chains, not just the full objects", () =>
    Effect.promise(async () => {
      // A pack where every object were stored whole would make the test above
      // pass for the wrong reason, so find the delta-encoded ones and read
      // those specifically.
      //
      // `verify-pack -v` prints the *resolved* type, not `ofs-delta` — a delta
      // is a row that carries the two extra columns, chain depth and base oid:
      //   <oid> <type> <size> <packed-size> <offset> [<depth> <base-oid>]
      const packDirectory = path.join(root, ".git", "objects", "pack");
      const idx = (await fs.readdir(packDirectory)).find((name) => name.endsWith(".idx"))!;
      const verify = execFileSync("git", ["verify-pack", "-v", path.join(packDirectory, idx)], {
        cwd: root,
        encoding: "utf8",
        maxBuffer: 64 * 1024 * 1024,
      });

      const deltas = verify
        .split("\n")
        .map((line) => line.trim().split(/\s+/))
        .filter((fields) => fields.length >= 7 && /^[0-9a-f]{40}$/.test(fields[6] ?? ""))
        .map((fields) => fields[0]!);
      assert.ok(deltas.length > 0, "git produced no deltas; the test proves less than it claims");

      const parsed = parsePackIndex(indexBytes);
      assert.ok(Result.isSuccess(parsed));
      const byOid = new Map(parsed.success.map((entry) => [entry.oid, entry]));
      const source = bufferSource(packBytes);

      for (const oid of deltas) {
        // SAFETY: each entry in `deltas` is the first column of a verify-pack
        // object row — a full 40-hex object id.
        const entry = byOid.get(oid as Oid)!;
        const object = await readAt(source, entry.offset, noBases);
        assert.equal(
          object.data.length,
          Number(git(root, "cat-file", "-s", oid).trim()),
          `delta-encoded ${oid} did not reconstruct`,
        );
      }
    }),
  );

  it.effect("reads through a windowed source without ever holding the pack", () =>
    Effect.promise(async () => {
      // The shape R2 and a file descriptor both take: reads are ranges, and
      // the test records how much of the pack each object actually touched.
      let bytesRead = 0;
      const ranged: PackSource = {
        size: packBytes.length,
        read: (offset, length) => {
          bytesRead += Math.min(length, packBytes.length - offset);
          return Promise.resolve(packBytes.subarray(offset, offset + length));
        },
      };

      const parsed = parsePackIndex(indexBytes);
      assert.ok(Result.isSuccess(parsed));

      // A small object near the end: the interesting case, because a reader
      // that scanned from the front would touch the whole file to reach it.
      const last = [...parsed.success].sort((a, b) => b.offset - a.offset)[0]!;
      bytesRead = 0;
      const object = await readAt(ranged, last.offset, noBases);
      assert.equal(object.data.length, Number(git(root, "cat-file", "-s", last.oid).trim()));
      assert.ok(
        bytesRead < packBytes.length,
        `read ${bytesRead} of ${packBytes.length} bytes to fetch one object`,
      );
    }),
  );

  it.effect("refuses an offset that is not an object", () =>
    Effect.promise(async () => {
      const source = bufferSource(packBytes);
      await assert.rejects(() => readAt(source, packBytes.length + 1, noBases));
      // Into the trailer, which is a hash rather than an object header.
      await assert.rejects(() => readAt(source, packBytes.length - 10, noBases));
    }),
  );

  it.effect("decodes every object identically on the platform's own zlib", () =>
    Effect.promise(async () => {
      const parsed = parsePackIndex(indexBytes);
      assert.ok(Result.isSuccess(parsed));
      const source = bufferSource(packBytes);

      // The seam only pays for itself if the two decoders are the same
      // function: this pack has delta chains in it, so the comparison covers
      // reconstructed objects as well as whole ones.
      for (const entry of parsed.success) {
        const portable = await readAt(source, entry.offset, noBases, 0, portableInflate);
        const native = await readAt(source, entry.offset, noBases, 0, zlibInflate);
        assert.equal(native.type, portable.type, `type of ${entry.oid}`);
        assert.deepEqual(native.data, portable.data, `bytes of ${entry.oid}`);
      }
    }),
  );
});

/**
 * The decoder itself, away from any pack.
 *
 * `readAt` never asks where a stream ended, which is what lets this one be
 * native — but it does hand over a source that yields whatever size it likes,
 * and the answers for "not yet" and "never" have to stay apart.
 */
describe("the zlib-backed pack inflate", () => {
  /** A source that yields `size` bytes at a time, as a windowed pack does. */
  const chunked = (bytes: Uint8Array, size: number): ByteSource => {
    let at = 0;
    const pending: Uint8Array[] = [];
    return {
      next: () => {
        const held = pending.shift();
        if (held !== undefined) return Promise.resolve(held);
        if (at >= bytes.length) return Promise.resolve(null);
        const chunk = bytes.subarray(at, at + size);
        at += chunk.length;
        return Promise.resolve(chunk);
      },
      pushBack: (rest) => {
        if (rest.length > 0) pending.unshift(rest);
      },
    };
  };

  const payload = new TextEncoder().encode("pack payload ".repeat(4000));
  const stream = new Uint8Array(deflateSync(payload));

  it.effect("reassembles a stream that spans many reads", () =>
    Effect.promise(async () => {
      // 64 bytes at a time is nothing like the real 64 KiB window, which is the
      // point: an object bigger than one read is the case that has to pull
      // again, and the pulls grow rather than crawling one at a time.
      assert.deepEqual(await zlibInflate(chunked(stream, 64)), payload);
      assert.deepEqual(await zlibInflate(chunked(stream, 1)), payload);
    }),
  );

  it.effect("ignores whatever follows the stream, as the next object does", () =>
    Effect.promise(async () => {
      const trailing = new Uint8Array(stream.length + 5000);
      trailing.set(stream, 0);
      trailing.fill(0xaa, stream.length);
      assert.deepEqual(await zlibInflate(chunked(trailing, 64)), payload);
    }),
  );

  it.effect("tells a stream that has not arrived from one that never will", () =>
    Effect.promise(async () => {
      // Truncated: zlib says the same thing it says for "read me more", so the
      // difference has to come from the source running out.
      await assert.rejects(
        () => zlibInflate(chunked(stream.subarray(0, stream.length - 40), 64)),
        InflateError,
      );

      const corrupt = Uint8Array.from(stream);
      corrupt[20] = corrupt[20]! ^ 0xff;
      await assert.rejects(() => zlibInflate(chunked(corrupt, 4096)), InflateError);

      await assert.rejects(
        () => zlibInflate(chunked(new Uint8Array([0x78, 0x01, 0x00]), 64)),
        InflateError,
      );
    }),
  );

  it.effect("refuses to inflate past the caller's limit", () =>
    Effect.promise(async () => {
      // The bound `readAt` puts on a delta, whose expanded size the header
      // declares and nothing else checks.
      await assert.rejects(() => zlibInflate(chunked(stream, 4096), 100), InflateError);
      assert.deepEqual(await zlibInflate(chunked(stream, 4096), payload.length), payload);
    }),
  );

  it.effect("agrees with the portable decoder", () =>
    Effect.promise(async () => {
      assert.deepEqual(
        await zlibInflate(chunked(stream, 97)),
        await portableInflate(chunked(stream, 97)),
      );
    }),
  );
});
