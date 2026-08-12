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

import { parsePackIndex } from "./PackIndex.ts";
import { bufferSource, type PackSource, readAt } from "./PackFile.ts";
import { Result } from "effect";
import type { Oid } from "./Store.ts";

const hasGit = (() => {
  try {
    execFileSync("git", ["--version"], { stdio: "ignore" });
    return true;
  } catch {
    return false;
  }
})();

const git = (cwd: string, ...args: string[]) =>
  execFileSync("git", ["-c", "user.name=T", "-c", "user.email=t@e.com", ...args], {
    cwd,
    encoding: "utf8",
  });

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

  it("reads every object in a pack git wrote, and agrees with git on all of them", async () => {
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
  });

  it("resolves the delta chains, not just the full objects", async () => {
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
      const entry = byOid.get(oid as Oid)!;
      const object = await readAt(source, entry.offset, noBases);
      assert.equal(
        object.data.length,
        Number(git(root, "cat-file", "-s", oid).trim()),
        `delta-encoded ${oid} did not reconstruct`,
      );
    }
  });

  it("reads through a windowed source without ever holding the pack", async () => {
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
  });

  it("refuses an offset that is not an object", async () => {
    const source = bufferSource(packBytes);
    await assert.rejects(() => readAt(source, packBytes.length + 1, noBases));
    // Into the trailer, which is a hash rather than an object header.
    await assert.rejects(() => readAt(source, packBytes.length - 10, noBases));
  });
});
