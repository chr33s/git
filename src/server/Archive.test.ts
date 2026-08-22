/**
 * Archives, checked against the tools that will actually open them.
 *
 * A tar writer that only its own reader accepts is worth nothing, so the
 * important assertions here run the system `tar` and `unzip` over the bytes and
 * look at what landed on disk — paths, contents, the executable bit and the
 * symlink. Skipped, not failed, when a binary is missing.
 */
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { describe, it } from "@effect/vitest";

import { Effect, Layer, Stream } from "effect";

import { stores } from "../git/Memory.ts";
import { crc32 } from "../git/PackIndex.ts";
import * as GitRepository from "../git/Repository.ts";
import * as Archive from "./Archive.ts";

const encoder = new TextEncoder();
const decoder = new TextDecoder();

const has = (binary: string, ...args: string[]): boolean => {
  try {
    execFileSync(binary, args, { stdio: "ignore" });
    return true;
  } catch {
    return false;
  }
};

const hasTar = has("tar", "--version");
const hasUnzip = has("unzip", "-v");

const repository = GitRepository.layer.pipe(
  Layer.provide(GitRepository.hooksNoop),
  Layer.provide(stores),
);

const author = {
  name: "Alice",
  email: "alice@example.com",
  at: new Date(1_700_000_000_000),
  offset: 0,
};

/** 120 bytes, split across ustar's `prefix` and `name` fields. */
const LONG_SPLIT = `deep/${"segment-".repeat(8)}dir/${"file-name-".repeat(4)}end.txt`;
/** 124 bytes with no slash to split on, so it needs the pax extended header. */
const LONG_FLAT = `${"x".repeat(120)}.txt`;

const CONTENTS: ReadonlyArray<readonly [string, string]> = [
  ["readme.md", "hello\n"],
  ["src/main.ts", "export const main = () => {};\n"],
  [LONG_SPLIT, "split across ustar fields\n"],
  [LONG_FLAT, "pax extended header\n"],
];

const seed = Effect.gen(function* () {
  const git = yield* GitRepository.Repository;
  const tree = yield* git.writeFiles({
    changes: [
      ...CONTENTS.map(([file, content]) => ({ path: file, content: encoder.encode(content) })),
      { path: "bin/run.sh", content: encoder.encode("#!/bin/sh\necho hi\n"), mode: "100755" },
      { path: "link.txt", content: encoder.encode("readme.md"), mode: "120000" },
    ],
  });
  yield* git.commit({ branch: "main", tree, message: "seed\n", author });
  return tree;
});

const join = (chunks: ReadonlyArray<Uint8Array>): Uint8Array => {
  const bytes = new Uint8Array(chunks.reduce((total, chunk) => total + chunk.length, 0));
  let offset = 0;
  for (const chunk of chunks) {
    bytes.set(chunk, offset);
    offset += chunk.length;
  }
  return bytes;
};

const collect = (format: Archive.Format, prefix?: string) =>
  Effect.runPromise(
    Effect.gen(function* () {
      const tree = yield* seed;
      // Two calls rather than one with a conditional property: leaving
      // `prefix` out entirely is what exercises the default under test.
      const stream = yield* prefix === undefined
        ? Archive.archive({ tree, format })
        : Archive.archive({ tree, format, prefix });
      return join(yield* Stream.runCollect(stream));
    }).pipe(Effect.provide(repository)),
  );

/** The bytes on disk, unpacked by the system tar, and where they landed. */
const untar = async (bytes: Uint8Array, flag: "-xf" | "-xzf") => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "archive-"));
  const file = path.join(root, "archive");
  const into = path.join(root, "into");
  await fs.writeFile(file, bytes);
  await fs.mkdir(into);
  execFileSync("tar", [flag, file, "-C", into], { stdio: "pipe" });
  return { root, into };
};

const listing = async (root: string): Promise<ReadonlyArray<string>> => {
  const entries = await fs.readdir(root, { recursive: true, withFileTypes: true });
  return entries
    .filter((entry) => !entry.isDirectory())
    .map((entry) => path.relative(root, path.join(entry.parentPath, entry.name)))
    .sort();
};

describe.skipIf(!hasTar)("Archive through the system tar", () => {
  it.effect("writes a tar the system tar extracts, modes and symlinks included", () =>
    Effect.promise(async () => {
      const { into, root } = await untar(await collect("tar"), "-xf");
      try {
        assert.deepEqual(
          await listing(into),
          [LONG_FLAT, "bin/run.sh", LONG_SPLIT, "link.txt", "readme.md", "src/main.ts"].sort(),
        );

        for (const [file, content] of CONTENTS) {
          assert.equal(await fs.readFile(path.join(into, file), "utf8"), content);
        }

        // The bit git records in mode 100755, and the only thing an archive can
        // lose that makes the extracted tree unusable.
        const script = await fs.stat(path.join(into, "bin/run.sh"));
        assert.equal(script.mode & 0o111, 0o111);
        assert.equal((await fs.stat(path.join(into, "readme.md"))).mode & 0o111, 0);

        const link = await fs.lstat(path.join(into, "link.txt"));
        assert.ok(link.isSymbolicLink());
        assert.equal(await fs.readlink(path.join(into, "link.txt")), "readme.md");
      } finally {
        await fs.rm(root, { force: true, recursive: true });
      }
    }),
  );

  it.effect("writes a tar.gz the system tar decompresses", () =>
    Effect.promise(async () => {
      const bytes = await collect("tar.gz");
      // gzip's own magic, before tar ever sees it.
      assert.deepEqual([...bytes.subarray(0, 3)], [0x1f, 0x8b, 0x08]);

      const { into, root } = await untar(bytes, "-xzf");
      try {
        assert.equal(await fs.readFile(path.join(into, "readme.md"), "utf8"), "hello\n");
        assert.equal(await fs.readFile(path.join(into, LONG_SPLIT), "utf8"), CONTENTS[2]![1]);
        assert.equal(await fs.readFile(path.join(into, LONG_FLAT), "utf8"), CONTENTS[3]![1]);
      } finally {
        await fs.rm(root, { force: true, recursive: true });
      }
    }),
  );

  it.effect("puts everything under the prefix", () =>
    Effect.promise(async () => {
      const { into, root } = await untar(await collect("tar", "myrepo-v1"), "-xf");
      try {
        const files = await listing(into);
        assert.ok(files.length > 0);
        assert.ok(files.every((file) => file.startsWith("myrepo-v1/")));
        assert.equal(await fs.readFile(path.join(into, "myrepo-v1/readme.md"), "utf8"), "hello\n");
        assert.ok((await fs.stat(path.join(into, "myrepo-v1"))).isDirectory());
      } finally {
        await fs.rm(root, { force: true, recursive: true });
      }
    }),
  );

  it.effect("serves an archive request, extension picking the format", () =>
    Effect.promise(async () => {
      const response = await Effect.runPromise(
        Effect.gen(function* () {
          yield* seed;
          const request = new Request("http://host/repo/archive/myrepo-v1.tar.gz");
          const answer = yield* Archive.handle(request);
          assert.ok(answer !== null);
          return {
            type: answer.headers.get("content-type"),
            disposition: answer.headers.get("content-disposition"),
            bytes: new Uint8Array(yield* Effect.promise(() => answer.arrayBuffer())),
          };
        }).pipe(Effect.provide(repository)),
      );

      assert.equal(response.type, "application/gzip");
      assert.equal(response.disposition, 'attachment; filename="myrepo-v1.tar.gz"');

      const { into, root } = await untar(response.bytes, "-xzf");
      try {
        // The default prefix is the name without its extension, so an unpacked
        // archive leaves one directory behind rather than scattering files.
        assert.equal(await fs.readFile(path.join(into, "myrepo-v1/readme.md"), "utf8"), "hello\n");
      } finally {
        await fs.rm(root, { force: true, recursive: true });
      }
    }),
  );
});

/** A zip reader over the central directory, for the assertions unzip cannot make. */
const readZip = (bytes: Uint8Array) => {
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);

  let end = bytes.length - 22;
  while (end >= 0 && view.getUint32(end, true) !== 0x0605_4b50) end--;
  assert.ok(end >= 0, "no end-of-central-directory record");

  const count = view.getUint16(end + 10, true);
  let at = view.getUint32(end + 16, true);

  const entries: Array<{ name: string; content: Uint8Array; mode: number }> = [];
  for (let index = 0; index < count; index++) {
    assert.equal(view.getUint32(at, true), 0x0201_4b50);
    const crc = view.getUint32(at + 16, true);
    const size = view.getUint32(at + 24, true);
    const nameLength = view.getUint16(at + 28, true);
    const extraLength = view.getUint16(at + 30, true);
    const commentLength = view.getUint16(at + 32, true);
    const mode = view.getUint32(at + 38, true) >>> 16;
    const local = view.getUint32(at + 42, true);
    const name = decoder.decode(bytes.subarray(at + 46, at + 46 + nameLength));

    assert.equal(view.getUint32(local, true), 0x0403_4b50);
    const dataAt = local + 30 + view.getUint16(local + 26, true) + view.getUint16(local + 28, true);
    const content = bytes.subarray(dataAt, dataAt + size);
    assert.equal(crc32(content), crc, `crc mismatch for ${name}`);

    entries.push({ name, content, mode });
    at += 46 + nameLength + extraLength + commentLength;
  }

  return { end, entries };
};

describe("Archive as zip", () => {
  it.effect("ends with a central directory its own reader walks back", () =>
    Effect.promise(async () => {
      const bytes = await collect("zip");
      const zip = readZip(bytes);

      // The signature `unzip` looks for first; without it nothing else is read.
      assert.equal(new DataView(bytes.buffer).getUint32(zip.end, true), 0x0605_4b50);

      const files = new Map(zip.entries.map((entry) => [entry.name, entry]));
      for (const [file, content] of CONTENTS) {
        assert.equal(decoder.decode(files.get(file)?.content), content);
      }
      assert.ok(files.has("src/"), "directory entries are present");
      assert.equal(files.get("bin/run.sh")!.mode & 0o111, 0o111);
      assert.equal(files.get("readme.md")!.mode & 0o111, 0);
      assert.equal(decoder.decode(files.get("link.txt")!.content), "readme.md");
    }),
  );

  it.effect("puts everything under the prefix", () =>
    Effect.promise(async () => {
      const zip = readZip(await collect("zip", "myrepo-v1"));
      assert.ok(zip.entries.every((entry) => entry.name.startsWith("myrepo-v1/")));
    }),
  );
});

describe.skipIf(!hasUnzip)("Archive through the system unzip", () => {
  it.effect("lists and prints what it was given", () =>
    Effect.promise(async () => {
      const root = await fs.mkdtemp(path.join(os.tmpdir(), "archive-"));
      const file = path.join(root, "archive.zip");
      try {
        await fs.writeFile(file, await collect("zip"));

        const list = execFileSync("unzip", ["-l", file], { encoding: "utf8" });
        for (const [name] of CONTENTS)
          assert.ok(list.includes(name), `${name} missing from listing`);

        assert.equal(
          execFileSync("unzip", ["-p", file, "readme.md"], { encoding: "utf8" }),
          "hello\n",
        );
        assert.equal(
          execFileSync("unzip", ["-p", file, LONG_FLAT], { encoding: "utf8" }),
          "pax extended header\n",
        );
      } finally {
        await fs.rm(root, { force: true, recursive: true });
      }
    }),
  );
});

describe("formatOf", () => {
  it.effect("maps extensions, and only the ones it can write", () =>
    Effect.sync(() => {
      assert.equal(Archive.formatOf("x.tar"), "tar");
      assert.equal(Archive.formatOf("x.tar.gz"), "tar.gz");
      assert.equal(Archive.formatOf("x.tgz"), "tar.gz");
      assert.equal(Archive.formatOf("x.zip"), "zip");
      assert.equal(Archive.formatOf("X.ZIP"), "zip");
      // `.gz` alone is a compressed file, not an archive of one.
      assert.equal(Archive.formatOf("x.gz"), null);
      assert.equal(Archive.formatOf("x.tar.bz2"), null);
      assert.equal(Archive.formatOf("x"), null);
      assert.equal(Archive.formatOf(""), null);
    }),
  );
});

describe("Archive.handle", () => {
  const answer = (url: string) =>
    Effect.runPromise(
      Effect.gen(function* () {
        yield* seed;
        return yield* Archive.handle(new Request(url));
      }).pipe(Effect.provide(repository)),
    );

  it.effect("declines anything that is not an archive request", () =>
    Effect.promise(async () => {
      assert.equal(await answer("http://host/repo/info/refs"), null);
      assert.equal(await answer("http://host/repo/archive/x.rar"), null);
      // The name is the last segment; a deeper path is somebody else's route.
      assert.equal(await answer("http://host/repo/archive/x.tar/more"), null);
    }),
  );

  it.effect("archives a subdirectory, and reports what it cannot find", () =>
    Effect.promise(async () => {
      const subdirectory = await answer("http://host/repo/archive/src.zip?path=src");
      assert.equal(subdirectory?.status, 200);
      const zip = readZip(new Uint8Array(await subdirectory!.arrayBuffer()));
      assert.deepEqual(
        zip.entries.map((entry) => entry.name),
        ["src/", "src/main.ts"],
      );

      assert.equal(
        (await answer("http://host/repo/archive/x.zip?ref=refs/heads/nope"))?.status,
        404,
      );
      assert.equal((await answer("http://host/repo/archive/x.zip?path=nope"))?.status, 404);
    }),
  );
});
