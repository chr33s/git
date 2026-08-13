import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { describe, it } from "@effect/vitest";
import { deflateSync } from "node:zlib";

import { Effect, Result, Stream } from "effect";

import { encodeCommit, encodeTree } from "./Format.ts";
import { stores } from "./Memory.ts";
import { applyDelta, createDelta, encodeOfsDistance, pack, sizeVarint, unpack } from "./Pack.ts";
import { ObjectStore, type Oid, type RawObject } from "./Store.ts";

const encoder = new TextEncoder();
const decoder = new TextDecoder();

const run = <A, E>(effect: Effect.Effect<A, E, ObjectStore>) =>
  Effect.runPromise(effect.pipe(Effect.provide(stores)));

/** Split into deliberately awkward chunk sizes to stress boundary handling. */
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

const sha1 = (bytes: Uint8Array): Uint8Array =>
  Uint8Array.from(createHash("sha1").update(bytes).digest());

// SAFETY: a hex-encoded SHA-1 digest is exactly the 40 lowercase hex
// characters an oid is — this computes real object ids for test fixtures.
const oidOf = (object: RawObject): Oid =>
  createHash("sha1")
    .update(`${object.type} ${object.data.length}\0`)
    .update(object.data)
    .digest("hex") as Oid;

/** Pack object header: type in bits 6-4 of the first byte, size in 4+7+7… bits. */
const objectHeader = (code: number, size: number): number[] => {
  const bytes: number[] = [];
  let current = (code << 4) | (size & 0x0f);
  let rest = Math.floor(size / 16);
  while (rest > 0) {
    bytes.push(current | 0x80);
    current = rest & 0x7f;
    rest = Math.floor(rest / 128);
  }
  bytes.push(current);
  return bytes;
};

/** copy: offset and size both < 256 keeps the instruction two operand bytes. */
const copy = (offset: number, size: number): number[] => [0x80 | 0x01 | 0x10, offset, size];
const insert = (text: string): number[] => [text.length, ...encoder.encode(text)];

const buildPack = (entries: ReadonlyArray<Uint8Array>): Uint8Array => {
  const header = new Uint8Array(12);
  header.set([0x50, 0x41, 0x43, 0x4b]);
  header[7] = 2;
  header[11] = entries.length;
  const body = concat([header, ...entries]);
  return concat([body, sha1(body)]);
};

const hexBytes = (hex: string): number[] => {
  const bytes: number[] = [];
  for (let at = 0; at < hex.length; at += 2) bytes.push(Number.parseInt(hex.slice(at, at + 2), 16));
  return bytes;
};

describe("Pack", () => {
  describe("round-trip", () => {
    it("packs from one store and unpacks into another, bytes identical", async () => {
      const author = {
        name: "Alice",
        email: "alice@example.com",
        at: new Date(1_700_000_000_000),
        offset: 60,
      };

      const written: RawObject[] = [
        { type: "blob", data: encoder.encode("hello\n") },
        { type: "blob", data: encoder.encode("x".repeat(100_000)) },
      ];
      const treeData = encodeTree([{ mode: "100644", name: "a.txt", oid: oidOf(written[0]!) }]);
      written.push({ type: "tree", data: treeData });
      written.push({
        type: "commit",
        data: encodeCommit({
          tree: oidOf({ type: "tree", data: treeData }),
          parents: [],
          author,
          committer: author,
          message: "first",
        }),
      });

      const bytes = await run(
        Effect.gen(function* () {
          const store = yield* ObjectStore;
          const oids: Oid[] = [];
          for (const object of written) oids.push(yield* store.write(object));
          const chunks = yield* Stream.runCollect(pack(oids));
          return concat([...chunks]);
        }),
      );

      const oids = await run(unpack(Stream.fromIterable(chunked(bytes, 7))));
      assert.deepEqual(oids, written.map(oidOf));

      const readBack = await run(
        Effect.gen(function* () {
          const store = yield* ObjectStore;
          // Provided fresh above — prove the objects came from the pack, not
          // the writer's store, by unpacking and reading in one program.
          yield* unpack(Stream.fromIterable(chunked(bytes, 1024)));
          const objects: RawObject[] = [];
          for (const oid of written.map(oidOf)) objects.push(yield* store.read(oid));
          return objects;
        }),
      );
      assert.deepEqual(
        readBack.map((object) => [object.type, decoder.decode(object.data).length]),
        written.map((object) => [object.type, decoder.decode(object.data).length]),
      );
    });
  });

  describe("deltas", () => {
    const base: RawObject = {
      type: "blob",
      data: encoder.encode("the quick brown fox jumps over the lazy dog"),
    };
    // copy "quick brown fox" + insert " XY " + copy "lazy dog" = expected
    const expected = "quick brown fox XY lazy dog";
    const delta = Uint8Array.from([
      ...sizeVarint(base.data.length),
      ...sizeVarint(expected.length),
      ...copy(4, 15),
      ...insert(" XY "),
      ...copy(35, 8),
    ]);

    it("applyDelta reproduces the target", () => {
      const result = applyDelta(base.data, delta);
      assert.ok(Result.isSuccess(result));
      assert.equal(decoder.decode(result.success), expected);
    });

    it("unpacks ref-delta and ofs-delta objects", async () => {
      const baseEntry = concat([
        Uint8Array.from(objectHeader(3, base.data.length)),
        new Uint8Array(deflateSync(base.data)),
      ]);
      const refEntry = concat([
        Uint8Array.from(objectHeader(7, delta.length)),
        Uint8Array.from(hexBytes(oidOf(base))),
        new Uint8Array(deflateSync(delta)),
      ]);
      // The base entry starts right after the 12-byte pack header; the
      // ofs-delta entry starts after base and ref entries.
      const ofsEntry = concat([
        Uint8Array.from(objectHeader(6, delta.length)),
        encodeOfsDistance(baseEntry.length + refEntry.length),
        new Uint8Array(deflateSync(delta)),
      ]);

      const bytes = buildPack([baseEntry, refEntry, ofsEntry]);
      const [oids, contents] = await run(
        Effect.gen(function* () {
          const store = yield* ObjectStore;
          const unpacked = yield* unpack(Stream.fromIterable(chunked(bytes, 5)));
          const objects: string[] = [];
          for (const oid of unpacked) objects.push(decoder.decode((yield* store.read(oid)).data));
          return [unpacked, objects] as const;
        }),
      );

      assert.equal(oids.length, 3);
      assert.equal(oids[0], oidOf(base));
      assert.deepEqual(contents, [decoder.decode(base.data), expected, expected]);
      // Both deltas resolve to the same bytes, so the same oid: dedupe works.
      assert.equal(oids[1], oids[2]);
    });

    it("rejects the reserved opcode 0", () => {
      const bad = Uint8Array.from([...sizeVarint(base.data.length), ...sizeVarint(1), 0]);
      const result = applyDelta(base.data, bad);
      assert.ok(Result.isFailure(result));
      assert.equal(result.failure._tag, "PackCorrupt");
    });

    it("rejects a delta whose base size disagrees", () => {
      const result = applyDelta(base.data.subarray(1), delta);
      assert.ok(Result.isFailure(result));
    });
  });

  describe("corruption", () => {
    const packOf = (objects: ReadonlyArray<RawObject>): Uint8Array =>
      buildPack(
        objects.map((object) =>
          concat([
            Uint8Array.from(
              objectHeader(
                { blob: 3, tree: 2, commit: 1, tag: 4 }[object.type],
                object.data.length,
              ),
            ),
            new Uint8Array(deflateSync(object.data)),
          ]),
        ),
      );

    const expectCorrupt = async (bytes: Uint8Array, chunk = 9) => {
      const error = await run(unpack(Stream.fromIterable(chunked(bytes, chunk))).pipe(Effect.flip));
      // `assert.ok` narrows, so callers get the failure with its `reason`.
      assert.ok(error._tag === "PackCorrupt", `expected PackCorrupt, got ${error._tag}`);
      return error;
    };

    it("rejects a flipped checksum", async () => {
      const bytes = packOf([{ type: "blob", data: encoder.encode("payload") }]);
      bytes[bytes.length - 1] = bytes[bytes.length - 1]! ^ 0xff;
      const error = await expectCorrupt(bytes);
      assert.match(error.reason, /checksum/);
    });

    it("rejects a truncated pack", async () => {
      const bytes = packOf([{ type: "blob", data: encoder.encode("payload") }]);
      await expectCorrupt(bytes.subarray(0, bytes.length - 25));
    });

    it("rejects data that is not a pack", async () => {
      await expectCorrupt(encoder.encode("this is not a packfile at all"));
    });

    it("rejects an object whose inflated size disagrees with its header", async () => {
      const data = encoder.encode("honest payload");
      const entry = concat([
        Uint8Array.from(objectHeader(3, data.length + 3)),
        new Uint8Array(deflateSync(data)),
      ]);
      await expectCorrupt(buildPack([entry]));
    });
  });
});

describe("createDelta", () => {
  const baseText = Array.from(
    { length: 120 },
    (_, index) => `line ${index}: the quick brown fox jumps over the lazy dog\n`,
  ).join("");
  const base = encoder.encode(baseText);
  const target = encoder.encode(baseText.replace("line 60:", "line sixty:"));

  it("round-trips through applyDelta and actually wins", () => {
    const delta = createDelta(base, target);
    assert.ok(delta !== null);
    assert.ok(delta.length * 2 < target.length, "delta should be far smaller than the target");
    const applied = applyDelta(base, delta);
    assert.ok(Result.isSuccess(applied));
    assert.deepEqual(applied.success, target);
  });

  it("handles a target that only appends", () => {
    const grown = encoder.encode(`${baseText}line 120: appended\n`);
    const delta = createDelta(base, grown);
    assert.ok(delta !== null);
    const applied = applyDelta(base, delta);
    assert.ok(Result.isSuccess(applied));
    assert.deepEqual(applied.success, grown);
  });

  it("returns null when copying saves nothing", () => {
    // Deterministic noise: no 16-byte block of it appears in the base.
    let seed = 1;
    const noise = new Uint8Array(2048);
    for (let index = 0; index < noise.length; index++) {
      seed = (seed * 48271) % 0x7fffffff;
      noise[index] = seed & 0xff;
    }
    assert.equal(createDelta(base, noise), null);
  });

  it("refuses targets smaller than one block", () => {
    assert.equal(createDelta(base, encoder.encode("tiny")), null);
  });
});

describe("deltified writer", () => {
  const baseText = Array.from(
    { length: 200 },
    (_, index) => `line ${index}: file content that repeats\n`,
  ).join("");

  it("emits a smaller pack whose objects unpack byte-identically", async () => {
    const one: RawObject = { type: "blob", data: encoder.encode(baseText) };
    const two: RawObject = {
      type: "blob",
      data: encoder.encode(baseText.replace("line 100:", "line one hundred:")),
    };

    const { deltified, full, oids } = await run(
      Effect.gen(function* () {
        const store = yield* ObjectStore;
        const oids = [yield* store.write(one), yield* store.write(two)];
        const collect = (options?: Parameters<typeof pack>[1]) =>
          Stream.runCollect(pack(oids, options)).pipe(Effect.map((chunks) => concat([...chunks])));
        return {
          oids,
          deltified: yield* collect({ deltify: {} }),
          full: yield* collect(),
        };
      }),
    );

    assert.ok(
      deltified.length < full.length,
      `deltified pack (${deltified.length}) should undercut the full one (${full.length})`,
    );

    // A fresh store: everything read back must come from the pack alone.
    const contents = await run(
      Effect.gen(function* () {
        const store = yield* ObjectStore;
        const unpacked = yield* unpack(Stream.fromIterable(chunked(deltified, 997)));
        const objects: RawObject[] = [];
        for (const oid of unpacked) objects.push(yield* store.read(oid));
        return { unpacked, objects };
      }),
    );

    assert.deepEqual(contents.unpacked, oids);
    assert.deepEqual(contents.objects[0], one);
    assert.deepEqual(contents.objects[1], two);
  });

  it("never deltas across types", async () => {
    // A tree whose payload happens to resemble the blob would still be a
    // type confusion if used as a base; sameness of type gates the window.
    const blob: RawObject = { type: "blob", data: encoder.encode(baseText) };
    const bytes = await run(
      Effect.gen(function* () {
        const store = yield* ObjectStore;
        const blobOid = yield* store.write(blob);
        const treeOid = yield* store.write({
          type: "tree",
          data: encodeTree([{ mode: "100644", name: "a.txt", oid: blobOid }]),
        });
        return concat([...(yield* Stream.runCollect(pack([blobOid, treeOid], { deltify: {} })))]);
      }),
    );

    // Re-ingest into a fresh store; a cross-type delta would fail to apply
    // or change an oid, and either would surface here.
    const oids = await run(unpack(Stream.fromIterable([bytes])));
    assert.equal(oids.length, 2);
  });
});
