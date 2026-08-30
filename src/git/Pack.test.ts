import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { describe, it } from "@effect/vitest";
import { deflateSync } from "node:zlib";

import { Effect, Layer, Result, Stream } from "effect";

import { encodeCommit, encodeTree } from "./Format.ts";
import { stores } from "./Memory.ts";
import {
  applyDelta,
  createDelta,
  encodeOfsDistance,
  ingest,
  MAX_OBJECT_BYTES,
  maxObject,
  pack,
  sizeVarint,
  unpack,
} from "./Pack.ts";
import { PackStore } from "./Packed.ts";
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

describe("applyDelta bounds", () => {
  it.effect("refuses a delta that claims more than it could hold", () =>
    Effect.sync(() => {
      // A varint target size is a claim by whoever wrote the pack: the
      // allocation happens before a single byte of it is justified, so a
      // sixty-byte push could otherwise ask for gigabytes.
      const base = new Uint8Array([0x61]);
      const delta = new Uint8Array([
        0x01, // base size 1, which matches
        0xff,
        0xff,
        0xff,
        0xff,
        0x07, // target size 0x7fffffff
        0x90,
        0x01, // one copy instruction
      ]);

      const result = applyDelta(base, delta);
      assert.equal(result._tag, "Failure");
    }),
  );
});

describe("Pack", () => {
  describe("round-trip", () => {
    it.effect("packs from one store and unpacks into another, bytes identical", () =>
      Effect.promise(async () => {
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
      }),
    );
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

    it.effect("applyDelta reproduces the target", () =>
      Effect.sync(() => {
        const result = applyDelta(base.data, delta);
        assert.ok(Result.isSuccess(result));
        assert.equal(decoder.decode(result.success), expected);
      }),
    );

    it.effect("unpacks ref-delta and ofs-delta objects", () =>
      Effect.promise(async () => {
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
      }),
    );

    it.effect("rejects the reserved opcode 0", () =>
      Effect.sync(() => {
        const bad = Uint8Array.from([...sizeVarint(base.data.length), ...sizeVarint(1), 0]);
        const result = applyDelta(base.data, bad);
        assert.ok(Result.isFailure(result));
        assert.equal(result.failure._tag, "PackCorrupt");
      }),
    );

    it.effect("rejects a delta whose base size disagrees", () =>
      Effect.sync(() => {
        const result = applyDelta(base.data.subarray(1), delta);
        assert.ok(Result.isFailure(result));
      }),
    );
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

    it.effect("rejects a flipped checksum", () =>
      Effect.promise(async () => {
        const bytes = packOf([{ type: "blob", data: encoder.encode("payload") }]);
        bytes[bytes.length - 1] = bytes[bytes.length - 1]! ^ 0xff;
        const error = await expectCorrupt(bytes);
        assert.match(error.reason, /checksum/);
      }),
    );

    it.effect("rejects a truncated pack", () =>
      Effect.promise(async () => {
        const bytes = packOf([{ type: "blob", data: encoder.encode("payload") }]);
        await expectCorrupt(bytes.subarray(0, bytes.length - 25));
      }),
    );

    it.effect("rejects data that is not a pack", () =>
      Effect.promise(async () => {
        await expectCorrupt(encoder.encode("this is not a packfile at all"));
      }),
    );

    it.effect("rejects an object whose inflated size disagrees with its header", () =>
      Effect.promise(async () => {
        const data = encoder.encode("honest payload");
        const entry = concat([
          Uint8Array.from(objectHeader(3, data.length + 3)),
          new Uint8Array(deflateSync(data)),
        ]);
        await expectCorrupt(buildPack([entry]));
      }),
    );

    /**
     * The declared size is what the inflate below it is bounded by, and it is
     * written by whoever sent the pack. Bounded only by `Inflate.MAX_INFLATED`
     * — 512 MiB, four times what a Durable Object gets — one object could ask
     * for more memory than the isolate has, from about half a megabyte of
     * pack, because deflate reaches ~1000:1. The size check that follows the
     * inflate can only report a bomb that has already been built.
     */
    it.effect("refuses a declared size past the ceiling before inflating it", () =>
      Effect.promise(async () => {
        // Eight megabytes of zeros, which deflate carries in a few kilobytes —
        // the shape of the thing, at a size a test can afford to build.
        const payload = new Uint8Array(8 * 1024 * 1024);
        const entry = concat([
          Uint8Array.from(objectHeader(3, payload.length)),
          new Uint8Array(deflateSync(payload)),
        ]);
        const bytes = buildPack([entry]);
        assert.ok(bytes.length < 64 * 1024, `the bomb itself is ${bytes.length} bytes`);

        let peak = 0;
        const sample = setInterval(() => {
          peak = Math.max(peak, process.memoryUsage().arrayBuffers);
        }, 1);
        const before = process.memoryUsage().arrayBuffers;
        const error = await Effect.runPromise(
          unpack(Stream.fromIterable(chunked(bytes, 4096))).pipe(
            Effect.flip,
            // A host that says it has less says so here; see `MaxObject`.
            Effect.provide(Layer.mergeAll(stores, maxObject(1024))),
            Effect.ensuring(Effect.sync(() => clearInterval(sample))),
          ),
        );

        assert.equal(error._tag, "PackCorrupt");
        assert.match(error.reason, /declares 8388608 bytes, more than the 1024/);
        // Refused *before* the allocation, which is the whole point: inflating
        // first and comparing after would put all eight megabytes here.
        assert.ok(
          peak - before < 4 * 1024 * 1024,
          `inflated ${peak - before} bytes before refusing`,
        );
      }),
    );

    it.effect("refuses a size varint too long to be a size", () =>
      Effect.promise(async () => {
        // A hundred and fifty continuation bytes. Read to the end, `size`
        // accumulates `2 ** shift` past the safe-integer range and arrives at
        // `Infinity` — which satisfies every ceiling, equals no inflated
        // length, and reports itself as "header says Infinity bytes".
        const entry = concat([
          // Blob, low size nibble set, continuation bit on — then nothing but
          // continuations.
          Uint8Array.from([0xbf, ...Array.from({ length: 150 }, () => 0x80), 0x00]),
          new Uint8Array(deflateSync(encoder.encode("x"))),
        ]);
        const error = await expectCorrupt(buildPack([entry]));
        assert.match(error.reason, /size varint is too long/);
      }),
    );

    it.effect("takes the default ceiling where no host has said otherwise", () =>
      Effect.promise(async () => {
        const entry = concat([
          Uint8Array.from(objectHeader(3, MAX_OBJECT_BYTES + 1)),
          new Uint8Array(deflateSync(encoder.encode("x"))),
        ]);
        const error = await expectCorrupt(buildPack([entry]));
        assert.match(error.reason, new RegExp(`more than the ${String(MAX_OBJECT_BYTES)}`));
      }),
    );
  });
});

describe("createDelta", () => {
  const baseText = Array.from(
    { length: 120 },
    (_, index) => `line ${index}: the quick brown fox jumps over the lazy dog\n`,
  ).join("");
  const base = encoder.encode(baseText);
  const target = encoder.encode(baseText.replace("line 60:", "line sixty:"));

  it.effect("round-trips through applyDelta and actually wins", () =>
    Effect.sync(() => {
      const delta = createDelta(base, target);
      assert.ok(delta !== null);
      assert.ok(delta.length * 2 < target.length, "delta should be far smaller than the target");
      const applied = applyDelta(base, delta);
      assert.ok(Result.isSuccess(applied));
      assert.deepEqual(applied.success, target);
    }),
  );

  it.effect("handles a target that only appends", () =>
    Effect.sync(() => {
      const grown = encoder.encode(`${baseText}line 120: appended\n`);
      const delta = createDelta(base, grown);
      assert.ok(delta !== null);
      const applied = applyDelta(base, delta);
      assert.ok(Result.isSuccess(applied));
      assert.deepEqual(applied.success, grown);
    }),
  );

  it.effect("returns null when copying saves nothing", () =>
    Effect.sync(() => {
      // Deterministic noise: no 16-byte block of it appears in the base.
      let seed = 1;
      const noise = new Uint8Array(2048);
      for (let index = 0; index < noise.length; index++) {
        seed = (seed * 48271) % 0x7fffffff;
        noise[index] = seed & 0xff;
      }
      assert.equal(createDelta(base, noise), null);
    }),
  );

  it.effect("refuses targets smaller than one block", () =>
    Effect.sync(() => {
      assert.equal(createDelta(base, encoder.encode("tiny")), null);
    }),
  );
});

describe("deltified writer", () => {
  const baseText = Array.from(
    { length: 200 },
    (_, index) => `line ${index}: file content that repeats\n`,
  ).join("");

  it.effect("emits a smaller pack whose objects unpack byte-identically", () =>
    Effect.promise(async () => {
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
            Stream.runCollect(pack(oids, options)).pipe(
              Effect.map((chunks) => concat([...chunks])),
            );
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
    }),
  );

  it.effect("never deltas across types", () =>
    Effect.promise(async () => {
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
    }),
  );
});

describe("ingest", () => {
  const runIngest = <A, E>(effect: Effect.Effect<A, E, ObjectStore | PackStore>) =>
    Effect.runPromise(effect.pipe(Effect.provide(stores)));

  const hex = (bytes: Uint8Array): string => Buffer.from(bytes).toString("hex");

  const blobEntry = (text: string): Uint8Array => {
    const data = encoder.encode(text);
    return concat([
      Uint8Array.from(objectHeader(3, data.length)),
      new Uint8Array(deflateSync(data)),
    ]);
  };

  it.effect("keeps a pack worth keeping, and serves every read from it", () =>
    Effect.promise(async () => {
      const texts = Array.from({ length: 8 }, (_, index) => `retained object ${index}\n`);
      const packBytes = buildPack(texts.map(blobEntry));
      const name = `pack-${hex(sha1(packBytes.subarray(0, packBytes.length - 20)))}`;

      const outcome = await runIngest(
        Effect.gen(function* () {
          const oids = yield* ingest(Stream.fromIterable(chunked(packBytes, 7)));
          const packs = yield* PackStore;
          const handles = yield* packs.list;
          const store = yield* ObjectStore;
          // The overlay's `delete` touches loose objects only — so a read that
          // survives deleting every oid is a read served from the pack, which
          // is the whole claim: nothing was exploded.
          for (const oid of oids) yield* store.delete(oid);
          const first = yield* store.read(oids[0]!);
          return { oids, names: handles.map((handle) => handle.name), first };
        }),
      );

      assert.deepEqual(outcome.names, [name]);
      assert.deepEqual(
        outcome.oids,
        texts.map((text) => oidOf({ type: "blob", data: encoder.encode(text) })),
      );
      assert.equal(decoder.decode(outcome.first.data), texts[0]);
    }),
  );

  it.effect("retains a thin pack, resolving its base from the store", () =>
    Effect.promise(async () => {
      const baseText = "the base the pack does not carry\n";
      const targetText = `${baseText} plus what the delta adds`;
      const base = encoder.encode(baseText);
      const target = encoder.encode(targetText);
      const delta = Uint8Array.from([
        ...sizeVarint(base.length),
        ...sizeVarint(target.length),
        ...copy(0, base.length),
        ...insert(" plus what the delta adds"),
      ]);

      const fillers = Array.from({ length: 7 }, (_, index) => `filler ${index}\n`);
      const thinEntry = concat([
        Uint8Array.from(objectHeader(7, delta.length)),
        Uint8Array.from(hexBytes(oidOf({ type: "blob", data: base }))),
        new Uint8Array(deflateSync(delta)),
      ]);
      const packBytes = buildPack([...fillers.map(blobEntry), thinEntry]);

      const outcome = await runIngest(
        Effect.gen(function* () {
          const store = yield* ObjectStore;
          yield* store.write({ type: "blob", data: base });
          const oids = yield* ingest(Stream.fromIterable(chunked(packBytes, 11)));
          const packs = yield* PackStore;
          const resolved = yield* store.read(oids.at(-1)!);
          return { oids, packCount: (yield* packs.list).length, resolved };
        }),
      );

      assert.equal(outcome.packCount, 1, "a thin pack is still worth keeping");
      assert.equal(outcome.oids.at(-1), oidOf({ type: "blob", data: target }));
      assert.equal(decoder.decode(outcome.resolved.data), targetText);
    }),
  );

  it.effect("explodes a small push loose, exactly as before", () =>
    Effect.promise(async () => {
      const packBytes = buildPack([blobEntry("tiny 0\n"), blobEntry("tiny 1\n")]);

      const outcome = await runIngest(
        Effect.gen(function* () {
          const oids = yield* ingest(Stream.fromIterable([packBytes]));
          const packs = yield* PackStore;
          const store = yield* ObjectStore;
          const held = yield* store.read(oids[0]!);
          // Deleting a loose object removes it — the inverse of the retained
          // case above, proving where these bytes actually landed.
          yield* store.delete(oids[0]!);
          const gone = yield* store.read(oids[0]!).pipe(Effect.flip);
          return { packCount: (yield* packs.list).length, held, gone: gone._tag };
        }),
      );

      assert.equal(outcome.packCount, 0);
      assert.equal(decoder.decode(outcome.held.data), "tiny 0\n");
      assert.equal(outcome.gone, "ObjectNotFound");
    }),
  );

  it.effect("streams an oversized push loose rather than buffering it", () =>
    Effect.promise(async () => {
      const texts = Array.from({ length: 8 }, (_, index) => `too big to hold ${index}\n`);
      const packBytes = buildPack(texts.map(blobEntry));

      const outcome = await runIngest(
        Effect.gen(function* () {
          const oids = yield* ingest(Stream.fromIterable(chunked(packBytes, 16)), {
            retainUpTo: 32,
          });
          const packs = yield* PackStore;
          const store = yield* ObjectStore;
          const last = yield* store.read(oids.at(-1)!);
          return { count: oids.length, packCount: (yield* packs.list).length, last };
        }),
      );

      assert.equal(outcome.count, 8);
      assert.equal(outcome.packCount, 0, "past the cap the pack is not retained");
      assert.equal(decoder.decode(outcome.last.data), texts.at(-1));
    }),
  );

  it.effect("refuses a corrupt trailer before anything is registered", () =>
    Effect.promise(async () => {
      const packBytes = buildPack(
        Array.from({ length: 8 }, (_, index) => blobEntry(`honest ${index}\n`)),
      );
      packBytes[packBytes.length - 1]! ^= 0xff;

      const outcome = await runIngest(
        Effect.gen(function* () {
          const failure = yield* ingest(Stream.fromIterable([packBytes])).pipe(Effect.flip);
          const packs = yield* PackStore;
          return { tag: failure._tag, packCount: (yield* packs.list).length };
        }),
      );

      assert.equal(outcome.tag, "PackCorrupt");
      assert.equal(outcome.packCount, 0);
    }),
  );
});
