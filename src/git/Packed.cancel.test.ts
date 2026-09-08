import assert from "node:assert/strict";
import { setImmediate } from "node:timers/promises";
import { deflateSync } from "node:zlib";
import { describe, it } from "@effect/vitest";
import { Effect, Fiber, Stream } from "effect";

import { ObjectNotFound, StorageFailure } from "./Error.ts";
import { concatBytes, hashObject, hexToBytes } from "./Format.ts";
import { inflate } from "./Inflate.zlib.ts";
import { bufferSource } from "./PackFile.ts";
import { buildPackIndex, crc32 } from "./PackIndex.ts";
import { packed } from "./Packed.ts";
import { Sha1 } from "./Sha1.ts";
import type { ObjectStore, Oid, RawObject } from "./Store.ts";

const loose = (read: ObjectStore["Service"]["read"]): ObjectStore["Service"] => ({
  read,
  readStream: (oid) => Effect.fail(new ObjectNotFound({ oid })),
  write: () => Effect.fail(new StorageFailure({ operation: "write", path: "test" })),
  has: () => Effect.succeed(false),
  delete: () => Effect.void,
  list: Stream.empty,
});

const packedEntry = (entry: Uint8Array, oid: Oid) => {
  const body = concatBytes([Uint8Array.of(0x50, 0x41, 0x43, 0x4b, 0, 0, 0, 2, 0, 0, 0, 1), entry]);
  const checksum = new Sha1().update(body).digest();
  return {
    name: "pack-cancellation",
    index: buildPackIndex([{ oid, offset: 12, crc32: crc32(entry) }], checksum),
    source: bufferSource(concatBytes([body, checksum])),
  };
};

describe("packed read cancellation", () => {
  for (const stopAt of [1, 2]) {
    it.live(`stops range reads after cancellation during range ${stopAt}`, () =>
      Effect.promise(async () => {
        const data = new Uint8Array(256 * 1024).fill(0x61);
        const oid = await Effect.runPromise(hashObject({ type: "blob", data }));
        // Type 3, size 262144; stored zlib blocks require several 64-KiB reads.
        const handle = packedEntry(
          concatBytes([Uint8Array.of(0xb0, 0x80, 0x80, 0x01), deflateSync(data, { level: 0 })]),
          oid,
        );
        const reached = Promise.withResolvers<void>();
        const release = Promise.withResolvers<void>();
        let reads = 0;
        const store = packed(
          loose((oid) => Effect.fail(new ObjectNotFound({ oid }))),
          {
            inflate,
            list: Effect.succeed([
              {
                ...handle,
                source: {
                  size: handle.source.size,
                  read: async (offset, length) => {
                    if (++reads === stopAt) {
                      reached.resolve();
                      await release.promise;
                    }
                    return handle.source.read(offset, length);
                  },
                },
              },
            ]),
            write: () => Effect.void,
            delete: () => Effect.void,
          },
          "test",
        );
        const fiber = Effect.runFork(store.read(oid));
        try {
          await reached.promise;
          await Effect.runPromise(Fiber.interrupt(fiber));
          release.resolve();
          // All remaining source reads resolve as microtasks; flush them
          // before checking whether cancellation stopped the read loop.
          await setImmediate();
          assert.equal(reads, stopAt);
        } finally {
          release.resolve();
          await Effect.runPromise(Fiber.interrupt(fiber));
        }
      }),
    );
  }

  it.live("interrupts the isolated runtime resolving a loose delta base", () =>
    Effect.promise(async () => {
      const base: RawObject = { type: "blob", data: new TextEncoder().encode("base") };
      const oid = await Effect.runPromise(hashObject(base));
      const target: RawObject = { type: "blob", data: new TextEncoder().encode("ase") };
      const targetOid = await Effect.runPromise(hashObject(target));
      const delta = Uint8Array.of(4, 3, 0x91, 1, 3);
      const handle = packedEntry(
        concatBytes([Uint8Array.of(0x75), hexToBytes(oid), deflateSync(delta)]),
        targetOid,
      );
      const reached = Promise.withResolvers<void>();
      let release = () => {};
      let interrupted = false;
      const store = packed(
        loose((wanted) =>
          wanted === oid
            ? Effect.callback<RawObject>((resume) => {
                release = () => resume(Effect.succeed(base));
                reached.resolve();
                return Effect.sync(() => {
                  interrupted = true;
                });
              })
            : Effect.fail(new ObjectNotFound({ oid: wanted })),
        ),
        { list: Effect.succeed([handle]), write: () => Effect.void, delete: () => Effect.void },
        "test",
      );
      const fiber = Effect.runFork(store.read(targetOid));
      try {
        await reached.promise;
        await Effect.runPromise(Fiber.interrupt(fiber));
        await setImmediate();
        assert.equal(interrupted, true, "the base read must not outlive its caller");
      } finally {
        release();
        await Effect.runPromise(Fiber.interrupt(fiber));
      }
    }),
  );
});
