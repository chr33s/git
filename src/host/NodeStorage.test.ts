import assert from "node:assert/strict";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { it } from "@effect/vitest";
import { Effect } from "effect";

import { PackStore } from "../git/Packed.ts";
import { ObjectStore, RefStore } from "../git/Store.ts";
import { stores } from "./NodeStorage.ts";

it.effect("retries failed initialization and shares it across concurrent writes", () =>
  Effect.promise(async () => {
    const root = await fs.mkdtemp(path.join(os.tmpdir(), "host-store-init-"));
    const directory = path.join(root, "repo");
    try {
      await Effect.runPromise(
        Effect.gen(function* () {
          const objects = yield* ObjectStore;
          const refs = yield* RefStore;
          const packs = yield* PackStore;
          assert.deepEqual(yield* refs.list(), []);
          assert.deepEqual(yield* packs.list, []);
          assert.deepEqual(yield* refs.apply([]), []);
          yield* Effect.promise(() => assert.rejects(fs.stat(directory), { code: "ENOENT" }));

          const lock = path.join(directory, "HEAD.lock");
          yield* Effect.promise(async () => {
            await fs.mkdir(directory);
            await fs.writeFile(lock, "", { flag: "wx" });
          });
          const failed = yield* objects
            .write({ type: "blob", data: new Uint8Array([0]) })
            .pipe(Effect.flip);
          assert.equal(failed._tag, "StorageFailure");
          yield* Effect.promise(() => fs.unlink(lock));

          const written = yield* Effect.forEach(
            Array.from({ length: 8 }, (_, index) => index),
            (index) => objects.write({ type: "blob", data: new Uint8Array([index]) }),
            { concurrency: "unbounded" },
          );
          assert.equal(new Set(written).size, 8);
          for (const [index, oid] of written.entries()) {
            assert.deepEqual((yield* objects.read(oid)).data, new Uint8Array([index]));
          }
          yield* refs.setHead("refs/heads/trunk");
          yield* objects.write({ type: "blob", data: new Uint8Array([9]) });
          assert.equal(yield* refs.head, "refs/heads/trunk");
          assert.equal(
            yield* Effect.promise(() => fs.readFile(path.join(directory, "HEAD"), "utf8")),
            "ref: refs/heads/trunk\n",
          );
        }).pipe(Effect.provide(stores(directory))),
      );
    } finally {
      await fs.rm(root, { recursive: true, force: true });
    }
  }),
);
