import assert from "node:assert/strict";
import { it } from "@effect/vitest";
import { Effect, Layer, Logger, Stream } from "effect";
import { StorageFailure } from "./Error.ts";
import * as Maintenance from "./Maintenance.ts";
import { stores } from "./Memory.ts";
import { PackStore } from "./Packed.ts";
import { ObjectStore, RefStore } from "./Store.ts";

const secret = "private-repository-token-and-path";
const failure = new StorageFailure({ operation: "write", path: secret, cause: secret });

for (const operation of ["fsck", "gc"] as const) {
  it.effect(`${operation} reports a redacted failure and preserves the original error`, () => {
    const messages: string[] = [];
    const logger = Logger.make<unknown, void>(({ message }) =>
      messages.push(JSON.stringify(message)),
    );
    return Effect.gen(function* () {
      const objects = yield* ObjectStore;
      const refs = yield* RefStore;
      const packs = yield* PackStore;
      const oid = yield* objects.write({ type: "blob", data: new TextEncoder().encode(secret) });
      const broken = ObjectStore.of({
        ...objects,
        list: Stream.fail(failure),
        delete: () => Effect.fail(failure),
      });
      const result =
        operation === "fsck"
          ? yield* Maintenance.fsck({ objects: broken, refs }).pipe(Effect.result)
          : yield* Maintenance.gc({
              objects: ObjectStore.of({ ...objects, delete: broken.delete }),
              refs,
              packs,
            }).pipe(Effect.result);
      assert.equal(result._tag, "Failure");
      if (result._tag === "Failure") assert.equal(result.failure, failure);
      assert.equal(messages.length, 1);
      assert.match(messages[0] ?? "", /maintenance_failed/);
      assert.match(messages[0] ?? "", new RegExp(operation));
      assert.doesNotMatch(messages.join(""), new RegExp(secret));
      assert.equal(yield* objects.has(oid), true);
      // Restoring the real store permits a complete retry, without another alarm.
      if (operation === "fsck") yield* Maintenance.fsck({ objects, refs });
      else yield* Maintenance.gc({ objects, refs, packs });
      assert.equal(messages.length, 1);
    }).pipe(Effect.provide(Layer.merge(stores, Logger.layer([logger]))));
  });
}

it.effect(
  "fsck signals an unsuccessful integrity report without leaking ref names or content",
  () => {
    const messages: string[] = [];
    const logger = Logger.make<unknown, void>(({ message }) =>
      messages.push(JSON.stringify(message)),
    );
    return Effect.gen(function* () {
      const objects = yield* ObjectStore;
      const refs = yield* RefStore;
      const oid = yield* objects.write({ type: "blob", data: new TextEncoder().encode(secret) });
      yield* refs.apply([{ name: `refs/heads/${secret}`, value: oid }]);
      yield* objects.delete(oid);
      const report = yield* Maintenance.fsck({ objects, refs });
      assert.equal(report.danglingRefs.length, 1);
      assert.equal(report.danglingRefs[0]?.ref, `refs/heads/${secret}`);
      assert.equal(messages.length, 1);
      assert.match(messages[0] ?? "", /integrity_failed/);
      assert.doesNotMatch(messages.join(""), new RegExp(`${secret}|${oid}`));
    }).pipe(Effect.provide(Layer.merge(stores, Logger.layer([logger]))));
  },
);
