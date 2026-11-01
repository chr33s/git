import assert from "node:assert/strict";
import { Effect } from "effect";
import { refStore } from "./Cloudflare.ts";
import { hashObject } from "./Format.ts";
import { RefStore } from "./Store.ts";
import type { Runner } from "./Store.contract.ts";

export const cloudflareCorruptionContract = (
  storage: DurableObjectStorage,
  repo: string,
  runner: Runner,
): void => {
  for (const mode of [
    "read",
    "list",
    "apply",
    "shallow",
    "reflog-old",
    "reflog-new",
    "reflog-date",
    "head",
  ] as const)
    runner.it(`SQLite metadata corruption: ${mode}`, async () => {
      const namespace = `${repo}/corruption-${crypto.randomUUID()}`;
      const layer = refStore(storage, namespace);
      const name = "refs/heads/main";
      const bad = "private-corrupt-metadata";
      const oid = await Effect.runPromise(hashObject({ type: "blob", data: new Uint8Array([1]) }));
      const sql = storage.sql;
      await Effect.runPromise(
        Effect.gen(function* () {
          const refs = yield* RefStore;
          yield* refs.apply([{ name, value: oid }]);
          try {
            let read;
            if (mode === "head") {
              yield* Effect.promise(() => storage.put(`HEAD:${namespace}`, { bad }));
              read = refs.head.pipe(Effect.asVoid);
            } else if (mode === "shallow") {
              sql.exec("INSERT INTO shallow (repo, oid) VALUES (?, ?)", namespace, bad);
              read = refs.shallow.pipe(Effect.asVoid);
            } else if (mode.startsWith("reflog")) {
              const column =
                mode === "reflog-old" ? "old_oid" : mode === "reflog-new" ? "new_oid" : "at";
              sql.exec(`UPDATE reflog SET ${column} = ? WHERE repo = ?`, bad, namespace);
              read = refs.reflog(name).pipe(Effect.asVoid);
            } else {
              sql.exec("UPDATE refs SET oid = ? WHERE repo = ?", bad, namespace);
              read =
                mode === "read"
                  ? refs.read(name).pipe(Effect.asVoid)
                  : mode === "list"
                    ? refs.list().pipe(Effect.asVoid)
                    : refs
                        .apply([{ name, value: oid, expected: null }], { atomic: true })
                        .pipe(Effect.asVoid);
            }
            const result = yield* read.pipe(Effect.result);
            assert.equal(result._tag, "Failure", "corrupt rows must never become missing metadata");
            if (result._tag === "Failure") assert.equal(result.failure._tag, "StorageFailure");
            if (mode === "head") {
              assert.equal((yield* refs.setHead(name).pipe(Effect.result))._tag, "Failure");
              assert.deepEqual(yield* Effect.promise(() => storage.get(`HEAD:${namespace}`)), {
                bad,
              });
              yield* Effect.promise(() => storage.put(`HEAD:${namespace}`, name));
              assert.equal(yield* refs.head, name);
            } else if (mode === "shallow") {
              assert.equal(
                (yield* refs.updateShallow({ add: [oid], remove: [] }).pipe(Effect.result))._tag,
                "Failure",
              );
              assert.deepEqual(
                sql
                  .exec<{ oid: string }>("SELECT oid FROM shallow WHERE repo = ?", namespace)
                  .toArray(),
                [{ oid: bad }],
              );
              sql.exec("DELETE FROM shallow WHERE repo = ?", namespace);
              yield* refs.updateShallow({ add: [oid], remove: [] });
              assert.deepEqual(yield* refs.shallow, new Set([oid]));
            } else if (!mode.startsWith("reflog")) {
              assert.equal(
                sql.exec<{ oid: string }>("SELECT oid FROM refs WHERE repo = ?", namespace).one()
                  .oid,
                bad,
              );
              assert.equal(
                sql
                  .exec<{ count: number }>(
                    "SELECT count(*) AS count FROM reflog WHERE repo = ?",
                    namespace,
                  )
                  .one().count,
                1,
              );
              sql.exec("UPDATE refs SET oid = ? WHERE repo = ?", oid, namespace);
              const updated = yield* refs.apply([{ name, expected: oid, value: oid }], {
                atomic: true,
              });
              assert.equal(updated[0]?.applied, true);
              assert.equal(yield* refs.read(name), oid);
            } else {
              const column =
                mode === "reflog-old" ? "old_oid" : mode === "reflog-new" ? "new_oid" : "at";
              assert.equal(
                sql
                  .exec<{ value: string }>(
                    `SELECT ${column} AS value FROM reflog WHERE repo = ?`,
                    namespace,
                  )
                  .one().value,
                bad,
              );
              const restored =
                mode === "reflog-old"
                  ? null
                  : mode === "reflog-new"
                    ? oid
                    : new Date(0).toISOString();
              sql.exec(`UPDATE reflog SET ${column} = ? WHERE repo = ?`, restored, namespace);
              const entries = yield* refs.reflog(name);
              assert.equal(entries.length, 1);
              assert.equal(entries[0]?.from, null);
              assert.equal(entries[0]?.to, oid);
            }
          } finally {
            sql.exec("DELETE FROM refs WHERE repo = ?", namespace);
            sql.exec("DELETE FROM reflog WHERE repo = ?", namespace);
            sql.exec("DELETE FROM shallow WHERE repo = ?", namespace);
            yield* Effect.promise(() => storage.delete(`HEAD:${namespace}`));
          }
        }).pipe(Effect.provide(layer)),
      );
    });
};
