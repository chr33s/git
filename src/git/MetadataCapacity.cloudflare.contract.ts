import assert from "node:assert/strict";
import { Effect } from "effect";
import { refStore } from "./Cloudflare.ts";
import { hashObject } from "./Format.ts";
import { RefStore } from "./Store.ts";
import type { Runner } from "./Store.contract.ts";

export const metadataCapacityContract = (
  storage: DurableObjectStorage,
  repo: string,
  runner: Runner,
): void => {
  for (const mode of ["rows", "bytes", "shallow", "legacy", "legacy-over-limit", "reopen"] as const)
    runner.it(`Git metadata quota: ${mode}`, async () => {
      const namespace = `${repo}/metadata-${crypto.randomUUID()}`;
      const limits = {
        bytes: mode === "bytes" ? 1 : 1_000_000,
        rows: mode === "shallow" || mode === "legacy-over-limit" ? 1 : 3,
      };
      const open = (capacity = limits) => refStore(storage, namespace, capacity);
      const oid = await Effect.runPromise(hashObject({ type: "blob", data: new Uint8Array([1]) }));
      const sql = storage.sql;
      const snapshot = () =>
        Effect.runPromise(
          RefStore.use((refs) =>
            Effect.all({
              refs: refs.list(),
              log: refs.reflog("refs/heads/main"),
              shallow: refs.shallow,
            }),
          ).pipe(Effect.provide(open())),
        );
      try {
        // Build tables/guards; reads must work even with a one-byte limit.
        const empty = await snapshot();
        if (mode.startsWith("legacy")) {
          sql.exec("INSERT INTO refs VALUES (?, ?, ?)", namespace, "refs/heads/main", oid);
          sql.exec("INSERT INTO shallow VALUES (?, ?)", namespace, oid);
        }
        const initial = await snapshot();
        if (mode === "shallow") {
          await Effect.runPromise(
            RefStore.use((refs) => refs.updateShallow({ add: [oid], remove: [] })).pipe(
              Effect.provide(open()),
            ),
          );
          const second = await Effect.runPromise(
            hashObject({ type: "blob", data: new Uint8Array([2]) }),
          );
          const refused = await Effect.runPromise(
            RefStore.use((refs) => refs.updateShallow({ add: [second], remove: [] })).pipe(
              Effect.provide(open()),
              Effect.result,
            ),
          );
          assert.equal(refused._tag, "Failure");
          assert.deepEqual((await snapshot()).shallow, new Set([oid]));
          await Effect.runPromise(
            RefStore.use((refs) => refs.updateShallow({ add: [second], remove: [oid] })).pipe(
              Effect.provide(open()),
            ),
          );
          assert.deepEqual(
            (await snapshot()).shallow,
            new Set([second]),
            "removal frees capacity in the same transaction",
          );
        } else if (mode !== "bytes" && !mode.startsWith("legacy")) {
          await Effect.runPromise(
            RefStore.use((refs) => refs.apply([{ name: "refs/heads/main", value: oid }])).pipe(
              Effect.provide(open()),
            ),
          );
        }
        const before = await snapshot();
        const failure = await Effect.runPromise(
          RefStore.use((refs) =>
            refs.apply(
              [
                { name: "refs/heads/main", value: oid },
                { name: "refs/heads/second", value: oid },
              ],
              { atomic: true },
            ),
          ).pipe(Effect.provide(open()), Effect.result),
        );
        assert.equal(failure._tag, "Failure");
        if (failure._tag === "Failure") {
          assert.equal(failure.failure._tag, "StorageFailure");
          if (failure.failure._tag === "StorageFailure")
            assert.equal(
              failure.failure.operation,
              mode === "bytes" ? "quota.metadata.bytes" : "quota.metadata.rows",
            );
        }
        assert.deepEqual(
          await snapshot(),
          before,
          "quota refusal must roll back every ref, log and counter change",
        );
        if (mode === "bytes") assert.deepEqual(before, empty);
        if (mode.startsWith("legacy")) assert.deepEqual(before, initial);
        const raised = { bytes: 1_000_000, rows: 100 };
        await Effect.runPromise(
          RefStore.use((refs) =>
            refs.apply([{ name: "refs/heads/main", value: oid, reason: "恢复 🐈" }]),
          ).pipe(Effect.provide(open(raised))),
        );
        const counter = sql
          .exec<{ bytes: number; rows: number }>(
            "SELECT bytes, rows FROM git_ref_capacity WHERE repo = ?",
            namespace,
          )
          .one();
        const actual = sql
          .exec<{ rows: number }>(
            "SELECT (SELECT count(*) FROM refs WHERE repo = ?) + (SELECT count(*) FROM reflog WHERE repo = ?) + (SELECT count(*) FROM shallow WHERE repo = ?) AS rows",
            namespace,
            namespace,
            namespace,
          )
          .one();
        assert.equal(counter.rows, actual.rows);
        let bytes = 0;
        for (const table of ["refs", "reflog", "shallow"]) {
          for (const row of sql.exec<Record<string, string | null>>(
            `SELECT * FROM ${table} WHERE repo = ?`,
            namespace,
          ))
            for (const value of Object.values(row))
              if (value !== null) bytes += new TextEncoder().encode(value).byteLength;
        }
        assert.equal(
          counter.bytes,
          bytes,
          "accounting charges UTF-8 bytes, including repository keys and messages",
        );
        if (mode === "reopen") {
          const reduced = await Effect.runPromise(
            RefStore.use((refs) => refs.apply([{ name: "refs/heads/main", value: oid }])).pipe(
              Effect.provide(open({ bytes: 1, rows: 100 })),
              Effect.result,
            ),
          );
          assert.equal(reduced._tag, "Failure");
          assert.equal((await snapshot()).refs.length, 1);
        }
      } finally {
        for (const table of ["refs", "reflog", "shallow", "git_ref_capacity"])
          sql.exec(`DELETE FROM ${table} WHERE repo = ?`, namespace);
      }
    });
};
