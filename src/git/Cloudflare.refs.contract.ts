import assert from "node:assert/strict";
import { Effect } from "effect";
import { refStore } from "./Cloudflare.ts";
import { hashObject } from "./Format.ts";
import { RefStore } from "./Store.ts";
import type { Runner } from "./Store.contract.ts";

export const cloudflareRefsContract = (
  storage: DurableObjectStorage,
  repo: string,
  runner: Runner,
): void => {
  for (const mode of ["ref-write", "ref-delete", "reflog"] as const)
    runner.it(`SQLite ref failure rollback: ${mode}`, async () => {
      const namespace = `${repo}/ref-failure-${crypto.randomUUID()}`;
      const trigger = `ref_failure_${crypto.randomUUID().replaceAll("-", "")}`;
      const quote = (value: string) => "'" + value.replaceAll("'", "''") + "'";
      const sql = storage.sql;
      const layer = refStore(storage, namespace);
      const oldOid = await Effect.runPromise(
        hashObject({ type: "blob", data: new Uint8Array([1]) }),
      );
      const newOid = await Effect.runPromise(
        hashObject({ type: "blob", data: new Uint8Array([2]) }),
      );
      const first = "refs/heads/first";
      const second = "refs/heads/second";
      const snapshot = () =>
        Effect.runPromise(
          RefStore.use((refs) =>
            Effect.all({
              refs: refs.list(),
              first: refs.reflog(first),
              second: refs.reflog(second),
            }),
          ).pipe(Effect.provide(layer)),
        );
      try {
        await Effect.runPromise(
          RefStore.use((refs) =>
            refs.apply(
              [
                { name: first, value: oldOid },
                { name: second, value: oldOid },
              ],
              { atomic: true },
            ),
          ).pipe(Effect.provide(layer)),
        );
        const before = await snapshot();
        const table = mode === "reflog" ? "reflog" : "refs";
        const event = mode === "ref-delete" ? "DELETE" : "INSERT";
        const row = mode === "ref-delete" ? "OLD" : "NEW";
        sql.exec(`CREATE TRIGGER ${trigger} BEFORE ${event} ON ${table}
          WHEN ${row}.repo = ${quote(namespace)} AND ${row}.name = ${quote(second)}
          BEGIN SELECT RAISE(ABORT, 'fixture-private-sql-failure'); END`);
        const updates = [first, second].map((name) => ({
          name,
          expected: oldOid,
          value: mode === "ref-delete" ? null : newOid,
        }));
        const result = await Effect.runPromise(
          RefStore.use((refs) => refs.apply(updates, { atomic: true })).pipe(
            Effect.provide(layer),
            Effect.result,
          ),
        );
        assert.equal(result._tag, "Failure");
        if (result._tag === "Failure") assert.equal(result.failure._tag, "StorageFailure");
        assert.deepEqual(
          await snapshot(),
          before,
          "failed SQL must roll back every ref and reflog entry",
        );
        sql.exec(`DROP TRIGGER ${trigger}`);
        const retried = await Effect.runPromise(
          RefStore.use((refs) => refs.apply(updates, { atomic: true })).pipe(Effect.provide(layer)),
        );
        assert.ok(retried.every((update) => update.applied));
        const after = await snapshot();
        assert.equal(after.first.length, before.first.length + 1);
        assert.equal(after.second.length, before.second.length + 1);
        assert.deepEqual(
          after.refs,
          mode === "ref-delete"
            ? []
            : [
                [first, newOid],
                [second, newOid],
              ],
        );
      } finally {
        sql.exec(`DROP TRIGGER IF EXISTS ${trigger}`);
        sql.exec("DELETE FROM refs WHERE repo = ?", namespace);
        sql.exec("DELETE FROM reflog WHERE repo = ?", namespace);
      }
    });
};
