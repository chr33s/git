import assert from "node:assert/strict";
import { ConfigProvider, Effect, Result } from "effect";
import { ArtifactsError } from "@alchemy.run/cloudflare/Artifacts/ReadWriteNamespace";
import type { TransactionalSql } from "../git/Sql.ts";
import type { Runner } from "../git/Store.contract.ts";
import { Registry, Tokens } from "./Namespace.ts";
import { sqlite } from "./Sqlite.ts";

const meta = { description: "東京", defaultBranch: "main", readOnly: false, source: null };
const refused = Effect.fn("SqliteCapacity.refused")(function* <A>(
  operation: Effect.Effect<A, ArtifactsError>,
) {
  const result = yield* Effect.result(operation);
  assert.ok(Result.isFailure(result));
  assert.ok(result.failure instanceof ArtifactsError);
  assert.match(result.failure.message, /^RESOURCE_EXHAUSTED:/);
});

export const sqliteCapacityContract = (
  withSql: (body: (sql: TransactionalSql) => Promise<void>) => Promise<void>,
  runner: Runner,
): void => {
  const open = (sql: TransactionalSql, rows: number, bytes = 1024 ** 2) =>
    Effect.runPromise(
      Effect.gen(function* () {
        return { registry: yield* Registry, tokens: yield* Tokens };
      }).pipe(
        Effect.provide(sqlite(sql)),
        Effect.provide(
          ConfigProvider.layer(
            ConfigProvider.fromUnknown({
              GIT_CF_ARTIFACTS_REGISTRY_ROWS: rows,
              GIT_CF_ARTIFACTS_TOKEN_ROWS: rows,
              GIT_CF_ARTIFACTS_REGISTRY_BYTES: bytes,
              GIT_CF_ARTIFACTS_TOKEN_BYTES: bytes,
            }),
          ),
        ),
      ),
    );

  runner.it("SQLite capacity: concurrent namespace additions, recovery and revoked tokens", () =>
    withSql(async (sql) => {
      const { registry, tokens } = await open(sql, 1);
      await Effect.runPromise(registry.create("first", meta));
      await Effect.runPromise(refused(registry.create("second", meta)));
      const results = await Promise.all(
        ["first", "second", "third"].map(() =>
          Effect.runPromise(Effect.result(tokens.issue("first", "write", 3600))),
        ),
      );
      assert.equal(results.filter(Result.isSuccess).length, 1);
      for (const result of results)
        if (Result.isFailure(result)) assert.match(result.failure.message, /^RESOURCE_EXHAUSTED:/);
      const first = results.find(Result.isSuccess);
      assert.ok(first !== undefined && Result.isSuccess(first));
      const lowered = await open(sql, 1, 1);
      assert.equal(
        await Effect.runPromise(lowered.tokens.verify("first", first.success.plaintext)),
        "write",
      );
      assert.equal(await Effect.runPromise(lowered.tokens.revoke("first", first.success.id)), true);
      assert.equal(await Effect.runPromise(tokens.verify("first", first.success.plaintext)), null);
      await Effect.runPromise(refused(tokens.issue("second", "read", 3600)));
      assert.equal((await Effect.runPromise(lowered.registry.get("first")))?.name, "first");
      assert.equal(await Effect.runPromise(lowered.registry.delete("first")), true);
      await Effect.runPromise(registry.create("second", meta));
      await Effect.runPromise(tokens.issue("second", "read", 3600));
    }),
  );

  runner.it("SQLite capacity: UTF-8 byte boundary and update rollback", () =>
    withSql(async (sql) => {
      const { registry } = await open(sql, 10);
      const original = await Effect.runPromise(
        registry.create("東京", { ...meta, initializing: "create" }),
      );
      const stored = sql.exec("SELECT * FROM repos").toArray()[0];
      assert.ok(stored !== undefined);
      const bytes = Object.values(stored).reduce<number>(
        (sum, value) =>
          sum + (value === null ? 0 : new TextEncoder().encode(String(value)).byteLength),
        0,
      );
      const exact = await open(sql, 10, bytes);
      await Effect.runPromise(exact.registry.setDefaultBranch("東京", "test"));
      const before = sql.exec("SELECT * FROM repos").toArray();
      await Effect.runPromise(refused(exact.registry.setDefaultBranch("東京", "東京東京")));
      await Effect.runPromise(refused(exact.registry.touch("東京", new Date())));
      assert.deepEqual(sql.exec("SELECT * FROM repos").toArray(), before);
      const lowered = await open(sql, 10, bytes - 1);
      await Effect.runPromise(refused(lowered.registry.setDefaultBranch("東京", "x")));
      await Effect.runPromise(lowered.registry.finish("東京", original.id));
      assert.equal((await Effect.runPromise(lowered.registry.get("東京")))?.initializing, null);
      await Effect.runPromise(lowered.registry.delete("東京"));
      await Effect.runPromise(refused((await open(sql, 10, 1)).registry.create("tiny", meta)));
      assert.deepEqual(sql.exec("SELECT * FROM repos").toArray(), []);
    }),
  );

  runner.it("SQLite capacity: legacy over-limit inventory and raised limits", () =>
    withSql(async (sql) => {
      const high = await open(sql, 3);
      for (const name of ["a", "b", "c"]) {
        await Effect.runPromise(high.registry.create(name, meta));
        await Effect.runPromise(high.tokens.issue(name, "read", 3600));
      }
      const low = await open(sql, 1);
      await Effect.runPromise(refused(low.registry.create("d", meta)));
      await Effect.runPromise(refused(low.tokens.issue("d", "read", 3600)));
      assert.equal((await Effect.runPromise(low.registry.list())).total, 3);
      await Effect.runPromise(low.registry.delete("a"));
      await Effect.runPromise(low.registry.delete("b"));
      await Effect.runPromise(refused(low.registry.create("d", meta)));
      const raised = await open(sql, 2);
      await Effect.runPromise(raised.registry.create("d", meta));
      await Effect.runPromise(raised.tokens.issue("d", "read", 3600));
    }),
  );
  runner.it("SQLite capacity: token byte boundary and SQL failure rollback", () =>
    withSql(async (sql) => {
      const high = await open(sql, 10);
      const token = await Effect.runPromise(high.tokens.issue("東京", "write", 3600));
      const stored = sql.exec("SELECT * FROM tokens").toArray()[0];
      assert.ok(stored !== undefined);
      const bytes = Object.values(stored).reduce<number>(
        (sum, value) =>
          sum + (value === null ? 0 : new TextEncoder().encode(String(value)).byteLength),
        0,
      );
      sql.exec("DELETE FROM tokens");
      const below = await open(sql, 10, bytes - 1);
      await Effect.runPromise(refused(below.tokens.issue("東京", "write", 3600)));
      assert.deepEqual(sql.exec("SELECT * FROM tokens").toArray(), []);
      const exact = await open(sql, 10, bytes);
      await Effect.runPromise(exact.tokens.issue("東京", "write", 3600));
      assert.equal((await Effect.runPromise(exact.tokens.list("東京"))).total, 1);
      assert.equal(await Effect.runPromise(exact.tokens.verify("東京", token.plaintext)), null);
      sql.exec("DELETE FROM tokens");
      sql.exec(
        "CREATE TRIGGER capacity_fault AFTER INSERT ON tokens BEGIN SELECT RAISE(ABORT, 'private-fault'); END",
      );
      try {
        const result = await Effect.runPromise(
          Effect.result(exact.tokens.issue("東京", "write", 3600)),
        );
        assert.ok(Result.isFailure(result));
        assert.match(result.failure.message, /^STORAGE_FAILURE:/);
        assert.ok(!JSON.stringify(result.failure).includes("private-fault"));
        assert.deepEqual(sql.exec("SELECT * FROM tokens").toArray(), []);
      } finally {
        sql.exec("DROP TRIGGER capacity_fault");
      }
      await Effect.runPromise(exact.tokens.issue("東京", "write", 3600));
    }),
  );
};
