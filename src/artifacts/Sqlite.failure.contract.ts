import assert from "node:assert/strict";
import { Effect } from "effect";
import type { TransactionalSql } from "../git/Sql.ts";
import type { Runner } from "../git/Store.contract.ts";
import { Registry, Tokens } from "./Namespace.ts";
import { sqlite } from "./Sqlite.ts";

export const sqliteFailureContract = (
  withSql: (body: (sql: TransactionalSql) => Promise<void>) => Promise<void>,
  runner: Runner,
): void => {
  for (const [column, damaged] of [
    ["read_only", 2],
    ["initializing", "private-invalid-initialization"],
    ["created_at", "private-invalid-created"],
    ["updated_at", "private-invalid-updated"],
    ["last_push_at", "private-invalid-push"],
    ["default_branch", ""],
  ] as const)
    runner.it(`SQLite repository corruption: ${column}`, () =>
      withSql(async (sql) => {
        const name = `aaa-corrupt-${crypto.randomUUID()}`;
        const meta = { description: null, defaultBranch: "main", readOnly: true, source: null };
        await Effect.runPromise(
          Effect.gen(function* () {
            const registry = yield* Registry;
            const tokens = yield* Tokens;
            const original = yield* registry.create(name, meta);
            const token = yield* tokens.issue(name, "read", 3600);
            try {
              sql.exec(`UPDATE repos SET ${column} = ? WHERE name = ?`, damaged, name);
              const before = sql.exec("SELECT * FROM repos WHERE name = ?", name).toArray();
              for (const operation of [
                registry.get(name).pipe(Effect.asVoid),
                registry.list().pipe(Effect.asVoid),
                registry.create(name, meta).pipe(Effect.asVoid),
                registry.delete(name).pipe(Effect.asVoid),
                registry.finish(name, original.id),
                registry.touch(name, new Date()),
                registry.setDefaultBranch(name, "repaired"),
              ]) {
                const result = yield* operation.pipe(Effect.result);
                assert.equal(
                  result._tag,
                  "Failure",
                  "corrupt repository metadata must fail closed",
                );
                if (result._tag === "Failure") {
                  assert.match(result.failure.message, /STORAGE_FAILURE/);
                  assert.doesNotMatch(
                    JSON.stringify(result.failure),
                    /private-invalid|aaa-corrupt/,
                  );
                }
                assert.deepEqual(
                  sql.exec("SELECT * FROM repos WHERE name = ?", name).toArray(),
                  before,
                );
              }
              assert.equal(
                yield* tokens.verify(name, token.plaintext),
                "read",
                "refused deletion preserves credentials",
              );
              // Only the fixture performs offline repair, using known original values.
              sql.exec(
                "UPDATE repos SET read_only = 1, initializing = NULL, created_at = ?, updated_at = ?, last_push_at = NULL, default_branch = 'main' WHERE name = ?",
                original.createdAt.toISOString(),
                original.updatedAt.toISOString(),
                name,
              );
              assert.deepEqual(yield* registry.get(name), original);
              assert.equal(yield* registry.delete(name), true);
              assert.equal(yield* tokens.verify(name, token.plaintext), null);
            } finally {
              sql.exec("DELETE FROM tokens WHERE repo = ?", name);
              sql.exec("DELETE FROM repos WHERE name = ?", name);
            }
          }).pipe(Effect.provide(sqlite(sql))),
        );
      }),
    );
  for (const [column, damaged] of [
    ["expires_at", "private-corrupt-expiry"],
    ["created_at", "private-corrupt-created"],
    ["scope", "private-corrupt-scope"],
    ["revoked", 2],
  ] as const)
    runner.it(`SQLite token corruption: ${column}`, () =>
      withSql(async (sql) => {
        const name = `corrupt-${crypto.randomUUID()}`;
        await Effect.runPromise(
          Effect.gen(function* () {
            const tokens = yield* Tokens;
            const token = yield* tokens.issue(name, "write", 3600);
            try {
              sql.exec(`UPDATE tokens SET ${column} = ? WHERE id = ?`, damaged, token.id);
              const before = sql.exec("SELECT * FROM tokens WHERE id = ?", token.id).toArray();
              for (const operation of [
                tokens.verify(name, token.plaintext).pipe(Effect.asVoid),
                tokens.list(name).pipe(Effect.asVoid),
                tokens.revoke(name, token.id).pipe(Effect.asVoid),
              ]) {
                const result = yield* operation.pipe(Effect.result);
                assert.equal(result._tag, "Failure", "corrupt credentials must fail closed");
                if (result._tag === "Failure") {
                  assert.match(result.failure.message, /STORAGE_FAILURE/);
                  assert.doesNotMatch(JSON.stringify(result.failure), /private-corrupt|corrupt-/);
                }
              }
              assert.deepEqual(
                sql.exec("SELECT * FROM tokens WHERE id = ?", token.id).toArray(),
                before,
              );
              // Explicit offline fixture repair restores a valid but revoked credential.
              sql.exec(
                "UPDATE tokens SET scope = 'write', revoked = 1, created_at = ?, expires_at = ? WHERE id = ?",
                new Date().toISOString(),
                token.expiresAt,
                token.id,
              );
              assert.equal(yield* tokens.verify(name, token.plaintext), null);
              assert.equal((yield* tokens.list(name)).tokens[0]?.state, "revoked");
            } finally {
              sql.exec("DELETE FROM tokens WHERE repo = ?", name);
            }
          }).pipe(Effect.provide(sqlite(sql))),
        );
      }),
    );
  for (const table of ["repos", "tokens"] as const)
    runner.it(`SQLite registry deletion failure: ${table}`, () =>
      withSql(async (sql) => {
        const name = `delete-failure-${crypto.randomUUID()}`;
        const other = `${name}-other`;
        const trigger = `delete_failure_${crypto.randomUUID().replaceAll("-", "")}`;
        const quote = (value: string) => "'" + value.replaceAll("'", "''") + "'";
        const layer = sqlite(sql);
        const meta = { description: null, defaultBranch: "main", readOnly: false, source: null };
        await Effect.runPromise(
          Effect.gen(function* () {
            const registry = yield* Registry;
            const tokens = yield* Tokens;
            const original = yield* registry.create(name, meta);
            yield* registry.create(other, meta);
            const token = yield* tokens.issue(name, "write", 3600);
            const unrelated = yield* tokens.issue(other, "read", 3600);
            try {
              sql.exec(`CREATE TRIGGER ${trigger} BEFORE DELETE ON ${table}
            WHEN OLD.${table === "repos" ? "name" : "repo"} = ${quote(name)}
            BEGIN SELECT RAISE(ABORT, 'private-delete-failure'); END`);
              const failed = yield* registry.delete(name).pipe(Effect.result);
              assert.equal(failed._tag, "Failure");
              if (failed._tag === "Failure") {
                assert.equal(failed.failure._tag, "ArtifactsError");
                assert.match(failed.failure.message, /STORAGE_FAILURE/);
                assert.doesNotMatch(
                  JSON.stringify(failed.failure),
                  /private-delete-failure|delete-failure-/,
                );
              }
              assert.deepEqual(
                yield* registry.get(name),
                original,
                "failed deletion must preserve the repository identity",
              );
              assert.equal(yield* tokens.verify(name, token.plaintext), "write");
              assert.equal(yield* tokens.verify(other, unrelated.plaintext), "read");
              sql.exec(`DROP TRIGGER ${trigger}`);
              assert.equal(yield* registry.delete(name), true);
              assert.equal(yield* registry.get(name), null);
              assert.equal(yield* tokens.verify(name, token.plaintext), null);
              const replacement = yield* registry.create(name, meta);
              assert.notEqual(replacement.id, original.id);
              assert.equal(
                yield* tokens.verify(name, token.plaintext),
                null,
                "old credentials must not authorize a replacement repository",
              );
              assert.equal((yield* tokens.list(name)).total, 0);
              const fresh = yield* tokens.issue(name, "read", 3600);
              assert.equal(yield* tokens.verify(name, fresh.plaintext), "read");
              assert.equal(yield* tokens.verify(other, unrelated.plaintext), "read");
            } finally {
              sql.exec(`DROP TRIGGER IF EXISTS ${trigger}`);
              for (const owned of [name, other]) {
                sql.exec("DELETE FROM tokens WHERE repo = ?", owned);
                sql.exec("DELETE FROM repos WHERE name = ?", owned);
              }
            }
          }).pipe(Effect.provide(layer)),
        );
      }),
    );
  runner.it("SQLite registry deletion failure: legacy orphan credentials", () =>
    withSql(async (sql) => {
      const name = `orphan-${crypto.randomUUID()}`;
      const meta = { description: null, defaultBranch: "main", readOnly: false, source: null };
      await Effect.runPromise(
        Effect.gen(function* () {
          const registry = yield* Registry;
          const tokens = yield* Tokens;
          yield* registry.create(name, meta);
          const token = yield* tokens.issue(name, "write", 3600);
          try {
            // Reproduce persisted state left by a pre-transaction deletion failure.
            sql.exec("DELETE FROM repos WHERE name = ?", name);
            const recreated = yield* registry.create(name, meta).pipe(Effect.result);
            assert.equal(
              recreated._tag,
              "Failure",
              "legacy orphan credentials must prevent name reuse",
            );
            if (recreated._tag === "Failure")
              assert.match(recreated.failure.message, /ORPHANED_TOKENS/);
            assert.equal(yield* registry.get(name), null);
            assert.equal(
              (yield* tokens.list(name)).total,
              1,
              "preserve suspect metadata for recovery",
            );
            // The fixture owns this offline reconciliation; normal create must not purge tokens.
            sql.exec("DELETE FROM tokens WHERE repo = ?", name);
            yield* registry.create(name, meta);
            assert.equal(yield* tokens.verify(name, token.plaintext), null);
          } finally {
            sql.exec("DELETE FROM tokens WHERE repo = ?", name);
            sql.exec("DELETE FROM repos WHERE name = ?", name);
          }
        }).pipe(Effect.provide(sqlite(sql))),
      );
    }),
  );
};
