import assert from "node:assert/strict";
import { Effect, Layer, Logger } from "effect";
import type { Sql } from "../git/Sql.ts";
import type { Runner } from "../git/Store.contract.ts";
import * as Remotes from "./Remotes.ts";
import * as Subscribers from "./Subscribers.ts";
import type { Limits } from "./RegistryCapacity.ts";

export const registryCapacityContract = (db: Sql, runner: Runner): void => {
  for (const table of ["remotes", "webhooks"] as const)
    for (const mode of ["rows", "bytes", "legacy", "concurrent", "utf8"] as const)
      runner.it(`SQL registry quota: ${table}/${mode}`, async () => {
        const repo = `registry-capacity-${crypto.randomUUID()}`;
        const secret = "private-secret-恢复";
        const messages: string[] = [];
        const logger = Logger.make<unknown, void>(({ message }) => {
          messages.push(JSON.stringify(message));
        });
        const limits = { rows: 1, bytes: mode === "bytes" ? 1 : 1024 ** 2 };
        const layers = (capacity: Limits) =>
          Layer.mergeAll(
            Remotes.sql(db, repo, capacity),
            Subscribers.sql(db, repo, capacity),
            Logger.layer([logger]),
          );
        const add = (name: string, capacity = limits) =>
          Effect.gen(function* () {
            const remotes = yield* Remotes.Remotes;
            const webhooks = yield* Subscribers.Subscribers;
            return yield* table === "remotes"
              ? remotes
                  .add({ name, url: "https://example.com/repo", credential: secret })
                  .pipe(Effect.map((value) => value.name))
              : webhooks
                  .add({ url: "https://example.com/hook", secret })
                  .pipe(Effect.map((value) => value.id));
          }).pipe(Effect.provide(layers(capacity)));
        const list = () =>
          Effect.runPromise(
            Effect.gen(function* () {
              const remotes = yield* Remotes.Remotes;
              const webhooks = yield* Subscribers.Subscribers;
              return yield* table === "remotes"
                ? remotes.list.pipe(Effect.map((rows) => rows.map((row) => row.name)))
                : webhooks.list.pipe(Effect.map((rows) => rows.map((row) => row.id)));
            }).pipe(Effect.provide(layers(limits))),
          );
        const remove = (id: string) =>
          Effect.runPromise(
            Effect.gen(function* () {
              const remotes = yield* Remotes.Remotes;
              const webhooks = yield* Subscribers.Subscribers;
              return yield* table === "remotes" ? remotes.remove(id) : webhooks.remove(id);
            }).pipe(Effect.provide(layers(limits))),
          );
        const refuse = async (name: string, capacity = limits) => {
          const result = await Effect.runPromise(add(name, capacity).pipe(Effect.result));
          assert.equal(result._tag, "Failure");
          if (result._tag === "Failure") {
            assert.equal(result.failure._tag, "StorageFailure");
            if (result.failure._tag === "StorageFailure")
              assert.equal(result.failure.operation, "quota.registry");
          }
        };
        try {
          if (mode === "bytes") {
            await refuse("one");
            assert.deepEqual(await list(), []);
          } else if (mode === "concurrent") {
            const results = await Promise.all(
              ["one", "two"].map((name) => Effect.runPromise(add(name).pipe(Effect.result))),
            );
            assert.equal(results.filter((result) => result._tag === "Success").length, 1);
            assert.equal(results.filter((result) => result._tag === "Failure").length, 1);
            assert.equal((await list()).length, 1);
          } else {
            await Effect.runPromise(add("one"));
            if (mode === "legacy")
              await Effect.runPromise(add("two", { bytes: 1024 ** 2, rows: 10 }));
            const before = await list();
            if (mode === "utf8") {
              const row = db
                .exec<Record<string, string | null>>(`SELECT * FROM ${table} WHERE repo = ?`, repo)
                .toArray()[0];
              assert.ok(row);
              const bytes = Object.values(row).reduce(
                (total, value) =>
                  total + (value === null ? 0 : new TextEncoder().encode(value).byteLength),
                0,
              );
              await refuse("two", { bytes: bytes * 2 - 1, rows: 10 });
              assert.deepEqual(await list(), before);
              await Effect.runPromise(add("two", { bytes: bytes * 2, rows: 10 }));
              assert.equal((await list()).length, 2);
            } else {
              await refuse("new");
              assert.deepEqual(await list(), before);
            }
          }
          for (const id of await list()) assert.equal(await remove(id), true);
          await Effect.runPromise(add("new", { bytes: 1024 ** 2, rows: 1 }));
          assert.equal((await list()).length, 1);
          assert.match(messages.join(""), /quota_exceeded/);
          assert.ok(!messages.join("").includes(secret));
          assert.ok(!messages.join("").includes(repo));
          assert.doesNotMatch(messages.join(""), /https:/);
        } finally {
          db.exec("DELETE FROM remotes WHERE repo = ?", repo);
          db.exec("DELETE FROM webhooks WHERE repo = ?", repo);
        }
      });
};
