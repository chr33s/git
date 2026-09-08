/**
 * The registry/token contract against the backends node can host: in-memory,
 * the JSON-file form the self-hosted provider uses, and the DO SQLite tables
 * over `node:sqlite` — the same statements workerd runs, checked out here so
 * a SQL mistake fails fast. `Cloudflare.integration.ts` then runs the very
 * same suite inside the real Durable Object.
 */
import { DatabaseSync } from "node:sqlite";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import assert from "node:assert/strict";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { describe, it } from "@effect/vitest";

import { Effect, Layer } from "effect";

import { Registry, registryMemory, registryNode, tokensMemory, tokensNode } from "./Namespace.ts";
import { registryContract } from "./Registry.contract.ts";
import { type Sql, sqlite } from "./Sqlite.ts";

it.effect("independent Node registry handles preserve and observe each other's rows", () =>
  Effect.promise(async () => {
    const root = await fs.mkdtemp(path.join(os.tmpdir(), "registry-shared-"));
    const open = () => Effect.runPromise(Registry.pipe(Effect.provide(registryNode(root))));
    const meta = { description: null, defaultBranch: "main", readOnly: false, source: null };
    try {
      const first = await open();
      const second = await open();
      await Effect.runPromise(first.create("first", meta));
      await Effect.runPromise(second.create("second", meta));
      const reopened = await open();
      assert.deepEqual(
        (await Effect.runPromise(reopened.list())).repos.map((repo) => repo.name),
        ["first", "second"],
      );
      assert.equal((await Effect.runPromise(first.get("second")))?.name, "second");
      await Effect.runPromise(first.delete("first"));
      await Effect.runPromise(second.touch("second", new Date(1700000000000)));
      assert.equal(await Effect.runPromise((await open()).get("first")), null);
    } finally {
      await fs.rm(root, { recursive: true, force: true });
    }
  }),
);

it.effect("separate processes retain every acknowledged registry create", () =>
  Effect.promise(async () => {
    const root = await fs.mkdtemp(path.join(os.tmpdir(), "registry-processes-"));
    const script = `
      import { Effect } from "effect";
      import { Registry, registryNode } from ${JSON.stringify(new URL("./Namespace.ts", import.meta.url).href)};
      const registry = await Effect.runPromise(Registry.pipe(Effect.provide(registryNode(process.argv[1]))));
      const meta = { description: null, defaultBranch: "main", readOnly: false, source: null };
      for (let index = 0; index < 12; index++) {
        for (let attempt = 0; ; attempt++) {
          try {
            await Effect.runPromise(registry.create(process.argv[2] + "-" + index, meta));
            break;
          } catch (error) {
            if (error.cause?.code !== "EEXIST" || attempt >= 500) throw error;
            await new Promise(resolve => setTimeout(resolve, 5));
          }
        }
      }
    `;
    try {
      const results = await Promise.allSettled(
        ["first", "second"].map((name) =>
          promisify(execFile)(process.execPath, ["--input-type=module", "-e", script, root, name], {
            timeout: 20_000,
          }),
        ),
      );
      for (const result of results) {
        if (result.status === "rejected") throw result.reason;
      }
      const reopened = await Effect.runPromise(Registry.pipe(Effect.provide(registryNode(root))));
      assert.deepEqual(
        (await Effect.runPromise(reopened.list())).repos.map((row) => row.name).sort(),
        ["first", "second"]
          .flatMap((name) => Array.from({ length: 12 }, (_, i) => `${name}-${i}`))
          .sort(),
      );
      assert.deepEqual(await fs.readdir(root), [".registry.json"]);
    } finally {
      await fs.rm(root, { recursive: true, force: true });
    }
  }),
);

registryContract(
  "Memory",
  {
    run: (effect) =>
      Effect.runPromise(effect.pipe(Effect.provide(Layer.mergeAll(registryMemory, tokensMemory)))),
  },
  { describe, it },
);

registryContract(
  "Node",
  {
    run: async (effect) => {
      const root = await fs.mkdtemp(path.join(os.tmpdir(), "registry-node-"));
      try {
        return await Effect.runPromise(
          effect.pipe(Effect.provide(Layer.mergeAll(registryNode(root), tokensNode(root)))),
        );
      } finally {
        await fs.rm(root, { recursive: true, force: true });
      }
    },
  },
  { describe, it },
);

/**
 * `node:sqlite` in the shape `DurableObjectStorage["sql"]` exposes. The DO
 * API takes one `exec` for everything; `node:sqlite` splits DDL (`exec`),
 * row-returning statements (`all`) and the rest (`run`).
 */
const nodeSql = (database: DatabaseSync): Sql => ({
  exec: <Row extends Record<string, ArrayBuffer | string | number | null>>(
    query: string,
    ...bindings: ReadonlyArray<string | number | null>
  ) => {
    const kind = query.trimStart().slice(0, 6).toUpperCase();
    if (kind === "CREATE" || kind === "DROP") {
      database.exec(query);
      return { toArray: (): Row[] => [] };
    }
    const statement = database.prepare(query);
    if (kind !== "SELECT") {
      statement.run(...bindings);
      return { toArray: (): Row[] => [] };
    }
    // SAFETY: the caller names `Row` after the columns its SELECT projects,
    // and the suite's tables hold only TEXT and INTEGER values, so every row
    // `node:sqlite` hands back already has that form.
    return { toArray: () => statement.all(...bindings) as Row[] };
  },
});

it.effect("migrates an existing SQLite registry without hiding completed repositories", () =>
  Effect.promise(async () => {
    const database = new DatabaseSync(":memory:");
    try {
      database.exec(`CREATE TABLE repos (
        id TEXT NOT NULL, name TEXT PRIMARY KEY, description TEXT,
        default_branch TEXT NOT NULL, read_only INTEGER NOT NULL, source TEXT,
        created_at TEXT NOT NULL, updated_at TEXT NOT NULL, last_push_at TEXT
      );
      INSERT INTO repos VALUES ('old-id', 'legacy', NULL, 'trunk', 0, NULL,
        '2026-09-01T00:00:00.000Z', '2026-09-01T00:00:00.000Z', NULL);`);
      const registry = await Effect.runPromise(
        Registry.pipe(Effect.provide(sqlite(nodeSql(database)))),
      );
      const legacy = await Effect.runPromise(registry.get("legacy"));
      assert.equal(legacy?.id, "old-id");
      assert.equal(legacy?.defaultBranch, "trunk");
      assert.equal(legacy?.initializing, null);
      const pending = await Effect.runPromise(
        registry.create("new", {
          description: null,
          defaultBranch: "main",
          readOnly: false,
          source: null,
          initializing: "fork",
        }),
      );
      const reopened = await Effect.runPromise(
        Registry.pipe(Effect.provide(sqlite(nodeSql(database)))),
      );
      assert.equal((await Effect.runPromise(reopened.get("new")))?.initializing, "fork");
      await Effect.runPromise(reopened.finish("new", pending.id));
      assert.equal((await Effect.runPromise(registry.get("new")))?.initializing, null);
    } finally {
      database.close();
    }
  }),
);

registryContract(
  "SQLite",
  {
    run: (effect) => {
      const database = new DatabaseSync(":memory:");
      return Effect.runPromise(effect.pipe(Effect.provide(sqlite(nodeSql(database))))).finally(
        () => {
          database.close();
        },
      );
    },
  },
  { describe, it },
);
