/**
 * The registry/token contract against the backends node can host: in-memory,
 * the JSON-file form the self-hosted provider uses, and the DO SQLite tables
 * over `node:sqlite` — the same statements workerd runs, checked out here so
 * a SQL mistake fails fast. `Cloudflare.integration.ts` then runs the very
 * same suite inside the real Durable Object.
 */
import { DatabaseSync } from "node:sqlite";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { describe, it } from "@effect/vitest";

import { Effect, Layer } from "effect";

import { registryMemory, registryNode, tokensMemory, tokensNode } from "./Namespace.ts";
import { registryContract } from "./Registry.contract.ts";
import { type Sql, sqlite } from "./Sqlite.ts";

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
