import { Config, Effect, Schema } from "effect";
import { StorageFailure } from "../git/Error.ts";
import type { Sql } from "../git/Sql.ts";

export interface Limits {
  readonly bytes: number;
  readonly rows: number;
}
export const defaults: Limits = { bytes: 1024 ** 2, rows: 128 };
export const configuration = Effect.fn("RegistryCapacity.configuration")(function* (
  prefix: "GIT" | "GIT_CF" = "GIT_CF",
) {
  return {
    bytes: yield* Config.schema(
      Schema.Int.check(
        Schema.isGreaterThan(0),
        Schema.isLessThanOrEqualTo(Number.MAX_SAFE_INTEGER),
      ),
      `${prefix}_REGISTRY_BYTES`,
    ).pipe(Config.withDefault(defaults.bytes)),
    rows: yield* Config.schema(
      Schema.Int.check(Schema.isGreaterThan(0), Schema.isLessThanOrEqualTo(4096)),
      `${prefix}_REGISTRY_ROWS`,
    ).pipe(Config.withDefault(defaults.rows)),
  };
});

const columns = {
  remotes: ["repo", "name", "url", "credential", "key", "sync", "created_at"],
  webhooks: ["id", "repo", "url", "secret", "created_at"],
} as const;
type Table = keyof typeof columns;

/** One SQL statement both checks capacity and inserts; no read/write race. */
export const insert = (
  db: Sql,
  table: Table,
  repo: string,
  values: readonly (string | null)[],
  limits: Limits,
): void => {
  if (values.length !== columns[table].length)
    throw new StorageFailure({ operation: "registry.columns", path: repo });
  const bytes = values.reduce(
    (total, value) => total + (value === null ? 0 : new TextEncoder().encode(value).byteLength),
    0,
  );
  const size = columns[table]
    .map((column) => `COALESCE(length(CAST(${column} AS BLOB)), 0)`)
    .join(" + ");
  db.exec(
    `INSERT INTO ${table} (${columns[table].join(", ")})
    SELECT ${values.map(() => "?").join(", ")}
    FROM (SELECT COUNT(*) AS used_rows, COALESCE(SUM(size), 0) AS used_bytes
      FROM (SELECT ${size} AS size FROM ${table} WHERE repo = ? LIMIT ?))
    WHERE used_rows < ? AND used_bytes <= ?`,
    ...values,
    repo,
    limits.rows + 1,
    limits.rows,
    limits.bytes - bytes,
  );
  if (db.exec<{ count: number }>("SELECT changes() AS count").toArray()[0]?.count !== 1)
    throw new StorageFailure({ operation: "quota.registry", path: repo });
};

export const report = (table: Table) =>
  Effect.fn("RegistryCapacity.report")(function* (error: StorageFailure) {
    if (error.operation === "quota.registry")
      yield* Effect.logWarning("Administrative registry capacity exhausted", {
        component: "Registry",
        event: "quota_exceeded",
        registry: table,
      });
  });
