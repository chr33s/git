import { Config, Effect, Schema } from "effect";
import { StorageFailure } from "./Error.ts";

export interface Limits {
  readonly bytes: number;
  readonly rows: number;
}

export const defaults: Limits = { bytes: 8 * 1024 ** 2, rows: 100_000 };
export const configuration = Effect.fn("GitMetadataCapacity.configuration")(function* () {
  return {
    bytes: yield* Config.schema(
      Schema.Int.check(
        Schema.isGreaterThan(0),
        Schema.isLessThanOrEqualTo(Number.MAX_SAFE_INTEGER),
      ),
      "GIT_CF_REF_METADATA_BYTES",
    ).pipe(Config.withDefault(defaults.bytes)),
    rows: yield* Config.schema(
      Schema.Int.check(Schema.isGreaterThan(0), Schema.isLessThanOrEqualTo(1_000_000)),
      "GIT_CF_REF_METADATA_ROWS",
    ).pipe(Config.withDefault(defaults.rows)),
  };
});

const tables = [
  { name: "refs", columns: ["repo", "name", "oid"] },
  { name: "reflog", columns: ["repo", "name", "old_oid", "new_oid", "at", "message"] },
  { name: "shallow", columns: ["repo", "oid"] },
] as const;

/** Call prepare/check inside the same transaction as each metadata mutation. */
export const accounting = (sql: SqlStorage, repo: string, limits: Limits) => {
  const failed = (resource: string) =>
    new StorageFailure({ operation: `quota.metadata.${resource}`, path: repo });
  const size = (columns: readonly string[], prefix: string) =>
    columns.map((column) => `COALESCE(length(CAST(${prefix}${column} AS BLOB)), 0)`).join(" + ");
  sql.exec(`CREATE TABLE IF NOT EXISTS git_ref_capacity (
    repo TEXT PRIMARY KEY,
    bytes INTEGER NOT NULL CHECK(bytes >= 0),
    rows INTEGER NOT NULL CHECK(rows >= 0)
  )`);
  for (const table of tables) {
    const charge = `UPDATE git_ref_capacity SET bytes = bytes + (${size(table.columns, "NEW.")}), rows = rows + 1 WHERE repo = NEW.repo;`;
    const release = `UPDATE git_ref_capacity SET bytes = bytes - (${size(table.columns, "OLD.")}), rows = rows - 1 WHERE repo = OLD.repo;`;
    for (const operation of ["INSERT", "DELETE", "UPDATE"] as const)
      sql.exec(`CREATE TRIGGER IF NOT EXISTS git_ref_capacity_${table.name}_${operation}
        AFTER ${operation} ON ${table.name} BEGIN
        ${operation === "INSERT" ? charge : operation === "DELETE" ? release : release + charge}
      END`);
  }
  const read = () =>
    sql
      .exec<{ bytes: number; rows: number }>(
        "SELECT bytes, rows FROM git_ref_capacity WHERE repo = ?",
        repo,
      )
      .toArray()[0];
  const check = () => {
    const used = read();
    if (
      used === undefined ||
      !Number.isSafeInteger(used.bytes) ||
      used.bytes < 0 ||
      !Number.isSafeInteger(used.rows) ||
      used.rows < 0
    )
      throw failed("accounting");
    if (used.bytes > limits.bytes) throw failed("bytes");
    if (used.rows > limits.rows) throw failed("rows");
  };
  return {
    check,
    prepare: () => {
      if (read() === undefined) {
        // Bound legacy inventory by rows; SQL aggregates without buffering it in JS.
        const inventory = tables
          .map(
            (table) =>
              `SELECT ${size(table.columns, "")} AS size FROM ${table.name} WHERE repo = ?`,
          )
          .join(" UNION ALL ");
        const used = sql
          .exec<{ bytes: number; rows: number }>(
            `SELECT COALESCE(SUM(size), 0) AS bytes, COUNT(*) AS rows FROM (${inventory} LIMIT ?)`,
            repo,
            repo,
            repo,
            limits.rows + 1,
          )
          .one();
        if (used.rows > limits.rows) throw failed("rows");
        if (used.bytes > limits.bytes) throw failed("bytes");
        sql.exec(
          "INSERT INTO git_ref_capacity (repo, bytes, rows) VALUES (?, ?, ?)",
          repo,
          used.bytes,
          used.rows,
        );
      }
      check();
    },
  };
};
