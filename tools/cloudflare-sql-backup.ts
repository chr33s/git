/** Bounded SQL capture. Caller must keep all writers quiesced across every page. */
import type { SqlStorage } from "@cloudflare/workers-types";
import { Schema } from "effect";
import { encodeBackupFrame } from "./cloudflare-backup-codec.ts";

const Name = Schema.NonEmptyString;
export const sqlRestoreTable = "git_plus_sql_restore_v1";
export const SqlCatalog = Schema.Struct({
  kind: Schema.Literal("durable-sql-catalog"),
  objects: Schema.Array(
    Schema.Struct({
      type: Schema.Literals(["table", "index", "trigger", "view"]),
      name: Name,
      table: Name,
      sql: Schema.String,
    }),
  ).check(Schema.isMaxLength(256)),
});
const Cell = Schema.Union([
  Schema.Struct({ type: Schema.Literal("null"), value: Schema.Null }),
  Schema.Struct({ type: Schema.Literal("integer"), value: Schema.String }),
  Schema.Struct({ type: Schema.Literal("real"), value: Schema.Finite }),
  Schema.Struct({ type: Schema.Literal("text"), value: Schema.String }),
  Schema.Struct({ type: Schema.Literal("blob"), value: Schema.instanceOf(ArrayBuffer) }),
]);
export const SqlPage = Schema.Struct({
  kind: Schema.Literal("durable-sql-rows"),
  table: Name,
  after: Schema.NullOr(Schema.String),
  next: Schema.NullOr(Schema.String),
  columns: Schema.Array(Name),
  rows: Schema.Array(Schema.Struct({ rowid: Schema.String, cells: Schema.Array(Cell) })).check(
    Schema.isMaxLength(16),
  ),
});
export interface SqlPage extends Schema.Schema.Type<typeof SqlPage> {}

const quote = (name: string) => `"${name.replaceAll('"', '""')}"`;

export const captureSqlCatalog = (sql: SqlStorage) => {
  if (
    sql.exec("SELECT name FROM sqlite_schema WHERE name = ?", sqlRestoreTable).toArray().length !==
    0
  )
    throw new Error("Cannot capture an unfinished SQL restore");
  const objects = sql
    .exec<{ type: string; name: string; table: string; sql: string }>(
      `SELECT type, name, tbl_name AS "table", sql FROM sqlite_schema
       WHERE name NOT GLOB 'sqlite_*' AND name NOT GLOB '_cf_*'
       AND name != '__cf_kv' AND sql IS NOT NULL ORDER BY type, name LIMIT 257`,
    )
    .toArray();
  if (objects.length > 256) throw new Error("SQL catalog exceeds object budget");
  return encodeBackupFrame({ kind: "durable-sql-catalog", objects });
};

export const captureSqlPage = (sql: SqlStorage, table: string, after: string | null, limit = 8) => {
  if (!Number.isInteger(limit) || limit < 1 || limit > 16)
    throw new Error("Invalid SQL capture page size");
  if (after !== null && (after.length > 20 || !/^-?(0|[1-9][0-9]*)$/.test(after)))
    throw new Error("Invalid SQL capture cursor");
  if (after !== null && (BigInt(after) < -(1n << 63n) || BigInt(after) >= 1n << 63n))
    throw new Error("SQL capture cursor exceeds integer range");
  const tables = sql
    .exec<{ name: string; type: string; wr: number }>(
      "SELECT name, type, wr FROM pragma_table_list WHERE name = ? LIMIT 2",
      table,
    )
    .toArray();
  const found = tables[0];
  if (
    tables.length !== 1 ||
    found?.type !== "table" ||
    found.wr !== 0 ||
    table.startsWith("sqlite_") ||
    table.startsWith("_cf_") ||
    table === "__cf_kv" ||
    table === sqlRestoreTable
  )
    throw new Error("Unsupported SQL capture table");
  const info = sql
    .exec<{ name: string; hidden: number }>(
      "SELECT name, hidden FROM pragma_table_xinfo(?) ORDER BY cid LIMIT 257",
      table,
    )
    .toArray();
  if (info.length === 0 || info.length > 256 || info.some((column) => column.hidden !== 0))
    throw new Error("Unsupported SQL capture columns");
  const columns = info.map((column) => column.name);
  const rowid = ["rowid", "_rowid_", "oid"].find(
    (alias) => !columns.some((column) => column.toLowerCase() === alias),
  );
  if (rowid === undefined) throw new Error("SQL row identifier is shadowed");
  const expressions = columns.flatMap((column) => {
    const name = quote(column);
    return [
      `typeof(${name})`,
      `CASE WHEN typeof(${name}) = 'integer' THEN CAST(${name} AS TEXT) ELSE ${name} END`,
    ];
  });
  const cursor = sql.exec(
    `SELECT CAST(${quote(rowid)} AS TEXT), ${expressions.join(", ")} FROM ${quote(table)}
     ${after === null ? "" : `WHERE ${quote(rowid)} > CAST(? AS INTEGER)`}
     ORDER BY ${quote(rowid)} LIMIT ?`,
    ...(after === null ? [limit + 1] : [after, limit + 1]),
  );
  const rows = [];
  let more = false;
  for (const raw of cursor.raw()) {
    if (rows.length === limit) {
      more = true;
      break;
    }
    const cells = columns.map((_, index) => ({
      type: raw[1 + index * 2],
      value: raw[2 + index * 2],
    }));
    rows.push({ rowid: raw[0], cells });
  }
  const page = Schema.decodeUnknownSync(SqlPage)({
    kind: "durable-sql-rows",
    table,
    after,
    next: more ? rows.at(-1)?.rowid : null,
    columns,
    rows,
  });
  return encodeBackupFrame(page);
};
