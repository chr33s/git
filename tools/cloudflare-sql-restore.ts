/** Private SQL import. Catalog DDL must come from an authenticated, trusted backup. */
import type { DurableObjectStorage } from "@cloudflare/workers-types";
import { Schema } from "effect";
import {
  SqlCatalog,
  SqlPage,
  captureSqlPage,
  captureSqlCatalog,
  sqlRestoreTable,
} from "./cloudflare-sql-backup.ts";
import { decodeBackupFrame, encodeBackupFrame } from "./cloudflare-backup-codec.ts";

type Storage = Pick<DurableObjectStorage, "sql" | "transactionSync">;
const Progress = Schema.Struct({
  snapshotId: Schema.NonEmptyString.check(Schema.isMaxLength(128)),
  catalog: SqlCatalog,
  tableIndex: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  after: Schema.NullOr(Schema.String),
});
const quote = (name: string) => `"${name.replaceAll('"', '""')}"`;
const integer = (value: string) => {
  if (value.length > 20 || !/^-?(0|[1-9][0-9]*)$/.test(value))
    throw new Error("Invalid SQL integer");
  const parsed = BigInt(value);
  if (parsed < -(1n << 63n) || parsed >= 1n << 63n) throw new Error("Invalid SQL integer");
  return parsed;
};
const transaction = <A>(storage: Storage, run: () => A): A => {
  try {
    return storage.transactionSync(run);
  } catch {
    // SQLite errors can contain backup data, schema text or private identifiers.
    throw new Error("SQL restore transaction rejected");
  }
};
const read = (storage: Storage, snapshotId: string) => {
  const row = storage.sql
    .exec<{ frame: string }>(`SELECT frame FROM ${sqlRestoreTable} WHERE id = 1`)
    .one();
  const progress = decodeBackupFrame(row.frame, Progress);
  if (progress.snapshotId !== snapshotId) throw new Error("SQL restore snapshot mismatch");
  return progress;
};

export const beginSqlRestore = (storage: Storage, snapshotId: string, frame: string) => {
  const catalog = decodeBackupFrame(frame, SqlCatalog);
  const progress = encodeBackupFrame(
    Schema.decodeSync(Progress)({ snapshotId, catalog, tableIndex: 0, after: null }),
  );
  transaction(storage, () => {
    const existing = storage.sql
      .exec(
        "SELECT name FROM sqlite_schema WHERE name NOT GLOB 'sqlite_*' AND name NOT GLOB '_cf_*' AND name != '__cf_kv' LIMIT 1",
      )
      .toArray();
    if (existing.length !== 0) throw new Error("SQL target must be empty");
    if (new Set(catalog.objects.map((object) => object.name)).size !== catalog.objects.length)
      throw new Error("Duplicate SQL catalog object");
    for (const object of catalog.objects) {
      if (
        object.name === sqlRestoreTable ||
        object.name.startsWith("sqlite_") ||
        object.name.startsWith("_cf_") ||
        object.name === "__cf_kv"
      )
        throw new Error("Reserved SQL catalog object");
      if (object.type === "table") {
        // Sequence restoration is not implemented; never silently reset AUTOINCREMENT.
        if (/\bAUTOINCREMENT\b/i.test(object.sql)) throw new Error("Unsupported SQL sequence");
        storage.sql.exec(object.sql);
        // Enforce the same supported table/column shapes as capture before accepting data.
        captureSqlPage(storage.sql, object.name, null, 1);
      }
    }
    storage.sql.exec(
      `CREATE TABLE ${sqlRestoreTable} (id INTEGER PRIMARY KEY CHECK(id = 1), frame TEXT NOT NULL)`,
    );
    storage.sql.exec(`INSERT INTO ${sqlRestoreTable} VALUES (1, ?)`, progress);
  });
};

export const restoreSqlPage = (storage: Storage, snapshotId: string, frame: string) => {
  const page = decodeBackupFrame(frame, SqlPage);
  transaction(storage, () => {
    const progress = read(storage, snapshotId);
    const tables = progress.catalog.objects.filter((object) => object.type === "table");
    if (tables[progress.tableIndex]?.name !== page.table || progress.after !== page.after)
      throw new Error("SQL page out of sequence");
    const info = storage.sql
      .exec<{ name: string }>("SELECT name FROM pragma_table_xinfo(?) ORDER BY cid", page.table)
      .toArray();
    const columns = info.map((column) => column.name);
    if (JSON.stringify(columns) !== JSON.stringify(page.columns))
      throw new Error("SQL columns differ");
    const rowid = ["rowid", "_rowid_", "oid"].find(
      (alias) => !columns.some((column) => column.toLowerCase() === alias),
    );
    if (rowid === undefined) throw new Error("SQL row identifier shadowed");
    let previous = page.after === null ? undefined : integer(page.after);
    for (const row of page.rows) {
      const current = integer(row.rowid);
      if ((previous !== undefined && current <= previous) || row.cells.length !== columns.length)
        throw new Error("Invalid SQL row order or width");
      const placeholders = row.cells.map((cell) => {
        if (cell.type === "integer") integer(cell.value);
        return cell.type === "integer" ? "CAST(? AS INTEGER)" : "?";
      });
      storage.sql.exec(
        `INSERT INTO ${quote(page.table)} (${[rowid, ...columns].map(quote).join(", ")}) VALUES (CAST(? AS INTEGER), ${placeholders.join(", ")})`,
        row.rowid,
        ...row.cells.map((cell) => cell.value),
      );
      previous = current;
    }
    if (page.rows.length > 0) {
      const restored = decodeBackupFrame(
        captureSqlPage(storage.sql, page.table, page.after, page.rows.length),
        SqlPage,
      );
      if (encodeBackupFrame(restored.rows) !== encodeBackupFrame(page.rows))
        throw new Error("SQL restored rows differ");
    }
    if (page.next !== null && (page.rows.length === 0 || page.next !== page.rows.at(-1)?.rowid))
      throw new Error("Invalid SQL continuation");
    storage.sql.exec(
      `UPDATE ${sqlRestoreTable} SET frame = ? WHERE id = 1`,
      encodeBackupFrame({
        ...progress,
        tableIndex: progress.tableIndex + (page.next === null ? 1 : 0),
        after: page.next,
      }),
    );
  });
};

/** Call only after the outer coordinator has verified all SQL/KV/R2 inventory and policy. */
export const finishSqlRestore = (storage: Storage, snapshotId: string) =>
  transaction(storage, () => {
    const progress = read(storage, snapshotId);
    if (!sqlRestoreComplete(storage, snapshotId)) throw new Error("SQL restore incomplete");
    for (const object of progress.catalog.objects)
      if (object.type !== "table") storage.sql.exec(object.sql);
    if (!storage.sql.exec("PRAGMA foreign_key_check").next().done)
      throw new Error("SQL foreign key violation");
    storage.sql.exec(`DROP TABLE ${sqlRestoreTable}`);
    if (captureSqlCatalog(storage.sql) !== encodeBackupFrame(progress.catalog))
      throw new Error("Restored SQL catalog differs");
  });

/** Data completion only; deferred schema objects remain pending until signed finalization. */
export const sqlRestoreComplete = (storage: Storage, snapshotId: string) => {
  const progress = read(storage, snapshotId);
  return (
    progress.tableIndex ===
      progress.catalog.objects.filter((object) => object.type === "table").length &&
    progress.after === null
  );
};
