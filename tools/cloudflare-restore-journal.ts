/** Private SQLite journal shared by namespace restore operations. */
import * as fs from "node:fs/promises";
import * as path from "node:path";
import { DatabaseSync } from "node:sqlite";
import { Invalid } from "../src/git/Error.ts";

const invalid = () => new Invalid({ field: "backup", reason: "namespace restore rejected" });

export const openRestoreJournal = async (directory: string) => {
  try {
    await fs.mkdir(directory, { mode: 0o700 });
  } catch (error) {
    if (!(error instanceof Error && "code" in error && error.code === "EEXIST")) throw error;
  }
  const folder = await fs.lstat(directory);
  if (!folder.isDirectory() || (folder.mode & 0o077) !== 0) throw invalid();
  const file = path.join(directory, "restore.sqlite");
  try {
    await fs.writeFile(file, "", { flag: "wx", mode: 0o600 });
  } catch (error) {
    if (!(error instanceof Error && "code" in error && error.code === "EEXIST")) throw error;
  }
  const stat = await fs.lstat(file);
  if (!stat.isFile() || (stat.mode & 0o077) !== 0) throw invalid();
  const database = new DatabaseSync(file);
  try {
    // Keep the OS-backed exclusive lock across commits; process exit releases it.
    database.exec(
      "PRAGMA busy_timeout=0; PRAGMA locking_mode=EXCLUSIVE; PRAGMA journal_mode=DELETE; PRAGMA synchronous=FULL; PRAGMA fullfsync=ON; PRAGMA cache_size=-2048; BEGIN EXCLUSIVE; CREATE TABLE IF NOT EXISTS meta (id INTEGER PRIMARY KEY CHECK(id=1), identity TEXT NOT NULL, phase TEXT NOT NULL); CREATE TABLE IF NOT EXISTS objects (sequence INTEGER PRIMARY KEY, source TEXT NOT NULL UNIQUE, target TEXT NOT NULL UNIQUE, receipt TEXT); CREATE TABLE IF NOT EXISTS release_meta (id INTEGER PRIMARY KEY CHECK(id=1), verification TEXT NOT NULL); CREATE TABLE IF NOT EXISTS release_objects (sequence INTEGER PRIMARY KEY, phase TEXT NOT NULL); COMMIT",
    );
    const handle = await fs.open(directory, "r");
    try {
      await handle.sync();
    } finally {
      await handle.close();
    }
    const parent = await fs.open(path.dirname(path.resolve(directory)), "r");
    try {
      await parent.sync();
    } finally {
      await parent.close();
    }
    return database;
  } catch (error) {
    database.close();
    throw error;
  }
};
