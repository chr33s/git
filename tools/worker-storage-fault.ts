/** Local-verifier fault injection only. Call while all workerd/proxy handles are stopped. */
import assert from "node:assert/strict";
import * as fs from "node:fs/promises";
import * as path from "node:path";
import { DatabaseSync } from "node:sqlite";

/** Offline accounting reset for this verifier's repository after an old writer. */
export const reconcileGitCapacity = async (storage: string, repo: string) => {
  assert.equal(repo, "release-proof");
  const matches: string[] = [];
  const walk = async (directory: string): Promise<void> => {
    for (const entry of await fs.readdir(directory, { withFileTypes: true })) {
      const file = path.join(directory, entry.name);
      if (entry.isDirectory()) await walk(file);
      else if (entry.isFile() && entry.name.endsWith(".sqlite")) {
        const database = new DatabaseSync(file, { readOnly: true });
        try {
          if (
            database
              .prepare(
                "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'git_capacity'",
              )
              .get() !== undefined &&
            database.prepare("SELECT repo FROM git_capacity WHERE repo = ?").get(repo) !== undefined
          )
            matches.push(file);
        } finally {
          database.close();
        }
      }
    }
  };
  await walk(storage);
  assert.equal(matches.length, 1, "exactly one local SQLite owner must hold Git accounting");
  const file = matches[0];
  assert.ok(file);
  const database = new DatabaseSync(file);
  try {
    assert.equal(database.prepare("DELETE FROM git_capacity WHERE repo = ?").run(repo).changes, 1);
  } finally {
    database.close();
  }
};

/** Locate only the newly created test bucket, identified by its unique sentinel. */
export const snapshotFault = async (
  storage: string,
  sentinel: string,
  key: string,
  fault: "delete" | "write" | "none",
) => {
  const directory = path.join(storage, "r2", "cloudflare-runtime-R2BucketObject");
  const matches: string[] = [];
  for (const name of await fs.readdir(directory)) {
    if (!name.endsWith(".sqlite")) continue;
    const file = path.join(directory, name);
    const database = new DatabaseSync(file, { readOnly: true });
    try {
      if (
        database
          .prepare("SELECT name FROM sqlite_master WHERE type = 'table' AND name = '_mf_objects'")
          .get() !== undefined &&
        database.prepare("SELECT key FROM _mf_objects WHERE key = ?").get(sentinel) !== undefined
      )
        matches.push(file);
    } finally {
      database.close();
    }
  }
  assert.equal(
    matches.length,
    1,
    "fault injection requires exactly one sentinel-owned test bucket",
  );
  const file = matches[0];
  assert.ok(file);
  const database = new DatabaseSync(file);
  try {
    database.exec(
      "DROP TRIGGER IF EXISTS git_plus_verify_snapshot_delete; DROP TRIGGER IF EXISTS git_plus_verify_snapshot_insert; DROP TRIGGER IF EXISTS git_plus_verify_snapshot_update;",
    );
    const quoted = "'" + key.replaceAll("'", "''") + "'";
    if (fault === "delete")
      database.exec(
        `CREATE TRIGGER git_plus_verify_snapshot_delete BEFORE DELETE ON _mf_objects WHEN OLD.key = ${quoted} BEGIN SELECT RAISE(ABORT, 'injected snapshot delete failure'); END`,
      );
    if (fault === "write") {
      database.exec(
        `CREATE TRIGGER git_plus_verify_snapshot_insert BEFORE INSERT ON _mf_objects WHEN NEW.key = ${quoted} BEGIN SELECT RAISE(ABORT, 'injected snapshot write failure'); END`,
      );
      database.exec(
        `CREATE TRIGGER git_plus_verify_snapshot_update BEFORE UPDATE ON _mf_objects WHEN NEW.key = ${quoted} BEGIN SELECT RAISE(ABORT, 'injected snapshot write failure'); END`,
      );
    }
  } finally {
    database.close();
  }
};
