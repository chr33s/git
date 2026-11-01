/** Offline legacy-fixture setup for one newly created local verifier namespace. */
import assert from "node:assert/strict";
import * as fs from "node:fs/promises";
import * as path from "node:path";
import { DatabaseSync } from "node:sqlite";

export const seedLegacyRef = async (
  storage: string,
  namespace: string,
  repo: string,
  oid: string,
) => {
  assert.match(namespace, /^git-git-verify-worker-[a-z0-9-]+-Repo$/);
  assert.match(oid, /^[a-f0-9]{40}$/);
  const directory = path.join(storage, namespace);
  const matches: string[] = [];
  for (const name of await fs.readdir(directory)) {
    if (!name.endsWith(".sqlite")) continue;
    const file = path.join(directory, name);
    const database = new DatabaseSync(file, { readOnly: true });
    try {
      if (
        database
          .prepare("SELECT name FROM sqlite_master WHERE type = 'table' AND name = '_cf_KV'")
          .get() !== undefined &&
        database.prepare("SELECT key FROM _cf_KV WHERE key = ?").get(`snapshot-seq:${repo}`) !==
          undefined
      )
        matches.push(file);
    } finally {
      database.close();
    }
  }
  assert.equal(matches.length, 1, "legacy setup requires exactly one initialized test repository");
  const file = matches[0];
  assert.ok(file);
  const database = new DatabaseSync(file);
  try {
    assert.equal(
      database.prepare("SELECT count(*) AS count FROM refs WHERE repo = ?").get(repo)?.count,
      0,
    );
    assert.equal(
      database
        .prepare("SELECT count(*) AS count FROM git_plus_provisioning WHERE repo = ?")
        .get(repo)?.count,
      0,
    );
    database
      .prepare("INSERT INTO refs (repo, name, oid) VALUES (?, 'refs/heads/main', ?)")
      .run(repo, oid);
  } finally {
    database.close();
  }
  // Only undo the exact fixture ref, with the verifier runtime stopped again.
  return () => {
    const cleanup = new DatabaseSync(file);
    try {
      assert.equal(
        cleanup.prepare("SELECT count(*) AS count FROM refs WHERE repo = ?").get(repo)?.count,
        1,
      );
      assert.equal(
        cleanup
          .prepare("SELECT count(*) AS count FROM git_plus_provisioning WHERE repo = ?")
          .get(repo)?.count,
        0,
      );
      assert.equal(
        cleanup
          .prepare("DELETE FROM refs WHERE repo = ? AND name = 'refs/heads/main' AND oid = ?")
          .run(repo, oid).changes,
        1,
      );
    } finally {
      cleanup.close();
    }
  };
};
