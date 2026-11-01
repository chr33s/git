import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import * as fs from "node:fs/promises";
import { tmpdir } from "node:os";
import * as path from "node:path";
import { promisify } from "node:util";
import { it } from "@effect/vitest";
import { openRestoreJournal } from "../../tools/cloudflare-restore-journal.ts";

const crash = `
  import { openRestoreJournal } from ${JSON.stringify(new URL("../../tools/cloudflare-restore-journal.ts", import.meta.url).href)};
  const [directory, boundary] = process.argv.slice(1);
  const database = await openRestoreJournal(directory);
  database.exec("BEGIN IMMEDIATE");
  database.prepare("INSERT INTO meta VALUES (1, ?, 'restoring')").run("authenticated-archive-identity");
  database.prepare("INSERT INTO objects VALUES (0, ?, ?, NULL)").run("a".repeat(64), "b".repeat(64));
  if (boundary === "mapping-uncommitted") process.kill(process.pid, "SIGKILL");
  database.exec("COMMIT");
  if (boundary === "mapping-committed") process.kill(process.pid, "SIGKILL");
  if (boundary === "receipt-uncommitted") database.exec("BEGIN IMMEDIATE");
  database.prepare("UPDATE objects SET receipt = ? WHERE sequence = 0").run("complete-receipt");
  if (boundary === "receipt-uncommitted" || boundary === "receipt-committed") process.kill(process.pid, "SIGKILL");
  database.exec("UPDATE meta SET phase = 'ready' WHERE id = 1");
  process.kill(process.pid, "SIGKILL");
`;

for (const boundary of [
  "mapping-uncommitted",
  "mapping-committed",
  "receipt-uncommitted",
  "receipt-committed",
  "ready-committed",
] as const) {
  it(`reopens the exclusive restore journal after SIGKILL at ${boundary}`, async () => {
    const scratch = await fs.mkdtemp(path.join(tmpdir(), "restore-journal-crash-"));
    const directory = path.join(scratch, "journal");
    try {
      await assert.rejects(
        promisify(execFile)(
          process.execPath,
          ["--input-type=module", "-e", crash, directory, boundary],
          { timeout: 20_000 },
        ),
        (error: Error) => "signal" in error && error.signal === "SIGKILL",
      );
      // Reacquisition proves the dead process leaves no stale application lock.
      const database = await openRestoreJournal(directory);
      try {
        assert.equal(database.prepare("PRAGMA integrity_check").get()?.integrity_check, "ok");
        const meta = database.prepare("SELECT identity, phase FROM meta").get();
        const object = database.prepare("SELECT source, target, receipt FROM objects").get();
        if (boundary === "mapping-uncommitted") {
          assert.equal(meta, undefined);
          assert.equal(object, undefined, "uncommitted mapping and identity roll back together");
          database.exec(
            "BEGIN IMMEDIATE; INSERT INTO meta VALUES (1, 'retry', 'restoring'); COMMIT",
          );
        } else {
          assert.equal(meta?.identity, "authenticated-archive-identity");
          assert.equal(meta?.phase, boundary === "ready-committed" ? "ready" : "restoring");
          assert.equal(object?.source, "a".repeat(64));
          assert.equal(object?.target, "b".repeat(64));
          assert.equal(
            object?.receipt,
            boundary === "receipt-committed" || boundary === "ready-committed"
              ? "complete-receipt"
              : null,
          );
          // The recovered journal remains writable, including after hot-journal rollback.
          database.exec("UPDATE meta SET phase = 'ready' WHERE id = 1");
        }
      } finally {
        database.close();
      }
      assert.equal((await fs.stat(directory)).mode & 0o777, 0o700);
      assert.equal((await fs.stat(path.join(directory, "restore.sqlite"))).mode & 0o777, 0o600);
    } finally {
      await fs.rm(scratch, { recursive: true, force: true });
    }
  });
}

const releaseCrash = `
  import { openRestoreJournal } from ${JSON.stringify(new URL("../../tools/cloudflare-restore-journal.ts", import.meta.url).href)};
  const [directory, boundary] = process.argv.slice(1);
  const database = await openRestoreJournal(directory);
  database.exec("BEGIN IMMEDIATE");
  database.prepare("INSERT INTO meta VALUES (1, ?, 'ready')").run("authenticated-archive-identity");
  database.prepare("INSERT INTO objects VALUES (0, ?, ?, ?)").run("a".repeat(64), "b".repeat(64), "complete-receipt");
  database.exec("COMMIT");
  database.exec("BEGIN IMMEDIATE");
  database.prepare("INSERT INTO release_meta VALUES (1, ?)").run("c".repeat(64));
  database.exec("INSERT INTO release_objects SELECT sequence, 'pending' FROM objects; UPDATE meta SET phase = 'releasing' WHERE id = 1");
  if (boundary === "intent-uncommitted") process.kill(process.pid, "SIGKILL");
  database.exec("COMMIT");
  if (boundary === "intent-committed") process.kill(process.pid, "SIGKILL");
  if (boundary === "retirement-uncommitted") database.exec("BEGIN IMMEDIATE");
  database.exec("UPDATE release_objects SET phase = 'retired' WHERE sequence = 0");
  if (boundary === "retirement-uncommitted" || boundary === "retirement-committed") process.kill(process.pid, "SIGKILL");
  if (boundary === "release-uncommitted") database.exec("BEGIN IMMEDIATE");
  database.exec("UPDATE release_objects SET phase = 'released' WHERE sequence = 0");
  if (boundary === "release-uncommitted" || boundary === "release-committed") process.kill(process.pid, "SIGKILL");
  if (boundary === "completion-uncommitted") database.exec("BEGIN IMMEDIATE");
  database.exec("UPDATE meta SET phase = 'released' WHERE id = 1");
  process.kill(process.pid, "SIGKILL");
`;

for (const [boundary, phase, progress] of [
  ["intent-uncommitted", "ready", undefined],
  ["intent-committed", "releasing", "pending"],
  ["retirement-uncommitted", "releasing", "pending"],
  ["retirement-committed", "releasing", "retired"],
  ["release-uncommitted", "releasing", "retired"],
  ["release-committed", "releasing", "released"],
  ["completion-uncommitted", "releasing", "released"],
  ["completion-committed", "released", "released"],
] as const) {
  it(`recovers release progress after SIGKILL at ${boundary}`, async () => {
    const scratch = await fs.mkdtemp(path.join(tmpdir(), "release-journal-crash-"));
    const directory = path.join(scratch, "journal");
    try {
      await assert.rejects(
        promisify(execFile)(
          process.execPath,
          ["--input-type=module", "-e", releaseCrash, directory, boundary],
          { timeout: 20_000 },
        ),
        (error: Error) => "signal" in error && error.signal === "SIGKILL",
      );
      const database = await openRestoreJournal(directory);
      try {
        assert.equal(database.prepare("PRAGMA integrity_check").get()?.integrity_check, "ok");
        assert.equal(database.prepare("SELECT phase FROM meta").get()?.phase, phase);
        assert.equal(
          database.prepare("SELECT receipt FROM objects").get()?.receipt,
          "complete-receipt",
        );
        assert.equal(database.prepare("SELECT phase FROM release_objects").get()?.phase, progress);
        assert.equal(
          database.prepare("SELECT verification FROM release_meta").get()?.verification,
          progress === undefined ? undefined : "c".repeat(64),
        );
        // Readiness cannot be published without its matching verification/progress transaction.
        assert.equal(
          database.prepare("SELECT COUNT(*) AS count FROM release_objects").get()?.count,
          progress === undefined ? 0 : 1,
        );
        database.exec("BEGIN IMMEDIATE; COMMIT");
      } finally {
        database.close();
      }
    } finally {
      await fs.rm(scratch, { recursive: true, force: true });
    }
  });
}

it("refuses non-private or symlinked restore journal paths", async () => {
  const scratch = await fs.mkdtemp(path.join(tmpdir(), "restore-journal-path-"));
  const directory = path.join(scratch, "journal");
  try {
    const database = await openRestoreJournal(directory);
    database.close();
    await fs.chmod(directory, 0o755);
    await assert.rejects(openRestoreJournal(directory));
    await fs.chmod(directory, 0o700);
    const file = path.join(directory, "restore.sqlite");
    await fs.chmod(file, 0o644);
    await assert.rejects(openRestoreJournal(directory));
    await fs.chmod(file, 0o600);
    const alias = path.join(scratch, "alias");
    await fs.symlink(directory, alias);
    await assert.rejects(openRestoreJournal(alias));
    const saved = path.join(scratch, "saved.sqlite");
    await fs.rename(file, saved);
    await fs.symlink(saved, file);
    await assert.rejects(openRestoreJournal(directory));
  } finally {
    await fs.rm(scratch, { recursive: true, force: true });
  }
});
