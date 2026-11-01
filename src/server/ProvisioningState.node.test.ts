import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { DatabaseSync } from "node:sqlite";
import { describe, it } from "@effect/vitest";
import { Effect } from "effect";
import type { Sql } from "../git/Sql.ts";
import { GENESIS_REF } from "../trust/Genesis.ts";
import { LOG_REF } from "../trust/Log.ts";
import * as ProvisioningState from "./ProvisioningState.ts";

const reservation: ProvisioningState.Reservation = {
  digest: "a".repeat(64),
  repoId: "SHA256:" + "A".repeat(43),
  genesis: "b".repeat(40),
  trust: "c".repeat(40),
};

const open = async (file = ":memory:") => {
  const database = new DatabaseSync(file);
  database.exec(`CREATE TABLE IF NOT EXISTS refs (repo TEXT, name TEXT, oid TEXT, PRIMARY KEY(repo, name));
    CREATE TABLE IF NOT EXISTS shallow (repo TEXT, oid TEXT, PRIMARY KEY(repo, oid));
    CREATE TABLE IF NOT EXISTS reflog (repo TEXT);`);
  const sql: Sql = {
    exec: <Row extends Record<string, ArrayBuffer | string | number | null>>(
      query: string,
      ...bindings: ReadonlyArray<string | number | null>
    ) => {
      const statement = database.prepare(query);
      if (!query.trimStart().startsWith("SELECT")) {
        statement.run(...bindings);
        return { toArray: (): Row[] => [] };
      }
      // SAFETY: callers of the SQL port declare the rows their own query selects.
      return { toArray: () => statement.all(...bindings) as Row[] };
    },
  };
  const transaction = <A>(work: () => A): A => {
    database.exec("BEGIN IMMEDIATE");
    try {
      const result = work();
      database.exec("COMMIT");
      return result;
    } catch (error) {
      database.exec("ROLLBACK");
      throw error;
    }
  };
  const state = await Effect.runPromise(ProvisioningState.make(sql, transaction));
  return { database, state };
};

describe("durable provisioning authority publication", () => {
  it("recovers a reservation after process death between the authority-ref inserts", async () => {
    const root = await fs.mkdtemp(path.join(os.tmpdir(), "git-provisioning-kill-"));
    const file = path.join(root, "state.sqlite");
    let current = await open(file);
    try {
      await Effect.runPromise(current.state.reserve("repo", reservation));
      current.database.close();
      const script = `
        import { DatabaseSync } from "node:sqlite";
        import { Effect } from "effect";
        import { make } from ${JSON.stringify(new URL("./ProvisioningState.ts", import.meta.url).href)};
        const database = new DatabaseSync(process.argv[1]);
        const sql = { exec(query, ...bindings) {
          const statement = database.prepare(query);
          const rows = query.trimStart().startsWith("SELECT") ? statement.all(...bindings) : (statement.run(...bindings), []);
          if (query.startsWith("INSERT INTO refs") && bindings[1] === ${JSON.stringify(GENESIS_REF)})
            process.kill(process.pid, "SIGKILL");
          return { toArray: () => rows };
        }};
        const transaction = work => {
          database.exec("BEGIN IMMEDIATE");
          const result = work();
          database.exec("COMMIT");
          return result;
        };
        const state = await Effect.runPromise(make(sql, transaction));
        await Effect.runPromise(state.publish("repo", ${JSON.stringify(reservation)}));
      `;
      try {
        await assert.rejects(
          promisify(execFile)(process.execPath, ["--input-type=module", "-e", script, file], {
            timeout: 20_000,
          }),
          (error: Error) => "signal" in error && error.signal === "SIGKILL",
        );
      } finally {
        current = await open(file);
      }
      assert.equal((await Effect.runPromise(current.state.read("repo")))?.status, "pending");
      assert.equal(current.database.prepare("SELECT COUNT(*) AS count FROM refs").get()?.count, 0);
      await Effect.runPromise(current.state.publish("repo", reservation));
      assert.equal((await Effect.runPromise(current.state.read("repo")))?.status, "installed");
    } finally {
      current.database.close();
      await fs.rm(root, { recursive: true, force: true });
    }
  });

  it("reserves empty state, resumes after reopening, and never rewinds an installed trust log", async () => {
    const root = await fs.mkdtemp(path.join(os.tmpdir(), "git-provisioning-state-"));
    const file = path.join(root, "state.sqlite");
    let current = await open(file);
    try {
      assert.equal(await Effect.runPromise(current.state.read("repo")), null);
      assert.equal(await Effect.runPromise(current.state.reserve("repo", reservation)), "pending");
      current.database.close();
      current = await open(file);
      assert.equal((await Effect.runPromise(current.state.read("repo")))?.status, "pending");
      assert.equal(await Effect.runPromise(current.state.reserve("repo", reservation)), "pending");
      const changed = await Effect.runPromise(
        current.state.reserve("repo", { ...reservation, digest: "d".repeat(64) }).pipe(Effect.flip),
      );
      assert.equal(changed._tag, "Invalid");
      await Effect.runPromise(current.state.publish("repo", reservation));
      current.database.close();
      current = await open(file);
      assert.equal((await Effect.runPromise(current.state.read("repo")))?.status, "installed");
      const advanced = "e".repeat(40);
      current.database
        .prepare("UPDATE refs SET oid = ? WHERE repo = 'repo' AND name = ?")
        .run(advanced, LOG_REF);
      await Effect.runPromise(current.state.publish("repo", reservation));
      assert.equal(
        current.database.prepare("SELECT oid FROM refs WHERE name = ?").get(LOG_REF)?.oid,
        advanced,
      );
      assert.equal(
        await Effect.runPromise(current.state.reserve("repo", reservation)),
        "installed",
      );
    } finally {
      current.database.close();
      await fs.rm(root, { recursive: true, force: true });
    }
  });

  it("rolls back both authority refs when publication fails and retains a retryable reservation", async () => {
    const { database, state } = await open();
    try {
      await Effect.runPromise(state.reserve("repo", reservation));
      database.exec(
        `CREATE TRIGGER fail_trust BEFORE INSERT ON refs WHEN NEW.name = '${LOG_REF}' BEGIN SELECT RAISE(ABORT, 'injected publication failure'); END`,
      );
      const failed = await Effect.runPromise(state.publish("repo", reservation).pipe(Effect.flip));
      assert.equal(failed._tag, "StorageFailure");
      assert.equal(database.prepare("SELECT COUNT(*) AS count FROM refs").get()?.count, 0);
      assert.equal((await Effect.runPromise(state.read("repo")))?.status, "pending");
      database.exec("DROP TRIGGER fail_trust");
      await Effect.runPromise(state.publish("repo", reservation));
      assert.equal(database.prepare("SELECT COUNT(*) AS count FROM refs").get()?.count, 2);
      assert.equal((await Effect.runPromise(state.read("repo")))?.status, "installed");
    } finally {
      database.close();
    }
  });

  it("refuses existing refs, shallow history, and writes appearing after reservation", async () => {
    const { database, state } = await open();
    try {
      database
        .prepare("INSERT INTO refs VALUES (?, ?, ?)")
        .run("existing", "refs/heads/main", reservation.genesis);
      database.prepare("INSERT INTO shallow VALUES (?, ?)").run("shallow", reservation.genesis);
      database.prepare("INSERT INTO reflog VALUES (?)").run("historical");
      for (const repo of ["existing", "shallow", "historical"]) {
        assert.equal(
          (await Effect.runPromise(state.reserve(repo, reservation).pipe(Effect.flip)))._tag,
          "Invalid",
        );
        assert.equal(await Effect.runPromise(state.read(repo)), null);
      }
      await Effect.runPromise(state.reserve("raced", reservation));
      database
        .prepare("INSERT INTO refs VALUES (?, ?, ?)")
        .run("raced", "refs/heads/main", reservation.genesis);
      assert.equal(
        (await Effect.runPromise(state.publish("raced", reservation).pipe(Effect.flip)))._tag,
        "Invalid",
      );
      assert.equal(
        database.prepare("SELECT COUNT(*) AS count FROM refs WHERE repo = 'raced'").get()?.count,
        1,
      );
      assert.equal((await Effect.runPromise(state.read("raced")))?.status, "pending");
    } finally {
      database.close();
    }
  });

  it("fails closed on malformed markers or missing installed authority", async () => {
    const { database, state } = await open();
    try {
      await Effect.runPromise(state.reserve("repo", reservation));
      database.exec("UPDATE git_plus_provisioning SET digest = 'corrupt'");
      assert.equal(
        (await Effect.runPromise(state.read("repo").pipe(Effect.flip)))._tag,
        "StorageFailure",
      );
      database.prepare("UPDATE git_plus_provisioning SET digest = ?").run(reservation.digest);
      await Effect.runPromise(state.publish("repo", reservation));
      database.prepare("DELETE FROM refs WHERE name = ?").run(LOG_REF);
      assert.equal((await Effect.runPromise(state.read("repo").pipe(Effect.flip)))._tag, "Invalid");
      database.prepare("INSERT INTO refs VALUES (?, ?, ?)").run("repo", LOG_REF, reservation.trust);
      database.prepare("UPDATE refs SET oid = ? WHERE name = ?").run("f".repeat(40), GENESIS_REF);
      assert.equal(
        (await Effect.runPromise(state.publish("repo", reservation).pipe(Effect.flip)))._tag,
        "Invalid",
      );
    } finally {
      database.close();
    }
  });
});
