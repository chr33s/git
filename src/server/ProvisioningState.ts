/** Durable provisioning reservation and atomic authority publication. */
import { Effect, Schema } from "effect";
import { Invalid, StorageFailure } from "../git/Error.ts";
import type { Sql } from "../git/Sql.ts";
import { isOid } from "../git/Oid.ts";
import { GENESIS_REF } from "../trust/Genesis.ts";
import { LOG_REF } from "../trust/Log.ts";

export const Reservation = Schema.Struct({
  digest: Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/)),
  repoId: Schema.String.check(Schema.isPattern(/^SHA256:[A-Za-z0-9+/]{43}$/)),
  genesis: Schema.String.check(Schema.isPattern(/^[a-f0-9]{40}$/)),
  trust: Schema.String.check(Schema.isPattern(/^[a-f0-9]{40}$/)),
});
export interface Reservation extends Schema.Schema.Type<typeof Reservation> {}
const Record = Schema.Struct({
  ...Reservation.fields,
  status: Schema.Literals(["pending", "installed"]),
});
export interface Record extends Schema.Schema.Type<typeof Record> {}

const invalid = (reason: string) => new Invalid({ field: "provisioning", reason });
const decode = Schema.decodeUnknownSync(Record);
const same = (left: Reservation, right: Reservation) =>
  left.digest === right.digest &&
  left.repoId === right.repoId &&
  left.genesis === right.genesis &&
  left.trust === right.trust;

/** Use the same SQLite database and synchronous transaction as the ref store. */
export const make = Effect.fn("ProvisioningState.make")(function* (
  sql: Sql,
  transaction: <A>(work: () => A) => A,
) {
  const attempt = <A>(operation: string, work: () => A) =>
    Effect.try({
      try: work,
      catch: (cause) =>
        Schema.is(Invalid)(cause)
          ? cause
          : new StorageFailure({
              operation: `provisioning.${operation}`,
              path: "provisioning",
              cause,
            }),
    });
  yield* attempt("initialize", () => {
    sql.exec(`CREATE TABLE IF NOT EXISTS git_plus_provisioning (
      repo TEXT PRIMARY KEY, digest TEXT NOT NULL, repoId TEXT NOT NULL,
      genesis TEXT NOT NULL, trust TEXT NOT NULL, status TEXT NOT NULL
    )`);
  });
  const ref = (repo: string, name: string) =>
    sql
      .exec<{ oid: string }>("SELECT oid FROM refs WHERE repo = ? AND name = ?", repo, name)
      .toArray()[0]?.oid;
  const read = (repo: string): Record | null => {
    const row = sql
      .exec(
        "SELECT digest, repoId, genesis, trust, status FROM git_plus_provisioning WHERE repo = ?",
        repo,
      )
      .toArray()[0];
    if (row === undefined) return null;
    const record = decode(row);
    if (
      record.status === "installed" &&
      (ref(repo, GENESIS_REF) !== record.genesis || !isOid(ref(repo, LOG_REF) ?? ""))
    )
      throw invalid("installed authority is missing or changed; restore storage before serving");
    return record;
  };
  const empty = (repo: string) => {
    if (
      sql.exec("SELECT name FROM refs WHERE repo = ? LIMIT 1", repo).toArray().length > 0 ||
      sql.exec("SELECT oid FROM shallow WHERE repo = ? LIMIT 1", repo).toArray().length > 0 ||
      sql.exec("SELECT repo FROM reflog WHERE repo = ? LIMIT 1", repo).toArray().length > 0
    )
      throw invalid(
        "provisioning requires a repository without refs, reflogs, or shallow boundaries",
      );
  };
  const checkReservation = (reservation: Reservation) => {
    // Programmatic callers must satisfy the same persisted-data contract.
    Schema.decodeSync(Reservation)(reservation);
  };
  return {
    read: Effect.fn("ProvisioningState.read")((repo: string) => attempt("read", () => read(repo))),
    reserve: Effect.fn("ProvisioningState.reserve")((repo: string, reservation: Reservation) =>
      attempt("reserve", () =>
        transaction(() => {
          checkReservation(reservation);
          const current = read(repo);
          if (current !== null) {
            if (!same(current, reservation))
              throw invalid("a different provisioning artifact is already reserved");
            return current.status;
          }
          empty(repo);
          sql.exec(
            "INSERT INTO git_plus_provisioning (repo, digest, repoId, genesis, trust, status) VALUES (?, ?, ?, ?, ?, 'pending')",
            repo,
            reservation.digest,
            reservation.repoId,
            reservation.genesis,
            reservation.trust,
          );
          return "pending" as const;
        }),
      ),
    ),
    publish: Effect.fn("ProvisioningState.publish")((repo: string, reservation: Reservation) =>
      attempt("publish", () =>
        transaction(() => {
          checkReservation(reservation);
          const current = read(repo);
          if (current === null || !same(current, reservation))
            throw invalid("provisioning reservation does not match");
          if (current.status === "installed") return;
          empty(repo);
          for (const [name, oid] of [
            [GENESIS_REF, reservation.genesis],
            [LOG_REF, reservation.trust],
          ] as const)
            sql.exec("INSERT INTO refs (repo, name, oid) VALUES (?, ?, ?)", repo, name, oid);
          sql.exec("UPDATE git_plus_provisioning SET status = 'installed' WHERE repo = ?", repo);
        }),
      ),
    ),
  };
});
