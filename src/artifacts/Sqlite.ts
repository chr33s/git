/**
 * `Registry` and `Tokens` on Durable Object SQLite — the durable form for a
 * Workers-hosted Artifacts provider.
 *
 * Repository listing needs an application index: the Worker namespace binding
 * cannot enumerate repositories, and the account-level object-list API returns
 * IDs/storage-presence flags rather than repository names and metadata. One DO
 * holding these two tables is that index. Registry deletion uses an explicit
 * synchronous transaction so a caught SQL failure cannot detach surviving
 * credentials from the repository identity they authorized.
 *
 * Written against minimal SQL ports rather than `DurableObjectStorage`,
 * so the tables can be exercised anywhere a SQLite-shaped executor exists.
 */
import * as Capacity from "./MetadataCapacity.ts";
import { mutate } from "./MetadataCapacity.sql.ts";
import { bytesToHex } from "../git/Format.ts";
import { Effect, Layer, Result, Schema } from "effect";

import { ArtifactsError } from "@alchemy.run/cloudflare/Artifacts/ReadWriteNamespace";

import { Registry, type RepoMeta, type RepoRecord, Tokens } from "./Namespace.ts";

import type { Sql, TransactionalSql } from "../git/Sql.ts";

export type { Sql, TransactionalSql };

const failure = (code: string, message: string) =>
  new ArtifactsError({ message: `${code}: ${message}`, cause: new Error(code) });

const Timestamp = Schema.String.check(
  Schema.makeFilter((value) => {
    const date = new Date(value);
    return Number.isFinite(date.getTime()) && date.toISOString() === value;
  }),
);
const RepoRow = Schema.Struct({
  id: Schema.NonEmptyString,
  name: Schema.NonEmptyString,
  description: Schema.NullOr(Schema.String),
  default_branch: Schema.NonEmptyString,
  read_only: Schema.Literals([0, 1]),
  source: Schema.NullOr(Schema.String),
  created_at: Timestamp,
  updated_at: Timestamp,
  last_push_at: Schema.NullOr(Timestamp),
  initializing: Schema.NullOr(Schema.Literals(["create", "import", "fork"])),
});
interface RepoRow extends Schema.Schema.Type<typeof RepoRow> {}

const toRecord = (stored: RepoRow): RepoRecord => {
  const decoded = Schema.decodeResult(RepoRow)(stored);
  if (Result.isFailure(decoded))
    throw failure("STORAGE_FAILURE", "invalid persisted repository metadata");
  const row = decoded.success;
  return {
    id: row.id,
    name: row.name,
    description: row.description,
    defaultBranch: row.default_branch,
    readOnly: row.read_only === 1,
    source: row.source,
    initializing: row.initializing,
    createdAt: new Date(row.created_at),
    updatedAt: new Date(row.updated_at),
    lastPushAt: row.last_push_at === null ? null : new Date(row.last_push_at),
  };
};

const createTables = (sql: Sql): void => {
  sql.exec(`
    CREATE TABLE IF NOT EXISTS repos (
      id             TEXT NOT NULL,
      name           TEXT PRIMARY KEY,
      description    TEXT,
      default_branch TEXT NOT NULL,
      read_only      INTEGER NOT NULL,
      source         TEXT,
      created_at     TEXT NOT NULL,
      updated_at     TEXT NOT NULL,
      last_push_at   TEXT
    )
  `);
  // Existing registries predate initialization tracking. A NULL migration
  // preserves their completed repositories without guessing from source URLs.
  if (
    !sql
      .exec<{ name: string }>(`SELECT name FROM pragma_table_info('repos')`)
      .toArray()
      .some((column) => column.name === "initializing")
  ) {
    sql.exec(`ALTER TABLE repos ADD COLUMN initializing TEXT`);
  }
  sql.exec(`
    CREATE TABLE IF NOT EXISTS tokens (
      id         TEXT PRIMARY KEY,
      repo       TEXT NOT NULL,
      scope      TEXT NOT NULL,
      digest     TEXT NOT NULL,
      created_at TEXT NOT NULL,
      expires_at TEXT NOT NULL,
      revoked    INTEGER NOT NULL
    )
  `);
  // `verify` runs on every authenticated request; it must not scan.
  sql.exec(`CREATE INDEX IF NOT EXISTS tokens_repo_digest ON tokens (repo, digest)`);
};

export const registrySqlite = (sql: TransactionalSql) =>
  Layer.effect(Registry)(
    Effect.gen(function* () {
      const limits = yield* Capacity.configuration("repositories", "GIT_CF").pipe(Effect.orDie);
      createTables(sql);

      const find = (name: string): RepoRecord | null => {
        const rows = sql.exec<RepoRow>(`SELECT * FROM repos WHERE name = ?`, name).toArray();
        return rows[0] === undefined ? null : toRecord(rows[0]);
      };

      return Registry.of({
        create: Effect.fn("Artifacts.Registry.create")((name: string, meta: RepoMeta) =>
          Effect.try({
            try: () =>
              mutate(sql, "repositories", limits, () => {
                if (find(name) !== null) throw failure("ALREADY_EXISTS", `repo '${name}' exists`);
                if (
                  sql
                    .exec<{ id: string }>("SELECT id FROM tokens WHERE repo = ? LIMIT 1", name)
                    .toArray().length > 0
                )
                  throw failure(
                    "ORPHANED_TOKENS",
                    "reconcile retained credentials before reusing repository name",
                  );
                const now = new Date().toISOString();
                const id = crypto.randomUUID();
                sql.exec(
                  `INSERT INTO repos
               (id, name, description, default_branch, read_only, source, created_at, updated_at, last_push_at, initializing)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL, ?)`,
                  id,
                  name,
                  meta.description,
                  meta.defaultBranch,
                  meta.readOnly ? 1 : 0,
                  meta.source,
                  now,
                  now,
                  meta.initializing ?? null,
                );
                const created = find(name);
                if (created === null)
                  throw failure("STORAGE_FAILURE", "repository creation failed");
                return created;
              }),
            catch: (cause) =>
              cause instanceof ArtifactsError
                ? cause
                : failure("STORAGE_FAILURE", "repository creation failed"),
          }).pipe(Effect.tapError(Capacity.report("repositories"))),
        ),
        get: Effect.fn("Artifacts.Registry.get")((name) =>
          Effect.try({
            try: () => find(name),
            catch: () => failure("STORAGE_FAILURE", "repository read failed"),
          }),
        ),
        list: Effect.fn("Artifacts.Registry.list")((options) =>
          Effect.try({
            try: () => {
              const limit = options?.limit ?? 100;
              const offset =
                options?.cursor === undefined ? 0 : Number.parseInt(options.cursor, 10);
              const total =
                sql.exec<{ count: number }>(`SELECT COUNT(*) AS count FROM repos`).toArray()[0]
                  ?.count ?? 0;
              // One row past the page: the cheapest "is there more" there is.
              const rows = sql
                .exec<RepoRow>(
                  `SELECT * FROM repos ORDER BY name LIMIT ? OFFSET ?`,
                  limit + 1,
                  offset,
                )
                .toArray();
              const page = rows.slice(0, limit).map(toRecord);
              return rows.length > limit
                ? { repos: page, total, cursor: String(offset + limit) }
                : { repos: page, total };
            },
            catch: () => failure("STORAGE_FAILURE", "repository listing failed"),
          }),
        ),
        delete: Effect.fn("Artifacts.Registry.delete")((name: string) =>
          Effect.try({
            try: () =>
              sql.transactionSync(() => {
                if (find(name) === null) return false;
                sql.exec(`DELETE FROM repos WHERE name = ?`, name);
                sql.exec(`DELETE FROM tokens WHERE repo = ?`, name);
                return true;
              }),
            catch: () => failure("STORAGE_FAILURE", "repository deletion failed"),
          }),
        ),
        touch: Effect.fn("Artifacts.Registry.touch")((name, at) =>
          Effect.try({
            try: () =>
              mutate(sql, "repositories", limits, () => {
                if (find(name) === null) return;
                sql.exec(
                  `UPDATE repos SET updated_at = ?, last_push_at = ? WHERE name = ?`,
                  at.toISOString(),
                  at.toISOString(),
                  name,
                );
              }),
            catch: (cause) =>
              cause instanceof ArtifactsError
                ? cause
                : failure("STORAGE_FAILURE", "repository timestamp update failed"),
          }).pipe(Effect.tapError(Capacity.report("repositories"))),
        ),
        setDefaultBranch: Effect.fn("Artifacts.Registry.setDefaultBranch")((name, branch) =>
          Effect.try({
            try: () =>
              mutate(sql, "repositories", limits, () => {
                if (find(name) === null) return;
                sql.exec(`UPDATE repos SET default_branch = ? WHERE name = ?`, branch, name);
              }),
            catch: (cause) =>
              cause instanceof ArtifactsError
                ? cause
                : failure("STORAGE_FAILURE", "repository branch update failed"),
          }).pipe(Effect.tapError(Capacity.report("repositories"))),
        ),
        finish: Effect.fn("Artifacts.Registry.finish")((name: string, id: string) =>
          Effect.try({
            try: () => {
              const record = find(name);
              if (record === null || record.id !== id)
                throw failure("NOT_FOUND", `initializing repo '${name}' no longer exists`);
              sql.exec(`UPDATE repos SET initializing = NULL WHERE name = ? AND id = ?`, name, id);
            },
            catch: (cause) =>
              cause instanceof ArtifactsError
                ? cause
                : failure("STORAGE_FAILURE", "repository initialization completion failed"),
          }),
        ),
      });
    }),
  );

const TokenRow = Schema.Struct({
  id: Schema.NonEmptyString,
  repo: Schema.NonEmptyString,
  scope: Schema.Literals(["read", "write"]),
  digest: Schema.String.check(Schema.isPattern(/^[0-9a-f]{64}$/)),
  created_at: Timestamp,
  expires_at: Timestamp,
  revoked: Schema.Literals([0, 1]),
});
interface TokenRow extends Schema.Schema.Type<typeof TokenRow> {}
const decodeToken = (row: TokenRow) =>
  Schema.decodeEffect(TokenRow)(row).pipe(
    Effect.mapError(() => failure("STORAGE_FAILURE", "invalid persisted token metadata")),
  );

const stateOf = (row: TokenRow): "active" | "expired" | "revoked" =>
  row.revoked === 1
    ? "revoked"
    : new Date(row.expires_at).getTime() <= Date.now()
      ? "expired"
      : "active";

export const tokensSqlite = (sql: TransactionalSql) =>
  Layer.effect(Tokens)(
    Effect.gen(function* () {
      const limits = yield* Capacity.configuration("tokens", "GIT_CF").pipe(Effect.orDie);
      createTables(sql);

      const digestOf = (plaintext: string) =>
        Effect.promise(async () => {
          const bytes = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(plaintext));
          return bytesToHex(new Uint8Array(bytes));
        });

      return Tokens.of({
        issue: Effect.fn("Artifacts.Tokens.issue")(
          function* (repo, scope, ttlSeconds) {
            if (!(ttlSeconds > 0)) return yield* failure("INVALID_TTL", `ttl ${ttlSeconds}`);
            const plaintext = `art_${crypto.randomUUID().replaceAll("-", "")}`;
            const id = crypto.randomUUID();
            const expiresAt = new Date(Date.now() + ttlSeconds * 1000).toISOString();
            // Only the digest is stored: plaintext exists in this response only.
            const digest = yield* digestOf(plaintext);
            yield* Effect.try({
              try: () =>
                mutate(sql, "tokens", limits, () =>
                  sql.exec(
                    `INSERT INTO tokens (id, repo, scope, digest, created_at, expires_at, revoked)
             VALUES (?, ?, ?, ?, ?, ?, 0)`,
                    id,
                    repo,
                    scope,
                    digest,
                    new Date().toISOString(),
                    expiresAt,
                  ),
                ),
              catch: (cause) =>
                cause instanceof ArtifactsError
                  ? cause
                  : failure("STORAGE_FAILURE", "token issuance failed"),
            });
            return { id, plaintext, scope, expiresAt };
          },
          Effect.tapError(Capacity.report("tokens")),
        ),
        list: Effect.fn("Artifacts.Tokens.list")(function* (repo) {
          const stored = sql
            .exec<TokenRow>(`SELECT * FROM tokens WHERE repo = ? ORDER BY created_at`, repo)
            .toArray();
          const rows = yield* Effect.forEach(stored, decodeToken);
          return {
            tokens: rows.map((row) => ({
              id: row.id,
              scope: row.scope,
              state: stateOf(row),
              createdAt: row.created_at,
              expiresAt: row.expires_at,
            })),
            total: rows.length,
          };
        }),
        revoke: Effect.fn("Artifacts.Tokens.revoke")(function* (repo, tokenOrId) {
          const digest = yield* digestOf(tokenOrId);
          const rows = sql
            .exec<TokenRow>(
              `SELECT * FROM tokens WHERE repo = ? AND (id = ? OR digest = ?)`,
              repo,
              tokenOrId,
              digest,
            )
            .toArray();
          if (rows[0] === undefined) return false;
          const row = yield* decodeToken(rows[0]);
          if (row.revoked === 1) return false;
          sql.exec(`UPDATE tokens SET revoked = 1 WHERE id = ?`, row.id);
          return true;
        }),
        verify: Effect.fn("Artifacts.Tokens.verify")(function* (repo, presented) {
          const digest = yield* digestOf(presented);
          const rows = sql
            .exec<TokenRow>(`SELECT * FROM tokens WHERE repo = ? AND digest = ?`, repo, digest)
            .toArray();
          if (rows[0] === undefined) return null;
          const row = yield* decodeToken(rows[0]);
          return stateOf(row) === "active" ? row.scope : null;
        }),
      });
    }),
  );

/** Both tables over one Durable Object's SQLite. */
export const sqlite = (sql: TransactionalSql) =>
  Layer.mergeAll(registrySqlite(sql), tokensSqlite(sql));
