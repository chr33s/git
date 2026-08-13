/**
 * `Registry` and `Tokens` on Durable Object SQLite — the durable form for a
 * Workers-hosted Artifacts provider.
 *
 * The shape follows from one constraint: DO namespaces cannot be
 * enumerated, so `list({ limit, cursor })` needs an index. One DO holding
 * these two tables *is* that index, and the same instance serializes every
 * write through the input gate — no locking, for the same reason a
 * repository maps onto a DO so neatly.
 *
 * Written against a minimal `Sql` port rather than `DurableObjectStorage`,
 * so the tables can be exercised anywhere a SQLite-shaped executor exists.
 */
import { bytesToHex } from "../git/Format.ts";
import { Effect, Layer } from "effect";

import { ArtifactsError } from "alchemy/Cloudflare/Artifacts/ReadWriteNamespace";

import { Registry, type RepoRecord, Tokens } from "./Namespace.ts";

import type { Sql } from "../git/Sql.ts";

export type { Sql };

const failure = (code: string, message: string) =>
  new ArtifactsError({ message: `${code}: ${message}`, cause: new Error(code) });

type RepoRow = {
  readonly id: string;
  readonly name: string;
  readonly description: string | null;
  readonly default_branch: string;
  readonly read_only: number;
  readonly source: string | null;
  readonly created_at: string;
  readonly updated_at: string;
  readonly last_push_at: string | null;
};

const toRecord = (row: RepoRow): RepoRecord => ({
  id: row.id,
  name: row.name,
  description: row.description,
  defaultBranch: row.default_branch,
  readOnly: row.read_only === 1,
  source: row.source,
  createdAt: new Date(row.created_at),
  updatedAt: new Date(row.updated_at),
  lastPushAt: row.last_push_at === null ? null : new Date(row.last_push_at),
});

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

export const registrySqlite = (sql: Sql) =>
  Layer.sync(Registry)(() => {
    createTables(sql);

    const find = (name: string): RepoRecord | null => {
      const rows = sql.exec<RepoRow>(`SELECT * FROM repos WHERE name = ?`, name).toArray();
      return rows[0] === undefined ? null : toRecord(rows[0]);
    };

    return Registry.of({
      create: (name, meta) =>
        Effect.suspend(() => {
          if (find(name) !== null) {
            return Effect.fail(failure("ALREADY_EXISTS", `repo '${name}' exists`));
          }
          const now = new Date().toISOString();
          const id = crypto.randomUUID();
          sql.exec(
            `INSERT INTO repos
               (id, name, description, default_branch, read_only, source, created_at, updated_at, last_push_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL)`,
            id,
            name,
            meta.description,
            meta.defaultBranch,
            meta.readOnly ? 1 : 0,
            meta.source,
            now,
            now,
          );
          return Effect.succeed(find(name)!);
        }),
      get: (name) => Effect.sync(() => find(name)),
      list: (options) =>
        Effect.sync(() => {
          const limit = options?.limit ?? 100;
          const offset = options?.cursor === undefined ? 0 : Number.parseInt(options.cursor, 10);
          const total =
            sql.exec<{ count: number }>(`SELECT COUNT(*) AS count FROM repos`).toArray()[0]
              ?.count ?? 0;
          // One row past the page: the cheapest "is there more" there is.
          const rows = sql
            .exec<RepoRow>(`SELECT * FROM repos ORDER BY name LIMIT ? OFFSET ?`, limit + 1, offset)
            .toArray();
          const page = rows.slice(0, limit);
          return {
            repos: page.map(toRecord),
            total,
            ...(rows.length > limit ? { cursor: String(offset + limit) } : {}),
          };
        }),
      delete: (name) =>
        Effect.sync(() => {
          if (find(name) === null) return false;
          sql.exec(`DELETE FROM repos WHERE name = ?`, name);
          sql.exec(`DELETE FROM tokens WHERE repo = ?`, name);
          return true;
        }),
      touch: (name, at) =>
        Effect.sync(() => {
          sql.exec(
            `UPDATE repos SET updated_at = ?, last_push_at = ? WHERE name = ?`,
            at.toISOString(),
            at.toISOString(),
            name,
          );
        }),
      setDefaultBranch: (name, branch) =>
        Effect.sync(() => {
          sql.exec(`UPDATE repos SET default_branch = ? WHERE name = ?`, branch, name);
        }),
    });
  });

type TokenRow = {
  readonly id: string;
  readonly repo: string;
  readonly scope: "read" | "write";
  readonly digest: string;
  readonly created_at: string;
  readonly expires_at: string;
  readonly revoked: number;
};

const stateOf = (row: TokenRow): "active" | "expired" | "revoked" =>
  row.revoked === 1
    ? "revoked"
    : new Date(row.expires_at).getTime() <= Date.now()
      ? "expired"
      : "active";

export const tokensSqlite = (sql: Sql) =>
  Layer.sync(Tokens)(() => {
    createTables(sql);

    const digestOf = (plaintext: string) =>
      Effect.promise(async () => {
        const bytes = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(plaintext));
        return bytesToHex(new Uint8Array(bytes));
      });

    return Tokens.of({
      issue: (repo, scope, ttlSeconds) =>
        Effect.gen(function* () {
          if (!(ttlSeconds > 0)) return yield* failure("INVALID_TTL", `ttl ${ttlSeconds}`);
          const plaintext = `art_${crypto.randomUUID().replaceAll("-", "")}`;
          const id = crypto.randomUUID();
          const expiresAt = new Date(Date.now() + ttlSeconds * 1000).toISOString();
          // Only the digest is stored: plaintext exists in this response only.
          sql.exec(
            `INSERT INTO tokens (id, repo, scope, digest, created_at, expires_at, revoked)
             VALUES (?, ?, ?, ?, ?, ?, 0)`,
            id,
            repo,
            scope,
            yield* digestOf(plaintext),
            new Date().toISOString(),
            expiresAt,
          );
          return { id, plaintext, scope, expiresAt };
        }),
      list: (repo) =>
        Effect.sync(() => {
          const rows = sql
            .exec<TokenRow>(`SELECT * FROM tokens WHERE repo = ? ORDER BY created_at`, repo)
            .toArray();
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
      revoke: (repo, tokenOrId) =>
        Effect.gen(function* () {
          const digest = yield* digestOf(tokenOrId);
          const rows = sql
            .exec<TokenRow>(
              `SELECT * FROM tokens WHERE repo = ? AND (id = ? OR digest = ?)`,
              repo,
              tokenOrId,
              digest,
            )
            .toArray();
          const row = rows[0];
          if (row === undefined || row.revoked === 1) return false;
          sql.exec(`UPDATE tokens SET revoked = 1 WHERE id = ?`, row.id);
          return true;
        }),
      verify: (repo, presented) =>
        Effect.gen(function* () {
          const digest = yield* digestOf(presented);
          const rows = sql
            .exec<TokenRow>(`SELECT * FROM tokens WHERE repo = ? AND digest = ?`, repo, digest)
            .toArray();
          const row = rows[0];
          return row !== undefined && stateOf(row) === "active" ? row.scope : null;
        }),
    });
  });

/** Both tables over one Durable Object's SQLite. */
export const sqlite = (sql: Sql) => Layer.mergeAll(registrySqlite(sql), tokensSqlite(sql));
