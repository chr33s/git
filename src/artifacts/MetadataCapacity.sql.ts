import { ArtifactsError } from "@alchemy.run/cloudflare/Artifacts/ReadWriteNamespace";
import { Result, Schema } from "effect";
import type { TransactionalSql } from "../git/Sql.ts";
import { check, type Limits, type RegistryKind } from "./MetadataCapacity.ts";

const tables = {
  repositories: {
    name: "repos",
    columns: [
      "id",
      "name",
      "description",
      "default_branch",
      "read_only",
      "source",
      "created_at",
      "updated_at",
      "last_push_at",
      "initializing",
    ],
  },
  tokens: {
    name: "tokens",
    columns: ["id", "repo", "scope", "digest", "created_at", "expires_at", "revoked"],
  },
} as const;

const Count = Schema.Int.check(
  Schema.isGreaterThanOrEqualTo(0),
  Schema.isLessThanOrEqualTo(Number.MAX_SAFE_INTEGER),
);
const Usage = Schema.Struct({ rows: Count, bytes: Count });

/** SQL aggregates at most rows + 1 records; no metadata enters JS or a durable cache. */
const inspect = (
  sql: TransactionalSql,
  kind: Exclude<RegistryKind, "forks">,
  limits: Limits,
): void => {
  const table = tables[kind];
  const size = table.columns
    .map((column) => `COALESCE(length(CAST(${column} AS BLOB)), 0)`)
    .join(" + ");
  const stored = sql
    .exec<{ rows: number; bytes: number }>(
      `SELECT COUNT(*) AS rows, COALESCE(SUM(size), 0) AS bytes
     FROM (SELECT ${size} AS size FROM ${table.name} LIMIT ?)`,
      limits.rows + 1,
    )
    .toArray()[0];
  const decoded = Schema.decodeUnknownResult(Usage)(stored);
  if (Result.isFailure(decoded))
    throw new ArtifactsError({
      message: "STORAGE_FAILURE: administrative registry capacity unavailable",
      cause: new Error("ARTIFACTS_METADATA_CAPACITY_INVALID"),
    });
  check(limits, decoded.success.rows, decoded.success.bytes);
};

/** A refusal rolls back the mutation, including writes from its SQL triggers. */
export const mutate = <A>(
  sql: TransactionalSql,
  kind: Exclude<RegistryKind, "forks">,
  limits: Limits,
  write: () => A,
): A =>
  sql.transactionSync(() => {
    inspect(sql, kind, limits);
    const value = write();
    inspect(sql, kind, limits);
    return value;
  });
