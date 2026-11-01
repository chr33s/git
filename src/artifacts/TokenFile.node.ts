/** Fresh token reads and cross-process mutation exclusion. Never cache revocations. */
import * as fs from "node:fs/promises";
import * as path from "node:path";
import { ArtifactsError } from "@alchemy.run/cloudflare/Artifacts/ReadWriteNamespace";
import { Effect, Schema, Semaphore } from "effect";

import * as Capacity from "./MetadataCapacity.node.ts";

import type { Tokens, TokenRow } from "./Namespace.ts";
import { sync as syncRegistryFile } from "./MetadataFile.node.ts";

const Timestamp = Schema.String.check(
  Schema.makeFilter((value) => Number.isFinite(Date.parse(value))),
);
const Rows = Schema.fromJsonString(
  Schema.Array(
    Schema.Struct({
      id: Schema.NonEmptyString,
      repo: Schema.NonEmptyString,
      scope: Schema.Literals(["read", "write"]),
      digest: Schema.String.check(Schema.isPattern(/^[0-9a-f]{64}$/)),
      createdAt: Timestamp,
      expiresAt: Timestamp,
      revoked: Schema.Boolean,
    }),
  ),
);

type Factory = (
  rows: ReadonlyArray<TokenRow>,
  persist: (rows: ReadonlyArray<TokenRow>) => Effect.Effect<void, ArtifactsError>,
) => Tokens["Service"];

export const make = Effect.fn("TokenFile.make")(function* (root: string, factory: Factory) {
  const capacity = yield* Capacity.configuration("tokens").pipe(Effect.orDie);
  const target = path.join(root, ".tokens.json");
  const lock = `${target}.lock`;
  const writers = yield* Semaphore.make(1);
  // Do not include parsed bytes or parse diagnostics: this is credential metadata.
  const failed = () =>
    new ArtifactsError({
      message: `INTERNAL_ERROR: cannot access token registry '${target}'`,
      cause: new Error(
        "token registry unavailable or invalid; preserve the file and inspect permissions/format",
      ),
    });
  const load = Effect.tryPromise({
    try: async () => {
      try {
        return await fs.readFile(target, "utf8");
      } catch (cause) {
        if (cause instanceof Error && "code" in cause && cause.code === "ENOENT") return "[]";
        throw cause;
      }
    },
    catch: failed,
  }).pipe(
    Effect.flatMap((text) => Schema.decodeEffect(Rows)(text).pipe(Effect.mapError(failed))),
    Effect.flatMap((rows) =>
      new Set(rows.map((row) => row.id)).size === rows.length &&
      new Set(rows.map((row) => row.digest)).size === rows.length
        ? Effect.succeed(rows)
        : Effect.fail(failed()),
    ),
  );
  const persist = (rows: ReadonlyArray<TokenRow>, bounded: boolean) =>
    Effect.tryPromise({
      try: async () => {
        if (bounded) Capacity.check(capacity, rows.length, 0);
        const bytes = Buffer.from(`${JSON.stringify(rows)}\n`);
        if (bounded) Capacity.check(capacity, rows.length, bytes.byteLength);
        const temporary = `${target}.${crypto.randomUUID()}.tmp`;
        try {
          const file = await fs.open(temporary, "wx", 0o600);
          try {
            await file.writeFile(bytes);
            await file.sync();
          } finally {
            await file.close();
          }
          await fs.rename(temporary, target);
        } finally {
          await fs.rm(temporary, { force: true });
        }
      },
      catch: (cause) => (cause instanceof ArtifactsError ? cause : failed()),
    });
  const syncRegistry = Effect.tryPromise({
    // A no-op revocation must complete any earlier failed publication sync.
    try: () => syncRegistryFile(target),
    catch: failed,
  });
  const read = <A>(
    use: (tokens: Tokens["Service"]) => Effect.Effect<A, ArtifactsError>,
    bounded = false,
  ) => load.pipe(Effect.flatMap((rows) => use(factory(rows, (next) => persist(next, bounded)))));
  const edit = <A>(
    use: (tokens: Tokens["Service"]) => Effect.Effect<A, ArtifactsError>,
    bounded = true,
  ) =>
    Effect.acquireUseRelease(
      Effect.tryPromise({
        try: async () => {
          await fs.mkdir(root, { recursive: true, mode: 0o700 });
          const handle = await fs.open(lock, "wx", 0o600);
          try {
            await handle.close();
          } catch (cause) {
            await fs.rm(lock, { force: true });
            throw cause;
          }
        },
        catch: (cause) =>
          cause instanceof Error && "code" in cause && cause.code === "EEXIST"
            ? new ArtifactsError({
                message: `PRECONDITION_FAILED: token registry '${target}' is locked`,
                cause: new Error("token registry lock contention"),
              })
            : failed(),
      }),
      () =>
        (bounded
          ? Effect.tryPromise({
              try: () => Capacity.inspect(target, capacity),
              catch: (cause) => (cause instanceof ArtifactsError ? cause : failed()),
            })
          : Effect.void
        ).pipe(
          Effect.andThen(read(use, bounded)),
          Effect.tap(() => syncRegistry),
        ),
      () => Effect.promise(() => fs.rm(lock, { force: true })),
    ).pipe(
      Effect.uninterruptible,
      Semaphore.withPermit(writers),
      Effect.tapError(Capacity.report("tokens")),
    );
  return {
    issue: Effect.fn("TokenFile.issue")((repo, scope, ttl) =>
      edit((tokens) => tokens.issue(repo, scope, ttl)),
    ),
    list: Effect.fn("TokenFile.list")((repo) => read((tokens) => tokens.list(repo))),
    revoke: Effect.fn("TokenFile.revoke")((repo, token) =>
      edit((tokens) => tokens.revoke(repo, token), false),
    ),
    verify: Effect.fn("TokenFile.verify")((repo, token) =>
      read((tokens) => tokens.verify(repo, token)),
    ),
  } satisfies Tokens["Service"];
});
