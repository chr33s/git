/** Fresh reads and locked updates for the local provider's repository registry. */
import * as fs from "node:fs/promises";
import * as path from "node:path";
import { ArtifactsError } from "@alchemy.run/cloudflare/Artifacts/ReadWriteNamespace";
import { Effect, Schema, Semaphore } from "effect";

import * as Capacity from "./MetadataCapacity.node.ts";

import type { Registry, RepoRecord } from "./Namespace.ts";
import { sync as syncRegistry } from "./MetadataFile.node.ts";

const Rows = Schema.fromJsonString(
  Schema.Array(
    Schema.Struct({
      id: Schema.NonEmptyString,
      name: Schema.NonEmptyString,
      description: Schema.NullOr(Schema.String),
      defaultBranch: Schema.NonEmptyString,
      readOnly: Schema.Boolean,
      source: Schema.NullOr(Schema.String),
      initializing: Schema.optional(Schema.NullOr(Schema.Literals(["create", "import", "fork"]))),
      createdAt: Schema.DateFromString,
      updatedAt: Schema.DateFromString,
      lastPushAt: Schema.NullOr(Schema.DateFromString),
    }),
  ),
);

type Factory = (
  rows: ReadonlyMap<string, RepoRecord>,
  persist: (rows: ReadonlyMap<string, RepoRecord>) => Effect.Effect<void, ArtifactsError>,
) => Registry["Service"];

export const make = Effect.fn("RegistryFile.make")(function* (root: string, factory: Factory) {
  const capacity = yield* Capacity.configuration("repositories").pipe(Effect.orDie);
  const target = path.join(root, ".registry.json");
  const lock = `${target}.lock`;
  const writers = yield* Semaphore.make(1);
  // Parser/native error details can contain private metadata or namespace paths.
  const failed = () =>
    new ArtifactsError({
      message: "INTERNAL_ERROR: repository registry unavailable or invalid",
      cause: new Error("preserve .registry.json and inspect permissions/format"),
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
    Effect.filterOrFail(
      (rows) =>
        new Set(rows.map((row) => row.name)).size === rows.length &&
        new Set(rows.map((row) => row.id)).size === rows.length,
      failed,
    ),
    Effect.map((rows) => new Map(rows.map((row) => [row.name, row]))),
  );
  const persist = (rows: ReadonlyMap<string, RepoRecord>, bounded: boolean) =>
    Effect.tryPromise({
      try: async () => {
        if (bounded) Capacity.check(capacity, rows.size, 0);
        const bytes = Buffer.from(`${JSON.stringify([...rows.values()], null, 1)}\n`);
        if (bounded) Capacity.check(capacity, rows.size, bytes.byteLength);
        // Keep the reservation until release; renaming it would let another
        // writer acquire a lock that this writer's finalizer could then remove.
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
  const read = <A>(
    use: (registry: Registry["Service"]) => Effect.Effect<A, ArtifactsError>,
    bounded = false,
  ) => load.pipe(Effect.flatMap((rows) => use(factory(rows, (next) => persist(next, bounded)))));
  const edit = <A>(
    use: (registry: Registry["Service"]) => Effect.Effect<A, ArtifactsError>,
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
                message: "PRECONDITION_FAILED: repository registry is locked",
                cause: Object.assign(new Error("repository registry lock contention"), {
                  code: "EEXIST",
                }),
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
          Effect.tap(() => Effect.tryPromise({ try: () => syncRegistry(target), catch: failed })),
        ),
      () => Effect.promise(() => fs.rm(lock, { force: true }).catch(() => undefined)),
    ).pipe(
      Effect.uninterruptible,
      Semaphore.withPermit(writers),
      Effect.tapError(Capacity.report("repositories")),
    );

  return {
    create: Effect.fn("RegistryFile.create")((name, meta) =>
      edit((registry) => registry.create(name, meta)),
    ),
    get: Effect.fn("RegistryFile.get")((name) => read((registry) => registry.get(name))),
    list: Effect.fn("RegistryFile.list")((options) => read((registry) => registry.list(options))),
    delete: Effect.fn("RegistryFile.delete")((name) =>
      edit((registry) => registry.delete(name), false),
    ),
    touch: Effect.fn("RegistryFile.touch")((name, at) =>
      edit((registry) => registry.touch(name, at)),
    ),
    setDefaultBranch: Effect.fn("RegistryFile.setDefaultBranch")((name, branch) =>
      edit((registry) => registry.setDefaultBranch(name, branch)),
    ),
    finish: Effect.fn("RegistryFile.finish")((name, id) =>
      edit((registry) => registry.finish(name, id), false),
    ),
  } satisfies Registry["Service"];
});
