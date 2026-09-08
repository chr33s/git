/** Fresh reads and locked updates for the local provider's repository registry. */
import * as fs from "node:fs/promises";
import * as path from "node:path";
import { ArtifactsError } from "alchemy/Cloudflare/Artifacts/ReadWriteNamespace";
import { Effect, Schema, Semaphore } from "effect";

import type { Registry, RepoRecord } from "./Namespace.ts";

const Rows = Schema.fromJsonString(
  Schema.Array(
    Schema.Struct({
      id: Schema.String,
      name: Schema.String,
      description: Schema.NullOr(Schema.String),
      defaultBranch: Schema.String,
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
  const target = path.join(root, ".registry.json");
  const lock = `${target}.lock`;
  const writers = yield* Semaphore.make(1);
  const failed = (cause: unknown) =>
    new ArtifactsError({
      message: `INTERNAL_ERROR: registry '${target}': ${String(cause)}`,
      cause: cause instanceof Error ? cause : new Error(String(cause)),
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
    Effect.map((rows) => new Map(rows.map((row) => [row.name, row]))),
  );
  const persist = (rows: ReadonlyMap<string, RepoRecord>) =>
    Effect.tryPromise({
      try: async () => {
        // Keep the reservation until release; renaming it would let another
        // writer acquire a lock that this writer's finalizer could then remove.
        const temporary = `${target}.${crypto.randomUUID()}.tmp`;
        try {
          await fs.writeFile(temporary, `${JSON.stringify([...rows.values()], null, 1)}\n`);
          await fs.rename(temporary, target);
        } finally {
          await fs.rm(temporary, { force: true });
        }
      },
      catch: failed,
    });
  const read = <A>(use: (registry: Registry["Service"]) => Effect.Effect<A, ArtifactsError>) =>
    load.pipe(Effect.flatMap((rows) => use(factory(rows, persist))));
  const edit = <A>(use: (registry: Registry["Service"]) => Effect.Effect<A, ArtifactsError>) =>
    Effect.acquireUseRelease(
      Effect.tryPromise({
        try: async () => {
          await fs.mkdir(root, { recursive: true });
          const handle = await fs.open(lock, "wx", 0o600);
          try {
            await handle.close();
          } catch (cause) {
            await fs.rm(lock, { force: true });
            throw cause;
          }
        },
        catch: failed,
      }),
      () => read(use),
      () => Effect.promise(() => fs.rm(lock, { force: true }).catch(() => undefined)),
    ).pipe(Effect.uninterruptible, Semaphore.withPermit(writers));

  return {
    create: Effect.fn("RegistryFile.create")((name, meta) =>
      edit((registry) => registry.create(name, meta)),
    ),
    get: Effect.fn("RegistryFile.get")((name) => read((registry) => registry.get(name))),
    list: Effect.fn("RegistryFile.list")((options) => read((registry) => registry.list(options))),
    delete: Effect.fn("RegistryFile.delete")((name) => edit((registry) => registry.delete(name))),
    touch: Effect.fn("RegistryFile.touch")((name, at) =>
      edit((registry) => registry.touch(name, at)),
    ),
    setDefaultBranch: Effect.fn("RegistryFile.setDefaultBranch")((name, branch) =>
      edit((registry) => registry.setDefaultBranch(name, branch)),
    ),
    finish: Effect.fn("RegistryFile.finish")((name, id) =>
      edit((registry) => registry.finish(name, id)),
    ),
  } satisfies Registry["Service"];
});
