/**
 * A local Cloudflare Artifacts provider, backed by this server.
 *
 * Alchemy ships three implementations of the R2 binding (native, HTTP, local)
 * and exactly one of Artifacts — the native one. This is the local one: the
 * same `ReadWriteNamespaceClient` surface a Worker sees, served from this
 * repository's own stores, so tests and `alchemy dev` get an offline loop.
 *
 * What Cloudflare has implicitly is named here as ports — `Registry` (DO
 * namespaces cannot be enumerated; a registry is what makes `list` readable
 * and gives the metadata somewhere to live) and `Tokens` (scoped, TTL'd,
 * revocable). `fork` is git's own answer, alternates: the child's object
 * store starts empty and reads fall through to the parent until first write.
 *
 * The two `raw` escape hatches follow alchemy's `LocalR2Gateway` precedent:
 * the namespace-level one dies with guidance (its type says `never`), and
 * `RepoClient.raw` — deferred by `patches/alchemy+2.0.0-beta.70.patch` —
 * fails with a typed `ArtifactsError` a caller can handle.
 */
// Deep imports on purpose: the `alchemy/Cloudflare` barrel drags in modules
// whose runtime needs `@effect/platform-node` and a newer `effect` than this
// repo pins. The patch adds these two file-level export entries.
import type { Namespace as ArtifactsNamespace } from "alchemy/Cloudflare/Artifacts/Namespace";
import {
  ArtifactsError,
  ReadWriteNamespace,
  type ReadWriteNamespaceClient,
  type RepoClient,
} from "alchemy/Cloudflare/Artifacts/ReadWriteNamespace";
import { Context, Effect, Layer, Stream } from "effect";

import { fetchRepository } from "../client/Fetch.ts";
import { stores as memoryStores } from "../git/Memory.ts";
import { ObjectStore, RefStore } from "../git/Store.ts";

const failure = (code: string, message: string) =>
  new ArtifactsError({ message: `${code}: ${message}`, cause: new Error(code) });

/** Cloudflare's rule, applied locally too: it is the portable subset. */
const REPO_NAME = /^[a-z0-9][a-z0-9._-]{0,99}$/;

export interface RepoMeta {
  readonly description: string | null;
  readonly defaultBranch: string;
  readonly readOnly: boolean;
  /** `artifacts:ns/repo` for forks, the source URL for imports, else null. */
  readonly source: string | null;
}

export interface RepoRecord extends RepoMeta {
  readonly id: string;
  readonly name: string;
  readonly createdAt: Date;
  readonly updatedAt: Date;
  readonly lastPushAt: Date | null;
}

/**
 * The registry Cloudflare has implicitly and we do not: one row per repo, so
 * `list({ limit, cursor })` has something to read and metadata somewhere to
 * live.
 */
export class Registry extends Context.Service<
  Registry,
  {
    readonly create: (name: string, meta: RepoMeta) => Effect.Effect<RepoRecord, ArtifactsError>;
    readonly get: (name: string) => Effect.Effect<RepoRecord | null>;
    readonly list: (options?: {
      readonly limit?: number;
      readonly cursor?: string;
    }) => Effect.Effect<{
      readonly repos: ReadonlyArray<RepoRecord>;
      readonly total: number;
      readonly cursor?: string;
    }>;
    readonly delete: (name: string) => Effect.Effect<boolean>;
    readonly touch: (name: string, at: Date) => Effect.Effect<void>;
  }
>()("artifacts/Registry") {}

export const registryMemory = Layer.sync(Registry)(() => {
  const rows = new Map<string, RepoRecord>();
  return Registry.of({
    create: (name, meta) =>
      Effect.suspend(() => {
        if (rows.has(name)) return Effect.fail(failure("ALREADY_EXISTS", `repo '${name}' exists`));
        const now = new Date();
        const record: RepoRecord = {
          ...meta,
          id: crypto.randomUUID(),
          name,
          createdAt: now,
          updatedAt: now,
          lastPushAt: null,
        };
        rows.set(name, record);
        return Effect.succeed(record);
      }),
    get: (name) => Effect.sync(() => rows.get(name) ?? null),
    list: (options) =>
      Effect.sync(() => {
        const all = [...rows.values()].sort((a, b) => a.name.localeCompare(b.name));
        const start = options?.cursor === undefined ? 0 : Number.parseInt(options.cursor, 10);
        const limit = options?.limit ?? 100;
        const page = all.slice(start, start + limit);
        return {
          repos: page,
          total: all.length,
          ...(start + limit < all.length ? { cursor: String(start + limit) } : {}),
        };
      }),
    delete: (name) => Effect.sync(() => rows.delete(name)),
    touch: (name, at) =>
      Effect.sync(() => {
        const record = rows.get(name);
        if (record !== undefined) rows.set(name, { ...record, updatedAt: at, lastPushAt: at });
      }),
  });
});

/**
 * Scoped, TTL'd, revocable per-repo tokens, plaintext returned exactly once.
 * Only SHA-256 digests are kept at rest; `verify` is what the smart-HTTP and
 * JSON middleware will call, accepting the token as an HTTP Basic password.
 */
export class Tokens extends Context.Service<
  Tokens,
  {
    readonly issue: (
      repo: string,
      scope: "read" | "write",
      ttlSeconds: number,
    ) => Effect.Effect<ArtifactsCreateTokenResult, ArtifactsError>;
    readonly list: (repo: string) => Effect.Effect<ArtifactsTokenListResult>;
    readonly revoke: (repo: string, tokenOrId: string) => Effect.Effect<boolean>;
    readonly verify: (repo: string, presented: string) => Effect.Effect<"read" | "write" | null>;
  }
>()("artifacts/Tokens") {}

export const tokensMemory = Layer.sync(Tokens)(() => {
  interface Row {
    readonly id: string;
    readonly repo: string;
    readonly scope: "read" | "write";
    readonly digest: string;
    readonly createdAt: Date;
    readonly expiresAt: Date;
    revoked: boolean;
  }
  const rows: Row[] = [];

  const digestOf = (plaintext: string) =>
    Effect.promise(async () => {
      const bytes = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(plaintext));
      return [...new Uint8Array(bytes)].map((byte) => byte.toString(16).padStart(2, "0")).join("");
    });
  const stateOf = (row: Row): "active" | "expired" | "revoked" =>
    row.revoked ? "revoked" : row.expiresAt.getTime() <= Date.now() ? "expired" : "active";

  return Tokens.of({
    issue: (repo, scope, ttlSeconds) =>
      Effect.gen(function* () {
        if (!(ttlSeconds > 0)) return yield* failure("INVALID_TTL", `ttl ${ttlSeconds}`);
        const plaintext = `art_${crypto.randomUUID().replaceAll("-", "")}`;
        const row: Row = {
          id: crypto.randomUUID(),
          repo,
          scope,
          digest: yield* digestOf(plaintext),
          createdAt: new Date(),
          expiresAt: new Date(Date.now() + ttlSeconds * 1000),
          revoked: false,
        };
        rows.push(row);
        return { id: row.id, plaintext, scope, expiresAt: row.expiresAt.toISOString() };
      }),
    list: (repo) =>
      Effect.sync(() => {
        const mine = rows.filter((row) => row.repo === repo);
        return {
          tokens: mine.map((row) => ({
            id: row.id,
            scope: row.scope,
            state: stateOf(row),
            createdAt: row.createdAt.toISOString(),
            expiresAt: row.expiresAt.toISOString(),
          })),
          total: mine.length,
        };
      }),
    revoke: (repo, tokenOrId) =>
      Effect.gen(function* () {
        const digest = yield* digestOf(tokenOrId);
        const row = rows.find(
          (row) => row.repo === repo && (row.id === tokenOrId || row.digest === digest),
        );
        if (row === undefined || row.revoked) return false;
        row.revoked = true;
        return true;
      }),
    verify: (repo, presented) =>
      Effect.gen(function* () {
        const digest = yield* digestOf(presented);
        const row = rows.find((row) => row.repo === repo && row.digest === digest);
        return row !== undefined && stateOf(row) === "active" ? row.scope : null;
      }),
  });
});

export interface StoreInstances {
  readonly objects: ObjectStore["Service"];
  readonly refs: RefStore["Service"];
}

/**
 * `fork`, the cheap way: reads fall through to the parent until the first
 * write, so a fork costs one registry row and its refs — no object copies.
 */
export const alternates = (
  child: ObjectStore["Service"],
  parent: ObjectStore["Service"],
): ObjectStore["Service"] => ({
  read: (oid) => child.read(oid).pipe(Effect.catchTag("ObjectNotFound", () => parent.read(oid))),
  readStream: (oid) =>
    child.readStream(oid).pipe(Effect.catchTag("ObjectNotFound", () => parent.readStream(oid))),
  write: child.write,
  has: (oid) =>
    child.has(oid).pipe(Effect.flatMap((own) => (own ? Effect.succeed(true) : parent.has(oid)))),
  delete: child.delete,
  list: () =>
    Stream.unwrap(
      Stream.runCollect(child.list()).pipe(
        Effect.map((own) => {
          const mine = new Set(own);
          return Stream.concat(
            Stream.fromIterable(own),
            parent.list().pipe(Stream.filter((oid) => !mine.has(oid))),
          );
        }),
      ),
    ),
});

/** One pair of stores per repository, alive for the provider's lifetime. */
export class RepoStores extends Context.Service<
  RepoStores,
  {
    readonly open: (name: string) => Effect.Effect<StoreInstances>;
    /** Open `child` with its object reads falling through to `parent`. */
    readonly fork: (child: string, parent: string) => Effect.Effect<StoreInstances>;
    readonly drop: (name: string) => Effect.Effect<void>;
  }
>()("artifacts/RepoStores") {}

export const repoStoresMemory = Layer.sync(RepoStores)(() => {
  const instances = new Map<string, StoreInstances>();

  const build = Effect.gen(function* () {
    return { objects: yield* ObjectStore, refs: yield* RefStore };
  }).pipe(Effect.provide(memoryStores));

  const open = (name: string) =>
    Effect.gen(function* () {
      const existing = instances.get(name);
      if (existing !== undefined) return existing;
      const built = yield* build;
      instances.set(name, built);
      return built;
    });

  return RepoStores.of({
    open,
    fork: (child, parent) =>
      Effect.gen(function* () {
        const base = yield* open(parent);
        const own = yield* build;
        const forked: StoreInstances = {
          objects: alternates(own.objects, base.objects),
          refs: own.refs,
        };
        instances.set(child, forked);
        return forked;
      }),
    drop: (name) =>
      Effect.sync(() => {
        instances.delete(name);
      }),
  });
});

/** `import`, through the shared smart-HTTP client, errors mapped to Artifacts codes. */
const clone = (source: string, branch: string | undefined, target: StoreInstances) =>
  fetchRepository({ url: source, branch, stores: target }).pipe(
    Effect.catch((error) =>
      Effect.fail(
        error._tag === "Invalid"
          ? failure(error.field === "branch" ? "NOT_FOUND" : "UPSTREAM_UNAVAILABLE", error.reason)
          : failure(
              "INTERNAL_ERROR",
              `${error._tag}${"reason" in error ? ` — ${error.reason}` : ""}`,
            ),
      ),
    ),
  );

export interface LocalOptions {
  /** What `remote` fields advertise, e.g. the node host's URL. */
  readonly remoteBase?: string;
}

/**
 * The binding implementation: the same tag alchemy's native
 * `ReadWriteNamespaceBinding` provides, so a stack swaps providers with one
 * `Layer.provide` — that is the entire point.
 */
export const localNamespace = (
  options?: LocalOptions,
): Layer.Layer<ReadWriteNamespace, never, Registry | Tokens | RepoStores> =>
  Layer.effect(
    ReadWriteNamespace,
    Effect.gen(function* () {
      const registry = yield* Registry;
      const tokens = yield* Tokens;
      const repoStores = yield* RepoStores;
      const remoteBase = options?.remoteBase ?? "http://127.0.0.1:8080";
      const remoteOf = (name: string) => `${remoteBase}/${name}`;

      const make = (namespace: ArtifactsNamespace): ReadWriteNamespaceClient => {
        const created = (record: RepoRecord, token: ArtifactsCreateTokenResult) => ({
          id: record.id,
          name: record.name,
          description: record.description,
          defaultBranch: record.defaultBranch,
          remote: remoteOf(record.name),
          token: token.plaintext,
          tokenExpiresAt: token.expiresAt,
        });

        const create = (
          name: string,
          opts?: { readOnly?: boolean; description?: string; setDefaultBranch?: string },
          source: string | null = null,
        ) =>
          Effect.gen(function* () {
            if (!REPO_NAME.test(name)) {
              return yield* failure("INVALID_REPO_NAME", `'${name}'`);
            }
            const record = yield* registry.create(name, {
              description: opts?.description ?? null,
              defaultBranch: opts?.setDefaultBranch ?? "main",
              readOnly: opts?.readOnly ?? false,
              source,
            });
            const stores = yield* repoStores.open(name);
            yield* stores.refs.setHead(`refs/heads/${record.defaultBranch}`).pipe(Effect.orDie);
            const token = yield* tokens.issue(name, "write", 86_400);
            return { record, stores, result: created(record, token) };
          });

        const repoClient = (record: RepoRecord): RepoClient => ({
          raw: Effect.fail(
            failure(
              "INTERNAL_ERROR",
              "ArtifactsRepo cannot be produced off-platform; use the client methods",
            ),
          ),
          createToken: (scope, ttl) => tokens.issue(record.name, scope ?? "read", ttl ?? 3600),
          listTokens: () => tokens.list(record.name),
          revokeToken: (tokenOrId) => tokens.revoke(record.name, tokenOrId),
          fork: (name, opts) =>
            Effect.gen(function* () {
              if (!REPO_NAME.test(name)) {
                return yield* failure("INVALID_REPO_NAME", `'${name}'`);
              }
              const child = yield* registry.create(name, {
                description: opts?.description ?? null,
                defaultBranch: record.defaultBranch,
                readOnly: opts?.readOnly ?? false,
                source: `artifacts:${namespace.namespace}/${record.name}`,
              });
              const stores = yield* repoStores.fork(name, record.name);
              const parent = yield* repoStores.open(record.name);

              const refs = yield* parent.refs.list("refs/").pipe(Effect.orDie);
              const copied = refs.filter(
                ([refName]) =>
                  opts?.defaultBranchOnly !== true ||
                  refName === `refs/heads/${record.defaultBranch}`,
              );
              yield* stores.refs
                .apply(copied.map(([refName, oid]) => ({ name: refName, value: oid })))
                .pipe(Effect.orDie);
              yield* stores.refs.setHead(`refs/heads/${child.defaultBranch}`).pipe(Effect.orDie);

              const token = yield* tokens.issue(name, "write", 86_400);
              return created(child, token);
            }),
        });

        const info = (record: RepoRecord): ArtifactsRepoInfo => ({
          id: record.id,
          name: record.name,
          description: record.description,
          defaultBranch: record.defaultBranch,
          createdAt: record.createdAt.toISOString(),
          updatedAt: record.updatedAt.toISOString(),
          lastPushAt: record.lastPushAt?.toISOString() ?? null,
          source: record.source,
          readOnly: record.readOnly,
          remote: remoteOf(record.name),
        });

        return {
          // The type says `never` fails, so the only honest off-platform
          // answer is a defect with guidance — `LocalR2Gateway`'s precedent.
          raw: Effect.die(
            failure("INTERNAL_ERROR", "the raw Artifacts binding does not exist off-platform"),
          ),
          create: (name, opts) => create(name, opts).pipe(Effect.map(({ result }) => result)),
          get: (name) =>
            Effect.gen(function* () {
              const record = yield* registry.get(name);
              if (record === null) return yield* failure("NOT_FOUND", `repo '${name}'`);
              return repoClient(record);
            }),
          list: (opts) =>
            registry.list(opts).pipe(
              Effect.map(({ cursor, repos, total }) => ({
                repos: repos.map((record) => {
                  const { remote: _remote, ...rest } = info(record);
                  return rest;
                }),
                total,
                ...(cursor === undefined ? {} : { cursor }),
              })),
            ),
          delete: (name) =>
            Effect.gen(function* () {
              const existed = yield* registry.delete(name);
              yield* repoStores.drop(name);
              return existed;
            }),
          import: (opts) =>
            Effect.gen(function* () {
              yield* Effect.try({
                try: () => new URL(opts.source.url),
                catch: () => failure("INVALID_URL", opts.source.url),
              }).pipe(Effect.catch((error) => Effect.fail(error)));

              const { record, result, stores } = yield* create(
                opts.target.name,
                opts.target.opts,
                opts.source.url,
              );
              const cloned = yield* clone(opts.source.url, opts.source.branch, stores);
              if (cloned.defaultBranch !== undefined) {
                yield* stores.refs.setHead(`refs/heads/${cloned.defaultBranch}`).pipe(Effect.orDie);
              }
              yield* registry.touch(record.name, new Date());
              return result;
            }),
        };
      };

      // The tag's service is `(namespace) => Effect<client>` — the same
      // shape the native binding registers.
      return (namespace: ArtifactsNamespace) => Effect.succeed(make(namespace));
    }),
  );

/** Everything in memory: the test and `alchemy dev` composition. */
export const localMemory = (options?: LocalOptions) =>
  localNamespace(options).pipe(
    Layer.provideMerge(Layer.mergeAll(registryMemory, tokensMemory, repoStoresMemory)),
  );
