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
 * `RepoClient.raw` — deferred by `patches/alchemy+2.0.0-beta.72.patch` —
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

import * as fs from "node:fs/promises";
import * as path from "node:path";

import { fetchRepository } from "../client/Fetch.ts";
import { stores as memoryStores } from "../git/Memory.ts";
import { stores as nodeStores } from "../git/Node.ts";
import { bytesToHex } from "../git/Format.ts";
import { checkRefName, ObjectStore, RefStore } from "../git/Store.ts";

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
    /** The default branch an import discovered, which `create` could only guess. */
    readonly setDefaultBranch: (name: string, branch: string) => Effect.Effect<void>;
  }
>()("artifacts/Registry") {}

const makeRegistry = (rows: Map<string, RepoRecord>, persist: () => Promise<void>) =>
  Registry.of({
    create: (name, meta) =>
      Effect.gen(function* () {
        if (rows.has(name)) {
          return yield* failure("ALREADY_EXISTS", `repo '${name}' exists`);
        }
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
        yield* Effect.promise(persist);
        return record;
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
    delete: (name) =>
      Effect.gen(function* () {
        const existed = rows.delete(name);
        if (existed) yield* Effect.promise(persist);
        return existed;
      }),
    touch: (name, at) =>
      Effect.gen(function* () {
        const record = rows.get(name);
        if (record === undefined) return;
        rows.set(name, { ...record, updatedAt: at, lastPushAt: at });
        yield* Effect.promise(persist);
      }),
    setDefaultBranch: (name, branch) =>
      Effect.gen(function* () {
        const record = rows.get(name);
        if (record === undefined || record.defaultBranch === branch) return;
        rows.set(name, { ...record, defaultBranch: branch });
        yield* Effect.promise(persist);
      }),
  });

export const registryMemory = Layer.sync(Registry)(() =>
  makeRegistry(new Map(), () => Promise.resolve()),
);

/** Atomic enough for one process: temp file plus rename, like every backend. */
const saveJson = async (target: string, value: unknown) => {
  await fs.mkdir(path.dirname(target), { recursive: true });
  const temporary = `${target}.${crypto.randomUUID()}.tmp`;
  await fs.writeFile(temporary, JSON.stringify(value, null, 1));
  await fs.rename(temporary, target);
};

const loadJson = async <A>(target: string): Promise<A | null> => {
  try {
    return JSON.parse(await fs.readFile(target, "utf8")) as A;
  } catch {
    return null;
  }
};

/** The durable form: one JSON file, rows revived with their `Date`s. */
export const registryNode = (root: string) =>
  Layer.effect(
    Registry,
    Effect.promise(async () => {
      const file = path.join(root, ".registry.json");
      interface Stored extends Omit<RepoRecord, "createdAt" | "updatedAt" | "lastPushAt"> {
        readonly createdAt: string;
        readonly updatedAt: string;
        readonly lastPushAt: string | null;
      }
      const stored = (await loadJson<Stored[]>(file)) ?? [];
      const rows = new Map<string, RepoRecord>(
        stored.map((row) => [
          row.name,
          {
            ...row,
            createdAt: new Date(row.createdAt),
            updatedAt: new Date(row.updatedAt),
            lastPushAt: row.lastPushAt === null ? null : new Date(row.lastPushAt),
          },
        ]),
      );
      return makeRegistry(rows, () => saveJson(file, [...rows.values()]));
    }),
  );

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

export interface TokenRow {
  readonly id: string;
  readonly repo: string;
  readonly scope: "read" | "write";
  readonly digest: string;
  readonly createdAt: string;
  readonly expiresAt: string;
  revoked: boolean;
}

const makeTokens = (rows: TokenRow[], persist: () => Promise<void>) => {
  const digestOf = (plaintext: string) =>
    Effect.promise(async () => {
      const bytes = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(plaintext));
      return bytesToHex(new Uint8Array(bytes));
    });
  const stateOf = (row: TokenRow): "active" | "expired" | "revoked" =>
    row.revoked
      ? "revoked"
      : new Date(row.expiresAt).getTime() <= Date.now()
        ? "expired"
        : "active";

  return Tokens.of({
    issue: (repo, scope, ttlSeconds) =>
      Effect.gen(function* () {
        if (!(ttlSeconds > 0)) return yield* failure("INVALID_TTL", `ttl ${ttlSeconds}`);
        const plaintext = `art_${crypto.randomUUID().replaceAll("-", "")}`;
        const row: TokenRow = {
          id: crypto.randomUUID(),
          repo,
          scope,
          digest: yield* digestOf(plaintext),
          createdAt: new Date().toISOString(),
          expiresAt: new Date(Date.now() + ttlSeconds * 1000).toISOString(),
          revoked: false,
        };
        rows.push(row);
        yield* Effect.promise(persist);
        return { id: row.id, plaintext, scope, expiresAt: row.expiresAt };
      }),
    list: (repo) =>
      Effect.sync(() => {
        const mine = rows.filter((row) => row.repo === repo);
        return {
          tokens: mine.map((row) => ({
            id: row.id,
            scope: row.scope,
            state: stateOf(row),
            createdAt: row.createdAt,
            expiresAt: row.expiresAt,
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
        yield* Effect.promise(persist);
        return true;
      }),
    verify: (repo, presented) =>
      Effect.gen(function* () {
        const digest = yield* digestOf(presented);
        const row = rows.find((row) => row.repo === repo && row.digest === digest);
        return row !== undefined && stateOf(row) === "active" ? row.scope : null;
      }),
  });
};

export const tokensMemory = Layer.sync(Tokens)(() => makeTokens([], () => Promise.resolve()));

/** Digests only at rest, in the durable form too. */
export const tokensNode = (root: string) =>
  Layer.effect(
    Tokens,
    Effect.promise(async () => {
      const file = path.join(root, ".tokens.json");
      const rows = (await loadJson<TokenRow[]>(file)) ?? [];
      return makeTokens(rows, () => saveJson(file, rows));
    }),
  );

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
  list: Stream.unwrap(
    Stream.runCollect(child.list).pipe(
      Effect.map((own) => {
        const mine = new Set(own);
        return Stream.concat(
          Stream.fromIterable(own),
          parent.list.pipe(Stream.filter((oid) => !mine.has(oid))),
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
    /**
     * The repositories whose object reads fall through to this one.
     *
     * A fork holds no copy of what it inherited, so dropping the store it
     * reads through erases the fork's history with it — and the fork's refs,
     * registry row and remote URL all survive to advertise objects that are
     * no longer anywhere. Asking first is what makes that refusable.
     */
    readonly dependents: (name: string) => Effect.Effect<ReadonlyArray<string>>;
    readonly drop: (name: string) => Effect.Effect<void>;
  }
>()("artifacts/RepoStores") {}

export const repoStoresMemory = Layer.sync(RepoStores)(() => {
  const instances = new Map<string, StoreInstances>();
  const forks = new Map<string, string>();

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
        forks.set(child, parent);
        return forked;
      }),
    dependents: (name) =>
      Effect.sync(() => [...forks].filter(([, parent]) => parent === name).map(([child]) => child)),
    drop: (name) =>
      Effect.sync(() => {
        instances.delete(name);
        forks.delete(name);
      }),
  });
});

/** The durable factory: node stores on disk, fork links in `.forks.json`. */
export const repoStoresNode = (root: string) =>
  Layer.effect(
    RepoStores,
    Effect.promise(async () => {
      const forksFile = path.join(root, ".forks.json");
      const forks = new Map<string, string>(
        Object.entries((await loadJson<Record<string, string>>(forksFile)) ?? {}),
      );
      const instances = new Map<string, StoreInstances>();

      const buildAt = (name: string) =>
        Effect.gen(function* () {
          return { objects: yield* ObjectStore, refs: yield* RefStore };
        }).pipe(Effect.provide(nodeStores(path.join(root, name))));

      const open = (name: string): Effect.Effect<StoreInstances> =>
        Effect.gen(function* () {
          const cached = instances.get(name);
          if (cached !== undefined) return cached;
          const own = yield* buildAt(name);
          const parent = forks.get(name);
          const built: StoreInstances =
            parent === undefined
              ? own
              : { objects: alternates(own.objects, (yield* open(parent)).objects), refs: own.refs };
          instances.set(name, built);
          return built;
        });

      return RepoStores.of({
        open,
        fork: (child, parent) =>
          Effect.gen(function* () {
            forks.set(child, parent);
            yield* Effect.promise(() => saveJson(forksFile, Object.fromEntries(forks)));

            // git's own way of saying "my objects are over there too". The
            // in-memory fall-through below only exists inside this process,
            // and the fork is served over HTTP by a host that opens the
            // directory directly — and read by `git` itself, which has read
            // this file since 2005.
            yield* Effect.promise(async () => {
              const info = path.join(root, child, "objects", "info");
              await fs.mkdir(info, { recursive: true });
              await fs.writeFile(
                path.join(info, "alternates"),
                `${path.resolve(root, parent, "objects")}\n`,
              );

              // And the other direction, which git has no file for: the parent
              // needs to know it is lent out, because its own `gc` cannot see
              // the refs that keep these objects alive.
              const lent = path.join(root, parent, "objects", "info");
              await fs.mkdir(lent, { recursive: true });
              await fs.appendFile(path.join(lent, "borrowers"), `${child}\n`);
            });

            instances.delete(child);
            return yield* open(child);
          }),
        dependents: (name) =>
          Effect.sync(() =>
            [...forks].filter(([, parent]) => parent === name).map(([child]) => child),
          ),
        drop: (name) =>
          Effect.gen(function* () {
            instances.delete(name);
            const parent = forks.get(name);
            if (forks.delete(name)) {
              yield* Effect.promise(() => saveJson(forksFile, Object.fromEntries(forks)));
            }
            // The parent can collect again once nothing borrows from it.
            if (parent !== undefined) {
              yield* Effect.promise(async () => {
                const borrowers = path.join(root, parent, "objects", "info", "borrowers");
                const remaining = [...forks]
                  .filter(([, from]) => from === parent)
                  .map(([child]) => child);
                if (remaining.length === 0) await fs.rm(borrowers, { force: true });
                else await fs.writeFile(borrowers, `${remaining.join("\n")}\n`);
              });
            }
            yield* Effect.promise(() =>
              fs.rm(path.join(root, name), { recursive: true, force: true }),
            );
          }),
      });
    }),
  );

/** `import`, through the shared smart-HTTP client, errors mapped to Artifacts codes. */
const clone = (source: string, branch: string | undefined, target: StoreInstances) =>
  fetchRepository({ url: source, branch, stores: target }).pipe(
    Effect.mapError((error) =>
      error._tag === "Invalid"
        ? failure(error.field === "branch" ? "NOT_FOUND" : "UPSTREAM_UNAVAILABLE", error.reason)
        : failure(
            "INTERNAL_ERROR",
            `${error._tag}${"reason" in error ? ` — ${error.reason}` : ""}`,
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
            // Checked here rather than discovered by the store: a branch name
            // the ref rules refuse would otherwise fail *after* the registry
            // row exists, as a defect rather than as an answer.
            const branch = `refs/heads/${opts?.setDefaultBranch ?? "main"}`;
            const problem = checkRefName(branch);
            if (problem !== null) {
              return yield* failure("INVALID_REPO_NAME", `default branch '${branch}': ${problem}`);
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
              // The same check `create` and `fork` apply, because this name
              // reaches `fs.rm(join(root, name), { recursive: true })`: a
              // caller passing `../../home/alice` would otherwise have an
              // arbitrary directory removed and be told the repo did not exist.
              if (!REPO_NAME.test(name)) {
                return yield* failure("INVALID_REPO_NAME", `'${name}'`);
              }

              // A fork keeps no copy of the history it inherited, so deleting
              // what it reads through would take that history with it and
              // leave a repository advertising objects nothing holds. The
              // caller is told which forks stand in the way instead.
              const dependents = yield* repoStores.dependents(name);
              if (dependents.length > 0) {
                return yield* failure(
                  "PRECONDITION_FAILED",
                  `repo '${name}' is the source of ${dependents.join(", ")}; delete those first`,
                );
              }

              // Tokens are keyed by repository *name*, and the registries do
              // not cascade — so a token left behind would authorise pushes
              // into whatever is created under this name next, by whoever
              // creates it. It goes before the row it is scoped to.
              for (const token of (yield* tokens.list(name)).tokens) {
                yield* tokens.revoke(name, token.id);
              }
              // Storage first, then the row: `drop` can fail — a locked pack
              // file, a racing write from the host serving the same directory
              // — and a name freed ahead of it would let the next caller
              // create that repository over a directory still holding the old
              // one's objects and refs, and clone somebody else's history.
              yield* repoStores.drop(name);
              return yield* registry.delete(name);
            }),
          import: (opts) =>
            Effect.gen(function* () {
              yield* Effect.try({
                try: () => new URL(opts.source.url),
                catch: () => failure("INVALID_URL", opts.source.url),
              });

              const { record, result, stores } = yield* create(
                opts.target.name,
                opts.target.opts,
                opts.source.url,
              );

              // An import that fails part-way has already taken the name,
              // issued a token and opened storage. Leaving that behind means a
              // repository nothing cloned into and a name the caller cannot
              // retry with, so the creation is undone with the clone.
              const cloned = yield* clone(opts.source.url, opts.source.branch, stores).pipe(
                Effect.onError(() =>
                  Effect.gen(function* () {
                    // The write token `create` issued outlives the row it was
                    // issued against — the registries do not cascade — so it
                    // would still authorise pushes to whatever is created
                    // under this name next. It goes first, while the name it
                    // is scoped to still resolves.
                    for (const token of (yield* tokens.list(record.name)).tokens) {
                      yield* tokens.revoke(record.name, token.id);
                    }
                    // Storage before the row, the same order `delete` uses: a
                    // `drop` that fails must not leave the name free while the
                    // half-cloned objects are still on disk under it.
                    yield* repoStores.drop(record.name);
                    yield* registry.delete(record.name);
                  }).pipe(Effect.ignore),
                ),
              );
              if (cloned.defaultBranch !== undefined) {
                // The branch name comes from the remote's advertisement, so
                // it is as untrusted as any other name off the network.
                const head = `refs/heads/${cloned.defaultBranch}`;
                if (checkRefName(head) === null) {
                  yield* stores.refs.setHead(head).pipe(Effect.orDie);
                  // And the row has to agree with HEAD: `create` could only
                  // guess `main` before the remote had been asked.
                  yield* registry.setDefaultBranch(record.name, cloned.defaultBranch);
                }
              }
              yield* registry.touch(record.name, new Date());

              // The row now says which branch the remote actually had, and the
              // answer has to agree with it: `result` was built before the
              // clone, when `main` was still a guess.
              const current = yield* registry.get(record.name);
              return current === null
                ? result
                : { ...result, defaultBranch: current.defaultBranch };
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

/** Everything durable under one directory: the self-hosted provider. */
export const localNode = (options: { readonly root: string } & LocalOptions) =>
  localNamespace(options).pipe(
    Layer.provideMerge(
      Layer.mergeAll(
        registryNode(options.root),
        tokensNode(options.root),
        repoStoresNode(options.root),
      ),
    ),
  );
