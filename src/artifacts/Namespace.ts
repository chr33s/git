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

import { realpathSync, statSync } from "node:fs";
import * as fs from "node:fs/promises";
import * as path from "node:path";

import { fetchRepository } from "../client/Fetch.ts";
import { stores as memoryStores } from "../git/Memory.ts";
import { borrowersOf, retirePacksAndRemove, stores as nodeStores } from "../git/Node.ts";
import { bytesToHex } from "../git/Format.ts";
import { checkRefName, ObjectStore, RefStore } from "../git/Store.ts";

/**
 * Whether a path is definitely not there.
 *
 * `ENOENT` and nothing else. "Could not look" — no permission, an unreadable
 * mount, a symlink loop — has to read as "still there": these answers decide
 * whether a fork's objects may be collected, and that is not undoable.
 */
const missing = (target: string): boolean => {
  try {
    statSync(target);
    return false;
  } catch (error) {
    return error instanceof Error && "code" in error && error.code === "ENOENT";
  }
};

/** A path as the filesystem spells it, or as written when it cannot say. */
const canonical = (target: string): string => {
  try {
    return realpathSync.native(target);
  } catch {
    return path.resolve(target);
  }
};

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
        const next = start + limit;
        return next < all.length
          ? { repos: page, total: all.length, cursor: String(next) }
          : { repos: page, total: all.length };
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

/** The documents this provider keeps on disk: registry rows, token rows, fork links. */
type PersistedDocument =
  | ReadonlyArray<RepoRecord>
  | ReadonlyArray<TokenRow>
  | Record<string, string>;

/** Atomic enough for one process: temp file plus rename, like every backend. */
const saveJson = async (target: string, value: PersistedDocument) => {
  await fs.mkdir(path.dirname(target), { recursive: true });
  const temporary = `${target}.${crypto.randomUUID()}.tmp`;
  await fs.writeFile(temporary, JSON.stringify(value, null, 1));
  await fs.rename(temporary, target);
};

const loadJson = async <A>(target: string): Promise<A | null> => {
  try {
    // SAFETY: these files are written only by `saveJson`, so a successful
    // parse yields the caller's persisted shape; anything unreadable lands in
    // the catch and reads as an empty store.
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
    /**
     * Open `child` with its object reads falling through to `parent`.
     *
     * Fails when there is no `parent` left to read through. A fork is nothing
     * but a pointer at somebody else's objects, so one made against storage
     * that has been deleted is an empty repository wearing a name and a write
     * token — and every guard that would have stopped it is a guard against
     * putting the deleted parent back, not against answering.
     */
    readonly fork: (child: string, parent: string) => Effect.Effect<StoreInstances, ArtifactsError>;
    /**
     * The repositories whose object reads fall through to this one.
     *
     * A fork holds no copy of what it inherited, so dropping the store it
     * reads through erases the fork's history with it — and the fork's refs,
     * registry row and remote URL all survive to advertise objects that are
     * no longer anywhere. Asking first is what makes that refusable.
     */
    readonly dependents: (name: string) => Effect.Effect<ReadonlyArray<string>>;
    /**
     * Fails, and has to: on the node store this unlinks a directory, which a
     * pack still open elsewhere can refuse. `delete` reads that failure to
     * decide whether the registry row may be freed, so it cannot be a defect.
     */
    readonly drop: (name: string) => Effect.Effect<void, ArtifactsError>;
    /**
     * Drop what is remembered about a name, so that it opens as a new one.
     *
     * `create` calls this because a fork link outlives the fork it describes:
     * a `drop` removes the storage first and records that second, and a crash
     * in between leaves a line saying this name reads through a parent. The
     * repository created here is a new one, and it inherits nothing.
     *
     * How much that costs is the provider's own. The node store drops the
     * link and the open handle, and the directory the `drop` already removed
     * stays removed; the in-memory store keeps no such separation — the
     * handle *is* the repository — so forgetting a name there ends it. What
     * both owe, and what `delete` reads this for, is that no name but the one
     * asked for is touched.
     *
     * Fails for the same reason `drop` does: the node store writes what it
     * forgets, and `create` has already taken the name by the time this runs.
     * A defect there would leave a registry row nobody can explain.
     */
    readonly forget: (name: string) => Effect.Effect<void, ArtifactsError>;
  }
>()("artifacts/RepoStores") {}

export const repoStoresMemory = Layer.sync(RepoStores)(() => {
  /**
   * A repository's own objects, kept apart from the fall-through built over
   * them.
   *
   * The node store gets this separation from the filesystem: a directory
   * stays where it is and a handle over it can be thrown away and made again.
   * Here the handle *is* the repository, so without somewhere to keep what a
   * name owns, rebuilding a store to pick up a changed fork link would throw
   * that name's objects away with it.
   */
  const own = new Map<string, StoreInstances>();
  /** The same repositories, with their fall-through — discardable. */
  const instances = new Map<string, StoreInstances>();
  const forks = new Map<string, string>();

  const build = Effect.gen(function* () {
    return { objects: yield* ObjectStore, refs: yield* RefStore };
  }).pipe(Effect.provide(memoryStores));

  const ownOf = (name: string) =>
    Effect.gen(function* () {
      const existing = own.get(name);
      if (existing !== undefined) return existing;
      const built = yield* build;
      own.set(name, built);
      return built;
    });

  /** This name's objects, with its parent's underneath, however deep. */
  const compose = (
    name: string,
    seen: ReadonlySet<string> = new Set(),
  ): Effect.Effect<StoreInstances> =>
    Effect.gen(function* () {
      const mine = yield* ownOf(name);
      const parent = forks.get(name);
      if (parent === undefined || seen.has(parent)) return mine;
      const base = yield* compose(parent, new Set(seen).add(name));
      return { objects: alternates(mine.objects, base.objects), refs: mine.refs };
    });

  const open = (name: string) =>
    Effect.gen(function* () {
      const existing = instances.get(name);
      if (existing !== undefined) return existing;
      const built = yield* compose(name);
      instances.set(name, built);
      return built;
    });

  /**
   * Forget the composed stores built over this one, however deep.
   *
   * `alternates` closes over the store it was handed, so a fork of a fork
   * goes on reading through the parent its parent used to have. The node
   * store does the same thing for the same reason; only the objects survive
   * here, in `own`.
   */
  const recompose = (name: string, seen = new Set<string>()): void => {
    if (seen.has(name)) return;
    seen.add(name);
    for (const [child, parent] of forks) {
      if (parent !== name) continue;
      instances.delete(child);
      recompose(child, seen);
    }
  };

  return RepoStores.of({
    open,
    fork: (child, parent) =>
      Effect.gen(function* () {
        // `ownOf` here would build the parent rather than find it, and hand
        // back a fork of a repository that does not exist — the same answer
        // the node store refuses. A repository's objects are the whole of it
        // in this provider, so having some is what being there means.
        if (!own.has(parent)) {
          return yield* failure("NOT_FOUND", `repo '${parent}' has no storage to fork from`);
        }
        forks.set(child, parent);
        // The child's own objects are kept — a re-fork moves what a
        // repository reads through, it does not empty it — and everything
        // built over the child is dropped, because it closed over the store
        // this replaces.
        instances.delete(child);
        recompose(child);
        return yield* open(child);
      }),
    dependents: (name) =>
      Effect.sync(() => [...forks].filter(([, parent]) => parent === name).map(([child]) => child)),
    drop: (name) =>
      Effect.gen(function* () {
        // Asked again here, as the node store asks again under its lock:
        // `delete` asks before it revokes a repository's tokens, and a `fork`
        // landing in between would read through a parent that goes anyway.
        // Nothing would say so afterwards either — `ownOf` builds what it
        // cannot find, so the fork would quietly serve an empty store where
        // its inherited history used to be.
        const borrowers = [...forks].filter(([, parent]) => parent === name).map(([it]) => it);
        if (borrowers.length > 0) {
          return yield* failure(
            "PRECONDITION_FAILED",
            `repo '${name}' is the source of ${borrowers.join(", ")}; delete those first`,
          );
        }
        own.delete(name);
        instances.delete(name);
        forks.delete(name);
        recompose(name);
      }),
    // The same lines as `drop`, and it cannot be fewer: the objects here are
    // the repository rather than a pointer at one, so leaving them in place
    // would hand `create` the repository it is replacing, alternates and all.
    // Nothing else can reach them — every path to a repository goes through
    // the registry, and the row this name had is gone.
    forget: (name) =>
      Effect.sync(() => {
        own.delete(name);
        instances.delete(name);
        forks.delete(name);
        recompose(name);
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

      /** Create a directory, but only where its own parent already exists. */
      const beside = async (target: string): Promise<void> => {
        try {
          await fs.mkdir(target);
        } catch (error) {
          if (error instanceof Error && "code" in error && error.code === "EEXIST") return;
          throw error;
        }
      };

      /**
       * Rewrite a parent's `borrowers` from the fork links that remain.
       *
       * `links` is passed rather than read, because this runs *before* the
       * map it describes is adopted — see `relink`.
       */
      const unlend = async (
        parent: string,
        links: ReadonlyMap<string, string>,
        dropped: string,
      ): Promise<void> => {
        const borrowers = path.join(root, parent, "objects", "info", "borrowers");
        // The one line that stopped being true, taken out of whatever is
        // already there — not a file rewritten from the fork links alone.
        // `borrowersOf` documents a second way to become a borrower, `git
        // clone --shared`, which this provider neither makes nor knows about;
        // rebuilding from the links wipes those lines the first time the last
        // fork of this parent goes, and the `gc` that protected them stops.
        //
        // Read where "not there" is the only answer taken for an empty file.
        // Anything else — a descriptor limit, an unreadable mount — would
        // otherwise rewrite the file from the links and take those lines with
        // it, on the strength of a read that never happened. It throws to the
        // `bookkeeping` above, which reports it and leaves the retry able to
        // finish.
        let written: string;
        try {
          written = await fs.readFile(borrowers, "utf8");
        } catch (error) {
          if (!(error instanceof Error && "code" in error && error.code === "ENOENT")) throw error;
          written = "";
        }
        const existing = written
          .split("\n")
          .map((line) => line.trim())
          .filter((line) => line.length > 0);
        const remaining = [...new Set(existing)].filter((child) => child !== dropped);
        for (const [child, from] of links) {
          if (from === parent && !remaining.includes(child)) remaining.push(child);
        }
        if (remaining.length === 0) {
          await fs.rm(borrowers, { force: true });
          return;
        }
        try {
          await fs.writeFile(borrowers, `${remaining.join("\n")}\n`);
        } catch (error) {
          // No directory to write into means no parent to tell — it was
          // deleted while this ran. Creating one would put a deleted
          // repository back on disk, and checking first only narrows the
          // window rather than closing it: `fork` is what makes this
          // directory, so where there is something to say it already exists.
          if (!(error instanceof Error && "code" in error && error.code === "ENOENT")) throw error;
        }
      };

      /**
       * The repositories a name reads through, off its own `alternates` —
       * and `undefined` where the repository is not there to be asked.
       *
       * The difference is what decides whether a delete has to go looking:
       * a repository that is there and names nobody is lent nothing, while
       * one whose directory has already gone could have been lent anything.
       *
       * Only the lenders in this namespace: a line naming an object directory
       * somewhere else belongs to whoever wrote it, and there is no
       * `borrowers` file of ours to take this name out of.
       */
      const lendersOf = async (name: string): Promise<ReadonlyArray<string> | undefined> => {
        const objects = path.join(root, name, "objects");
        if (missing(path.join(root, name))) return undefined;
        // Only "there is no such file" is an answer here. A read that failed
        // for any other reason has not established that this name borrows
        // from nobody, and answering `[]` would leave the delete neither
        // unlending anybody nor going to look — which is a `borrowers` line
        // for a name that no longer exists, and a lender no `gc` will ever
        // collect again.
        let text: string;
        try {
          text = await fs.readFile(path.join(objects, "info", "alternates"), "utf8");
        } catch (error) {
          if (!(error instanceof Error && "code" in error && error.code === "ENOENT")) {
            return undefined;
          }
          text = "";
        }

        const found: string[] = [];
        for (const line of text.split("\n").map((entry) => entry.trim())) {
          if (line.length === 0 || line.startsWith("#")) continue;
          // Resolved as the filesystem resolves it, because `borrowersOf`
          // does: a line spelled through a symlink or a `..` is a borrow to
          // `gc` and to `dependents`, and matching the strings instead would
          // make it one this delete cannot see.
          const target = canonical(path.resolve(objects, line));
          const lender = path.basename(path.dirname(target));
          // A name that does not check out is a lender this cannot name —
          // a repository whose directory is itself a link out of the tree,
          // or an object store belonging to something else entirely. The
          // first has a `borrowers` file to be taken out of and no name to
          // find it by, so the answer is "could not tell" and the delete goes
          // looking rather than leaving a line nothing will ever release.
          if (canonical(path.join(root, lender, "objects")) !== target) return undefined;
          found.push(lender);
        }
        return found;
      };

      /**
       * Take a name out of every list that lends to it.
       *
       * One `readdir` and one small read per repository, on a delete — which
       * already unlinks a directory tree — and only the files that name this
       * one are rewritten.
       *
       * This is the last thing that will ever look for these lines, so a read
       * that does not answer must not read as "this one does not name it".
       * The line would stay for a repository that is gone, and `collected`
       * releases a line only where a repository stands at the name — so the
       * lender would never be collectable or deletable again. Everything but
       * "there is no such file" is reported and left to the retry.
       */
      const unlendAll = async (
        child: string,
        links: ReadonlyMap<string, string>,
      ): Promise<void> => {
        const names = await fs.readdir(root);
        for (const lender of names) {
          const file = path.join(root, lender, "objects", "info", "borrowers");
          let written: string;
          try {
            written = await fs.readFile(file, "utf8");
          } catch (error) {
            const code = error instanceof Error && "code" in error ? error.code : undefined;
            // A name with no `borrowers` file lends to nobody, and a name that
            // is not a directory at all — the registry's own JSON lives here —
            // is not a lender either.
            if (code === "ENOENT" || code === "ENOTDIR") continue;
            throw error;
          }
          if (!written.split("\n").some((line) => line.trim() === child)) continue;
          await unlend(lender, links, child);
        }
      };

      /**
       * A record of a change, written where the caller can see it fail.
       *
       * A full disk or a read-only mount is exactly when these stop working,
       * and by then the thing they describe has already happened. As a defect
       * that would take the fiber down with nothing said about why; as a
       * failure it is the caller's to report, and — given the order `relink`
       * writes in — the same call put again does the same work.
       */
      const bookkeeping = (name: string, write: () => Promise<void>) =>
        Effect.tryPromise({
          try: write,
          catch: (error) =>
            failure(
              "INTERNAL_ERROR",
              `could not record the fork links of '${name}': ${String(error)}`,
            ),
        });

      /** The same, for the repository's own directory rather than the record. */
      const onDisk = <A>(name: string, write: () => Promise<A>) =>
        Effect.tryPromise({
          try: write,
          catch: (error) =>
            failure("INTERNAL_ERROR", `could not write '${name}': ${String(error)}`),
        });

      /**
       * Point a name's fork link at `parent`, or at nothing, and tell the
       * parent it stops borrowing from.
       *
       * Every change to `forks` goes through here, in one order, because the
       * three callers had three orders between them and each lost something
       * different. The order is: the parent being let go, then the file, then
       * the map. The map is what a retry reads to decide whether there is
       * anything left to do, so it moves last — a failure at either write
       * leaves the map still describing the world as it was, and the same
       * call put again does the same work. Written the other way round, a
       * failed `unlend` became unreachable the moment the link left the map,
       * which is a `borrowers` line no later call ever goes back for, and a
       * parent its own `gc` refuses from then on.
       *
       * `unlend` first, and against the map this is about to become, means a
       * parent can be let go a moment before the link is. That is the safe
       * direction: `gc` reads the child's own `alternates` as well as the
       * parent's `borrowers`, so a fork that really does still read through
       * this parent is still found by the scan.
       *
       * One at a time, because the map is read at the start and written at the
       * end and the file is rewritten whole in between. Two forks running
       * together would each serialize a snapshot taken before the other's
       * link existed, and the second write would drop the first — a fork that
       * survives in memory until the process ends and is gone after it, with
       * `dependents` no longer naming it and its parent's objects collectable
       * out from under it. The lock is the file's, and the work under it is
       * two small writes.
       */
      let pending: Promise<unknown> = Promise.resolve();
      const alone = <A>(work: () => Promise<A>): Promise<A> => {
        // Both arms, so one caller's failure does not strand the queue behind
        // a rejected promise.
        const done = pending.then(work, work);
        pending = done.catch(() => undefined);
        return done;
      };

      /**
       * The links and the disk, which are written one after the other.
       *
       * The disk half is a `readdir` of the namespace and a small read per
       * repository, synchronously: 14 ms across 500 repositories and 46 ms
       * across 2000, measured, and `delete` pays it twice — once for the
       * answer it reports, and once inside `drop` under the lock, where it is
       * the answer that decides. Nothing else on the request path asks.
       *
       * Neither of the two can go. The second is what closes the window a
       * `fork` would otherwise land in. Dropping the first would mean
       * revoking a repository's tokens before finding out it may not be
       * deleted — and revoking them afterwards instead is worse, because
       * between the storage going and the row going the old write token still
       * authorises a push, which would put objects back under a name about to
       * be freed.
       */
      const dependentsOf = (name: string): ReadonlyArray<string> => [
        ...new Set([
          ...[...forks].filter(([, parent]) => parent === name).map(([child]) => child),
          ...borrowersOf(path.join(root, name)),
        ]),
      ];

      /**
       * The body of `relink`, for callers that already hold the lock.
       *
       * `removing` is what a name being deleted was reading through, off its
       * own `alternates` and read before the storage went — or `undefined`
       * where the storage was already gone and could not say. Two sources
       * again, and for the same reason: the links do not know about a line
       * written by a `git clone --shared`, and that line left behind is one
       * nothing else ever goes back for.
       *
       * Asking every repository in turn is what is left when neither source
       * can answer at all, which is the vanished-storage case alone. A
       * repository that was there and named nobody is lent nothing, and a
       * name being *created* is not being deleted — neither pays the read per
       * repository that the sweep costs.
       */
      const relinkHeld = async (
        name: string,
        parent: string | undefined,
        removing?: {
          readonly lenders: ReadonlyArray<string> | undefined;
          readonly existed: boolean;
        },
      ): Promise<void> => {
        const previous = forks.get(name);
        const next = new Map(forks);
        if (parent === undefined) next.delete(name);
        else next.set(name, parent);
        const lenders = new Set(removing?.lenders ?? []);
        if (previous !== undefined && previous !== parent) lenders.add(previous);
        for (const lender of lenders) await unlend(lender, next, name);
        // And the sweep when the storage could not be asked — even where a
        // link named somebody, because the link is one lender and a name
        // recorded by a `git clone --shared` was never in the links at all.
        //
        // Only for a name that was a repository here, though: a directory
        // when this looked, or a fork link. A name with neither is one this
        // provider never finished making — a `create` or a `fork` that failed
        // before writing is undone through here — and every `borrowers` line
        // naming it belongs to somebody else's repository under that name.
        // Sweeping then is not tidying up after a delete, it is deleting a
        // stranger's record of a borrower that is still reading, and the
        // objects go on the lender's next `gc`. The other way costs a lender
        // that cannot be collected until somebody removes the line by hand.
        const ours = removing !== undefined && (removing.existed || previous !== undefined);
        if (ours && removing.lenders === undefined) {
          await unlendAll(name, next);
        }
        if (previous === parent) return;
        await saveJson(forksFile, Object.fromEntries(next));
        if (parent === undefined) forks.delete(name);
        else forks.set(name, parent);
      };

      const relink = (name: string, parent: string | undefined) =>
        bookkeeping(name, () => alone(() => relinkHeld(name, parent)));

      const buildAt = (name: string) =>
        Effect.gen(function* () {
          return { objects: yield* ObjectStore, refs: yield* RefStore };
        }).pipe(Effect.provide(nodeStores(path.join(root, name))));

      /**
       * Forget the cached stores built on top of this one, however deep.
       *
       * A fork holds its parent's object store by value — `alternates` closes
       * over the instance it was handed — so a fork re-pointed at a different
       * parent is not a fork anything already built over it ever sees.
       */
      const rebuilt = (name: string, seen = new Set<string>()): void => {
        // A cycle needs a corrupted `.forks.json` to exist at all, but this
        // walks the file's word for it, and looping here would hang a request.
        if (seen.has(name)) return;
        seen.add(name);
        for (const [child, parent] of forks) {
          if (parent !== name) continue;
          instances.delete(child);
          rebuilt(child, seen);
        }
      };

      /**
       * A store, and whether it is the whole of what its links describe.
       *
       * Only a whole one is cached. A store built without the parent its fork
       * link records answers "no such object" for history the repository does
       * have, and the parent may be a restore away — so it is handed out and
       * forgotten, and the next request looks again. That costs a rebuild of
       * the stack per request while the parent is missing, which is a state
       * nothing should be in for long.
       *
       * The alternative — cache it, mark the name, and invalidate on the way
       * back — is what this used to do. Getting it right took a walk up the
       * fork links on every open and a walk back down on every restore, and
       * two bugs on the way: a fork of a fork kept serving the pre-restore
       * store because only the marked name was checked, and again because
       * only the marked name was invalidated. Both were silently wrong
       * answers about history. Not caching cannot be wrong, only slow, and
       * only in the state where something is already broken.
       */
      interface Opened {
        readonly stores: StoreInstances;
        readonly whole: boolean;
      }

      /**
       * `seen` is the chain of names this open is already inside.
       *
       * A `.forks.json` naming a cycle is corrupt, but it is a file, and the
       * recursion below is what reads it: without this an `open` of either
       * name recurses until the stack ends, since nothing is memoized until
       * the recursive call returns. A link back into the chain is not
       * followed, and the store built without it is not cached — which name
       * in the cycle loses its fall-through depends on which was asked for
       * first, and that is not an answer to write down for the life of the
       * process.
       */
      const opened = (name: string, seen: ReadonlySet<string>): Effect.Effect<Opened> =>
        Effect.gen(function* () {
          const cached = instances.get(name);
          if (cached !== undefined) return { stores: cached, whole: true };

          const own = yield* buildAt(name);
          // A fork link whose parent is gone is a link left behind by a drop
          // that removed the storage and stopped before its bookkeeping. It
          // must not be followed: the repository standing at this name now is
          // a new one, and reading through somebody else's objects because of
          // a stale line in a JSON file is not a thing to do quietly.
          const recorded = forks.get(name);
          const cyclic = recorded !== undefined && seen.has(recorded);
          const absent = recorded !== undefined && !cyclic && missing(path.join(root, recorded));
          const parent = recorded !== undefined && !cyclic && !absent ? recorded : undefined;
          if (parent === undefined) {
            // Whole means "everything its links describe is in it". A link
            // skipped for being absent or for closing a cycle is a link not
            // followed either way, so neither is written down.
            const whole = !absent && !cyclic;
            if (whole) instances.set(name, own);
            return { stores: own, whole };
          }

          const base = yield* opened(parent, new Set(seen).add(name));
          const built: StoreInstances = {
            objects: alternates(own.objects, base.stores.objects),
            refs: own.refs,
          };
          // Whole only if everything underneath was: a fork of a fork whose
          // grandparent is missing is as incomplete as the fork itself, and
          // caching it is what would go on answering from the pre-restore
          // store once the grandparent came back.
          if (base.whole) instances.set(name, built);
          return { stores: built, whole: base.whole };
        });

      const open = (name: string): Effect.Effect<StoreInstances> =>
        Effect.map(opened(name, new Set()), (it) => it.stores);

      return RepoStores.of({
        open,
        fork: (child, parent) =>
          Effect.gen(function* () {
            // Asked before anything is written, because none of what follows
            // can tell: the alternates file is written from the name rather
            // than from the directory, and the parent's `borrowers` is the
            // one thing here deliberately allowed to go nowhere — writing it
            // would put a deleted repository back on disk. So a fork of a
            // parent that is gone would otherwise succeed, and hand back an
            // empty repository with a write token against it.
            //
            // Under the lock, together with the writes that follow from it: a
            // `drop` of the parent asks the same question from inside the same
            // lock, so the two orders are the only two there are — the parent
            // is gone before this looks, or the link exists before the drop
            // does. In between is where a link outlives the parent it names
            // and is inherited by whatever takes that name next.
            //
            // Everything a fork writes is in here, in one order, and nothing
            // is left outside: a `delete(child)` running alongside would
            // otherwise take the directory away between two of these and see
            // it made again by the `recursive` mkdir, with a `borrowers` line
            // appended to a parent that no `collected` will ever release.
            //
            // The disk before the link, because the disk is what `gc` reads.
            // `borrowersOf` knows a fork by the child's own `alternates` and
            // by the parent's `borrowers`, and knows nothing of
            // `.forks.json` — so a fork interrupted after the link and before
            // those two is a fork this process composes and `gc` cannot see,
            // and the parent's objects go while the child still reads them.
            // The other way round is a fork the disk knows about and the
            // links do not: `dependents` reads the disk too, so it refuses
            // the parent's delete, and `fork` run again finishes the job.
            //
            // A name can be forked twice — dropped, created again, forked
            // somewhere else — and the parent it used to read through is
            // still telling its own `gc` that this name borrows from it.
            // Nothing else ever revisits that file, so the old parent would
            // be uncollectable for as long as it exists; `relinkHeld` is what
            // goes back for it.
            const forkable = yield* onDisk(child, () =>
              alone(async () => {
                if (missing(path.join(root, parent))) return false;

                // git's own way of saying "my objects are over there too". The
                // in-memory fall-through only exists inside this process, and
                // the fork is served over HTTP by a host that opens the
                // directory directly — and read by `git` itself, which has
                // read this file since 2005.
                const info = path.join(root, child, "objects", "info");
                await fs.mkdir(info, { recursive: true });
                await fs.writeFile(
                  path.join(info, "alternates"),
                  `${path.resolve(root, parent, "objects")}\n`,
                );

                // And the other direction, which git has no file for: the
                // parent needs to know it is lent out, because its own `gc`
                // cannot see the refs that keep these objects alive.
                //
                // Appended where the parent's own directory already is, never
                // created. A `delete(parent)` that got in first would
                // otherwise be undone by this — the parent's directory back
                // on disk, holding one file, which is enough to block the
                // `gc` of whatever is created under that name next.
                const lent = path.join(root, parent, "objects", "info");
                try {
                  // One level at a time, never `recursive`: a recursive mkdir
                  // would rebuild the parent's own directory on the way down,
                  // which is the resurrection this is guarding against. Beside
                  // a directory that is already there, each of these either
                  // exists or is created; with the parent gone, the first
                  // fails and nothing is written.
                  await beside(path.join(root, parent, "objects"));
                  await beside(lent);
                  await fs.appendFile(path.join(lent, "borrowers"), `${child}\n`);
                } catch (error) {
                  if (!(error instanceof Error && "code" in error && error.code === "ENOENT")) {
                    throw error;
                  }
                }

                await relinkHeld(child, parent);
                return true;
              }),
            );
            if (!forkable) {
              return yield* failure("NOT_FOUND", `repo '${parent}' has no storage to fork from`);
            }

            // And what was built on top of it, which closed over the store
            // this fork just replaced: a fork of this fork would go on
            // reading through the parent it used to have, and would keep
            // answering from it for as long as the process lives — while its
            // own `alternates` on disk says otherwise.
            instances.delete(child);
            rebuilt(child);
            return yield* open(child);
          }),
        forget: (name) =>
          Effect.gen(function* () {
            instances.delete(name);
            rebuilt(name);
            // Both halves of the record, not just this one: the parent's
            // `borrowers` file is what stops its `gc` collecting what this
            // name used to read, and a line there for a fork that is gone
            // stops it forever. `relink` is what makes both happen.
            yield* relink(name, undefined);
            // And git's own half of it, which no map holds: `alternates` is
            // read off the disk by every reader of this repository, `git`
            // included, so a directory that outlived its registry row would
            // hand the repository created here next the old parent's objects
            // — the one thing this call exists to prevent.
            yield* bookkeeping(name, () =>
              fs.rm(path.join(root, name, "objects", "info", "alternates"), { force: true }),
            );
          }),
        // The links, and not what the disk makes of them. A fork whose
        // directory is gone looks like nothing to protect, but the link
        // outliving the directory is exactly what a `drop` that failed its
        // bookkeeping leaves — and reading it as "no dependents" lets the
        // parent be deleted, its name taken by somebody else's repository,
        // and the link then adopted by that one: `open` asks whether the
        // recorded parent is there, not whether it is the same one. Refusing
        // to delete a parent whose fork is already gone costs a retry of the
        // fork's own delete, which is what clears the link.
        //
        // And the disk as well as the links, because they are written one
        // after the other and a `fork` can stop in between. `borrowersOf` is
        // what `gc` asks, and it reads a child's own `alternates` — so a fork
        // interrupted before its link was persisted is invisible here and
        // plain to `gc`, and `delete` would take the parent out from under
        // it. Asking both means neither order of writes has a window.
        //
        // `drop` asks this again while it holds the lock: this answer is a
        // snapshot, and the caller acts on it later.
        dependents: (name) => Effect.sync(() => dependentsOf(name)),
        drop: (name) =>
          Effect.gen(function* () {
            instances.delete(name);

            // Storage first, bookkeeping second, because the removal can
            // fail — a pack still open, Windows refusing the unlink — and the
            // two orders fail differently. Recorded-but-gone makes
            // `dependents` name a child that no longer exists, and the worst
            // that costs is a refusal to delete its parent. Gone-but-still-
            // there is the other way round: the fork link erased while the
            // fork's directory survives, `dependents` answers empty, and the
            // next `delete` of the parent collects the objects the fork is
            // still reading through.
            //
            // Both under the lock, and the question asked again inside it:
            // `delete` asks `dependents` before it calls this, but a `fork`
            // can land between the two, and a link written after the check
            // survives the parent it names — to be inherited by whatever
            // takes that name next. The lock is the same one `relink` holds,
            // so the fork is either wholly before this or wholly after.
            //
            // `tryPromise`, not `promise`: this is the failure `delete` reads
            // before it frees the registry row. As a defect it would take the
            // fiber with it instead.
            const held = yield* Effect.tryPromise({
              try: () =>
                alone(async () => {
                  const borrowers = dependentsOf(name);
                  if (borrowers.length > 0) return borrowers;
                  // Read while the repository is still there to read: this is
                  // what says whose `borrowers` file names it, and it is the
                  // first thing the removal below takes away.
                  const repository = path.join(root, name);
                  const existed = !missing(repository);
                  const lenders = await lendersOf(name);
                  await retirePacksAndRemove(repository, () =>
                    fs.rm(repository, { recursive: true, force: true }),
                  );
                  // The lenders can collect again once nothing borrows from
                  // them. Under the same lock, so a failure leaves the links
                  // and the files agreeing and the same call finishes the job.
                  await relinkHeld(name, undefined, { existed, lenders });
                  return [];
                }),
              catch: (error) =>
                failure("INTERNAL_ERROR", `could not remove repo '${name}': ${String(error)}`),
            });
            if (held.length > 0) {
              // The same code and the same words as `delete`'s own check, so
              // that a `fork` landing between the two does not change the
              // answer a client reads — only which of them found it.
              return yield* failure(
                "PRECONDITION_FAILED",
                `repo '${name}' is the source of ${held.join(", ")}; delete those first`,
              );
            }
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
            // Everything after the row is undone with it, the same way
            // `fork`'s tail is: a name taken by a repository that was never
            // made is a name nobody can retry with, and `setHead` writing to
            // a full or read-only disk is a defect rather than an answer.
            return yield* Effect.gen(function* () {
              // Before the stores are opened, because opening is what would
              // follow a fork link left behind by a drop that did not finish.
              yield* repoStores.forget(name);
              const stores = yield* repoStores.open(name);
              yield* stores.refs.setHead(`refs/heads/${record.defaultBranch}`).pipe(Effect.orDie);
              const token = yield* tokens.issue(name, "write", 86_400);
              return { record, stores, result: created(record, token) };
            }).pipe(
              Effect.onError(() =>
                Effect.gen(function* () {
                  // Tokens first, while the name they are scoped to still
                  // resolves, and as their own step so that a revocation that
                  // fails does not take the storage cleanup with it. Then the
                  // storage, then the row — `delete`'s order, and `fork`'s.
                  yield* Effect.gen(function* () {
                    for (const issued of (yield* tokens.list(name)).tokens) {
                      yield* tokens.revoke(name, issued.id);
                    }
                  }).pipe(
                    Effect.ignore,
                    Effect.catchDefect(() => Effect.void),
                  );
                  yield* repoStores.drop(name);
                  yield* registry.delete(name);
                }).pipe(
                  Effect.ignore,
                  Effect.catchDefect(() => Effect.void),
                ),
              ),
            );
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
              // The row is taken before any of this, and every step after it
              // writes something: the child's directory, its alternates, the
              // fork link, the parent's `borrowers`, the refs, a token. A fork
              // that stops half way through leaves a name nobody can retry
              // with and, worse, a link that refuses its parent's `delete`
              // forever. So the whole tail is undone together, the same way an
              // `import` that fails part-way is — including the `fork` itself,
              // which writes two files before it can fail. Undoing a fork that
              // refused before writing anything is safe because `drop` goes
              // looking for stray `borrowers` lines only for a name it had a
              // link for, which that one never had.
              return yield* Effect.gen(function* () {
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
              }).pipe(
                Effect.onError(() =>
                  Effect.gen(function* () {
                    // Tokens first, while the name they are scoped to still
                    // resolves; then the storage, which takes the fork link
                    // with it; then the row. `import`'s rollback and `delete`
                    // itself use the same order, and for the same reasons.
                    //
                    // A `drop` that fails stops the rest on purpose, rather
                    // than being stepped over: freeing the row while the
                    // half-made fork is still on disk hands the next `create`
                    // of this name a directory with an `alternates` file in
                    // it. The name stays taken until an explicit `delete`
                    // retries the removal, which is a name to reclaim rather
                    // than a repository to explain.
                    //
                    // The tokens are their own step, though: a revocation
                    // that fails must not take the storage cleanup with it,
                    // or the fork stays on disk with an `alternates` file
                    // that blocks its parent's `delete` and its `gc` — worse
                    // than the token it was trying to withdraw.
                    yield* Effect.gen(function* () {
                      for (const issued of (yield* tokens.list(name)).tokens) {
                        yield* tokens.revoke(name, issued.id);
                      }
                    }).pipe(
                      Effect.ignore,
                      Effect.catchDefect(() => Effect.void),
                    );
                    yield* repoStores.drop(name);
                    yield* registry.delete(name);
                  }).pipe(
                    Effect.ignore,
                    Effect.catchDefect(() => Effect.void),
                  ),
                ),
              );
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
              Effect.map(({ cursor, repos, total }) => {
                const listed = repos.map((record) => {
                  const { remote: _remote, ...rest } = info(record);
                  return rest;
                });
                return cursor === undefined
                  ? { repos: listed, total }
                  : { repos: listed, total, cursor };
              }),
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

              // Tokens first. They are keyed by repository *name* and the
              // registries do not cascade, so one left live is a write into
              // whatever stands at this name next — and a push landing in the
              // window between the storage going and the row going would
              // rebuild the repository under a name about to be freed, for
              // the next caller to create over and clone.
              //
              // The cost of this order is the failure below: `drop` can refuse
              // — a locked pack, a racing write from a host serving the same
              // directory, or a `fork` landing between the check above and the
              // one `drop` makes under its own lock — and the repository is
              // then still listed with its tokens revoked. Nothing gives them
              // back; what fixes it is `createToken`, which the repository is
              // still there to answer. A retry of the delete only helps where
              // the cause was transient, which the fork race is not: that one
              // stands until the fork is deleted.
              //
              // Worth it against the other order. Revoking after the storage
              // goes leaves a window where a live write token still resolves
              // to a name whose row has not gone yet, and a push landing in it
              // rebuilds the repository under a name about to be freed — for
              // the next caller to create over, and clone somebody else's
              // history out of.
              for (const token of (yield* tokens.list(name)).tokens) {
                yield* tokens.revoke(name, token.id);
              }
              // Storage before the row, so the name is never free while the
              // objects are still there.
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
                    // Its own step: a revocation that fails must not take the
                    // storage cleanup with it and leave the half-cloned
                    // objects on disk under a name still held.
                    yield* Effect.gen(function* () {
                      for (const token of (yield* tokens.list(record.name)).tokens) {
                        yield* tokens.revoke(record.name, token.id);
                      }
                    }).pipe(
                      Effect.ignore,
                      Effect.catchDefect(() => Effect.void),
                    );
                    // Storage before the row, the same order `delete` uses: a
                    // `drop` that fails must not leave the name free while the
                    // half-cloned objects are still on disk under it.
                    yield* repoStores.drop(record.name);
                    yield* registry.delete(record.name);
                  }).pipe(
                    Effect.ignore,
                    Effect.catchDefect(() => Effect.void),
                  ),
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
