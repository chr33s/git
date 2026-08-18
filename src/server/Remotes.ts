/**
 * Where this repository fetches from and pushes to.
 *
 * The transport is `client/Fetch.ts` and `client/Push.ts`; this is the
 * registry behind them, and it is the half that has to be durable — a remote
 * held in a field lasts until the Durable Object is evicted, which is not a
 * remote.
 *
 * Every read here can fail, which is where this port parts company with
 * `Subscribers.forEvent`. A subscriber list that cannot be read costs a
 * delivery; a remote that cannot be read has nowhere to fetch from, and
 * answering "no such remote" because the disk is unreadable would send the
 * caller looking for a typo that is not there.
 *
 * A credential is stored beside the URL rather than passed per request: a
 * token in a request body is a token in an access log. It goes in once and
 * has no read path back out — `redact` is what the API is allowed to show.
 */
import { Context, Effect, Layer, Option, Schema } from "effect";

import { Invalid, StorageFailure } from "../git/Error.ts";
import type { Sql } from "../git/Sql.ts";

/**
 * What this repository does about a remote on its own.
 *
 * `manual` is the default and the old behaviour: a remote is somewhere a
 * person or a job fetches from when they say so. `push` and `mirror` are the
 * standing instruction this implementation acts on: what lands here is
 * forwarded, on the `post-receive` that made it durable.
 *
 * `fetch` is the half with no trigger. Pulling on a schedule needs a
 * scheduler, and nothing here has one — so storing it would leave a remote
 * configured to do something that never happens, which is worse than saying
 * no. It stays in the vocabulary because the shape is the spec's and the
 * trigger is the only thing missing; `mirror` is accepted for its push half,
 * and its fetch half waits on the same scheduler.
 *
 * `refs` is what the standing instruction covers, as ref patterns. Empty means
 * everything the mode would otherwise carry, which is the reading that makes
 * `{mode: "push"}` mean what it looks like it means.
 */
export interface Sync {
  readonly mode: "manual" | "fetch" | "push" | "mirror";
  readonly refs: ReadonlyArray<string>;
}

export interface Remote {
  readonly name: string;
  readonly url: string;
  /** Sent to the remote as a Bearer token, and never returned. */
  readonly credential: string | null;
  /** `null` is `manual`: nothing happens to this remote unless somebody asks. */
  readonly sync: Sync | null;
  readonly createdAt: Date;
}

export interface NewRemote {
  readonly name: string;
  readonly url: string;
  readonly credential?: string | undefined;
  readonly sync?: Sync | undefined;
}

export const MODES = ["manual", "fetch", "push", "mirror"] as const;

/** Whether this remote wants what lands here sent on. */
export const sends = (remote: Remote): boolean =>
  remote.sync !== null && (remote.sync.mode === "push" || remote.sync.mode === "mirror");

export class Remotes extends Context.Service<
  Remotes,
  {
    readonly list: Effect.Effect<ReadonlyArray<Remote>, StorageFailure>;
    /** `null` when nothing is registered under that name. */
    readonly get: (name: string) => Effect.Effect<Remote | null, StorageFailure>;
    readonly add: (input: NewRemote) => Effect.Effect<Remote, StorageFailure | Invalid>;
    /** `false` when there was nothing by that name. */
    readonly remove: (name: string) => Effect.Effect<boolean, StorageFailure>;
  }
>()("server/Remotes") {}

const LOOPBACK = new Set(["localhost", "127.0.0.1", "[::1]"]);

/**
 * A remote name becomes half of a ref — `refs/remotes/<name>/main` — and its
 * URL becomes a request this server makes. Both are refused at registration
 * rather than discovered at fetch time, when a bad name has already become a
 * ref nobody meant to create and a bad URL is a network error whose cause is
 * a typo.
 */
export const validate = (input: NewRemote): Effect.Effect<NewRemote, Invalid> =>
  Effect.suspend(() => {
    const bad = (reason: string) => Effect.fail(new Invalid({ field: "name", reason }));

    // What git refuses in a ref component, plus `/`: the tracking namespace
    // is one segment deep, and a name that could nest in it could also climb
    // out of it.
    if (input.name === "") return bad("remote name must not be empty");
    if (/[\s~^:?*[\\/]/.test(input.name)) {
      return bad(`bad remote name '${input.name}': no spaces, '/' or git's ref metacharacters`);
    }
    if (input.name.includes("..") || input.name.startsWith(".") || input.name.endsWith(".")) {
      return bad(`bad remote name '${input.name}': no '..', and no leading or trailing '.'`);
    }
    if (input.name.endsWith(".lock"))
      return bad(`bad remote name '${input.name}': reserved suffix`);

    // A mode nobody implements is a remote that quietly does nothing, and a
    // pattern that is not a ref is one that quietly matches nothing. Both are
    // refused where they are written rather than discovered as silence.
    if (input.sync !== undefined) {
      // SAFETY: widened to compare a caller-supplied string against the
      // literal union; the comparison is the check, and nothing is narrowed by
      // it.
      // Refused rather than stored: nothing drives a scheduled fetch here, so
      // a remote configured for one would sit doing nothing while its
      // configuration said otherwise.
      if (input.sync.mode === "fetch") {
        return Effect.fail(
          new Invalid({
            field: "sync",
            reason: "scheduled fetch is not implemented; use 'manual' and pull, or 'push'",
          }),
        );
      }
      const known: ReadonlyArray<string> = MODES;
      if (!known.includes(input.sync.mode)) {
        return Effect.fail(
          new Invalid({ field: "sync", reason: `unknown sync mode '${input.sync.mode}'` }),
        );
      }
      for (const pattern of input.sync.refs) {
        if (!pattern.startsWith("refs/")) {
          return Effect.fail(
            new Invalid({ field: "sync", reason: `'${pattern}' is not a ref pattern` }),
          );
        }
      }
    }

    let parsed: URL;
    try {
      parsed = new URL(input.url);
    } catch {
      return Effect.fail(new Invalid({ field: "url", reason: `not a URL: '${input.url}'` }));
    }
    // Loopback http is how a test — and a developer — reaches the server next
    // to them; everything else on the network is https or nothing.
    if (
      parsed.protocol !== "https:" &&
      !(parsed.protocol === "http:" && LOOPBACK.has(parsed.hostname))
    ) {
      return Effect.fail(
        new Invalid({ field: "url", reason: "remote URLs must be https (loopback http excepted)" }),
      );
    }
    // A URL is a read path: userinfo in it would come straight back out of
    // `list`, which is the one thing storing a credential separately is for.
    // `fetch` rejects it anyway.
    if (parsed.username !== "" || parsed.password !== "") {
      return Effect.fail(
        new Invalid({ field: "url", reason: "credentials belong in 'credential', not in the URL" }),
      );
    }
    if (input.credential === "") {
      return Effect.fail(
        new Invalid({ field: "credential", reason: "an empty credential is not a credential" }),
      );
    }
    return Effect.succeed(input);
  });

/** What a list endpoint may show: everything but the credential. */
export const redact = (remote: Remote) => ({
  name: remote.name,
  url: remote.url,
  // That there is one is worth knowing and cannot be guessed from the URL;
  // the value itself has no way out of this process.
  has_credential: remote.credential !== null,
  sync: remote.sync,
  created_at: remote.createdAt.toISOString(),
});

const readOnly = (what: string) =>
  Effect.fail(
    new StorageFailure({
      operation: "remotes.add",
      path: "remotes",
      cause: `${what} is read-only`,
    }),
  );

/** No remotes, and none can be added: the default for a server without policy. */
export const none = Layer.succeed(Remotes, {
  list: Effect.succeed([]),
  get: () => Effect.succeed(null),
  add: () => readOnly("this registry"),
  remove: () => Effect.succeed(false),
});

/**
 * A fixed list — what a test or a configured host composes when the remotes
 * are known up front. `createdAt` is filled in, because a caller stating a
 * list inline is describing remotes, not writing rows.
 */
export const of = (
  remotes: ReadonlyArray<
    Omit<Remote, "createdAt" | "credential" | "sync"> & {
      readonly credential?: string | null;
      readonly sync?: Sync | null;
      readonly createdAt?: Date;
    }
  >,
) => {
  const rows = remotes.map((remote) => ({
    createdAt: new Date(0),
    credential: null,
    sync: null,
    ...remote,
  }));
  return Layer.succeed(Remotes, {
    list: Effect.succeed(rows),
    get: (name) => Effect.succeed(rows.find((row) => row.name === name) ?? null),
    add: () => readOnly("a fixed registry"),
    remove: () => Effect.succeed(false),
  });
};

const remoteOf = (input: NewRemote): Remote => ({
  name: input.name,
  url: input.url,
  credential: input.credential ?? null,
  sync: input.sync ?? null,
  createdAt: new Date(),
});

/**
 * A name is the key, so registering one twice is refused rather than merged:
 * silently re-pointing a remote would move where a later push goes without
 * anybody having asked for it.
 */
export const duplicate = (name: string) =>
  new Invalid({ field: "name", reason: `remote '${name}' already exists` });

/** In-process and mutable: registration works, durability does not. */
export const memory = Layer.sync(Remotes, () => {
  const rows = new Map<string, Remote>();
  return Remotes.of({
    list: Effect.sync(() => [...rows.values()]),
    get: (name) => Effect.sync(() => rows.get(name) ?? null),
    add: (input) =>
      validate(input).pipe(
        Effect.flatMap(() => {
          if (rows.has(input.name)) return Effect.fail(duplicate(input.name));
          const remote = remoteOf(input);
          rows.set(remote.name, remote);
          return Effect.succeed(remote);
        }),
      ),
    remove: (name) => Effect.sync(() => rows.delete(name)),
  });
});

interface Row extends Record<string, string | number | null> {
  readonly name: string;
  readonly url: string;
  readonly credential: string | null;
  /** JSON, because a mode and a pattern list are one decision and travel as one. */
  readonly sync: string | null;
  readonly created_at: string;
}

/**
 * A stored `sync`, or `null` for anything this version cannot read as one.
 *
 * Decoded with a schema rather than by hand: a column written by a newer
 * version, or edited by somebody, is input from outside this process however
 * it got into the database — and unreadable has to mean `manual`, which is
 * the behaviour a repository that never configured one already has.
 */
const StoredSync = Schema.Struct({
  mode: Schema.Literals(MODES),
  refs: Schema.Array(Schema.String),
});

const decodeSync = Schema.decodeUnknownOption(StoredSync);

const syncOf = (stored: string | null): Sync | null => {
  if (stored === null) return null;
  let parsed: unknown;
  try {
    parsed = JSON.parse(stored);
  } catch {
    return null;
  }
  return Option.getOrElse(decodeSync(parsed), (): Sync | null => null);
};

const rowOf = (row: Row): Remote => ({
  name: row.name,
  url: row.url,
  credential: row.credential,
  sync: syncOf(row.sync),
  createdAt: new Date(row.created_at),
});

/**
 * SQLite-backed, which on Cloudflare means the repository's own Durable
 * Object: the remote list lives beside the refs a fetch will move, and the
 * input gate serializes writes to it for the same reason it serializes
 * `RefStore.apply`.
 */
export const sql = (db: Sql, repo: string): Layer.Layer<Remotes> =>
  Layer.sync(Remotes, () => {
    // Keyed by repo and name together: one Durable Object holds one
    // repository in practice, but the stores are keyed by name and this
    // table has no business being the exception.
    db.exec(`
      CREATE TABLE IF NOT EXISTS remotes (
        repo       TEXT NOT NULL,
        name       TEXT NOT NULL,
        url        TEXT NOT NULL,
        credential TEXT,
        sync       TEXT,
        created_at TEXT NOT NULL,
        PRIMARY KEY (repo, name)
      )
    `);

    // A table that predates standing instructions has no `sync` column, and
    // recreating it would drop every remote the repository had. Added instead,
    // and the failure ignored because "already there" is the ordinary case.
    try {
      db.exec(`ALTER TABLE remotes ADD COLUMN sync TEXT`);
    } catch {
      // The column is already there, which is what we wanted.
    }

    const failed = (operation: string) => (cause: unknown) =>
      new StorageFailure({ operation, path: `remotes/${repo}`, cause });

    const one = (name: string) =>
      Effect.try({
        try: () =>
          db
            .exec<Row>(`SELECT * FROM remotes WHERE repo = ? AND name = ?`, repo, name)
            .toArray()
            .map(rowOf)[0] ?? null,
        catch: failed("remotes.get"),
      });

    return Remotes.of({
      list: Effect.try({
        try: () =>
          db
            .exec<Row>(`SELECT * FROM remotes WHERE repo = ? ORDER BY name`, repo)
            .toArray()
            .map(rowOf),
        catch: failed("remotes.list"),
      }),
      get: one,
      add: (input) =>
        Effect.gen(function* () {
          yield* validate(input);
          if ((yield* one(input.name)) !== null) return yield* duplicate(input.name);
          return yield* Effect.try({
            try: () => {
              const remote = remoteOf(input);
              db.exec(
                `INSERT INTO remotes (repo, name, url, credential, sync, created_at) VALUES (?, ?, ?, ?, ?, ?)`,
                repo,
                remote.name,
                remote.url,
                remote.credential,
                remote.sync === null ? null : JSON.stringify(remote.sync),
                remote.createdAt.toISOString(),
              );
              return remote;
            },
            catch: failed("remotes.add"),
          });
        }),
      remove: (name) =>
        Effect.try({
          try: () => {
            const found = db
              .exec<Row>(`SELECT name FROM remotes WHERE repo = ? AND name = ?`, repo, name)
              .toArray();
            if (found.length === 0) return false;
            db.exec(`DELETE FROM remotes WHERE repo = ? AND name = ?`, repo, name);
            return true;
          },
          catch: failed("remotes.remove"),
        }),
    });
  });

/**
 * The wire shape of a registration, shared by the API and any host that wants
 * to seed remotes from configuration.
 */
export const SyncWire = Schema.Struct({
  mode: Schema.Literals(MODES),
  /** Empty, or absent, is everything the mode carries. */
  refs: Schema.optional(Schema.Array(Schema.String)),
});

export const NewRemoteWire = Schema.Struct({
  name: Schema.String,
  url: Schema.String,
  credential: Schema.optional(Schema.String),
  sync: Schema.optional(SyncWire),
});
