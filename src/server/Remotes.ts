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
import { Context, Effect, Layer, Schema } from "effect";

import { Invalid, StorageFailure } from "../git/Error.ts";
import type { Sql } from "../git/Sql.ts";

export interface Remote {
  readonly name: string;
  readonly url: string;
  /** Sent to the remote as a Bearer token, and never returned. */
  readonly credential: string | null;
  readonly createdAt: Date;
}

export interface NewRemote {
  readonly name: string;
  readonly url: string;
  readonly credential?: string | undefined;
}

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
    Omit<Remote, "createdAt" | "credential"> & {
      readonly credential?: string | null;
      readonly createdAt?: Date;
    }
  >,
) => {
  const rows = remotes.map((remote) => ({
    createdAt: new Date(0),
    credential: null,
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
  readonly created_at: string;
}

const rowOf = (row: Row): Remote => ({
  name: row.name,
  url: row.url,
  credential: row.credential,
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
        created_at TEXT NOT NULL,
        PRIMARY KEY (repo, name)
      )
    `);

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
                `INSERT INTO remotes (repo, name, url, credential, created_at) VALUES (?, ?, ?, ?, ?)`,
                repo,
                remote.name,
                remote.url,
                remote.credential,
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
export const NewRemoteWire = Schema.Struct({
  name: Schema.String,
  url: Schema.String,
  credential: Schema.optional(Schema.String),
});
