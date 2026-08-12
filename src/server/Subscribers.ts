/**
 * Who hears about a push.
 *
 * Delivery lives in `Webhooks.ts`; this is the registry behind it, and it is
 * the half that has to be durable — a subscriber list held in a field lasts
 * until the Durable Object is evicted, which is not a subscription.
 *
 * The port carries management as well as lookup because they are the same
 * table, and a webhook nobody can register is a webhook that never fires.
 * `forEvent` cannot fail: it runs on the push path, and a registry that is
 * unreadable must not turn a durable push into an error the client sees.
 */
import { Context, Effect, Layer, Schema } from "effect";

import { Invalid, StorageFailure } from "../git/Error.ts";
import type { Sql } from "../git/Sql.ts";

export interface Subscriber {
  readonly id: string;
  readonly url: string;
  /** Shared with the receiver; signs the body, and never leaves the server. */
  readonly secret: string;
  readonly createdAt: Date;
}

export interface NewSubscriber {
  readonly url: string;
  readonly secret: string;
}

export class Subscribers extends Context.Service<
  Subscribers,
  {
    /** The delivery path. Total by construction — see the module note. */
    readonly forEvent: (event: "push") => Effect.Effect<ReadonlyArray<Subscriber>>;
    readonly list: Effect.Effect<ReadonlyArray<Subscriber>, StorageFailure>;
    readonly add: (input: NewSubscriber) => Effect.Effect<Subscriber, StorageFailure | Invalid>;
    /** `false` when there was nothing with that id. */
    readonly remove: (id: string) => Effect.Effect<boolean, StorageFailure>;
  }
>()("server/Subscribers") {}

/**
 * A receiver must be reachable and its secret must be worth signing with.
 * Both are refused at registration rather than discovered at delivery, when
 * the only place left to report them is a log nobody reads.
 */
export const validate = (input: NewSubscriber): Effect.Effect<NewSubscriber, Invalid> =>
  Effect.suspend(() => {
    let parsed: URL;
    try {
      parsed = new URL(input.url);
    } catch {
      return Effect.fail(new Invalid({ field: "url", reason: `not a URL: '${input.url}'` }));
    }
    if (parsed.protocol !== "https:" && parsed.hostname !== "localhost") {
      return Effect.fail(
        new Invalid({ field: "url", reason: "webhook URLs must be https (localhost excepted)" }),
      );
    }
    if (input.secret.length < 16) {
      return Effect.fail(
        new Invalid({ field: "secret", reason: "secret must be at least 16 characters" }),
      );
    }
    return Effect.succeed(input);
  });

/** What a list endpoint may show: everything but the secret. */
export const redact = (subscriber: Subscriber) => ({
  id: subscriber.id,
  url: subscriber.url,
  created_at: subscriber.createdAt.toISOString(),
});

const readOnly = (what: string) =>
  Effect.fail(
    new StorageFailure({
      operation: "subscribers.add",
      path: "webhooks",
      cause: `${what} is read-only`,
    }),
  );

/** No subscribers, and none can be added: the default for a server without policy. */
export const none = Layer.succeed(Subscribers, {
  forEvent: () => Effect.succeed([]),
  list: Effect.succeed([]),
  add: () => readOnly("this registry"),
  remove: () => Effect.succeed(false),
});

/**
 * A fixed list — what a test or a configured host composes when it only cares
 * about delivery. `createdAt` is filled in, because a caller stating a list
 * inline is describing subscribers, not writing rows.
 */
export const of = (
  subscribers: ReadonlyArray<Omit<Subscriber, "createdAt"> & { readonly createdAt?: Date }>,
) => {
  const rows = subscribers.map((subscriber) => ({
    createdAt: new Date(0),
    ...subscriber,
  }));
  return Layer.succeed(Subscribers, {
    forEvent: () => Effect.succeed(rows),
    list: Effect.succeed(rows),
    add: () => readOnly("a fixed registry"),
    remove: () => Effect.succeed(false),
  });
};

const subscriberOf = (input: NewSubscriber): Subscriber => ({
  id: crypto.randomUUID(),
  url: input.url,
  secret: input.secret,
  createdAt: new Date(),
});

/** In-process and mutable: registration works, durability does not. */
export const memory = Layer.sync(Subscribers, () => {
  const rows = new Map<string, Subscriber>();
  return Subscribers.of({
    forEvent: () => Effect.succeed([...rows.values()]),
    list: Effect.sync(() => [...rows.values()]),
    add: (input) =>
      validate(input).pipe(
        Effect.map(() => {
          const subscriber = subscriberOf(input);
          rows.set(subscriber.id, subscriber);
          return subscriber;
        }),
      ),
    remove: (id) => Effect.sync(() => rows.delete(id)),
  });
});

interface Row extends Record<string, string | number | null> {
  readonly id: string;
  readonly url: string;
  readonly secret: string;
  readonly created_at: string;
}

const rowOf = (row: Row): Subscriber => ({
  id: row.id,
  url: row.url,
  secret: row.secret,
  createdAt: new Date(row.created_at),
});

/**
 * SQLite-backed, which on Cloudflare means the repository's own Durable
 * Object: the subscriber list lives beside the refs it reports on, and the
 * input gate serializes writes to it for the same reason it serializes
 * `RefStore.apply`.
 */
export const sql = (db: Sql, repo: string): Layer.Layer<Subscribers> =>
  Layer.sync(Subscribers, () => {
    // Scoped by repo rather than assuming one per database: a Durable Object
    // holds one repository in practice, but the stores are keyed by name and
    // this table has no business being the exception.
    db.exec(`
      CREATE TABLE IF NOT EXISTS webhooks (
        id         TEXT PRIMARY KEY,
        repo       TEXT NOT NULL,
        url        TEXT NOT NULL,
        secret     TEXT NOT NULL,
        created_at TEXT NOT NULL
      )
    `);

    const failed = (operation: string) => (cause: unknown) =>
      new StorageFailure({ operation, path: `webhooks/${repo}`, cause });

    const all = Effect.try({
      try: () =>
        db
          .exec<Row>(`SELECT * FROM webhooks WHERE repo = ? ORDER BY created_at`, repo)
          .toArray()
          .map(rowOf),
      catch: failed("subscribers.list"),
    });

    return Subscribers.of({
      // A registry this instance cannot read must not fail a push that is
      // already durable; it costs a delivery, not the write.
      forEvent: () => all.pipe(Effect.orElseSucceed(() => [])),
      list: all,
      add: (input) =>
        validate(input).pipe(
          Effect.flatMap(() =>
            Effect.try({
              try: () => {
                const subscriber = subscriberOf(input);
                db.exec(
                  `INSERT INTO webhooks (id, repo, url, secret, created_at) VALUES (?, ?, ?, ?, ?)`,
                  subscriber.id,
                  repo,
                  subscriber.url,
                  subscriber.secret,
                  subscriber.createdAt.toISOString(),
                );
                return subscriber;
              },
              catch: failed("subscribers.add"),
            }),
          ),
        ),
      remove: (id) =>
        Effect.try({
          try: () => {
            const found = db
              .exec<Row>(`SELECT id FROM webhooks WHERE id = ? AND repo = ?`, id, repo)
              .toArray();
            if (found.length === 0) return false;
            db.exec(`DELETE FROM webhooks WHERE id = ? AND repo = ?`, id, repo);
            return true;
          },
          catch: failed("subscribers.remove"),
        }),
    });
  });

/**
 * The wire shape of a registration, shared by the API and any host that wants
 * to seed subscribers from configuration.
 */
export const NewSubscriberWire = Schema.Struct({
  url: Schema.String,
  secret: Schema.String,
});
