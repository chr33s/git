/**
 * JSON API.
 *
 * Today: 45 endpoints in `src/server.api.ts` (2,515 lines). Payloads are
 * `Record<string, any>`; each handler re-checks its own fields, builds its own
 * `Response.json`, and the browser client re-declares the same shapes by hand
 * in `src/client.ts`. Pagination is copy-pasted per list endpoint.
 *
 * Sketch: one `HttpApi` definition. From it you get the server handlers (with
 * decoded, typed payloads), a derived client for `client.ts` and the CLI, and
 * an OpenAPI document — all from the same declaration, so they cannot drift.
 */
import { Effect, Layer, Schema, Stream } from "effect";
import { HttpApi, HttpApiBuilder, HttpApiEndpoint, HttpApiGroup } from "effect/unstable/httpapi";
import { Invalid, ObjectNotFound, RefConflict } from "../git/Error.ts";
import { Repository } from "../git/Repository.ts";
import { type Oid, RefStore } from "../git/Store.ts";

/** Wire representation of an oid; the domain's `Oid` is the branded form. */
const OidString = Schema.String.check(Schema.isPattern(/^[0-9a-f]{40}$/));

const Signature = Schema.Struct({
  name: Schema.String,
  email: Schema.String,
  date: Schema.optional(Schema.String),
});

const Commit = Schema.Struct({
  oid: OidString,
  tree: OidString,
  parents: Schema.Array(OidString),
  author: Signature,
  message: Schema.String,
});

/** Written once; every list endpoint reuses it instead of re-deriving it. */
const Page = <A extends Schema.Top>(item: A) =>
  Schema.Struct({
    items: Schema.Array(item),
    next_cursor: Schema.NullOr(Schema.String),
    has_more: Schema.Boolean,
  });

/** The `:repo` segment every group is prefixed with. */
const RepoParam = { repo: Schema.String };

const Cursor = Schema.Struct({
  cursor: Schema.optional(Schema.String),
  limit: Schema.optional(Schema.Number),
});

const refs = HttpApiGroup.make("refs")
  .add(
    HttpApiEndpoint.get("list", "/refs", {
      params: RepoParam,
      query: Cursor,
      success: Page(Schema.Struct({ name: Schema.String, oid: OidString })),
    }),
  )
  .add(
    HttpApiEndpoint.post("branch", "/branches/create", {
      params: RepoParam,
      payload: Schema.Struct({ name: Schema.String, base: Schema.String }),
      success: Schema.Struct({ name: Schema.String, oid: OidString }),
      error: [RefConflict, Invalid],
    }),
  )
  .prefix("/api/:repo");

const commits = HttpApiGroup.make("commits")
  .add(
    HttpApiEndpoint.post("log", "/log", {
      params: RepoParam,
      payload: Schema.Struct({ ref: Schema.String, limit: Schema.optional(Schema.Number) }),
      success: Page(Commit),
      error: ObjectNotFound,
    }),
  )
  .add(
    HttpApiEndpoint.post("commit", "/commit", {
      params: RepoParam,
      payload: Schema.Struct({
        branch: Schema.String,
        message: Schema.String,
        author: Signature,
        expected_head: Schema.optional(OidString),
        tree: OidString,
      }),
      success: Schema.Struct({ oid: OidString }),
      // The error union is part of the contract: the client gets `RefConflict`
      // as a tagged value it can match on, not a 409 whose `code` string it
      // has to sniff.
      error: [RefConflict, ObjectNotFound, Invalid],
    }),
  )
  .prefix("/api/:repo");

export const api = HttpApi.make("git").add(refs).add(commits);

export const refsLive = HttpApiBuilder.group(api, "refs", (handlers) =>
  handlers
    .handle("list", () =>
      Effect.gen(function* () {
        const store = yield* RefStore;
        const all = yield* store.list().pipe(Effect.orDie);
        return {
          items: all.map(([name, oid]) => ({ name, oid })),
          next_cursor: null,
          has_more: false,
        };
      }),
    )
    .handle("branch", ({ payload }) =>
      Effect.gen(function* () {
        const store = yield* RefStore;
        const base = yield* store.resolve(payload.base).pipe(Effect.orDie);
        if (base === null) {
          return yield* new Invalid({ field: "base", reason: "unknown ref" });
        }
        const [result] = yield* store
          .apply([{ name: `refs/heads/${payload.name}`, value: base, expected: null }])
          .pipe(Effect.catchTag("StorageFailure", Effect.die));
        if (!result?.applied) {
          return yield* new RefConflict({
            ref: payload.name,
            expected: null,
            actual: result?.current ?? null,
          });
        }
        return { name: payload.name, oid: base };
      }),
    ),
);

export const commitsLive = HttpApiBuilder.group(api, "commits", (handlers) =>
  handlers
    .handle("log", ({ payload }) =>
      Effect.gen(function* () {
        const repository = yield* Repository;
        const store = yield* RefStore;
        const head = yield* store.resolve(payload.ref).pipe(Effect.orDie);
        if (head === null) return { items: [], next_cursor: null, has_more: false };
        const items = yield* repository
          .log(head, { limit: payload.limit ?? 50 })
          .pipe(Stream.runCollect, Effect.catchTag("StorageFailure", Effect.die));
        return { items, next_cursor: null, has_more: false };
      }),
    )
    .handle("commit", ({ payload }) =>
      Effect.gen(function* () {
        const repository = yield* Repository;
        const oid = yield* repository
          .commit({
            branch: payload.branch,
            tree: payload.tree as Oid,
            message: payload.message,
            author: {
              name: payload.author.name,
              email: payload.author.email,
              at: new Date(),
              offset: 0,
            },
            expected: payload.expected_head as Oid | undefined,
          })
          .pipe(Effect.catchTag("StorageFailure", Effect.die));
        return { oid };
      }),
    ),
);

/**
 * `openapiPath` is free here — the spec is derived from the same declaration
 * the handlers are checked against, so `readme.md`'s endpoint tables stop being
 * hand-maintained.
 */
export const layer = Layer.mergeAll(
  HttpApiBuilder.layer(api, { openapiPath: "/api/openapi.json" }),
  refsLive,
  commitsLive,
);
