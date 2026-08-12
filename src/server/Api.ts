/**
 * JSON API — phase 4's schema half.
 *
 * One `HttpApi` definition: the server handlers decode payloads through the
 * same schemas a derived client would use, and the errors on the wire are the
 * tagged classes from `git/Error.ts` — a `RefConflict` arrives as
 * `{ _tag: "RefConflict", ref, expected, actual }` with its status read from
 * the class's own `httpApiStatus` annotation, not from a mapping table.
 *
 * `layer` is host-neutral: it needs `Repository` and a router, nothing else.
 * The Durable Object turns it into a fetch handler with
 * `HttpRouter.toWebHandler`; any other host would do the same.
 */
import { Effect, FileSystem, Layer, Path, Schema, Stream } from "effect";
import { Etag, HttpPlatform } from "effect/unstable/http";
import { HttpApi, HttpApiBuilder, HttpApiEndpoint, HttpApiGroup } from "effect/unstable/httpapi";

import { Invalid, ObjectNotFound, RefConflict } from "../git/Error.ts";
import { EMPTY_TREE_OID, type Signature } from "../git/Format.ts";
import { Repository } from "../git/Repository.ts";
import type { Oid } from "../git/Store.ts";

/** Wire representation of an oid; the domain's `Oid` is the branded form. */
const OidString = Schema.String.check(Schema.isPattern(/^[0-9a-f]{40}$/));

/** JSON has no `Date`: `at` crosses as an ISO string, `offset` in minutes. */
const SignatureWire = Schema.Struct({
  name: Schema.optional(Schema.String),
  email: Schema.optional(Schema.String),
  at: Schema.optional(Schema.String),
  offset: Schema.optional(Schema.Number),
});

const signatureFrom = (author: (typeof SignatureWire)["Type"] | undefined): Signature => ({
  name: author?.name ?? "Anonymous",
  email: author?.email ?? "anonymous@example.com",
  at: author?.at === undefined ? new Date() : new Date(author.at),
  offset: author?.offset ?? 0,
});

const RepoParam = { repo: Schema.String };

const repo = HttpApiGroup.make("repo")
  .add(
    HttpApiEndpoint.post("create", "/commit", {
      params: RepoParam,
      payload: Schema.Struct({
        author: Schema.optional(SignatureWire),
        branch: Schema.optional(Schema.String),
        message: Schema.optional(Schema.String),
        /** absent = append to the branch; `null` = branch must not exist. */
        expected: Schema.optional(Schema.NullOr(OidString)),
      }),
      success: Schema.Struct({ oid: OidString }),
      error: [RefConflict, ObjectNotFound, Invalid],
    }),
  )
  .add(
    HttpApiEndpoint.get("read", "/commit/:oid", {
      params: { ...RepoParam, oid: OidString },
      success: Schema.Struct({
        message: Schema.String,
        parents: Schema.Array(OidString),
        tree: OidString,
      }),
      error: ObjectNotFound,
    }),
  )
  .add(
    HttpApiEndpoint.get("log", "/log/:oid", {
      params: { ...RepoParam, oid: OidString },
      success: Schema.Struct({
        commits: Schema.Array(Schema.Struct({ message: Schema.String, oid: OidString })),
      }),
      error: ObjectNotFound,
    }),
  )
  .add(
    HttpApiEndpoint.get("refs", "/refs", {
      params: RepoParam,
      success: Schema.Struct({
        refs: Schema.Array(Schema.Struct({ name: Schema.String, oid: OidString })),
      }),
    }),
  )
  .prefix("/:repo");

export const api = HttpApi.make("git").add(repo);

/** `StorageFailure` is a defect here: a 500 no caller can act on. */
export const handlers = HttpApiBuilder.group(api, "repo", (group) =>
  group
    .handle("create", ({ payload }) =>
      Effect.gen(function* () {
        const repository = yield* Repository;
        const oid = yield* repository
          .commit({
            author: signatureFrom(payload.author),
            branch: payload.branch ?? "main",
            message: payload.message ?? "",
            tree: EMPTY_TREE_OID,
            ...(payload.expected === undefined ? {} : { expected: payload.expected as Oid | null }),
          })
          .pipe(Effect.catchTag("StorageFailure", Effect.die));
        return { oid };
      }),
    )
    .handle("read", ({ params }) =>
      Effect.gen(function* () {
        const repository = yield* Repository;
        const commit = yield* repository
          .readCommit(params.oid as Oid)
          .pipe(Effect.catchTag("StorageFailure", Effect.die));
        return { message: commit.message, parents: commit.parents, tree: commit.tree };
      }),
    )
    .handle("log", ({ params }) =>
      Effect.gen(function* () {
        const repository = yield* Repository;
        const commits = yield* Stream.runCollect(
          repository.log(params.oid as Oid, { limit: 50 }),
        ).pipe(Effect.catchTag("StorageFailure", Effect.die));
        return {
          commits: commits.map((commit) => ({ message: commit.message, oid: commit.oid })),
        };
      }),
    )
    .handle("refs", () =>
      Effect.gen(function* () {
        const repository = yield* Repository;
        const refs = yield* repository.refs.pipe(Effect.catchTag("StorageFailure", Effect.die));
        return { refs: refs.map(([name, oid]) => ({ name, oid })) };
      }),
    ),
);

/**
 * The API as one layer: routes registered, handlers wired, response plumbing
 * (etag, platform) satisfied from core with no filesystem underneath — a
 * Worker has none, and nothing here serves files.
 */
export const layer = HttpApiBuilder.layer(api).pipe(
  Layer.provide(handlers),
  Layer.provide(HttpPlatform.layer),
  Layer.provide(Etag.layerWeak),
  Layer.provide(FileSystem.layerNoop({})),
  Layer.provide(Path.layer),
);
