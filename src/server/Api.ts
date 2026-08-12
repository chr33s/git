/**
 * JSON API.
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
import { NewSubscriberWire, redact, Subscribers } from "./Subscribers.ts";

/** Wire representation of an oid; the domain's `Oid` is the branded form. */
const OidString = Schema.String.check(Schema.isPattern(/^[0-9a-f]{40}$/));

/** JSON has no `Date`: `at` crosses as an ISO string, `offset` in minutes. */
const SignatureWire = Schema.Struct({
  name: Schema.optional(Schema.String),
  email: Schema.optional(Schema.String),
  at: Schema.optional(Schema.String),
  offset: Schema.optional(Schema.Finite),
});

const signatureFrom = (author: (typeof SignatureWire)["Type"] | undefined): Signature => ({
  name: author?.name ?? "Anonymous",
  email: author?.email ?? "anonymous@example.com",
  at: author?.at === undefined ? new Date() : new Date(author.at),
  offset: author?.offset ?? 0,
});

const RepoParam = { repo: Schema.String };

/** Written once; every list endpoint reuses it instead of re-deriving it. */
const Page = <A extends Schema.Top>(item: A) =>
  Schema.Struct({
    items: Schema.Array(item),
    next_cursor: Schema.NullOr(Schema.String),
    has_more: Schema.Boolean,
  });

const Cursor = {
  cursor: Schema.optional(Schema.String),
  limit: Schema.optional(Schema.String),
};

const Ref = Schema.Struct({ name: Schema.String, oid: OidString });

/**
 * Blob content crosses as text by default and base64 when asked, because a
 * JSON API that demands base64 for a README is unusable and one that cannot
 * carry a PNG is incomplete. Reads always answer base64: the server cannot
 * know a blob is text, and guessing would corrupt the bytes that are not.
 */
const Encoding = Schema.Literals(["utf8", "base64"]);

const encoder = new TextEncoder();

const decodeContent = (content: string, encoding: "utf8" | "base64" | undefined): Uint8Array => {
  if (encoding !== "base64") return encoder.encode(content);
  const binary = atob(content);
  const bytes = new Uint8Array(binary.length);
  for (let index = 0; index < binary.length; index++) bytes[index] = binary.charCodeAt(index);
  return bytes;
};

const toBase64 = (bytes: Uint8Array): string => {
  let binary = "";
  for (const byte of bytes) binary += String.fromCharCode(byte);
  return btoa(binary);
};

/** A registered webhook as a client may see it: no secret, ever. */
const WebhookWire = Schema.Struct({
  id: Schema.String,
  url: Schema.String,
  created_at: Schema.String,
});

const TreeEntryWire = Schema.Struct({
  mode: Schema.String,
  name: Schema.String,
  oid: OidString,
});

/** A path to write, or — with `content: null` — to remove. */
const FileWire = Schema.Struct({
  path: Schema.String,
  content: Schema.NullOr(Schema.String),
  encoding: Schema.optional(Encoding),
  mode: Schema.optional(Schema.String),
});

const changesOf = (files: ReadonlyArray<(typeof FileWire)["Type"]>) =>
  files.map((file) => ({
    path: file.path,
    content: file.content === null ? null : decodeContent(file.content, file.encoding),
    ...(file.mode === undefined ? {} : { mode: file.mode }),
  }));

/** Cursors are opaque to clients; here they are simply an offset. */
const page = <A>(items: ReadonlyArray<A>, query: { cursor?: string; limit?: string }) => {
  const start = query.cursor === undefined ? 0 : Number.parseInt(query.cursor, 10);
  const size = query.limit === undefined ? 50 : Number.parseInt(query.limit, 10);
  const slice = items.slice(start, start + size);
  const more = start + size < items.length;
  return { items: slice, next_cursor: more ? String(start + size) : null, has_more: more };
};

/**
 * What a commit's tree should be: stated outright, built from paths on top of
 * whatever the branch already holds, or empty. The middle case is the one that
 * makes the JSON API able to create content at all — it reads the branch tip
 * so a commit adds to the tree instead of replacing it.
 */
const treeFor = (
  repository: Repository["Service"],
  branch: string,
  payload: {
    readonly tree?: string | undefined;
    readonly files?: ReadonlyArray<(typeof FileWire)["Type"]> | undefined;
  },
) =>
  Effect.gen(function* () {
    if (payload.tree !== undefined) return payload.tree as Oid;
    if (payload.files === undefined) return EMPTY_TREE_OID;

    const ref = branch.startsWith("refs/") ? branch : `refs/heads/${branch}`;
    const tip = yield* repository.resolve(ref);
    const base = tip === null ? undefined : (yield* repository.readCommit(tip)).tree;

    return yield* repository.writeFiles({
      ...(base === undefined ? {} : { base }),
      changes: changesOf(payload.files),
    });
  });

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
        /**
         * The content, three ways: an explicit `tree`, or `files` applied to
         * the branch's current tree, or neither for an empty commit.
         */
        tree: Schema.optional(OidString),
        files: Schema.optional(Schema.Array(FileWire)),
      }),
      success: Schema.Struct({ oid: OidString, tree: OidString }),
      error: [RefConflict, ObjectNotFound, Invalid],
    }),
  )
  .add(
    HttpApiEndpoint.post("blob", "/blob", {
      params: RepoParam,
      payload: Schema.Struct({
        content: Schema.String,
        encoding: Schema.optional(Encoding),
      }),
      success: Schema.Struct({ oid: OidString }),
    }),
  )
  .add(
    HttpApiEndpoint.get("readBlob", "/blob/:oid", {
      params: { ...RepoParam, oid: OidString },
      success: Schema.Struct({
        content: Schema.String,
        encoding: Schema.Literals(["base64"]),
        size: Schema.Finite,
      }),
      error: ObjectNotFound,
    }),
  )
  .add(
    HttpApiEndpoint.post("tree", "/tree", {
      params: RepoParam,
      payload: Schema.Struct({
        /** Entries as they are, or `files` to build them from paths. */
        entries: Schema.optional(Schema.Array(TreeEntryWire)),
        files: Schema.optional(Schema.Array(FileWire)),
        base: Schema.optional(OidString),
      }),
      success: Schema.Struct({ oid: OidString }),
      error: [ObjectNotFound, Invalid],
    }),
  )
  .add(
    HttpApiEndpoint.get("readTree", "/tree/:oid", {
      params: { ...RepoParam, oid: OidString },
      success: Schema.Struct({ entries: Schema.Array(TreeEntryWire) }),
      error: ObjectNotFound,
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
      success: Schema.Struct({ refs: Schema.Array(Ref) }),
    }),
  )
  .add(
    // The paged form. `refs` stays as it is: the smart-HTTP advertisement
    // needs every ref anyway, and breaking it would buy nothing.
    HttpApiEndpoint.get("branches", "/branches", {
      params: RepoParam,
      query: Cursor,
      success: Page(Ref),
    }),
  )
  .add(
    HttpApiEndpoint.post("branch", "/branches/create", {
      params: RepoParam,
      payload: Schema.Struct({ name: Schema.String, base: Schema.String }),
      success: Ref,
      error: [RefConflict, Invalid],
    }),
  )
  .add(
    // Registration, because a delivery engine nobody can subscribe to never
    // fires. The secret goes in and never comes back out.
    HttpApiEndpoint.post("webhookAdd", "/webhooks", {
      params: RepoParam,
      payload: NewSubscriberWire,
      success: WebhookWire,
      error: Invalid,
    }),
  )
  .add(
    HttpApiEndpoint.get("webhookList", "/webhooks", {
      params: RepoParam,
      success: Schema.Struct({ webhooks: Schema.Array(WebhookWire) }),
    }),
  )
  .add(
    HttpApiEndpoint.delete("webhookRemove", "/webhooks/:id", {
      params: { ...RepoParam, id: Schema.String },
      success: Schema.Struct({ deleted: Schema.Boolean }),
    }),
  )
  .add(
    HttpApiEndpoint.get("commits", "/commits/:oid", {
      params: { ...RepoParam, oid: OidString },
      query: Cursor,
      success: Page(Schema.Struct({ message: Schema.String, oid: OidString })),
      error: ObjectNotFound,
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
        const branch = payload.branch ?? "main";

        const tree = yield* treeFor(repository, branch, payload).pipe(
          Effect.catchTag("StorageFailure", Effect.die),
        );

        const oid = yield* repository
          .commit({
            author: signatureFrom(payload.author),
            branch,
            message: payload.message ?? "",
            tree,
            ...(payload.expected === undefined ? {} : { expected: payload.expected as Oid | null }),
          })
          .pipe(Effect.catchTag("StorageFailure", Effect.die));
        return { oid, tree };
      }),
    )
    .handle("blob", ({ payload }) =>
      Effect.gen(function* () {
        const repository = yield* Repository;
        const oid = yield* repository
          .writeBlob(decodeContent(payload.content, payload.encoding))
          .pipe(Effect.catchTag("StorageFailure", Effect.die));
        return { oid };
      }),
    )
    .handle("readBlob", ({ params }) =>
      Effect.gen(function* () {
        const repository = yield* Repository;
        const data = yield* repository
          .readBlob(params.oid as Oid)
          .pipe(Effect.catchTag("StorageFailure", Effect.die));
        return { content: toBase64(data), encoding: "base64" as const, size: data.length };
      }),
    )
    .handle("tree", ({ payload }) =>
      Effect.gen(function* () {
        const repository = yield* Repository;
        const oid = yield* (
          payload.entries === undefined
            ? repository.writeFiles({
                ...(payload.base === undefined ? {} : { base: payload.base as Oid }),
                changes: changesOf(payload.files ?? []),
              })
            : repository.writeTree(
                payload.entries.map((entry) => ({ ...entry, oid: entry.oid as Oid })),
              )
        ).pipe(Effect.catchTag("StorageFailure", Effect.die));
        return { oid };
      }),
    )
    .handle("readTree", ({ params }) =>
      Effect.gen(function* () {
        const repository = yield* Repository;
        const entries = yield* repository
          .readTree(params.oid as Oid)
          .pipe(Effect.catchTag("StorageFailure", Effect.die));
        return { entries };
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
    )
    .handle("branches", ({ query }) =>
      Effect.gen(function* () {
        const repository = yield* Repository;
        const refs = yield* repository.refs.pipe(Effect.catchTag("StorageFailure", Effect.die));
        // Sorted, because a cursor over an unstable order skips rows: two
        // requests must see the same sequence.
        const branches = refs
          .filter(([name]) => name.startsWith("refs/heads/"))
          .map(([name, oid]) => ({ name, oid }))
          .sort((left, right) => left.name.localeCompare(right.name));
        return page(branches, query);
      }),
    )
    .handle("branch", ({ payload }) =>
      Effect.gen(function* () {
        const repository = yield* Repository;
        const oid = yield* repository
          .branch(payload)
          .pipe(Effect.catchTag("StorageFailure", Effect.die));
        return { name: `refs/heads/${payload.name}`, oid };
      }),
    )
    .handle("webhookAdd", ({ payload }) =>
      Effect.gen(function* () {
        const subscribers = yield* Subscribers;
        const added = yield* subscribers
          .add(payload)
          .pipe(Effect.catchTag("StorageFailure", Effect.die));
        return redact(added);
      }),
    )
    .handle("webhookList", () =>
      Effect.gen(function* () {
        const subscribers = yield* Subscribers;
        const rows = yield* subscribers.list.pipe(Effect.catchTag("StorageFailure", Effect.die));
        return { webhooks: rows.map(redact) };
      }),
    )
    .handle("webhookRemove", ({ params }) =>
      Effect.gen(function* () {
        const subscribers = yield* Subscribers;
        const deleted = yield* subscribers
          .remove(params.id)
          .pipe(Effect.catchTag("StorageFailure", Effect.die));
        return { deleted };
      }),
    )
    .handle("commits", ({ params, query }) =>
      Effect.gen(function* () {
        const repository = yield* Repository;
        const start = query.cursor === undefined ? 0 : Number.parseInt(query.cursor, 10);
        const size = query.limit === undefined ? 50 : Number.parseInt(query.limit, 10);
        // Walk only as far as this page needs, plus one to answer `has_more`.
        const walked = yield* Stream.runCollect(
          repository.log(params.oid as Oid, { limit: start + size + 1 }),
        ).pipe(Effect.catchTag("StorageFailure", Effect.die));
        return page(
          walked.map((commit) => ({ message: commit.message, oid: commit.oid })),
          query,
        );
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
