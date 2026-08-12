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

import { isBinary, unified } from "../git/Diff.ts";
import { Invalid, ObjectNotFound, RefConflict } from "../git/Error.ts";
import { EMPTY_TREE_OID, type Signature } from "../git/Format.ts";
import { cherryPick, rebase } from "../git/Rebase.ts";
import { Repository } from "../git/Repository.ts";
import { isOid, type Oid } from "../git/Store.ts";
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

/**
 * What a replay produced, for both `cherry-pick` and `rebase`.
 *
 * `commits` lists every commit considered, not just the ones that produced
 * something: a `replayed` of `null` is a commit `onto` already had, or one
 * that conflicted, and dropping those would leave the caller unable to tell
 * an empty pick from a skipped one.
 */
const ReplayOutcomeWire = Schema.Struct({
  kind: Schema.Literals(["replayed", "up-to-date", "conflicted"]),
  head: Schema.NullOr(OidString),
  commits: Schema.Array(
    Schema.Struct({
      original: OidString,
      replayed: Schema.NullOr(OidString),
      conflicts: Schema.Array(
        Schema.Struct({
          path: Schema.String,
          reason: Schema.Literals(["content", "add/add", "modify/delete", "binary"]),
        }),
      ),
    }),
  ),
});

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
const decoder = new TextDecoder();

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

/** The tree a ref names, defaulting to HEAD — what "at this revision" means. */
const treeOfRef = (repository: Repository["Service"], ref: string | undefined) =>
  Effect.gen(function* () {
    const name = ref === undefined || ref === "" ? "HEAD" : ref;
    const oid = isOid(name) ? name : yield* repository.resolve(name);
    if (oid === null) return yield* new Invalid({ field: "ref", reason: `unknown ref '${name}'` });

    // A ref may name a commit, a tag that peels to one, or a tree outright.
    const object = yield* repository.readObject(oid);
    if (object.type === "tree") return oid;
    if (object.type === "tag") {
      const tag = yield* repository.readTag(oid);
      return (yield* repository.readCommit(tag.object)).tree;
    }
    return (yield* repository.readCommit(oid)).tree;
  }).pipe(Effect.catchTag("StorageFailure", Effect.die));

const subtreeAt = (repository: Repository["Service"], tree: Oid, path: string) =>
  repository.findPath(tree, path).pipe(
    Effect.catchTag("StorageFailure", Effect.die),
    Effect.flatMap((entry) =>
      entry === null || entry.mode !== "40000"
        ? Effect.fail(new ObjectNotFound({ oid: path }))
        : Effect.succeed(entry.oid),
    ),
  );

/**
 * A grep predicate. A bad pattern is the caller's mistake and says so, rather
 * than arriving as a 500 from deep inside the walk.
 */
const matcher = (payload: {
  readonly pattern: string;
  readonly fixed?: boolean | undefined;
  readonly ignore_case?: boolean | undefined;
}) =>
  Effect.suspend((): Effect.Effect<(line: string) => boolean, Invalid> => {
    if (payload.fixed === true) {
      const needle = payload.ignore_case === true ? payload.pattern.toLowerCase() : payload.pattern;
      return Effect.succeed((line) =>
        (payload.ignore_case === true ? line.toLowerCase() : line).includes(needle),
      );
    }
    try {
      const expression = new RegExp(payload.pattern, payload.ignore_case === true ? "i" : "");
      return Effect.succeed((line) => expression.test(line));
    } catch (cause) {
      return Effect.fail(
        new Invalid({ field: "pattern", reason: cause instanceof Error ? cause.message : "bad" }),
      );
    }
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
    HttpApiEndpoint.post("tagCreate", "/tags", {
      params: RepoParam,
      payload: Schema.Struct({
        name: Schema.String,
        /** A ref or an oid. */
        target: Schema.String,
        /** Present makes it annotated; absent makes it lightweight. */
        message: Schema.optional(Schema.String),
        tagger: Schema.optional(SignatureWire),
        force: Schema.optional(Schema.Boolean),
      }),
      success: Schema.Struct({ ref: Schema.String, oid: OidString, target: OidString }),
      error: [RefConflict, ObjectNotFound, Invalid],
    }),
  )
  .add(
    HttpApiEndpoint.get("tags", "/tags", {
      params: RepoParam,
      query: Cursor,
      success: Page(Ref),
    }),
  )
  .add(
    HttpApiEndpoint.get("tagRead", "/tag/:oid", {
      params: { ...RepoParam, oid: OidString },
      success: Schema.Struct({
        object: OidString,
        type: Schema.Literals(["blob", "tree", "commit", "tag"]),
        tag: Schema.String,
        message: Schema.String,
      }),
      error: ObjectNotFound,
    }),
  )
  .add(
    HttpApiEndpoint.delete("tagRemove", "/tags/:name", {
      params: { ...RepoParam, name: Schema.String },
      success: Schema.Struct({ deleted: Schema.Boolean }),
      error: Invalid,
    }),
  )
  .add(
    // Integrity, which the storage contract cannot answer: it proves the
    // store returns what it was given, not that what it kept is still a git
    // object.
    HttpApiEndpoint.post("fsck", "/fsck", {
      params: RepoParam,
      success: Schema.Struct({
        checked: Schema.Finite,
        ok: Schema.Boolean,
        problems: Schema.Array(Schema.Struct({ oid: OidString, problem: Schema.String })),
        dangling_refs: Schema.Array(Schema.Struct({ ref: Schema.String, oid: OidString })),
      }),
    }),
  )
  .add(
    HttpApiEndpoint.delete("branchRemove", "/branches/:name", {
      params: { ...RepoParam, name: Schema.String },
      success: Schema.Struct({ deleted: Schema.Boolean }),
      error: Invalid,
    }),
  )
  .add(
    /**
     * Move a ref to a commit. This is `reset --hard` and "restore this
     * branch to that commit" both — on a bare repository they are the same
     * operation, and the only question worth asking is what the ref was
     * expected to be, so a concurrent push is not silently discarded.
     */
    HttpApiEndpoint.post("reset", "/reset", {
      params: RepoParam,
      payload: Schema.Struct({
        ref: Schema.String,
        to: Schema.String,
        /** Absent moves whatever it is now; stating it makes this a CAS. */
        expected: Schema.optional(Schema.NullOr(OidString)),
      }),
      success: Schema.Struct({
        ref: Schema.String,
        oid: OidString,
        previous: Schema.NullOr(OidString),
      }),
      error: [RefConflict, ObjectNotFound, Invalid],
    }),
  )
  .add(
    HttpApiEndpoint.post("merge", "/merge", {
      params: RepoParam,
      payload: Schema.Struct({
        ours: Schema.String,
        theirs: Schema.String,
        author: Schema.optional(SignatureWire),
        message: Schema.optional(Schema.String),
        strategy: Schema.optional(Schema.Literals(["recursive", "ours", "theirs"])),
        /** The ref to move on success; absent computes and stops. */
        into: Schema.optional(Schema.String),
        no_fast_forward: Schema.optional(Schema.Boolean),
      }),
      success: Schema.Struct({
        kind: Schema.Literals(["up-to-date", "fast-forward", "merged", "conflicted"]),
        commit: Schema.NullOr(OidString),
        tree: Schema.NullOr(OidString),
        base: Schema.NullOr(OidString),
        conflicts: Schema.Array(
          Schema.Struct({
            path: Schema.String,
            reason: Schema.Literals(["content", "add/add", "modify/delete", "binary"]),
          }),
        ),
      }),
      error: [RefConflict, ObjectNotFound, Invalid],
    }),
  )
  .add(
    HttpApiEndpoint.post("cherry-pick", "/cherry-pick", {
      params: RepoParam,
      payload: Schema.Struct({
        commit: Schema.String,
        onto: Schema.String,
        /** Who is picking; the original commit stays the author either way. */
        author: Schema.optional(SignatureWire),
        into: Schema.optional(Schema.String),
      }),
      success: ReplayOutcomeWire,
      error: [RefConflict, ObjectNotFound, Invalid],
    }),
  )
  .add(
    HttpApiEndpoint.post("rebase", "/rebase", {
      params: RepoParam,
      payload: Schema.Struct({
        branch: Schema.String,
        onto: Schema.String,
        into: Schema.optional(Schema.String),
      }),
      success: ReplayOutcomeWire,
      error: [RefConflict, ObjectNotFound, Invalid],
    }),
  )
  .add(
    HttpApiEndpoint.post("diff", "/diff", {
      params: RepoParam,
      payload: Schema.Struct({
        /** Refs, oids or trees. */
        from: Schema.String,
        to: Schema.String,
        path: Schema.optional(Schema.String),
        context: Schema.optional(Schema.Finite),
      }),
      success: Schema.Struct({
        files: Schema.Array(
          Schema.Struct({
            path: Schema.String,
            status: Schema.Literals(["added", "removed", "modified"]),
            binary: Schema.Boolean,
            patch: Schema.String,
          }),
        ),
      }),
      error: [ObjectNotFound, Invalid],
    }),
  )
  .add(
    HttpApiEndpoint.get("files", "/files", {
      params: RepoParam,
      query: { ref: Schema.optional(Schema.String), path: Schema.optional(Schema.String) },
      success: Schema.Struct({
        files: Schema.Array(
          Schema.Struct({ path: Schema.String, mode: Schema.String, oid: OidString }),
        ),
      }),
      error: [ObjectNotFound, Invalid],
    }),
  )
  .add(
    HttpApiEndpoint.get("file", "/file", {
      params: RepoParam,
      query: { ref: Schema.optional(Schema.String), path: Schema.String },
      success: Schema.Struct({
        path: Schema.String,
        mode: Schema.String,
        oid: OidString,
        content: Schema.String,
        encoding: Schema.Literals(["base64"]),
        size: Schema.Finite,
      }),
      error: [ObjectNotFound, Invalid],
    }),
  )
  .add(
    HttpApiEndpoint.get("object", "/object/:oid", {
      params: { ...RepoParam, oid: OidString },
      success: Schema.Struct({
        oid: OidString,
        type: Schema.Literals(["blob", "tree", "commit", "tag"]),
        size: Schema.Finite,
        content: Schema.String,
        encoding: Schema.Literals(["base64"]),
      }),
      error: ObjectNotFound,
    }),
  )
  .add(
    // The ref name has slashes, so it is a query parameter rather than a
    // path segment — `refs/heads/main` in a path would need escaping every
    // caller would get wrong.
    HttpApiEndpoint.get("reflog", "/reflog", {
      params: RepoParam,
      query: { ref: Schema.String },
      success: Schema.Struct({
        entries: Schema.Array(
          Schema.Struct({
            from: Schema.NullOr(OidString),
            to: Schema.NullOr(OidString),
            at: Schema.String,
            message: Schema.String,
          }),
        ),
      }),
    }),
  )
  .add(
    HttpApiEndpoint.post("grep", "/grep", {
      params: RepoParam,
      payload: Schema.Struct({
        pattern: Schema.String,
        ref: Schema.optional(Schema.String),
        path: Schema.optional(Schema.String),
        ignore_case: Schema.optional(Schema.Boolean),
        fixed: Schema.optional(Schema.Boolean),
        /** Bounded by default: a grep over a big tree is a lot of lines. */
        max_matches: Schema.optional(Schema.Finite),
      }),
      success: Schema.Struct({
        matches: Schema.Array(
          Schema.Struct({
            path: Schema.String,
            line: Schema.Finite,
            text: Schema.String,
          }),
        ),
        truncated: Schema.Boolean,
      }),
      error: [ObjectNotFound, Invalid],
    }),
  )
  .add(
    HttpApiEndpoint.post("gc", "/gc", {
      params: RepoParam,
      payload: Schema.Struct({
        dry_run: Schema.optional(Schema.Boolean),
        /** Also write what survives into one pack and drop the loose copies. */
        repack: Schema.optional(Schema.Boolean),
      }),
      success: Schema.Struct({
        scanned: Schema.Finite,
        reachable: Schema.Finite,
        removed: Schema.Array(OidString),
        packed: Schema.NullOr(Schema.Struct({ name: Schema.String, objects: Schema.Finite })),
      }),
      error: ObjectNotFound,
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
    .handle("tagCreate", ({ payload }) =>
      Effect.gen(function* () {
        const repository = yield* Repository;
        return yield* repository
          .tag({
            name: payload.name,
            target: payload.target,
            ...(payload.message === undefined ? {} : { message: payload.message }),
            ...(payload.tagger === undefined ? {} : { tagger: signatureFrom(payload.tagger) }),
            ...(payload.force === undefined ? {} : { force: payload.force }),
          })
          .pipe(Effect.catchTag("StorageFailure", Effect.die));
      }),
    )
    .handle("tags", ({ query }) =>
      Effect.gen(function* () {
        const repository = yield* Repository;
        const refs = yield* repository.refs.pipe(Effect.catchTag("StorageFailure", Effect.die));
        const tags = refs
          .filter(([name]) => name.startsWith("refs/tags/"))
          .map(([name, oid]) => ({ name, oid }))
          .sort((left, right) => left.name.localeCompare(right.name));
        return page(tags, query);
      }),
    )
    .handle("tagRead", ({ params }) =>
      Effect.gen(function* () {
        const repository = yield* Repository;
        const tag = yield* repository
          .readTag(params.oid as Oid)
          .pipe(Effect.catchTag("StorageFailure", Effect.die));
        return { object: tag.object, type: tag.type, tag: tag.tag, message: tag.message };
      }),
    )
    .handle("tagRemove", ({ params }) =>
      Effect.gen(function* () {
        const repository = yield* Repository;
        const deleted = yield* repository
          .deleteTag(params.name)
          .pipe(Effect.catchTag("StorageFailure", Effect.die));
        return { deleted };
      }),
    )
    .handle("fsck", () =>
      Effect.gen(function* () {
        const repository = yield* Repository;
        const report = yield* repository.fsck.pipe(Effect.catchTag("StorageFailure", Effect.die));
        return {
          checked: report.checked,
          ok: report.problems.length === 0 && report.danglingRefs.length === 0,
          problems: report.problems,
          dangling_refs: report.danglingRefs,
        };
      }),
    )
    .handle("branchRemove", ({ params }) =>
      Effect.gen(function* () {
        const repository = yield* Repository;
        const deleted = yield* repository
          .deleteRef(`refs/heads/${params.name}`)
          .pipe(Effect.catchTag("StorageFailure", Effect.die));
        return { deleted };
      }),
    )
    .handle("reset", ({ payload }) =>
      Effect.gen(function* () {
        const repository = yield* Repository;
        const moved = yield* repository
          .setRef({
            name: payload.ref,
            to: payload.to,
            ...(payload.expected === undefined ? {} : { expected: payload.expected as Oid | null }),
          })
          .pipe(Effect.catchTag("StorageFailure", Effect.die));
        return moved;
      }),
    )
    .handle("merge", ({ payload }) =>
      Effect.gen(function* () {
        const repository = yield* Repository;
        const outcome = yield* repository
          .merge({
            ours: payload.ours,
            theirs: payload.theirs,
            author: signatureFrom(payload.author),
            ...(payload.message === undefined ? {} : { message: payload.message }),
            ...(payload.strategy === undefined ? {} : { strategy: payload.strategy }),
            ...(payload.into === undefined ? {} : { into: payload.into }),
            ...(payload.no_fast_forward === undefined
              ? {}
              : { noFastForward: payload.no_fast_forward }),
          })
          .pipe(Effect.catchTag("StorageFailure", Effect.die));
        return outcome;
      }),
    )
    .handle("cherry-pick", ({ payload }) =>
      cherryPick({
        commit: payload.commit,
        onto: payload.onto,
        ...(payload.author === undefined ? {} : { author: signatureFrom(payload.author) }),
        ...(payload.into === undefined ? {} : { into: payload.into }),
      }).pipe(Effect.catchTag("StorageFailure", Effect.die)),
    )
    .handle("rebase", ({ payload }) =>
      rebase({
        branch: payload.branch,
        onto: payload.onto,
        ...(payload.into === undefined ? {} : { into: payload.into }),
      }).pipe(Effect.catchTag("StorageFailure", Effect.die)),
    )
    .handle("diff", ({ payload }) =>
      Effect.gen(function* () {
        const repository = yield* Repository;

        const before = yield* treeOfRef(repository, payload.from);
        const after = yield* treeOfRef(repository, payload.to);

        const listing = (tree: Oid) =>
          repository.listFiles(tree).pipe(
            Effect.catchTag("StorageFailure", Effect.die),
            Effect.map((files) => new Map(files.map((file) => [file.path, file]))),
          );
        const from = yield* listing(before);
        const to = yield* listing(after);

        const paths = [...new Set([...from.keys(), ...to.keys()])]
          .filter((path) => payload.path === undefined || path.startsWith(payload.path))
          .sort();

        const files = yield* Effect.forEach(paths, (path) =>
          Effect.gen(function* () {
            const old = from.get(path);
            const now = to.get(path);
            if (old?.oid === now?.oid) return null;

            const read = (oid: Oid | undefined) =>
              oid === undefined
                ? Effect.succeed(new Uint8Array(0))
                : repository.readBlob(oid).pipe(Effect.catchTag("StorageFailure", Effect.die));

            const oldBytes = yield* read(old?.oid);
            const newBytes = yield* read(now?.oid);
            const status = old === undefined ? "added" : now === undefined ? "removed" : "modified";

            // A binary patch would be noise; git says "differ" and so do we.
            if (isBinary(oldBytes) || isBinary(newBytes)) {
              return { path, status, binary: true, patch: "" } as const;
            }

            return {
              path,
              status,
              binary: false,
              patch: unified(decoder.decode(oldBytes), decoder.decode(newBytes), {
                beforeName: path,
                afterName: path,
                ...(payload.context === undefined ? {} : { context: payload.context }),
              }),
            } as const;
          }),
        );

        return { files: files.filter((file) => file !== null) };
      }),
    )
    .handle("files", ({ query }) =>
      Effect.gen(function* () {
        const repository = yield* Repository;
        const tree = yield* treeOfRef(repository, query.ref);
        const root =
          query.path === undefined || query.path === ""
            ? tree
            : yield* subtreeAt(repository, tree, query.path);
        const files = yield* repository
          .listFiles(root, query.path === undefined ? {} : { prefix: query.path })
          .pipe(Effect.catchTag("StorageFailure", Effect.die));
        return { files };
      }),
    )
    .handle("file", ({ query }) =>
      Effect.gen(function* () {
        const repository = yield* Repository;
        const tree = yield* treeOfRef(repository, query.ref);
        const entry = yield* repository
          .findPath(tree, query.path)
          .pipe(Effect.catchTag("StorageFailure", Effect.die));
        if (entry === null || entry.mode === "40000") {
          return yield* new ObjectNotFound({ oid: query.path });
        }
        const data = yield* repository
          .readBlob(entry.oid)
          .pipe(Effect.catchTag("StorageFailure", Effect.die));
        return {
          path: query.path,
          mode: entry.mode,
          oid: entry.oid,
          content: toBase64(data),
          encoding: "base64" as const,
          size: data.length,
        };
      }),
    )
    .handle("object", ({ params }) =>
      Effect.gen(function* () {
        const repository = yield* Repository;
        const object = yield* repository
          .readObject(params.oid as Oid)
          .pipe(Effect.catchTag("StorageFailure", Effect.die));
        return {
          oid: params.oid,
          type: object.type,
          size: object.data.length,
          content: toBase64(object.data),
          encoding: "base64" as const,
        };
      }),
    )
    .handle("reflog", ({ query }) =>
      Effect.gen(function* () {
        const repository = yield* Repository;
        const entries = yield* repository
          .reflog(query.ref)
          .pipe(Effect.catchTag("StorageFailure", Effect.die));
        return {
          entries: entries.map((entry) => ({
            from: entry.from,
            to: entry.to,
            at: entry.at.toISOString(),
            message: entry.message,
          })),
        };
      }),
    )
    .handle("grep", ({ payload }) =>
      Effect.gen(function* () {
        const repository = yield* Repository;

        const test = yield* matcher(payload);
        const tree = yield* treeOfRef(repository, payload.ref);
        const files = yield* repository
          .listFiles(tree)
          .pipe(Effect.catchTag("StorageFailure", Effect.die));

        const limit = payload.max_matches ?? 200;
        const matches: Array<{ path: string; line: number; text: string }> = [];
        let truncated = false;

        for (const file of files) {
          if (matches.length >= limit) {
            truncated = true;
            break;
          }
          if (payload.path !== undefined && !file.path.startsWith(payload.path)) continue;

          const data = yield* repository
            .readBlob(file.oid)
            .pipe(Effect.catchTag("StorageFailure", Effect.die));
          // A binary file has no lines worth reporting, and git skips them
          // for the same reason.
          if (data.subarray(0, 8000).includes(0)) continue;

          const lines = new TextDecoder().decode(data).split("\n");
          for (let index = 0; index < lines.length; index++) {
            const text = lines[index]!;
            if (!test(text)) continue;
            matches.push({ path: file.path, line: index + 1, text });
            if (matches.length >= limit) {
              truncated = true;
              break;
            }
          }
        }

        return { matches, truncated };
      }),
    )
    .handle("gc", ({ payload }) =>
      Effect.gen(function* () {
        const repository = yield* Repository;
        const report = yield* repository
          .gc({
            ...(payload.dry_run === undefined ? {} : { dryRun: payload.dry_run }),
            ...(payload.repack === undefined ? {} : { repack: payload.repack }),
          })
          .pipe(Effect.catchTag("StorageFailure", Effect.die));
        return {
          scanned: report.scanned,
          reachable: report.reachable,
          removed: report.removed,
          packed: report.packed ?? null,
        };
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
