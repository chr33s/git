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

import { lsRemote } from "../client/Fetch.ts";
import { push as pushToRemote } from "../client/Push.ts";
import { isBinary, unified } from "../git/Diff.ts";
import { Invalid, ObjectNotFound, PackCorrupt, RefConflict } from "../git/Error.ts";
import { EMPTY_TREE_OID, type Signature } from "../git/Format.ts";
import { FLUSH, pkt, PktReader } from "../git/Pkt.ts";
import { cherryPick, rebase } from "../git/Rebase.ts";
import { Repository } from "../git/Repository.ts";
import { isOid, type Oid } from "../git/Store.ts";
import {
  NewRemoteWire,
  none as noRemotes,
  redact as redactRemote,
  Remotes,
  validate as validateRemote,
} from "./Remotes.ts";
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

/** A registered remote as a client may see it: no credential, ever. */
const RemoteWire = Schema.Struct({
  name: Schema.String,
  url: Schema.String,
  has_credential: Schema.Boolean,
  created_at: Schema.String,
});

/**
 * Which remote an operation acts on: a stored `name`, or a `url` outright.
 * Exactly one — a request that gives both has not said which credential it
 * expects to be used.
 */
const RemoteTarget = {
  name: Schema.optional(Schema.String),
  url: Schema.optional(Schema.String),
};

/** A ref after a fetch moved it, and where it was before. */
const FetchedRef = Schema.Struct({
  name: Schema.String,
  oid: OidString,
  from: Schema.NullOr(OidString),
});

/**
 * The remote a request names: one this repository stores, credential
 * included, or a URL taken as it stands. A URL carries no credential — one in
 * a request body is one in an access log — so an authenticated remote is one
 * that has been registered.
 */
const remoteFor = Effect.fn("Api.remoteFor")(function* (payload: {
  readonly name?: string | undefined;
  readonly url?: string | undefined;
}) {
  const { name, url } = payload;
  if (name !== undefined && url !== undefined) {
    return yield* new Invalid({
      field: "remote",
      reason: "name a stored remote or give a url, not both",
    });
  }
  if (url !== undefined) {
    // `origin` is what git calls the remote you cloned from, and a URL used
    // without a stored name still has to track somewhere.
    const checked = yield* validateRemote({ name: "origin", url });
    return { name: checked.name, url: checked.url, credential: null };
  }
  if (name === undefined) {
    return yield* new Invalid({
      field: "remote",
      reason: "give a stored remote 'name' or a 'url'",
    });
  }

  const remotes = yield* Remotes;
  const stored = yield* remotes.get(name).pipe(Effect.catchTag("StorageFailure", Effect.die));
  if (stored === null) {
    return yield* new Invalid({ field: "name", reason: `unknown remote '${name}'` });
  }
  return { name: stored.name, url: stored.url, credential: stored.credential };
});

/**
 * Which remote refs a request asked for. An entry is a full ref name
 * (`refs/heads/main`), its short form (`main`, `v1.0`), or a prefix with a
 * trailing `*`. Absent means every branch and tag the remote advertises.
 */
const selects = (filter: ReadonlyArray<string> | undefined, name: string): boolean => {
  if (filter === undefined) return true;
  const short = name.replace(/^refs\/(?:heads|tags)\//, "");
  return filter.some((entry) =>
    entry.endsWith("*") ? name.startsWith(entry.slice(0, -1)) : entry === name || entry === short,
  );
};

/**
 * Where a fetched ref lands. A branch becomes a remote-tracking ref, so a
 * fetch never moves a local branch — `pull` is the endpoint that does that,
 * and only once it can say the move is a fast-forward. A tag keeps its own
 * name, because a tag is not per-remote.
 */
const trackingOf = (remote: string, name: string): string =>
  name.startsWith("refs/heads/")
    ? `refs/remotes/${remote}/${name.slice("refs/heads/".length)}`
    : name;

const concat = (parts: ReadonlyArray<Uint8Array>): Uint8Array<ArrayBuffer> => {
  const out = new Uint8Array(parts.reduce((sum, part) => sum + part.length, 0));
  let offset = 0;
  for (const part of parts) {
    out.set(part, offset);
    offset += part.length;
  }
  return out;
};

/**
 * One `want … done` round against upload-pack, its prelude consumed.
 *
 * Everything in a single request, so the reply is the boundary section (only
 * when deepening), one ACK or NAK, and then the pack — no second round to
 * hold state for. The `have` lines are this repository's own tips, which is
 * what keeps a second fetch from re-downloading the whole history.
 */
const uploadPack = (input: {
  readonly url: string;
  readonly token: string | undefined;
  readonly wants: ReadonlyArray<Oid>;
  readonly haves: ReadonlyArray<Oid>;
  readonly depth: number | undefined;
}) =>
  Effect.tryPromise({
    try: async () => {
      const body = concat([
        ...input.wants.map((oid) => pkt(`want ${oid}\n`)),
        ...(input.depth === undefined ? [] : [pkt(`deepen ${input.depth}\n`)]),
        FLUSH,
        ...input.haves.map((oid) => pkt(`have ${oid}\n`)),
        pkt("done\n"),
      ]);

      const response = await fetch(`${input.url}/git-upload-pack`, {
        method: "POST",
        headers: {
          "content-type": "application/x-git-upload-pack-request",
          ...(input.token === undefined ? {} : { authorization: `Bearer ${input.token}` }),
        },
        body,
      });
      if (!response.ok || response.body === null) {
        throw new Error(`upload-pack returned ${response.status}`);
      }

      const reader = new PktReader(response.body as unknown as AsyncIterable<Uint8Array>);
      for (;;) {
        const item = await reader.next();
        if (item === "eof") throw new Error("upload-pack answered with no pack");
        // The flush that closes the boundary section, and the `shallow` lines
        // before it: nothing to record here, because these stores keep no
        // shallow list — see `fetchFrom` on what that costs.
        if (typeof item === "string") continue;
        const line = decoder.decode(item);
        if (line.startsWith("ACK") || line.startsWith("NAK")) break;
      }
      return reader.rest();
    },
    catch: (cause) => new Invalid({ field: "remote", reason: String(cause) }),
  });

/**
 * A fetch into this repository: advertisement, one pack, then the tracking
 * refs.
 *
 * `Client.fetchRepository` writes through an `ObjectStore` and `RefStore` it
 * is handed; this layer carries `Repository` and not the stores underneath
 * it, so the pack goes in through `Repository.unpack` — receive-pack's own
 * ingest, and the reason this can report how many objects arrived rather than
 * only which refs moved.
 *
 * `depth` is passed through as `deepen` and nothing more: the boundary
 * commits' parents stay on the remote and there is no shallow list in these
 * stores to record that in, so a depth-limited fetch leaves commits whose
 * parents are absent — which `fsck` will report. It is here so a caller after
 * the last few commits of a large history need not take all of it; it is not
 * an equivalent of `git clone --depth`.
 */
const fetchFrom = Effect.fn("Api.fetchFrom")(function* (input: {
  readonly remote: string;
  readonly url: string;
  readonly credential: string | null;
  readonly refs?: ReadonlyArray<string> | undefined;
  readonly depth?: number | undefined;
}) {
  const repository = yield* Repository;
  const token = input.credential ?? undefined;

  if (input.depth !== undefined && (!Number.isInteger(input.depth) || input.depth < 1)) {
    return yield* new Invalid({
      field: "depth",
      reason: `depth must be a positive integer, not '${input.depth}'`,
    });
  }

  const advertised = yield* lsRemote(input.url, { token });
  const local = new Map(yield* repository.refs);

  const wanted = advertised
    .filter(
      (ref) =>
        // `refs/tags/v1^{}` is the tag's target, not a ref to hold, and
        // `HEAD` is a symbolic ref this repository has one of already.
        !ref.name.endsWith("^{}") &&
        (ref.name.startsWith("refs/heads/") || ref.name.startsWith("refs/tags/")) &&
        selects(input.refs, ref.name),
    )
    .map((ref) => ({ name: trackingOf(input.remote, ref.name), oid: ref.oid }))
    .filter(
      (ref) =>
        local.get(ref.name) !== ref.oid &&
        // A tag is a name that does not move: re-pointing one on a fetch
        // would rewrite what this repository has already published under it.
        !(ref.name.startsWith("refs/tags/") && local.has(ref.name)),
    );

  if (wanted.length === 0) return { refs: [], objects: 0 };

  const wants: Array<Oid> = [];
  for (const oid of new Set(wanted.map((ref) => ref.oid))) {
    if (!(yield* repository.contains(oid))) wants.push(oid);
  }

  // Every wanted object is already here — a branch that was fetched under
  // another name, or a ref moved back to where it was. There is nothing to
  // ask for, and an empty `want` list is a request the server rejects.
  const arrived =
    wants.length === 0
      ? []
      : yield* repository.unpack(
          Stream.fromAsyncIterable(
            yield* uploadPack({
              url: input.url,
              token,
              wants,
              haves: [...new Set(local.values())],
              depth: input.depth,
            }),
            (cause) => new Invalid({ field: "remote", reason: String(cause) }),
          ),
        );

  const refs = yield* Effect.forEach(wanted, (ref) =>
    repository
      .setRef({ name: ref.name, to: ref.oid })
      .pipe(Effect.map((moved) => ({ name: moved.ref, oid: moved.oid, from: moved.previous }))),
  );

  return { refs, objects: arrived.length };
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

/**
 * This repository as a client of another one.
 *
 * A group of its own because these are the only endpoints that need the
 * remote registry, and because they are the only ones that leave the
 * process: everything in `repo` is answered from local storage, while every
 * one of these makes a request to a server it does not control and reports
 * what came back.
 */
const remotes = HttpApiGroup.make("remotes")
  .add(
    // Registration, because a fetch that has to be handed a URL and a token
    // every time is a fetch nothing can schedule. The credential goes in and
    // never comes back out.
    HttpApiEndpoint.post("remoteAdd", "/remotes", {
      params: RepoParam,
      payload: NewRemoteWire,
      success: RemoteWire,
      error: Invalid,
    }),
  )
  .add(
    HttpApiEndpoint.get("remoteList", "/remotes", {
      params: RepoParam,
      success: Schema.Struct({ remotes: Schema.Array(RemoteWire) }),
    }),
  )
  .add(
    HttpApiEndpoint.delete("remoteRemove", "/remotes/:name", {
      params: { ...RepoParam, name: Schema.String },
      success: Schema.Struct({ deleted: Schema.Boolean }),
    }),
  )
  .add(
    HttpApiEndpoint.post("fetch", "/fetch", {
      params: RepoParam,
      payload: Schema.Struct({
        ...RemoteTarget,
        /** Ref names, short names, or `prefix*`; absent takes everything. */
        refs: Schema.optional(Schema.Array(Schema.String)),
        depth: Schema.optional(Schema.Finite),
      }),
      success: Schema.Struct({
        /** The namespace the branches landed in: `refs/remotes/<remote>/…`. */
        remote: Schema.String,
        refs: Schema.Array(FetchedRef),
        objects: Schema.Finite,
      }),
      error: [RefConflict, PackCorrupt, ObjectNotFound, Invalid],
    }),
  )
  .add(
    HttpApiEndpoint.post("push", "/push", {
      params: RepoParam,
      payload: Schema.Struct({
        ...RemoteTarget,
        refs: Schema.Array(
          Schema.Struct({
            /** A local ref or an oid. */
            local: Schema.String,
            /** Where it lands; absent means the same name. */
            remote: Schema.optional(Schema.String),
            delete: Schema.optional(Schema.Boolean),
          }),
        ),
        force: Schema.optional(Schema.Boolean),
        atomic: Schema.optional(Schema.Boolean),
      }),
      /**
       * Every requested ref gets a line, and a rejection is a value: a push
       * of five branches where one lost a race is four successes.
       */
      success: Schema.Struct({
        refs: Schema.Array(
          Schema.Struct({
            ref: Schema.String,
            ok: Schema.Boolean,
            reason: Schema.NullOr(Schema.String),
          }),
        ),
      }),
      error: [PackCorrupt, ObjectNotFound, Invalid],
    }),
  )
  .add(
    HttpApiEndpoint.post("pull", "/pull", {
      params: RepoParam,
      payload: Schema.Struct({
        ...RemoteTarget,
        /** A branch name or a full `refs/heads/…`. */
        branch: Schema.String,
        depth: Schema.optional(Schema.Finite),
      }),
      /**
       * `non-fast-forward` is the outcome this endpoint exists to be able to
       * report: the tracking ref moved, the branch did not, and which of a
       * merge or a rebase was wanted is not something a pull can guess.
       */
      success: Schema.Struct({
        kind: Schema.Literals(["up-to-date", "created", "fast-forward", "non-fast-forward"]),
        branch: Schema.String,
        tracking: Schema.String,
        /** Where the branch was; `null` when it did not exist. */
        from: Schema.NullOr(OidString),
        /** What the remote had — where the branch is now, unless it diverged. */
        to: OidString,
        objects: Schema.Finite,
      }),
      error: [RefConflict, PackCorrupt, ObjectNotFound, Invalid],
    }),
  )
  .prefix("/:repo");

export const api = HttpApi.make("git").add(repo).add(remotes);

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

export const remoteHandlers = HttpApiBuilder.group(api, "remotes", (group) =>
  group
    .handle("remoteAdd", ({ payload }) =>
      Effect.gen(function* () {
        const registry = yield* Remotes;
        const added = yield* registry
          .add(payload)
          .pipe(Effect.catchTag("StorageFailure", Effect.die));
        return redactRemote(added);
      }),
    )
    .handle("remoteList", () =>
      Effect.gen(function* () {
        const registry = yield* Remotes;
        const rows = yield* registry.list.pipe(Effect.catchTag("StorageFailure", Effect.die));
        return { remotes: rows.map(redactRemote) };
      }),
    )
    .handle("remoteRemove", ({ params }) =>
      Effect.gen(function* () {
        const registry = yield* Remotes;
        const deleted = yield* registry
          .remove(params.name)
          .pipe(Effect.catchTag("StorageFailure", Effect.die));
        return { deleted };
      }),
    )
    .handle("fetch", ({ payload }) =>
      Effect.gen(function* () {
        const target = yield* remoteFor(payload);
        const fetched = yield* fetchFrom({
          remote: target.name,
          url: target.url,
          credential: target.credential,
          ...(payload.refs === undefined ? {} : { refs: payload.refs }),
          ...(payload.depth === undefined ? {} : { depth: payload.depth }),
        });
        return { remote: target.name, refs: fetched.refs, objects: fetched.objects };
      }).pipe(Effect.catchTag("StorageFailure", Effect.die)),
    )
    .handle("push", ({ payload }) =>
      Effect.gen(function* () {
        const target = yield* remoteFor(payload);
        const results = yield* pushToRemote({
          url: target.url,
          refs: payload.refs.map((ref) => ({
            local: ref.local,
            remote: ref.remote ?? ref.local,
            ...(ref.delete === undefined ? {} : { delete: ref.delete }),
          })),
          ...(target.credential === null ? {} : { token: target.credential }),
          ...(payload.force === undefined ? {} : { force: payload.force }),
          ...(payload.atomic === undefined ? {} : { atomic: payload.atomic }),
        });
        return {
          refs: results.map((result) => ({
            ref: result.ref,
            ok: result.ok,
            reason: result.reason ?? null,
          })),
        };
      }).pipe(Effect.catchTag("StorageFailure", Effect.die)),
    )
    .handle("pull", ({ payload }) =>
      Effect.gen(function* () {
        const repository = yield* Repository;
        const target = yield* remoteFor(payload);

        const short = payload.branch.startsWith("refs/heads/")
          ? payload.branch.slice("refs/heads/".length)
          : payload.branch;
        const branch = `refs/heads/${short}`;
        const tracking = `refs/remotes/${target.name}/${short}`;

        const fetched = yield* fetchFrom({
          remote: target.name,
          url: target.url,
          credential: target.credential,
          refs: [`refs/heads/${short}`],
          ...(payload.depth === undefined ? {} : { depth: payload.depth }),
        });

        // Absent from the fetch's own report means the tracking ref was
        // already where the remote is, not that the remote has no such branch.
        const moved = fetched.refs.find((ref) => ref.name === tracking);
        const to = moved?.oid ?? (yield* repository.resolve(tracking));
        if (to === null) {
          return yield* new Invalid({
            field: "branch",
            reason: `remote has no branch '${short}'`,
          });
        }

        const from = yield* repository.resolve(branch);
        const outcome = { branch, tracking, from, to, objects: fetched.objects };

        if (from === null) {
          yield* repository.setRef({ name: branch, to, expected: null });
          return { kind: "created" as const, ...outcome };
        }
        if (from === to) return { kind: "up-to-date" as const, ...outcome };
        // The remote is behind this branch: there is nothing to bring in, and
        // moving the branch back would drop commits only this side has.
        if (yield* repository.isAncestor(to, from)) {
          return { kind: "up-to-date" as const, ...outcome };
        }
        /**
         * Diverged. Reported rather than merged: `/merge` and `/rebase` are
         * where a caller says which one it meant, and both of them can start
         * from `tracking`, which this pull has already moved. Guessing here
         * would write a merge commit nobody asked for into a branch.
         */
        if (!(yield* repository.isAncestor(from, to))) {
          return { kind: "non-fast-forward" as const, ...outcome };
        }

        // A compare-and-swap, because the fetch above was not instantaneous
        // and this branch is one a push can move.
        yield* repository.setRef({ name: branch, to, expected: from });
        return { kind: "fast-forward" as const, ...outcome };
      }).pipe(Effect.catchTag("StorageFailure", Effect.die)),
    ),
);

/**
 * The API as one layer: routes registered, handlers wired, response plumbing
 * (etag, platform) satisfied from core with no filesystem underneath — a
 * Worker has none, and nothing here serves files.
 *
 * The remote registry is a parameter rather than a requirement of the layer
 * because a server without one is still a whole server: `Remotes.none`
 * refuses to store a remote, and fetch and push against an explicit `url` go
 * on working. A host that has a registry passes it here — that is the only
 * way in, since the handlers must be given theirs before the router is built.
 */
export const layerWith = (registry: Layer.Layer<Remotes> = noRemotes) =>
  HttpApiBuilder.layer(api).pipe(
    Layer.provide(handlers),
    Layer.provide(remoteHandlers),
    Layer.provide(HttpPlatform.layer),
    Layer.provide(Etag.layerWeak),
    Layer.provide(FileSystem.layerNoop({})),
    Layer.provide(Path.layer),
    // Merged rather than provided: a handler's own requirements are
    // request-scoped, and `HttpRouter.toWebHandler` resolves them from what
    // the app layer *outputs* — which is why every host merges `Repository`
    // in the same way.
    Layer.provideMerge(registry),
  );

export const layer = layerWith();
