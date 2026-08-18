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
 *
 * The file is long because the declaration is one value on purpose: the
 * derived client and the OpenAPI surface both hang off `api`, and splitting
 * the groups across modules would trade a scrollbar for a cycle between the
 * declaration and its handlers. Everything that is not declaration or a thin
 * handler lives elsewhere — the server-as-client machinery in `Sync.ts`, the
 * algorithms in `git/`.
 */
import { Effect, FileSystem, Layer, Option, Path, Schema, Stream } from "effect";
import { Etag, HttpPlatform } from "effect/unstable/http";
import { HttpApi, HttpApiBuilder, HttpApiEndpoint, HttpApiGroup } from "effect/unstable/httpapi";

import { push as pushToRemote, type PushRef } from "../client/Push.ts";
import { isBinary, unified } from "../git/Diff.ts";
import { Invalid, ObjectNotFound, PackCorrupt, RefConflict } from "../git/Error.ts";
import { EMPTY_TREE_OID, isGitlink, isTree, type Signature } from "../git/Format.ts";
import { next as bisectNext } from "../git/Bisect.ts";
import { forPath as pathHistory } from "../git/History.ts";
import { type Strategy as MergeStrategy } from "../git/Merge.ts";
import { cherryPick, rebase } from "../git/Rebase.ts";
import * as Redaction from "../hub/Redaction.ts";
import { type FileChange, Repository, treeAt } from "../git/Repository.ts";
import * as Policy from "./Policy.ts";
import * as Auth from "./Auth.ts";
import { permits } from "../trust/Certificate.ts";
import { readGenesis } from "../trust/Genesis.ts";
import { isOid, type Oid, type RefUpdate } from "../git/Store.ts";
import { NewRemoteWire, redact as redactRemote, Remotes } from "./Remotes.ts";

/** `NewRemote` under construction: built field by field, handed over as one. */
interface RemoteRequest {
  name: string;
  url: string;
  credential?: string;
  sync?: { mode: "manual" | "fetch" | "push" | "mirror"; refs: ReadonlyArray<string> };
}
import { NewSubscriberWire, redact, Subscribers } from "./Subscribers.ts";
import { fetchFrom, pull, remoteFor } from "./Sync.ts";

/**
 * An oid on the wire, decoded to the domain's branded `Oid` outright: the
 * refinement carries `isOid`'s type predicate, so a validated payload needs
 * no `as Oid` at the use sites — the schema is the one place the brand is
 * earned.
 */
const OidString = Schema.String.pipe(Schema.refine(isOid));

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

/**
 * A branch name as a ref, for the payloads that accept either spelling.
 *
 * `HEAD` is left alone: it is already a full ref name, and qualifying it
 * produced a literal branch called `refs/heads/HEAD` — so a merge `into:
 * "HEAD"` reported success while the checked-out branch never moved. Left as
 * it is, it reaches the ref-name check that refuses it, which is the clean
 * failure it had before.
 */
const refNameOf = (value: string): string =>
  value === "HEAD" || value.startsWith("refs/") ? value : `refs/heads/${value}`;

/**
 * The same, for a destination a caller may only name as a branch.
 *
 * An object id is not a destination: nothing moves it, and qualifying one made
 * `into: "<40 hex>"` create a branch *named after the object id* — silently,
 * and reported as success. Refused instead, which is the answer the caller can
 * act on. `Repository.merge` still resolves an oid `into` for its own callers;
 * what is closed here is the door that let a request spell one.
 */
const branchRefOf = Effect.fn("Api.branchRefOf")(function* (value: string) {
  if (isOid(value)) {
    return yield* new Invalid({
      field: "into",
      reason: "a destination is a branch, not an object id",
    });
  }
  return refNameOf(value);
});

const gateWrite = Effect.fn("Api.gateWrite")(function* (ref: string, rewrites = false) {
  // Fail closed: a policy that cannot be evaluated refuses the write rather
  // than allowing it. The alternative is a repository whose protection turns
  // itself off the moment its own trust state cannot be read.
  const refusal = yield* Policy.gateWrite(ref, rewrites).pipe(
    Effect.orElseSucceed(() => "the repository's policy could not be evaluated"),
  );
  if (refusal !== null) return yield* new Invalid({ field: "ref", reason: refusal });
});

/**
 * Judge one ref change and hand back the update to apply.
 *
 * The returned update carries the value the decision was made against, so the
 * write goes out under exactly that condition — deciding on one state and
 * writing against another is the race this boundary exists to close.
 *
 * `bindEnvelope: false`: an envelope describes a *push's* ref commands, and
 * this is not that conversation. Holding a JSON verb to commands it never
 * claimed would read silence as a denial.
 */
/**
 * The requester may read this repository.
 *
 * Every ordinary read is charged `repo.read` by the guard before a handler
 * runs. The exceptions are the verbs whose *primary* effect is a write but
 * whose work involves sending content elsewhere, which the guard charges
 * `source.push` — a capability that does not carry `repo.read`, deliberately,
 * because a contributor who may push need not be able to read everything.
 */
const requireRead = Effect.fn("Api.requireRead")(function* () {
  // Fail closed. Reading an unreadable genesis as "not hub-enabled" would let
  // the copy through at the moment storage was least trustworthy — the exact
  // fail-open `Auth.authenticate` and `Policy.gate` each refuse.
  const stored = yield* readGenesis().pipe(
    Effect.mapError(
      () => new Invalid({ field: "repo", reason: "the repository's identity could not be read" }),
    ),
  );
  if (stored === null) return;

  // No requester at all is a host that did not provide one, not an anonymous
  // request — and `Auth.anonymous` carries the *empty* projection, which has
  // no members and so reads as a repository anonymous readers may clone. Left
  // to the fallback, a hub-enabled repository that restricts reads answered
  // `POST /push` to anybody the moment the context was missing, which is the
  // fail-open this function's own comment says it refuses.
  const requester = yield* Effect.serviceOption(Auth.Requester);
  if (Option.isNone(requester)) {
    return yield* new Invalid({
      field: "capability",
      reason: "sending this repository's objects elsewhere needs repo.read",
    });
  }
  const who = requester.value;
  if (permits(who.capabilities, "repo.read")) return;

  // A repository the world may already clone has nothing to protect here: a
  // member scoped to `source.push` could copy it out by cloning it themselves,
  // and refusing them the verb would only be theatre.
  if (Auth.anonymousReadAllowed(who.projection)) return;

  return yield* new Invalid({
    field: "capability",
    reason: "sending this repository's objects elsewhere needs repo.read",
  });
});

/**
 * The requester holds a capability, for verbs the ref gates never see.
 *
 * A webhook or a remote is a *destination* this repository will send to, and
 * `gc` is an irreversible rewrite of the object store. None of them moves a
 * ref, so `Policy.gateWrite` has nothing to judge — and the guard's charge is
 * deliberately coarse there, `source.push` *or* `source.delete`, because it
 * cannot see a push's commands. Left at that, a bot scoped to delete a branch
 * could register a webhook receiving every push, or collect the repository.
 *
 * Fails closed on a missing requester for the reason `requireRead` gives: an
 * absent context is a host that did not say who is asking.
 */
const requireCapability = Effect.fn("Api.requireCapability")(function* (capability: string) {
  // A repository with no genesis has no membership to charge anything against.
  // The guard is no help here either: it lets every *read* through on such a
  // repository, exactly as a plain git repository has always done — and these
  // verbs are not reads whatever their method says. Left at "no genesis, no
  // charge", a plain repository served read-only handed its webhook delivery
  // URLs and every remote it pushes to, to anybody who could reach it.
  //
  // So the host's own decision stands in for the membership there is none of:
  // a repository opened to anonymous writes is one whose operator has said
  // anybody may administer it, and one that was not is closed. That keeps
  // `serve --open` working and stops a repository nobody opened from being
  // administered — or read — by strangers.
  const stored = yield* readGenesis().pipe(
    Effect.mapError(
      () => new Invalid({ field: "repo", reason: "the repository's identity could not be read" }),
    ),
  );
  if (stored === null) {
    const open = yield* Effect.serviceOption(Auth.AnonymousWrites);
    if (Option.getOrElse(open, () => false)) return;
    return yield* new Invalid({
      field: "capability",
      reason: `this repository has no membership to authorize ${capability}; run \`hub init\``,
    });
  }

  const requester = yield* Effect.serviceOption(Auth.Requester);
  if (Option.isSome(requester) && permits(requester.value.capabilities, capability)) return;
  return yield* new Invalid({ field: "capability", reason: `this needs ${capability}` });
});

/** Whether a ref is there to be rewritten; a create discards nothing. */
/**
 * Whether landing on `into` would drop commits it already holds.
 *
 * Asked before the work, which is why it asks about the *bases* rather than
 * about the result: a replay lands on top of `onto`, and a merge commit holds
 * both of its sides, so a destination either side already reaches is one the
 * write contains. Charged on whether `into` merely exists instead, an ordinary
 * fast-forward — `{onto: "main", into: "main"}` — was refused to a member
 * holding `source.push`, and comparing tips by oid missed the case where a
 * side reaches the destination without being it.
 */
export const discards = Effect.fn("Api.discards")(function* (
  into: string,
  bases: ReadonlyArray<string>,
) {
  const repository = yield* Repository;
  // Resolved exactly as `Repository.merge` resolves them: an oid as itself,
  // anything else through the ref store, which follows symrefs. Qualifying
  // first answered `null` for `HEAD`, and a `null` side is a side nothing
  // matches — so the write was charged a rewrite for the one spelling git
  // itself uses most.
  const at = (revision: string) =>
    isOid(revision)
      ? Effect.succeed(revision)
      : repository.resolve(revision).pipe(Effect.catchTag("StorageFailure", Effect.die));

  // Reported as well as judged. The verdict is about the value `into` held at
  // this instant, and the write happens after a merge or a replay that can
  // take as long as the history is deep — so the write has to swap against
  // *this* value, or a push landing in the window turns a write judged a
  // fast-forward into one that drops commits, which is `source.force-push`'s
  // to allow and not this caller's.
  //
  // Two readings of "what `into` is now", and they differ for a symbolic ref.
  // Reachability wants the commit it resolves to; the compare-and-swap wants
  // exactly what the store will compare against, which is the ref's own value
  // — `null` for a symref, and nothing at all when the destination was spelled
  // as an oid. Handing the resolved oid over as the swap names a value nobody
  // wrote, so every write to a symbolic destination failed as a conflict for
  // good. `Policy.evaluate` splits the same two readings for the same reason.
  const swap = isOid(into)
    ? undefined
    : yield* repository.readRef(into).pipe(Effect.catchTag("StorageFailure", Effect.die));
  const tip = yield* at(into);
  // A destination that does not exist yet holds nothing a write could discard.
  if (tip === null) return { rewrites: false, swap } as const;
  for (const base of bases) {
    const oid = yield* at(base);
    // A base this repository cannot resolve is not evidence of a rewrite. The
    // verb is about to fail on that revision anyway, and claiming a rewrite
    // here turns "unknown revision" into a `source.force-push` refusal — an
    // answer that is both wrong and only given to callers who lack that
    // capability, so the same request reports two different problems depending
    // on who asks it.
    if (oid === null) return { rewrites: false, swap } as const;
    if (oid === tip) return { rewrites: false, swap } as const;
    const reaches = yield* repository.isAncestor(tip, oid).pipe(
      Effect.catchTags({
        ObjectNotFound: () => Effect.succeed(false),
        StorageFailure: Effect.die,
      }),
    );
    if (reaches) return { rewrites: false, swap } as const;
  }
  return { rewrites: true, swap } as const;
});

const gateOne = Effect.fn("Api.gateOne")(function* (update: RefUpdate) {
  // Fail closed, for the reason `gateWrite` gives.
  const judged = yield* Policy.gate([update], true, false).pipe(Effect.orElseSucceed(() => null));
  if (judged === null) {
    return yield* new Invalid({
      field: "ref",
      reason: "the repository's policy could not be evaluated",
    });
  }

  const refusal = judged.refused.at(0);
  if (refusal !== undefined) {
    return yield* new Invalid({ field: "ref", reason: refusal.reason });
  }
  return judged.updates.at(0) ?? update;
});

const changesOf = (files: ReadonlyArray<(typeof FileWire)["Type"]>): ReadonlyArray<FileChange> =>
  files.map((file) => {
    const content = file.content === null ? null : decodeContent(file.content, file.encoding);
    return file.mode === undefined
      ? { path: file.path, content }
      : { path: file.path, content, mode: file.mode };
  });

/** `writeFiles`, with `base` threaded through only when the caller has one. */
const writeFilesOf = (
  repository: Repository["Service"],
  base: Oid | undefined,
  files: ReadonlyArray<(typeof FileWire)["Type"]>,
) => {
  const changes = changesOf(files);
  return base === undefined
    ? repository.writeFiles({ changes })
    : repository.writeFiles({ base, changes });
};

/**
 * A page's bounds, from query strings that are whatever a client sent.
 *
 * `Number.parseInt` alone answers `NaN` for `?limit=abc`, and `slice(0, NaN)`
 * is empty — so a repository with a hundred branches reports none, with
 * `has_more: false`, instead of an error. The cap is the other half: without
 * one, `?limit=1e9` asks a history walk to run to the end of the graph.
 */
const PAGE_MAX = 100;

/** The most matched lines one grep will hold, whatever the caller asks for. */
const GREP_MAX_MATCHES = 2_000;

/**
 * The largest file grep will scan.
 *
 * Bounding the matches bounds what is *kept*, not what is read: a repository
 * holding one 200 MB log had it read whole, decoded to a string of the same
 * size again, and split into an array of several hundred megabytes of line
 * objects — all before the first match was counted, and all inside a Durable
 * Object with 128 MiB. A file larger than this is reported as skipped rather
 * than scanned, which is also roughly what `git grep` does with one.
 */
const GREP_MAX_FILE_BYTES = 4 * 1024 * 1024;

/**
 * Whether a path is under a caller's prefix, on path boundaries.
 *
 * A bare `startsWith` makes `?path=src` match `src-generated/tmp.ts`, so a
 * scoped diff or grep silently reports files from a directory the caller
 * never named — and `?path=s` matches most of the repository.
 */
const under = (path: string, prefix: string | undefined): boolean => {
  if (prefix === undefined || prefix === "") return true;
  const trimmed = prefix.endsWith("/") ? prefix.slice(0, -1) : prefix;
  return path === trimmed || path.startsWith(`${trimmed}/`);
};
const CURSOR_MAX = 10_000;

const bounds = (query: { cursor?: string; limit?: string }) => {
  const whole = (value: string | undefined, fallback: number) => {
    if (value === undefined) return fallback;
    const parsed = Number.parseInt(value, 10);
    return Number.isInteger(parsed) && parsed >= 0 ? parsed : null;
  };

  const start = whole(query.cursor, 0);
  const size = whole(query.limit, 50);
  // The cursor is added to the walk limit, so capping only the page size
  // leaves `?cursor=999999999` asking for exactly the unbounded history walk
  // the cap exists to prevent. Paging deeper than this is a different query.
  if (start === null || size === null) return null;
  // Past the bound is the end of what this endpoint will page through: an
  // empty last page. Clamping the offset instead would hand the client the
  // same page again with `has_more`, which is a loop.
  //
  // The floor is the same argument from the other end: `?limit=0` advances the
  // cursor by nothing, so an empty page comes back with `has_more` and the
  // cursor the client just sent, and following it never terminates. One is the
  // smallest page that makes progress.
  return { size: Math.min(Math.max(size, 1), PAGE_MAX), start, beyond: start > CURSOR_MAX };
};

/** Cursors are opaque to clients; here they are simply an offset. */
const page = <A>(items: ReadonlyArray<A>, query: { cursor?: string; limit?: string }) => {
  const limits = bounds(query);
  // A cursor that is not a number, or is past the bound, addresses nothing:
  // an empty last page says so, where restarting at zero — or clamping —
  // would loop a client through the list again.
  if (limits === null || limits.beyond) return { items: [], next_cursor: null, has_more: false };
  const { size, start } = limits;
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
    readonly tree?: Oid | undefined;
    readonly files?: ReadonlyArray<(typeof FileWire)["Type"]> | undefined;
  },
) =>
  Effect.gen(function* () {
    if (payload.tree !== undefined) return { tree: payload.tree, from: undefined };
    if (payload.files === undefined) return { tree: EMPTY_TREE_OID, from: undefined };

    const ref = branch.startsWith("refs/") ? branch : `refs/heads/${branch}`;
    const tip = yield* repository.resolve(ref);
    const base = tip === null ? undefined : (yield* repository.readCommit(tip)).tree;

    const tree = yield* writeFilesOf(repository, base, payload.files);

    // The tip this tree was layered onto. `commit` re-reads the ref for its
    // parent, and the object writes above take long enough for another commit
    // to land in between — which would then be parented over and its files
    // silently reverted. `CommitPack.ts` pins the same value for the same
    // reason; a caller who named their own `expected` keeps it.
    return { tree, from: tip };
  });

/** The tree a ref names, defaulting to HEAD — what "at this revision" means. */
const treeOfRef = (repository: Repository["Service"], ref: string | undefined) =>
  Effect.gen(function* () {
    const name = ref === undefined || ref === "" ? "HEAD" : ref;
    const oid = isOid(name) ? name : yield* repository.resolve(name);
    if (oid === null) return yield* new Invalid({ field: "ref", reason: `unknown ref '${name}'` });
    return yield* treeAt(repository, oid);
  }).pipe(Effect.catchTag("StorageFailure", Effect.die));

const subtreeAt = (repository: Repository["Service"], tree: Oid, path: string) =>
  repository.findPath(tree, path).pipe(
    Effect.catchTag("StorageFailure", Effect.die),
    Effect.flatMap((entry) =>
      entry === null || !isTree(entry.mode)
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
      /**
       * A repeated group is the shape that backtracks.
       *
       * `(a+)+$`, `(a|a)+$` and `([ab])*c` all take exponential time on a run
       * of thirty characters, and this runs per line of every blob in the
       * tree on the one thread serving the repository. Enumerating the bad
       * forms is a losing game — an earlier version's list let `(a|a)+$`
       * straight through — so the rule is the general one: a group may not be
       * quantified. `foo|bar`, `[a-z]+` and `\d{3}` are unaffected.
       */
      /**
       * At most one repetition, and no groups.
       *
       * Catastrophic backtracking needs two repetitions that can match the
       * same text — `(a+)+`, `(a|a)+`, or plain `aa*aa*aa*b` with no
       * parenthesis at all. Two earlier versions of this guard tried to
       * enumerate the dangerous shapes and were wrong both times, so the rule
       * is the conservative one that admits no combination: a single
       * quantifier cannot pair with anything. `foo.*bar` and `\d{3}` pass;
       * anything needing more asks for `regex: false` and a literal search.
       */
      const quantifiers = payload.pattern.replace(/\\./g, "").match(/[*+?]|\{\d/g)?.length ?? 0;
      const grouped = /\((?!\?:)/.test(payload.pattern.replace(/\\./g, ""));
      if (quantifiers > 1 || grouped || payload.pattern.length > 200) {
        return Effect.fail(
          new Invalid({
            field: "pattern",
            reason:
              "this endpoint accepts at most one repetition and no groups, because more " +
              "can take unbounded time to match; use `regex: false` for a literal search",
          }),
        );
      }
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
  /** The standing instruction, or `null` for a remote nothing happens to. */
  sync: Schema.NullOr(Schema.Struct({ mode: Schema.String, refs: Schema.Array(Schema.String) })),
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
      // Writing objects is a write, and "you may not" is an answer this has
      // to be able to give.
      error: Invalid,
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
      // `Invalid` when the caller may not ask: a whole-store scan is charged
      // like the other maintenance verbs.
      error: [Invalid],
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
        /** Files too large to scan, named so the answer is not silently partial. */
        skipped: Schema.Array(Schema.String),
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
        /**
         * How long an object only the reflog still names is protected, in
         * milliseconds; `0` collects those too. Defaults to git's 90 days.
         */
        reflog_grace_ms: Schema.optional(Schema.Finite),
      }),
      success: Schema.Struct({
        scanned: Schema.Finite,
        reachable: Schema.Finite,
        removed: Schema.Array(OidString),
        /** Unreachable, but inside a pack: `repack` is what collects these. */
        retained: Schema.Array(OidString),
        packed: Schema.NullOr(Schema.Struct({ name: Schema.String, objects: Schema.Finite })),
        /**
         * Why a requested repack did not happen, when it did not. Without it a
         * fork that borrows through alternates gets `packed: null` and no way
         * to tell a refusal from a repository that had nothing to pack.
         */
        repack_skipped: Schema.NullOr(Schema.String),
      }),
      // `Invalid` when the repository lends its objects to a fork: refusing is
      // an answer the caller acts on, not a fault.
      error: [ObjectNotFound, Invalid],
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
      // `Invalid` when the caller may not ask: where a repository sends its
      // pushes is administrative, not public.
      error: Invalid,
    }),
  )
  .add(
    HttpApiEndpoint.delete("webhookRemove", "/webhooks/:id", {
      params: { ...RepoParam, id: Schema.String },
      success: Schema.Struct({ deleted: Schema.Boolean }),
      // Where this repository sends what it holds is administrative, and
      // "you may not" is an answer this has to be able to give.
      error: Invalid,
    }),
  )
  .add(
    HttpApiEndpoint.get("commits", "/commits/:oid", {
      params: { ...RepoParam, oid: OidString },
      query: Cursor,
      success: Page(Schema.Struct({ message: Schema.String, oid: OidString })),
      // `Invalid` because a cursor or a limit that is not a whole number is
      // the client's mistake, and answering an empty page would hide it.
      error: [ObjectNotFound, Invalid],
    }),
  )
  .add(
    HttpApiEndpoint.get("history", "/history/:oid", {
      params: { ...RepoParam, oid: OidString },
      /** `path` is the point of the endpoint, so it is not optional. */
      query: { ...Cursor, path: Schema.String },
      success: Page(
        Schema.Struct({
          oid: OidString,
          message: Schema.String,
          /** The path's blob here; `null` where the commit deleted it. */
          blob: Schema.NullOr(OidString),
        }),
      ),
      error: [ObjectNotFound, Invalid],
    }),
  )
  .add(
    HttpApiEndpoint.post("bisect", "/bisect", {
      params: RepoParam,
      /**
       * The caller keeps the marks and sends them back each time; there is no
       * session here, which is what lets a stateless server answer at all.
       */
      payload: Schema.Struct({
        bad: OidString,
        good: Schema.Array(OidString),
      }),
      success: Schema.Struct({
        kind: Schema.Literals(["test", "found"]),
        commit: OidString,
        remaining: Schema.Finite,
        steps: Schema.Finite,
      }),
      error: [ObjectNotFound, Invalid],
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
      // As `webhookList`: administrative, not public.
      error: Invalid,
    }),
  )
  .add(
    HttpApiEndpoint.delete("remoteRemove", "/remotes/:name", {
      params: { ...RepoParam, name: Schema.String },
      success: Schema.Struct({ deleted: Schema.Boolean }),
      error: Invalid,
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

/**
 * Wire payloads mark an omitted option with `undefined`, while the domain
 * contracts mark it by leaving the property out. These request types are the
 * mutable middle ground: a handler starts from the required fields and adds
 * an optional one only when the payload actually carried it, so the domain
 * never has to wonder whether `undefined` was said or merely implied.
 */
type CommitRequest = {
  branch: string;
  tree: Oid;
  message: string;
  author: Signature;
  expected?: Oid | null;
};

type TagRequest = {
  name: string;
  target: string;
  message?: string;
  tagger?: Signature;
  force?: boolean;
};

type SetRefRequest = {
  name: string;
  to: string;
  expected?: Oid | null;
};

type MergeRequest = {
  ours: string;
  theirs: string;
  author: Signature;
  message?: string;
  strategy?: MergeStrategy;
  into?: string;
  expected?: Oid | null;
  noFastForward?: boolean;
};

type CherryPickRequest = {
  commit: string;
  onto: string;
  author?: Signature;
  into?: string;
  expected?: Oid | null;
};

type RebaseRequest = {
  branch: string;
  onto: string;
  into?: string;
  expected?: Oid | null;
};

type PatchOptions = {
  beforeName: string;
  afterName: string;
  context?: number;
};

type GcRequest = {
  dryRun?: boolean;
  repack?: boolean;
  reflogGrace?: number;
  exclude?: ReadonlySet<Oid>;
};

type PushRequest = {
  url: string;
  refs: ReadonlyArray<PushRef>;
  token?: string;
  force?: boolean;
  atomic?: boolean;
};

/** `StorageFailure` is a defect here: a 500 no caller can act on. */
export const handlers = HttpApiBuilder.group(api, "repo", (group) =>
  group
    .handle("create", ({ payload }) =>
      Effect.gen(function* () {
        const repository = yield* Repository;
        const branch = payload.branch ?? "main";
        yield* gateWrite(branch.startsWith("refs/") ? branch : `refs/heads/${branch}`);

        const built = yield* treeFor(repository, branch, payload).pipe(
          Effect.catchTag("StorageFailure", Effect.die),
        );
        const tree = built.tree;

        const request: CommitRequest = {
          author: signatureFrom(payload.author),
          branch,
          message: payload.message ?? "",
          tree,
        };
        if (payload.expected !== undefined) request.expected = payload.expected;
        // Otherwise the tip the tree was layered onto, so a caller who named
        // no expectation still gets the one implied by the files they sent.
        else if (built.from !== undefined) request.expected = built.from;

        const oid = yield* repository
          .commit(request)
          .pipe(Effect.catchTag("StorageFailure", Effect.die));
        return { oid, tree };
      }),
    )
    .handle("blob", ({ payload }) =>
      Effect.gen(function* () {
        // Writing objects moves no ref, so the policy boundary never sees it
        // and the guard's charge is deliberately coarse — a write being
        // `source.push` *or* `source.delete`, since it cannot read a push's
        // commands. Left at that, a credential scoped to delete a branch could
        // put unbounded content into the object store, which is the same gap
        // `CommitPack` closed by gating before it writes the body.
        yield* requireCapability("source.push");
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
          .readBlob(params.oid)
          .pipe(Effect.catchTag("StorageFailure", Effect.die));
        return { content: toBase64(data), encoding: "base64" as const, size: data.length };
      }),
    )
    .handle("tree", ({ payload }) =>
      Effect.gen(function* () {
        // As `blob` above: objects are written here and no ref moves.
        yield* requireCapability("source.push");
        const repository = yield* Repository;
        const oid = yield* (
          payload.entries === undefined
            ? writeFilesOf(repository, payload.base, payload.files ?? [])
            : repository.writeTree(payload.entries.map((entry) => ({ ...entry, oid: entry.oid })))
        ).pipe(Effect.catchTag("StorageFailure", Effect.die));
        return { oid };
      }),
    )
    .handle("readTree", ({ params }) =>
      Effect.gen(function* () {
        const repository = yield* Repository;
        const entries = yield* repository
          .readTree(params.oid)
          .pipe(Effect.catchTag("StorageFailure", Effect.die));
        return { entries };
      }),
    )
    .handle("read", ({ params }) =>
      Effect.gen(function* () {
        const repository = yield* Repository;
        const commit = yield* repository
          .readCommit(params.oid)
          .pipe(Effect.catchTag("StorageFailure", Effect.die));
        return { message: commit.message, parents: commit.parents, tree: commit.tree };
      }),
    )
    .handle("log", ({ params }) =>
      Effect.gen(function* () {
        const repository = yield* Repository;
        const commits = yield* Stream.runCollect(repository.log(params.oid, { limit: 50 })).pipe(
          Effect.catchTag("StorageFailure", Effect.die),
        );
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
        yield* gateWrite(`refs/heads/${payload.name}`);
        const oid = yield* repository
          .branch(payload)
          .pipe(Effect.catchTag("StorageFailure", Effect.die));
        return { name: `refs/heads/${payload.name}`, oid };
      }),
    )
    .handle("tagCreate", ({ payload }) =>
      Effect.gen(function* () {
        const repository = yield* Repository;
        // `force` drops `Repository.tag`'s create-only compare-and-swap, which
        // is re-pointing a published tag at an arbitrary commit — the thing
        // receive-pack charges `source.force-push` for. Gating it as an
        // ordinary write let a member with `source.push` do it here instead.
        yield* gateWrite(`refs/tags/${payload.name}`, payload.force === true);
        const request: TagRequest = { name: payload.name, target: payload.target };
        if (payload.message !== undefined) request.message = payload.message;
        if (payload.tagger !== undefined) request.tagger = signatureFrom(payload.tagger);
        if (payload.force !== undefined) request.force = payload.force;
        return yield* repository.tag(request).pipe(Effect.catchTag("StorageFailure", Effect.die));
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
          .readTag(params.oid)
          .pipe(Effect.catchTag("StorageFailure", Effect.die));
        return { object: tag.object, type: tag.type, tag: tag.tag, message: tag.message };
      }),
    )
    .handle("tagRemove", ({ params }) =>
      Effect.gen(function* () {
        const repository = yield* Repository;
        // Applied under the value it was judged against, like every other
        // gated write: `deleteTag` takes a name and nothing else, so a tag
        // re-pointed between the decision and the delete would be removed at
        // a value the policy boundary never saw. `receive` is the delete that
        // carries a compare-and-swap.
        const judged = yield* gateOne({
          name: `refs/tags/${params.name}`,
          value: null,
          reason: "tag: delete",
        });
        // Whether there was anything to delete, asked before the delete and
        // through `resolve`. `receive` reports `from` as the value the command
        // was *judged* against, which for a symbolic ref is `null` — so a
        // symbolic tag was removed and the answer said it had not been.
        const existed = yield* repository
          .resolve(`refs/tags/${params.name}`)
          .pipe(Effect.catchTag("StorageFailure", Effect.die));
        const results = yield* repository.receive([judged]).pipe(
          Effect.catchTags({
            StorageFailure: Effect.die,
            // A hook refusing a deletion is the repository saying no, not
            // this server failing — and these endpoints can say so now.
            HookRejected: (error) => new Invalid({ field: "name", reason: error.message }),
          }),
        );
        const outcome = results.at(0);
        if (outcome !== undefined && !outcome.ok) {
          return yield* new Invalid({
            field: "name",
            reason: outcome.reason ?? `refs/tags/${params.name} moved while it was being deleted`,
          });
        }
        // `undefined` is "the store returned no result for a command we
        // submitted", which is not "it was deleted": reading it as `true`
        // would report a removal that never happened.
        return { deleted: outcome !== undefined && existed !== null };
      }),
    )
    .handle("fsck", () =>
      Effect.gen(function* () {
        // Charged like `gc`, for the same reason and not for `gc`'s reason:
        // this changes nothing, and it reads every object in the store. That
        // is the whole cost, and it has no ref for a gate to hang off, so
        // anybody who could push could drive a full-store scan in a loop. The
        // guard already keeps it off the anonymous path; this keeps it off the
        // contributor's.
        yield* requireCapability("repo.admin");
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
        // Under the judged value, for the reason `tagRemove` gives: a branch
        // that moved between the decision and the delete would otherwise lose
        // a push the boundary never judged.
        const judged = yield* gateOne({
          name: `refs/heads/${params.name}`,
          value: null,
          reason: "delete",
        });
        // As in `tagRemove`: asked before the delete and through `resolve`, so
        // a symbolic branch is not reported as one that was never there.
        const existed = yield* repository
          .resolve(`refs/heads/${params.name}`)
          .pipe(Effect.catchTag("StorageFailure", Effect.die));
        const results = yield* repository.receive([judged]).pipe(
          Effect.catchTags({
            StorageFailure: Effect.die,
            // A hook refusing a deletion is the repository saying no, not
            // this server failing — and these endpoints can say so now.
            HookRejected: (error) => new Invalid({ field: "name", reason: error.message }),
          }),
        );
        const outcome = results.at(0);
        if (outcome !== undefined && !outcome.ok) {
          return yield* new Invalid({
            field: "name",
            reason: outcome.reason ?? `refs/heads/${params.name} moved while it was being deleted`,
          });
        }
        // As in `tagRemove`: a missing result is not a deletion.
        return { deleted: outcome !== undefined && existed !== null };
      }),
    )
    .handle("reset", ({ payload }) =>
      Effect.gen(function* () {
        const repository = yield* Repository;
        // Through the policy boundary, not around it. This endpoint moves a
        // ref to an arbitrary value, which is every rule that boundary
        // exists for — genesis immutability, hub append-only, protected
        // branches — and reaching `setRef` directly skipped all of them.
        // A raw oid is a legal target here — `setRef` takes one — and not
        // every ref store resolves one, so it is recognised before asking.
        const target = isOid(payload.to)
          ? payload.to
          : yield* repository
              .resolve(payload.to)
              .pipe(Effect.catchTag("StorageFailure", Effect.die));
        if (target === null) {
          return yield* new Invalid({ field: "to", reason: `unknown revision '${payload.to}'` });
        }

        // The judged update carries the value the decision was made against,
        // and it is that value the write is applied under: deciding on one
        // state and writing against another is the race the boundary exists
        // to close.
        const judged = yield* gateOne({
          name: payload.ref,
          value: target,
          expected: payload.expected,
        });

        // The *resolved* oid, not the name it was resolved from: handing
        // `setRef` the name would resolve it a second time, and a source ref
        // that moved in between would land a value the policy never saw.
        const request: SetRefRequest = { name: payload.ref, to: target };
        if (judged.expected !== undefined) request.expected = judged.expected;
        const moved = yield* repository
          .setRef(request)
          .pipe(Effect.catchTag("StorageFailure", Effect.die));
        return moved;
      }),
    )
    .handle("merge", ({ payload }) =>
      Effect.gen(function* () {
        const repository = yield* Repository;
        const request: MergeRequest = {
          ours: payload.ours,
          theirs: payload.theirs,
          author: signatureFrom(payload.author),
        };
        if (payload.message !== undefined) request.message = payload.message;
        if (payload.strategy !== undefined) request.strategy = payload.strategy;
        // Objects are written whether or not `into` is set — a merge produces
        // a commit and a tree either way — and without `into` no ref moves, so
        // `gateWrite` below never runs and nothing else charges it. The same
        // gap `blob` and `tree` were closed for.
        yield* requireCapability("source.push");
        if (payload.no_fast_forward !== undefined) request.noFastForward = payload.no_fast_forward;
        // A merge *into a third branch* is a rewrite: the result is a commit
        // over `ours` and `theirs`, and nothing makes it contain what `into`
        // currently holds. Only merging into one of its own sides is the
        // fast-forward-or-descend transition an ordinary push is.
        //
        // The *qualified* name goes to both the gate and the write. Judged
        // qualified and written raw, `into: "main"` was gated as
        // `refs/heads/main` and written to a top-level ref called `main`: the
        // branch never moved and the response said it had.
        if (payload.into !== undefined) {
          const into = yield* branchRefOf(payload.into);
          request.into = into;
          // Compared as *revisions*, not as names. `ours` and `theirs` take
          // anything `merge` can resolve — an oid, a tag, a tracking ref — so
          // comparing the spellings charged `source.force-push` for a merge
          // into one of its own sides whenever the side happened to be
          // written another way, and refused it to a member holding only
          // `source.push`. An `into` that does not exist yet holds nothing
          // that a merge could discard.
          // Resolved exactly as `Repository.merge` resolves them: an oid as
          // itself, anything else through the ref store, which follows
          // symrefs. Qualifying first answered `null` for `HEAD` — and a
          // `null` side is a side nothing matches, so the merge was charged a
          // rewrite again for the one spelling git itself uses most.
          const judged = yield* discards(into, [payload.ours, payload.theirs]);
          yield* gateWrite(into, judged.rewrites);
          request.expected = judged.swap;
        }
        const outcome = yield* repository
          .merge(request)
          .pipe(Effect.catchTag("StorageFailure", Effect.die));
        return outcome;
      }),
    )
    .handle("cherry-pick", ({ payload }) =>
      Effect.gen(function* () {
        // As `merge` above: a cherry-pick writes a commit with or without a
        // branch to land it on.
        yield* requireCapability("source.push");
        const request: CherryPickRequest = { commit: payload.commit, onto: payload.onto };
        if (payload.author !== undefined) request.author = signatureFrom(payload.author);
        // Qualified once, for the gate and the write alike; see `merge` above.
        // And a rewrite only when there is something to rewrite: an `into`
        // that does not exist yet holds nothing a replay could discard, and
        // charging `source.force-push` for it refused the readme's own
        // contributor set a branch they were creating.
        if (payload.into !== undefined) {
          request.into = yield* branchRefOf(payload.into);
          const judged = yield* discards(request.into, [payload.onto]);
          yield* gateWrite(request.into, judged.rewrites);
          request.expected = judged.swap;
        }
        return yield* cherryPick(request).pipe(Effect.catchTag("StorageFailure", Effect.die));
      }),
    )
    .handle("rebase", ({ payload }) =>
      Effect.gen(function* () {
        // As `merge` above: a rebase writes its replayed commits either way.
        yield* requireCapability("source.push");
        const request: RebaseRequest = { branch: payload.branch, onto: payload.onto };
        // As `cherry-pick` above.
        if (payload.into !== undefined) {
          request.into = yield* branchRefOf(payload.into);
          const judged = yield* discards(request.into, [payload.onto]);
          yield* gateWrite(request.into, judged.rewrites);
          request.expected = judged.swap;
        }
        return yield* rebase(request).pipe(Effect.catchTag("StorageFailure", Effect.die));
      }),
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

        // A gitlink is a commit in another repository: there is no content
        // here to diff, and reading it as a blob fails on an object this
        // repository does not have.
        const content = (path: string) =>
          !isGitlink(from.get(path)?.mode ?? "") && !isGitlink(to.get(path)?.mode ?? "");

        const paths = [...new Set([...from.keys(), ...to.keys()])]
          .filter((path) => under(path, payload.path))
          .filter(content)
          .sort();

        const files = yield* Effect.forEach(paths, (path) =>
          Effect.gen(function* () {
            const old = from.get(path);
            const now = to.get(path);
            // The mode is part of what changed: a `chmod +x` moves no bytes,
            // so comparing oids alone reported nothing at all for it and the
            // diff came back empty for a commit that plainly changed something.
            if (old?.oid === now?.oid && old?.mode === now?.mode) return null;

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

            const options: PatchOptions = { beforeName: path, afterName: path };
            if (payload.context !== undefined) options.context = payload.context;

            return {
              path,
              status,
              binary: false,
              patch: unified(decoder.decode(oldBytes), decoder.decode(newBytes), options),
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
        if (entry === null || isTree(entry.mode)) {
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
          .readObject(params.oid)
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

        // Clamped, not taken: the caller picks how much of the budget to use,
        // not how large it is — `max_matches: 1e9` otherwise fills memory one
        // matched line at a time.
        const limit = Math.max(1, Math.min(payload.max_matches ?? 200, GREP_MAX_MATCHES));
        const matches: Array<{ path: string; line: number; text: string }> = [];
        const skipped: Array<string> = [];
        let truncated = false;

        for (const file of files) {
          if (matches.length >= limit) {
            truncated = true;
            break;
          }
          if (!under(file.path, payload.path)) continue;
          // A gitlink names a commit in another repository: there is nothing
          // here to read, let alone to search.
          if (isGitlink(file.mode)) continue;

          const data = yield* repository
            .readBlob(file.oid)
            .pipe(Effect.catchTag("StorageFailure", Effect.die));
          if (data.length > GREP_MAX_FILE_BYTES) {
            skipped.push(file.path);
            continue;
          }
          // A binary file has no lines worth reporting, and git skips them
          // for the same reason.
          if (isBinary(data)) continue;

          // One line decoded at a time. Decoding the whole blob and splitting
          // it holds three copies of the file at once — the bytes, the string
          // and the array — where this holds the bytes and one line.
          let start = 0;
          let line = 0;
          while (start <= data.length) {
            const newline = data.indexOf(0x0a, start);
            const end = newline === -1 ? data.length : newline;
            line++;
            const text = decoder.decode(data.subarray(start, end));
            if (test(text)) {
              matches.push({ path: file.path, line, text });
              if (matches.length >= limit) {
                truncated = true;
                break;
              }
            }
            if (newline === -1) break;
            start = newline + 1;
          }
        }

        return { matches, truncated, skipped };
      }),
    )
    .handle("gc", ({ payload }) =>
      Effect.gen(function* () {
        // Irreversible, and charged accordingly: this deletes objects and
        // rewrites packs, and a `--reflog-grace 0` collection takes the record
        // of where a branch was with them. Every other verb that reaches the
        // object store this hard is behind a ref gate; this one has no ref.
        yield* requireCapability("repo.admin");
        const repository = yield* Repository;
        const request: GcRequest = {};
        if (payload.dry_run !== undefined) request.dryRun = payload.dry_run;
        if (payload.repack !== undefined) request.repack = payload.repack;
        // Clamped: a negative grace would put the cutoff in the future and
        // expire every entry, which is `0` said confusingly.
        if (payload.reflog_grace_ms !== undefined) {
          request.reflogGrace = Math.max(0, payload.reflog_grace_ms);
        }
        // Redaction's other half: a tombstoned payload is still named by its
        // own event's tree, so it survives reachability unless excluded here.
        //
        // Computed for a dry run too, though it deletes nothing. A dry run
        // exists to predict the real one, and skipping the set to save a trust
        // fold made it predict the wrong answer: a tombstoned payload was
        // reported as reachable and "would remove 0", and the same call
        // without `dry_run` removed it.
        request.exclude = yield* Redaction.excluded().pipe(
          Effect.catchTag("StorageFailure", Effect.die),
        );
        const report = yield* repository
          .gc(request)
          .pipe(Effect.catchTag("StorageFailure", Effect.die));
        return {
          scanned: report.scanned,
          reachable: report.reachable,
          removed: report.removed,
          retained: report.retained,
          packed: report.packed ?? null,
          repack_skipped: report.repackSkipped ?? null,
        };
      }),
    )
    .handle("webhookAdd", ({ payload }) =>
      Effect.gen(function* () {
        // Where this repository sends what it holds is an administrative
        // question, and nothing about it moves a ref for the boundary to judge.
        yield* requireCapability("repo.admin");
        const subscribers = yield* Subscribers;
        const added = yield* subscribers
          .add(payload)
          .pipe(Effect.catchTag("StorageFailure", Effect.die));
        return redact(added);
      }),
    )
    .handle("webhookList", () =>
      Effect.gen(function* () {
        // Charged what registering one is charged. Listing looks like a read
        // and is not: what it hands back is every receiver's delivery URL —
        // where this repository's pushes are already being sent, and often an
        // internal address that was never meant to be published. On a
        // repository nobody granted `repo.read`, which is how an open-source
        // repository is served, it was readable by anyone who could reach it.
        yield* requireCapability("repo.admin");
        const subscribers = yield* Subscribers;
        const rows = yield* subscribers.list.pipe(Effect.catchTag("StorageFailure", Effect.die));
        return { webhooks: rows.map(redact) };
      }),
    )
    .handle("webhookRemove", ({ params }) =>
      Effect.gen(function* () {
        // Where this repository sends what it holds is an administrative
        // question, and nothing about it moves a ref for the boundary to judge.
        yield* requireCapability("repo.admin");
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
        const limits = bounds(query);
        if (limits === null) {
          return yield* new Invalid({
            field: "limit",
            reason: "cursor and limit must be whole numbers",
          });
        }
        if (limits.beyond) {
          return yield* new Invalid({
            field: "cursor",
            reason: `paging stops at ${CURSOR_MAX}; narrow the query instead`,
          });
        }
        const { size, start } = limits;
        // Walk only as far as this page needs, plus one to answer `has_more`.
        const walked = yield* Stream.runCollect(
          repository.log(params.oid, { limit: start + size + 1 }),
        ).pipe(Effect.catchTag("StorageFailure", Effect.die));
        return page(
          walked.map((commit) => ({ message: commit.message, oid: commit.oid })),
          query,
        );
      }),
    )
    .handle("history", ({ params, query }) =>
      Effect.gen(function* () {
        const limits = bounds(query);
        if (limits === null) {
          return yield* new Invalid({
            field: "limit",
            reason: "cursor and limit must be whole numbers",
          });
        }
        if (limits.beyond) {
          return yield* new Invalid({
            field: "cursor",
            reason: `paging stops at ${CURSOR_MAX}; narrow the query instead`,
          });
        }
        const { size, start } = limits;
        // Same bound as `commits`: a path history walks the whole graph to
        // find its next entry, so taking only what the page needs matters
        // more here than it does for a plain log.
        const walked = yield* Stream.runCollect(
          pathHistory(params.oid, query.path, { limit: start + size + 1 }),
        ).pipe(Effect.catchTag("StorageFailure", Effect.die));
        return page([...walked], query);
      }),
    )
    .handle("bisect", ({ payload }) =>
      bisectNext({ bad: payload.bad, good: payload.good }).pipe(
        Effect.catchTag("StorageFailure", Effect.die),
      ),
    ),
);

export const remoteHandlers = HttpApiBuilder.group(api, "remotes", (group) =>
  group
    .handle("remoteAdd", ({ payload }) =>
      Effect.gen(function* () {
        // Where this repository sends what it holds is an administrative
        // question, and nothing about it moves a ref for the boundary to judge.
        yield* requireCapability("repo.admin");
        const registry = yield* Remotes;
        // `refs` is optional on the wire and not in the registry: absent means
        // everything the mode carries, and saying so once here keeps every
        // reader from having to know that.
        const asked: RemoteRequest = { name: payload.name, url: payload.url };
        if (payload.credential !== undefined) asked.credential = payload.credential;
        if (payload.sync !== undefined) {
          asked.sync = { mode: payload.sync.mode, refs: payload.sync.refs ?? [] };
        }
        const added = yield* registry
          .add(asked)
          .pipe(Effect.catchTag("StorageFailure", Effect.die));
        return redactRemote(added);
      }),
    )
    .handle("remoteList", () =>
      Effect.gen(function* () {
        // As `webhookList`: every remote's URL, whether it holds a credential,
        // and the standing instruction that says what this repository sends
        // there. Administrative either way, and charged like the registration
        // that put it there.
        yield* requireCapability("repo.admin");
        const registry = yield* Remotes;
        const rows = yield* registry.list.pipe(Effect.catchTag("StorageFailure", Effect.die));
        return { remotes: rows.map(redactRemote) };
      }),
    )
    .handle("remoteRemove", ({ params }) =>
      Effect.gen(function* () {
        // Where this repository sends what it holds is an administrative
        // question, and nothing about it moves a ref for the boundary to judge.
        yield* requireCapability("repo.admin");
        const registry = yield* Remotes;
        const deleted = yield* registry
          .remove(params.name)
          .pipe(Effect.catchTag("StorageFailure", Effect.die));
        return { deleted };
      }),
    )
    .handle("fetch", ({ payload }) =>
      Effect.gen(function* () {
        // Negotiation is a disclosure. `Sync.fetchFrom` offers a `have` line
        // for every local ref, so pointing this at a URL of the caller's
        // choosing hands that URL the commit oids of a read-restricted
        // repository — and `source.push` does not carry `repo.read`. The
        // remote need not even be registered, so `remoteAdd`'s own charge is
        // not the one standing behind this.
        yield* requireRead();
        const target = yield* remoteFor(payload);
        // A fetch writes this repository's tracking refs, so it is a write.
        // Both namespaces it can reach, not only the tracking one: `Sync`
        // rewrites `refs/heads/*` into `refs/remotes/<name>/*` and leaves tag
        // names exactly as the remote spelled them, so gating tracking alone
        // let `refs/tags/*` in past the policy boundary.
        yield* gateWrite(`refs/remotes/${target.name}/*`);
        // Tags are asked about rather than demanded. A repository protecting
        // `refs/tags/*` still has tracking refs to update, and refusing the
        // whole verb for the half it may not do left it unable to replicate at
        // all — a stronger rule than the one the operator wrote. The tags it
        // did not take are visible in the answer, which lists what moved.
        const tags = yield* Policy.gateWrite("refs/tags/*").pipe(
          Effect.orElseSucceed(() => "the repository's policy could not be evaluated"),
        );
        // `fetchFrom` declares its options as possibly-undefined and treats an
        // absent value and an undefined one the same way.
        const fetched = yield* fetchFrom({
          remote: target.name,
          url: target.url,
          credential: target.credential,
          refs: payload.refs,
          depth: payload.depth,
          tags: tags === null,
        });
        return { remote: target.name, refs: fetched.refs, objects: fetched.objects };
      }).pipe(Effect.catchTag("StorageFailure", Effect.die)),
    )
    .handle("push", ({ payload }) =>
      Effect.gen(function* () {
        // Sending this repository's objects to a URL of the caller's choosing
        // is a *read* of everything named, and `source.push` does not imply
        // `repo.read`: a credential scoped exactly as the readme shows for a
        // contributor could otherwise copy a read-restricted repository out.
        yield* requireRead();
        const target = yield* remoteFor(payload);
        const request: PushRequest = {
          url: target.url,
          refs: payload.refs.map((ref): PushRef => {
            const remote = ref.remote ?? ref.local;
            return ref.delete === undefined
              ? { local: ref.local, remote }
              : { local: ref.local, remote, delete: ref.delete };
          }),
        };
        if (target.credential !== null) request.token = target.credential;
        if (payload.force !== undefined) request.force = payload.force;
        if (payload.atomic !== undefined) request.atomic = payload.atomic;
        const results = yield* pushToRemote(request);
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
        // A pull is a fetch with a branch move on the end; the disclosure
        // `fetch` above describes is the same one.
        yield* requireRead();
        const target = yield* remoteFor(payload);
        // A pull moves a local branch, which is the same transition a push
        // makes and has to meet the same rules — a protected branch is not
        // less protected because the commits arrived over a remote.
        yield* gateWrite(refNameOf(payload.branch));
        // And the fetch underneath it, which writes tracking refs. Not tags:
        // a pull asks for one branch by name, so it never takes any — and
        // gating a namespace it cannot write left a repository that protects
        // `refs/tags/*` unable to pull at all.
        yield* gateWrite(`refs/remotes/${target.name}/*`);
        // `pull` declares `depth` as possibly-undefined and treats an absent
        // value and an undefined one the same way.
        return yield* pull({ target, branch: payload.branch, depth: payload.depth });
      }).pipe(Effect.catchTag("StorageFailure", Effect.die)),
    ),
);

/**
 * The API as one layer: routes registered, handlers wired, response plumbing
 * (etag, platform) satisfied from core with no filesystem underneath — a
 * Worker has none, and nothing here serves files.
 *
 * The registry parameter has no default, deliberately. An earlier version
 * defaulted it to `Remotes.none`, and one host was still on the default —
 * serving `/remotes` as permanently empty with nothing in the types to say
 * so. A host without persistence says `Remotes.none` where it builds the
 * layer, so the choice is visible exactly where it is made.
 */
export const layer = (registry: Layer.Layer<Remotes>) =>
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
