/**
 * Context Packs: which repository evidence a harness exposed, and against
 * which exact tree it resolves.
 *
 * ```text
 * Repository View          the exact tree evidence resolves from
 *       ↓
 *  Context Pack            an immutable JSON manifest of that evidence
 *       ↓
 *  ContextRender           the bytes that crossed the boundary (Render.ts)
 *       ↓
 *  Context Exposure        the signed record that binds them (Exposure.ts)
 * ```
 *
 * Retrieval quality is deliberately outside the protocol. A pack proves what
 * was exposed; it proves nothing about whether the selector chose well, and
 * `Select.ts` is replaceable without any of this changing (docs/context-pack.md
 * §2). What is *not* probabilistic is evidence identity: every item resolves
 * from one `view.tree` by its own kind-specific rule, and an item that does
 * not resolve is not evidence for that view even when the object exists.
 *
 * Two item kinds, kept apart on purpose. A blob is bytes somebody could have
 * read; a gitlink is only the parent repository's claim about which submodule
 * commit it pointed at, and reading it as content would assert that submodule
 * files were exposed when nothing here retrieved any (§5.2).
 */
import { Effect, Predicate, Schema } from "effect";

import { Invalid, type ObjectNotFound, type StorageFailure } from "../git/Error.ts";
import { isGitlink, isTree } from "../git/Format.ts";
import { qualify, unqualify } from "../git/Oid.ts";
import { Repository } from "../git/Repository.ts";
import type { Oid } from "../git/Store.ts";
import { IndexStore, modeString, unchanged, WorkTree } from "../git/Work.ts";

export { qualify, unqualify };

/** The only pack version this implementation reads or writes. */
export const VERSION = 1;

/** Submodule mode, as a tree spells it. */
const GITLINK_MODE = "160000";

/**
 * How many items one pack may carry, and how large its JSON may be.
 *
 * Bounds rather than features (§12). A pack is read on the audit path, where
 * one malformed or hostile record must not be able to make every later read of
 * the ref it sits on expensive: verification resolves every item against the
 * view, which is a tree walk per item.
 */
export const MAX_ITEMS = 4096;
export const MAX_PAYLOAD = 1024 * 1024;

// -- schema ---------------------------------------------------------------------

/**
 * The exact source snapshot repository evidence resolves from.
 *
 * `base` anchors committed history and is useful for ancestry; it is not the
 * effective snapshot. `tree` is, and it is the only one verification reads —
 * a dirty worktree has bytes no commit holds, and calling that state `HEAD`
 * is the mislabelling this field exists to prevent (§4.2).
 */
export const View = Schema.Struct({
  base: Schema.String,
  tree: Schema.String,
});
export type View = typeof View.Type;

/**
 * A claim that a blob was supplied with repository-derived instruction
 * authority.
 *
 * Descriptive in `source`, verifiable in the other two: the root must be this
 * pack's own view and the path must resolve under it to the item's blob. An
 * annotation that fails those is an unverified instruction claim and nothing
 * more — the evidence item underneath it stays valid (§7).
 */
export const Authority = Schema.Struct({
  source: Schema.String,
  root: Schema.String,
  path: Schema.String,
});
export type Authority = typeof Authority.Type;

/**
 * Selection metadata, which a verifier must never need.
 *
 * Recorded because it is what makes a pack readable by a person, and kept
 * optional because requiring it would make the selector's vocabulary part of
 * the protocol (§6).
 */
const descriptive = {
  role: Schema.optional(Schema.String),
  reason: Schema.optional(Schema.String),
  symbol: Schema.optional(Schema.String),
};

export const Blob = Schema.Struct({
  kind: Schema.tag("blob"),
  path: Schema.String,
  blob: Schema.String,
  /** Half-open byte offsets into the exact blob bytes; absent means all of it. */
  range: Schema.optional(Schema.Tuple([Schema.Int, Schema.Int])),
  ...descriptive,
  authority: Schema.optional(Authority),
});
export type Blob = typeof Blob.Type;

export const Gitlink = Schema.Struct({
  kind: Schema.tag("gitlink"),
  path: Schema.String,
  commit: Schema.String,
  ...descriptive,
});
export type Gitlink = typeof Gitlink.Type;

export const Item = Schema.Union([Blob, Gitlink]);
export type Item = typeof Item.Type;

/** Recommended `reason` values; a producer may record others (§6). */
export const REASONS = [
  "explicit",
  "search",
  "reference",
  "definition",
  "call",
  "import",
  "test",
  "config",
  "instruction",
  "history",
  "memory",
  "tool-opened",
  "neighbor",
  "other",
] as const;

/** Recommended omission reasons (§6.1). */
export const OMISSIONS = ["budget", "unavailable", "filtered", "error", "other"] as const;

/**
 * One coarse omission.
 *
 * Non-exhaustive and non-ranked: order here means nothing, and an absence
 * means nothing either. A producer that cannot name a path without revealing
 * structure the invocation could not see records the aggregate form — a
 * `reason` and a `count` — instead (§6.1).
 */
export const Omission = Schema.Struct({
  path: Schema.optional(Schema.String),
  reason: Schema.String,
  count: Schema.optional(Schema.Int),
});
export type Omission = typeof Omission.Type;

export const Selector = Schema.Struct({
  name: Schema.String,
  version: Schema.String,
});

export const Pack = Schema.Struct({
  version: Schema.Literal(VERSION),
  view: View,
  selector: Schema.optional(Selector),
  items: Schema.Array(Item),
  omissions: Schema.optional(Schema.Array(Omission)),
});
export type Pack = typeof Pack.Type;

const decodePack = Schema.decodeUnknownEffect(Pack);
const encoder = new TextEncoder();
const decoder = new TextDecoder();

// -- serialization --------------------------------------------------------------

/**
 * Every key a pack document may carry, in the order it is written.
 *
 * `JSON.stringify`'s property list does both jobs at once: it fixes the order
 * of every object in the document and it filters out anything not named here,
 * so a pack cannot pick up a field from a value that happens to carry one.
 * Absent optional fields drop out on their own, since `undefined` is not JSON.
 */
const KEYS = [
  "version",
  "view",
  "base",
  "tree",
  "selector",
  "name",
  "items",
  "kind",
  "path",
  "blob",
  "commit",
  "range",
  "role",
  "reason",
  "symbol",
  "authority",
  "source",
  "root",
  "omissions",
  "count",
];

/**
 * The exact bytes a pack is persisted as, and so its identity.
 *
 * V1 does not require canonical JSON — two semantically equal packs may hash
 * differently (§5) — but a producer that re-serialized the same pack into two
 * different byte strings would give one exposure two identities, so the field
 * order and the framing are fixed here the way a signed record's are.
 *
 * Round-tripping a pack somebody else wrote is not what this is for: a decode
 * drops the descriptive fields this version does not know, so re-encoding one
 * can produce bytes — and therefore an identity — that is not the persisted
 * one. Verification reads the persisted bytes for exactly that reason.
 */
export const encode = (pack: Pack): Uint8Array =>
  encoder.encode(`${JSON.stringify(pack, KEYS, 2)}\n`);

/**
 * A pack from its persisted bytes.
 *
 * The gitlink check is here rather than in the schema because a `Schema.Union`
 * discriminated on `kind` silently drops the fields the chosen member does not
 * declare: a gitlink carrying `blob` and `range` would decode to a clean
 * gitlink, and the record would then read as having said something it did not.
 * §5.2 makes that item invalid, so it is refused where the bytes are still in
 * hand.
 */
export const decode = Effect.fn("context.Pack.decode")(function* (bytes: Uint8Array) {
  if (bytes.length > MAX_PAYLOAD) {
    return yield* new Invalid({
      field: "pack",
      reason: `a context pack may not exceed ${MAX_PAYLOAD} bytes; this one is ${bytes.length}`,
    });
  }

  const json: unknown = yield* Effect.try({
    try: () => JSON.parse(decoder.decode(bytes)),
    catch: () => new Invalid({ field: "pack", reason: "context pack is not valid JSON" }),
  });

  // Asked of the *raw* document, before the schema has had a chance to discard
  // the fields that make it wrong.
  if (Predicate.hasProperty(json, "items") && Array.isArray(json.items)) {
    for (const [index, entry] of json.items.entries()) {
      if (!Predicate.hasProperty(entry, "kind") || entry.kind !== "gitlink") continue;
      if (Predicate.hasProperty(entry, "blob") || Predicate.hasProperty(entry, "range")) {
        return yield* new Invalid({
          field: "items",
          reason: `item ${index} is a gitlink and may not carry blob or range`,
        });
      }
    }
  }

  const pack = yield* decodePack(json).pipe(
    Effect.mapError(
      (issue) => new Invalid({ field: "pack", reason: `malformed context pack: ${issue.message}` }),
    ),
  );

  if (pack.items.length > MAX_ITEMS) {
    return yield* new Invalid({
      field: "items",
      reason: `a context pack may not carry more than ${MAX_ITEMS} items`,
    });
  }
  return pack;
});

// -- repository view ------------------------------------------------------------

/**
 * The view a committed snapshot names, for a repository with no checkout.
 *
 * `view.tree` is the commit's own root tree, so the clean-worktree identity in
 * §4.2 holds by construction rather than by assertion.
 */
export const committed = Effect.fn("context.Pack.committed")(function* (base: Oid) {
  const repository = yield* Repository;
  const commit = yield* repository.readCommit(base);
  return { base: qualify(base), tree: qualify(commit.tree) } satisfies View;
});

/**
 * The view a live checkout names: tracked paths, as they are on disk now.
 *
 * Built as an overlay tree rather than reported as `HEAD`, which is the whole
 * of §4.2 — an agent that read a modified file was shown bytes no commit
 * holds, and a view naming the commit would make the audit resolve different
 * bytes than the ones exposed. Written, not merely hashed: a view nothing can
 * resolve is not a view, and §10 needs a real tree to hang the record's
 * `context/view` edge from.
 *
 * The clean case falls out for free. Every entry then hashes to what the index
 * already holds, so `writePaths` reproduces `HEAD`'s own tree oid and the
 * identity §4.2 states is a fact about the construction rather than a special
 * case in it.
 *
 * *Tracked* paths, and only those. Untracked files are readable on disk, so
 * excluding them narrows what this claims retrieval could see — but the work
 * tree here has no ignore rules, so including them would put every build
 * output and dependency directory into a tree that every exposure then pins
 * into the object graph for good. The selector never sees them, so they can
 * never become evidence, which is the honest reading of the narrower view.
 */
export const capture = Effect.fn("context.Pack.capture")(function* (base: Oid) {
  const repository = yield* Repository;
  const work = yield* WorkTree;
  const index = yield* IndexStore;

  const entries: Array<{ readonly path: string; readonly oid: Oid; readonly mode: string }> = [];
  for (const entry of yield* index.load) {
    // A submodule has no file of its own in this checkout — the other
    // repository's work tree is its own business — so `stat` says nothing
    // about it and the index is the only thing that can.
    if (modeString(entry.mode) === GITLINK_MODE) {
      entries.push({ path: entry.path, oid: entry.oid, mode: GITLINK_MODE });
      continue;
    }

    const stat = yield* work.stat(entry.path);
    // Deleted on disk: retrieval cannot read it, so the view must not say it
    // could. This is the case that makes the overlay differ from `HEAD` in
    // the direction nobody expects.
    if (stat === null) continue;

    if (unchanged(entry, stat)) {
      entries.push({ path: entry.path, oid: entry.oid, mode: modeString(entry.mode) });
      continue;
    }

    const oid = yield* repository.writeBlob(yield* work.read(entry.path));
    entries.push({ path: entry.path, oid, mode: modeString(stat.mode) });
  }

  const tree = yield* repository.writePaths(entries);
  return { base: qualify(base), tree: qualify(tree) } satisfies View;
});

// -- evidence -------------------------------------------------------------------

export type Check = { readonly ok: true } | { readonly ok: false; readonly reason: string };

const ok: Check = { ok: true };
const bad = (reason: string): Check => ({ ok: false, reason });

export interface Checked {
  readonly index: number;
  readonly kind: Item["kind"];
  readonly path: string;
  /** Whether this item resolves from `view.tree` by its kind's own rule. */
  readonly evidence: Check;
  /** `null` where the item made no instruction-authority claim. */
  readonly authority: Check | null;
}

export interface Report {
  /** Whether `view.tree` is a tree this repository holds. */
  readonly view: Check;
  readonly items: ReadonlyArray<Checked>;
  /** Whether the view and every item's evidence verified. */
  readonly ok: boolean;
}

/**
 * The exact bytes one blob item is evidence for.
 *
 * Fails rather than truncates on a range the blob cannot satisfy: a caller
 * holding fewer bytes than the record claims is holding different evidence,
 * and quietly clamping would let a pack name a range and be audited against a
 * shorter one.
 */
export const evidence = Effect.fn("context.Pack.evidence")(function* (view: View, item: Blob) {
  const repository = yield* Repository;
  const oid = unqualify(item.blob);
  if (oid === null) {
    return yield* new Invalid({ field: "blob", reason: `'${item.blob}' is not an object id` });
  }

  const bytes = yield* repository.readBlob(oid);
  if (item.range === undefined) return bytes;

  const [start, end] = item.range;
  if (!inRange(start, end, bytes.length)) {
    return yield* new Invalid({
      field: "range",
      reason: `[${start}, ${end}) is not a range within ${item.path}'s ${bytes.length} bytes`,
    });
  }
  return bytes.subarray(start, end);
});

/**
 * Whether a half-open range is one a blob of this size can satisfy.
 *
 * Non-empty by construction (§5.1): zero-length evidence is a producer saying
 * it exposed nothing while recording that it exposed something.
 */
export const inRange = (start: number, end: number, size: number): boolean =>
  Number.isSafeInteger(start) &&
  Number.isSafeInteger(end) &&
  start >= 0 &&
  start < end &&
  end <= size;

/**
 * Whether `[start, end)` would cut a UTF-8 codepoint in these bytes.
 *
 * A continuation byte is `10xxxxxx`, and a boundary is any byte that is not
 * one. The range's own bytes are never inspected — only the two edges — so
 * this is the check a renderer can afford to make on every segment (§5.1).
 */
export const splitsCodepoint = (bytes: Uint8Array, start: number, end: number): boolean => {
  const continuation = (at: number) => at < bytes.length && (bytes[at]! & 0xc0) === 0x80;
  return continuation(start) || continuation(end);
};

/**
 * The nearest range that starts and ends on codepoint boundaries.
 *
 * Widened rather than narrowed, so a selector never drops a character it meant
 * to include. A caller that already has a boundary-aligned range gets it back
 * unchanged.
 */
export const snap = (bytes: Uint8Array, start: number, end: number): readonly [number, number] => {
  let from = Math.max(0, Math.min(start, bytes.length));
  let to = Math.max(from, Math.min(end, bytes.length));
  while (from > 0 && (bytes[from]! & 0xc0) === 0x80) from -= 1;
  while (to < bytes.length && (bytes[to]! & 0xc0) === 0x80) to += 1;
  return [from, to];
};

/**
 * What one item resolves to under a view, or why it does not.
 *
 * The two rules are deliberately different. A blob must resolve through the
 * tree at its path *and* match the recorded oid — an object that exists in the
 * database but is unreachable from `view.tree` at `path` is not evidence for
 * that view (§5.1) — and a gitlink must additionally be mode 160000, because
 * a path that holds a file is not a submodule however the pack labels it.
 */
const checkItem = Effect.fn("context.Pack.checkItem")(function* (tree: Oid, item: Item) {
  const repository = yield* Repository;
  const entry = yield* repository
    .findPath(tree, item.path)
    .pipe(Effect.catchTag("ObjectNotFound", () => Effect.succeed(null)));

  if (entry === null) return bad(`'${item.path}' does not resolve under view.tree`);

  if (item.kind === "gitlink") {
    if (!isGitlink(entry.mode)) {
      return bad(`'${item.path}' is mode ${entry.mode}, not a submodule gitlink`);
    }
    const commit = unqualify(item.commit);
    if (commit === null) return bad(`'${item.commit}' is not an object id`);
    return entry.oid === commit
      ? ok
      : bad(`'${item.path}' points at ${qualify(entry.oid)}, not ${item.commit}`);
  }

  if (isTree(entry.mode) || isGitlink(entry.mode)) {
    return bad(`'${item.path}' is mode ${entry.mode}, not blob evidence`);
  }
  const blob = unqualify(item.blob);
  if (blob === null) return bad(`'${item.blob}' is not an object id`);
  if (entry.oid !== blob) {
    return bad(`'${item.path}' holds ${qualify(entry.oid)}, not ${item.blob}`);
  }

  if (item.range === undefined) return ok;
  const [start, end] = item.range;
  const bytes = yield* repository
    .readBlob(blob)
    .pipe(Effect.catchTag("ObjectNotFound", () => Effect.succeed(null)));
  // The tree resolved to it a moment ago, so an absent object here is a
  // repository missing something it says it has — reported, not substituted
  // for (§5.3).
  if (bytes === null) return bad(`${item.blob} is unavailable`);
  return inRange(start, end, bytes.length)
    ? ok
    : bad(`[${start}, ${end}) is not a range within ${bytes.length} bytes`);
});

/**
 * Whether an instruction-authority annotation holds.
 *
 * Its own check, and its own place in the report, because §7 makes an invalid
 * annotation an unverified *claim* rather than a reason to disbelieve the
 * bytes underneath it. Folding the two together would let a bad `authority`
 * field discredit evidence that resolves perfectly well.
 */
const checkAuthority = Effect.fn("context.Pack.checkAuthority")(function* (
  view: View,
  item: Blob,
  authority: Authority,
) {
  if (authority.root !== view.tree) {
    return bad(`authority.root ${authority.root} is not this pack's view.tree`);
  }
  const root = unqualify(authority.root);
  if (root === null) return bad(`'${authority.root}' is not an object id`);

  const entry = yield* Effect.flatMap(Repository, (repository) =>
    repository
      .findPath(root, authority.path)
      .pipe(Effect.catchTag("ObjectNotFound", () => Effect.succeed(null))),
  );
  if (entry === null) return bad(`'${authority.path}' does not resolve under authority.root`);
  return qualify(entry.oid) === item.blob
    ? ok
    : bad(`'${authority.path}' resolves to ${qualify(entry.oid)}, not the item's blob`);
});

/**
 * Every dimension of a pack's repository evidence, reported independently.
 *
 * One boolean would be the wrong answer to give an auditor: a pack whose view
 * is missing, one whose third item drifted, and one whose instruction claim
 * does not hold are three different situations, and §11 asks for them apart.
 */
export const verify = Effect.fn("context.Pack.verify")(function* (pack: Pack) {
  const repository = yield* Repository;
  const tree = unqualify(pack.view.tree);
  if (tree === null) {
    return {
      view: bad(`'${pack.view.tree}' is not an object id`),
      items: [],
      ok: false,
    } satisfies Report;
  }

  // `readTree`, which answers for the well-known empty tree and refuses
  // anything that is not a tree at all. The two failures read the same way to
  // an auditor — this view cannot be resolved here — and telling a missing
  // object from a blob wearing a tree's oid would not change what they do next.
  const entries = yield* repository
    .readTree(tree)
    .pipe(Effect.catchTag("ObjectNotFound", () => Effect.succeed(null)));
  if (entries === null) {
    return {
      view: bad(`view.tree ${pack.view.tree} is not a tree this repository holds`),
      items: [],
      ok: false,
    } satisfies Report;
  }

  const items: Array<Checked> = [];
  for (const [index, item] of pack.items.entries()) {
    const authority =
      item.kind === "blob" && item.authority !== undefined
        ? yield* checkAuthority(pack.view, item, item.authority)
        : null;
    items.push({
      index,
      kind: item.kind,
      path: item.path,
      evidence: yield* checkItem(tree, item),
      authority,
    });
  }

  return {
    view: ok,
    items,
    ok: items.every((checked) => checked.evidence.ok),
  } satisfies Report;
});

export type PackError = Invalid | ObjectNotFound | StorageFailure;
