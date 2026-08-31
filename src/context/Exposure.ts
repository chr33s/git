/**
 * Context Exposure: the signed record binding a pack, a render commitment and
 * a retained view to one invocation boundary.
 *
 * ```text
 * Context Exposure record commit          canonically: sha1:<hex>
 * └── tree
 *     ├── event.json          the signed payload
 *     ├── event.sig
 *     └── context/
 *         ├── pack.json       the exact bytes payload.pack names
 *         ├── render.bin      optional, under retention policy
 *         └── view/           tree entry oid == pack.view.tree
 * ```
 *
 * The `context/view` edge is the part that is easy to leave out and impossible
 * to add later. Writing an oid inside JSON does not make the object reachable:
 * a dirty view's overlay tree is referenced by nothing else in the graph, so
 * the first `gc` collects it and every later audit of a record that verified
 * yesterday reports a view it cannot resolve. A clean view is not safe either
 * — a rewritten history or a deleted branch orphans a committed tree just as
 * thoroughly — so the edge is unconditional (docs/context-pack.md §10).
 *
 * What an exposure is *not* is an input to anything. It is audit data on the
 * policy-invisible trace ref, and §2 invariant 7 keeps it out of authorization
 * and protected-branch folds — a repository whose agents are busy must not
 * thereby become a repository whose pushes are slow, or whose merge rules
 * depend on what a harness said it showed a model.
 */
import { DateTime, Effect, Result, Schema } from "effect";

import {
  fingerprint,
  type Fingerprint,
  NAMESPACE,
  type PrivateKey,
  verify,
} from "../crypto/SshSignature.ts";
import { Invalid, type ObjectNotFound, type StorageFailure } from "../git/Error.ts";
import { hashObject, type TreeEntry } from "../git/Format.ts";
import { qualify, unqualify } from "../git/Oid.ts";
import { Repository } from "../git/Repository.ts";
import { TRUST_LOG } from "../git/Refspec.ts";
import { isOid, type Oid } from "../git/Store.ts";
import * as Event from "../hub/Event.ts";
import * as Secrets from "../hub/Secrets.ts";
import * as Claim from "../hub/Claim.ts";
import * as Trace from "../hub/Trace.ts";
import { MAX_SIGNATURES } from "../trust/Certificate.ts";
import type { Projection } from "../trust/Projection.ts";
import { trustReach } from "../hub/Projection.ts";
import * as Record from "../trust/Record.ts";
import * as Verify from "../trust/Verify.ts";
import * as Pack from "./Pack.ts";
import * as Render from "./Render.ts";

/**
 * The capability a trace producer holds.
 *
 * Charged at the policy boundary when the ref moves, and asked again here when
 * a caller hands in a trust projection: a record that arrived by replication
 * never passed this host's boundary, so "who signed it, and could they" is a
 * question the audit has to be able to ask for itself (§11).
 */
export const CAPABILITY = "hub.trace";

/** Where the attached evidence sits inside the record's tree. */
export const DIRECTORY = "context";
export const PACK = `${DIRECTORY}/pack.json`;
export const RENDER = `${DIRECTORY}/render.bin`;
export const VIEW = `${DIRECTORY}/view`;

const BLOB_MODE = "100644";
/**
 * The subtree mode git itself writes.
 *
 * `040000` is the zero-padded spelling git *tolerates on read* — its own
 * `fsck` has a name for it, `zeroPaddedFilemode` — and writing it produced two
 * non-canonical trees per exposure: the record's root tree and its `context/`
 * tree. Both then hash differently from what canonical git writes for the same
 * content, so a round-trip through `git read-tree`/`write-tree` renames the
 * record — and the record's oid is the exposure's canonical identity, on a ref
 * this version cannot delete.
 */
const TREE_MODE = "40000";

/**
 * Whether a capture names a stage this version knows.
 *
 * A capture is runtime correlation and nothing more. An OTel trace id, a
 * provider request id and a harness event id are all identifiers somebody else
 * minted; they are recorded because an operator debugging a run needs the
 * join, and §9 does not let them affect pack identity, render verification,
 * authority or record identity — a record whose identity depended on a
 * provider's id would be a Git record a provider could rename. So this checks
 * the one field a *writer* is held to, and nothing about the rest.
 *
 * `stage` stays a bare string in the schema so an older reader can still read a
 * stage a newer producer names — but a *writer* is held to the vocabulary,
 * because a typo lands in a signed, immutable record on an append-only ref and
 * every later reader sees a stage that is not one. The CLI checks its own flag;
 * this is the check for `trace record --event` and for any library caller,
 * which take the value straight through.
 */
export const checkCapture = Effect.fnUntraced(function* (capture: Capture | null) {
  if (capture === null || capture.stage === undefined) return;
  if (!STAGES.some((known) => known === capture.stage)) {
    return yield* new Invalid({
      field: "stage",
      reason: `'${capture.stage}' is not a capture stage; one of ${STAGES.join(", ")}`,
    });
  }
});

/** Where a capture was taken, before anything could sample it (§12). */
export const STAGES = [
  "sdk-export",
  "local-collector",
  "remote-collector",
  "hook",
  "embedded",
  "other",
] as const;

export const Capture = Schema.Struct({
  transport: Schema.String,
  /**
   * Where the capture was taken, before anything could sample it.
   *
   * docs/telemetry.md §12: a normal observability pipeline may sample, filter
   * and transform, so *where* a signal was picked off decides whether a
   * completeness claim is available at all. Recorded here rather than inferred
   * from the transport, because "otel" says nothing about which processor ran
   * first.
   */
  stage: Schema.optional(Schema.String),
  traceId: Schema.optional(Schema.String),
  spanId: Schema.optional(Schema.String),
  /**
   * The semantic-convention profile this signal was interpreted under.
   *
   * Present only when the producer declared a revision. §4.1 allows a
   * documented best-effort mapping when none is known but forbids claiming
   * strict semconv adherence for that signal — so an absent `semconv` is the
   * difference between "interpreted under this revision" and "interpreted as
   * well as we could".
   */
  semconv: Schema.optional(Schema.Struct({ profile: Schema.String, revision: Schema.String })),
});
export type Capture = typeof Capture.Type;

/** What the commit message declares a record of this kind to be. */
export const TYPE = "context-exposure";

/**
 * The signed claim.
 *
 * The envelope is the one every hub record carries — repository, session, id,
 * time, trust head — because a payload that did not name its repository is one
 * that can be replayed into another, and cli.md §8 makes binding both the
 * recorder's job.
 */
export const Payload = Schema.Struct({
  type: Schema.tag(TYPE),
  version: Schema.Literal(1),
  repo: Schema.String,
  session: Schema.String,
  id: Schema.String,
  issuedAt: Schema.String,
  /** `null` means the author recorded none; see `hub/Event`'s own envelope. */
  trustHead: Schema.NullOr(Schema.String),
  /** The Git blob oid of the exact pack bytes retained at `context/pack.json`. */
  pack: Schema.String,
  renderFormat: Schema.String,
  renderDigest: Schema.String,
  capture: Schema.NullOr(Capture),
});
export type Payload = typeof Payload.Type;

const decodePayload = Schema.decodeUnknownEffect(Payload);
const encoder = new TextEncoder();
const decoder = new TextDecoder();

/**
 * The bytes that are signed and the bytes that are stored, in one encoding.
 *
 * Key order is fixed for the reason a session record's is: a signature covers
 * bytes, not a value, and two encodings that agree today are two that can
 * drift into signatures that verify nowhere.
 */
export const encode = (payload: Payload): Uint8Array =>
  encoder.encode(
    `${JSON.stringify(
      payload,
      [
        "version",
        "type",
        "repo",
        "session",
        "id",
        "issuedAt",
        "trustHead",
        "pack",
        "renderFormat",
        "renderDigest",
        "capture",
        "transport",
        "stage",
        "traceId",
        "spanId",
        "semconv",
        "profile",
        "revision",
      ],
      2,
    )}\n`,
  );

export const decode = Effect.fn("context.Exposure.decode")(function* (bytes: Uint8Array) {
  const json: unknown = yield* Effect.try({
    try: () => JSON.parse(decoder.decode(bytes)),
    catch: () => new Invalid({ field: "exposure", reason: "trace record is not valid JSON" }),
  });
  const payload = yield* decodePayload(json).pipe(
    Effect.mapError(
      (issue) =>
        new Invalid({ field: "exposure", reason: `malformed context exposure: ${issue.message}` }),
    ),
  );

  return payload;
});

// -- recording ------------------------------------------------------------------

/**
 * Record one exposure and return the commit that is its canonical identity.
 *
 * `retain` decides only whether the framing bytes are kept. The digest is
 * computed either way, because a commitment whose bytes have since expired is
 * still a commitment — §11 asks a verifier to tell "the render does not match"
 * apart from "the render is no longer here", and collapsing the two would make
 * an expired retention look like a failed audit.
 */
export const expose = Effect.fn("context.Exposure.expose")(function* (input: {
  /** The repository's own identity, so the record cannot be replayed. */
  readonly repo: string;
  readonly session: string;
  readonly key: PrivateKey;
  readonly pack: Pack.Pack;
  readonly segments: ReadonlyArray<Render.Segment>;
  readonly retain?: boolean;
  /**
   * Blob oids `gc` will actually remove, from `hub/Redaction.excluded`.
   *
   * `excluded`, not `covered`, and the difference decides whether this is a
   * safety rail or a denial of service. `covered` is deliberately the
   * *un-authorized* set — "what a tombstone names, not what still counts",
   * because a fetch has to be able to explain an absence whether or not the
   * removal was authorized. Replication is not policy-gated, so a peer holding
   * only `hub.trace` can land a tombstone naming any exposure; `gc` will never
   * honour it, but read from `covered` every later identical `context for`
   * would decline to retain its render on the word of a signer who could not
   * have removed anything. The bytes would be neither retained nor removed, on
   * an append-only ref.
   *
   * Handed in rather than read here, and not for taste: `hub/Redaction`
   * imports this module for the `context/` path names, so reading it back
   * would close a cycle whose module-init order decides whether those names
   * are defined. The caller that writes exposures is the caller that can ask.
   */
  readonly removed?: ReadonlySet<Oid>;
  readonly capture?: Capture | null;
  /**
   * The operator's own words, for the scan every other namespace runs.
   *
   * `Session.issue`, `Task`, `Queue` and `Records.record` all refuse a record
   * whose prose looks like a credential, and this chain had no equivalent: the
   * same string that `session open --prompt` turns away went into
   * `context/render.bin` verbatim and onto a ref this version cannot rewind.
   *
   * The task and not the segments. The evidence segments are the exposed
   * repository bytes, which a heuristic scanner would refuse constantly — that
   * is what `--retain-render=false` and `trace redact` are for. This is the
   * one part somebody typed.
   */
  readonly task?: string;
}) {
  const repository = yield* Repository;

  yield* checkCapture(input.capture ?? null);

  // Scanned before anything is written, for the reason `Session.issue` scans:
  // redaction is the way back and it is recovery rather than hygiene — a
  // tombstone reaches every replica that syncs, but only once the bytes are
  // already there.
  // What a caller *authored*, and only that. `telemetry/Records.prose` walks
  // its whole payload, and copying that here swept in the repository's own
  // values: a `Pack.Item`'s `path` and `authority.path` are what the selector
  // chose, not what anybody typed, and `Secrets.scan`'s dense-string rule
  // matches across a slash — so a content-hashed asset path like
  // `assets/a3F9…9p.js` refused the exposure and blamed the operator's prompt
  // for a file they cannot reword. `hub/Session.prose` and `Records.prose`
  // both leave repository-derived values out for exactly this reason; the
  // difference here is that a *pack* is half repository and half producer,
  // where a trace record is all producer.
  //
  // And named, so the refusal says which field. One message reading "this task
  // looks like it carries…" for a finding in `capture.traceId` is an operator
  // rewording a prompt that was never the problem.
  const authored: Array<readonly [string, string, Secrets.Reading]> = [
    ["task", input.task ?? "", {}],
  ];
  const capture = input.capture;
  if (capture !== undefined && capture !== null) {
    authored.push(
      ["capture.transport", capture.transport, {}],
      ["capture.stage", capture.stage ?? "", {}],
      // Identifiers somebody else minted, held to the pattern rules and not to
      // entropy. `telemetry/Records.record` exempts the same two, with a
      // docstring saying why — and this list did not, so a harness recording
      // the pre-call exposure with a base62 trace id was refused here while
      // the paired `trace record --otel` for the same span succeeded: a run
      // with a runtime half and no context half, provider-dependent.
      ["capture.traceId", capture.traceId ?? "", { opaque: true }],
      ["capture.spanId", capture.spanId ?? "", { opaque: true }],
      [
        "capture.semconv",
        `${capture.semconv?.profile ?? ""} ${capture.semconv?.revision ?? ""}`,
        {},
      ],
    );
  }
  for (const item of input.pack.items) {
    authored.push(
      ["pack.role", item.role ?? "", {}],
      ["pack.reason", item.reason ?? "", {}],
      ["pack.symbol", item.symbol ?? "", {}],
    );
    if (item.kind === "blob") authored.push(["pack.authority", item.authority?.source ?? "", {}]);
  }
  // The omission reasons and the selector's own name, which are as
  // caller-authored as anything above and were simply missing from the list.
  // `telemetry/Records.prose` gave up on a hand-kept list after it was wrong
  // four times; this one stays a list because a pack is half repository — an
  // item's `path` and an omission's `path` are what the selector chose, not
  // what anybody typed — but the halves have to be enumerated correctly.
  for (const omission of input.pack.omissions ?? []) {
    authored.push(["pack.omission", omission.reason, {}]);
  }
  if (input.pack.selector !== undefined) {
    authored.push(
      ["pack.selector", input.pack.selector.name, {}],
      ["pack.selector", input.pack.selector.version ?? "", {}],
    );
  }

  for (const [field, text, reading] of authored) {
    if (text === "") continue;
    const leaked = Secrets.scan(text, reading);
    if (leaked.length > 0) {
      return yield* new Invalid({
        field,
        reason: `${field} looks like it carries ${leaked
          .map((finding) => `a ${finding.kind} (${finding.hint})`)
          .join(", ")}; it is written verbatim onto an append-only ref`,
      });
    }
  }

  // Asked first, because `Trace.append` asks it last. Refused there, the pack
  // blob, the render blob and the `context/` tree had all been written
  // already — the orphaned objects the checks below exist to prevent, arriving
  // through the one check that was not among them.
  if (!Trace.isTraceId(input.session)) {
    return yield* new Invalid({
      field: "session",
      reason: `'${input.session}' cannot name a trace; it must be one ref path component`,
    });
  }

  // Held to exactly what `Pack.decode` will accept, and before a single object
  // is written. A record whose pack this repository's own reader refuses is
  // one whose every later audit reports "no readable pack naming a view" —
  // permanently, on a ref nothing can remove. The byte cap alone let that
  // through: four thousand terse items sit well inside a megabyte.
  const bytes = Pack.encode(input.pack);
  if (bytes.length > Pack.MAX_PAYLOAD) {
    return yield* new Invalid({
      field: "pack",
      reason: `a context pack may not exceed ${Pack.MAX_PAYLOAD} bytes; this one is ${bytes.length}`,
    });
  }
  if (input.pack.items.length > Pack.MAX_ITEMS) {
    return yield* new Invalid({
      field: "items",
      reason: `a context pack may not carry more than ${Pack.MAX_ITEMS} items; this one has ${input.pack.items.length}`,
    });
  }
  // And then actually asked, rather than the two bounds `decode` happens to
  // share being taken for the whole of it. `decode` also refuses a
  // cross-kind raw field — a `kind: "blob"` item carrying `commit`, a gitlink
  // carrying `blob` — and runs the schema, and `Pack.encode`'s key list emits
  // such a field faithfully. `expose` is a library entry point, so a caller
  // handing it a hand-built pack got that signed onto the append-only trace
  // ref, after which every audit of it reported no readable pack, forever.
  yield* Pack.decode(bytes);

  // Framed before anything is stored, for the same reason: the segment bound
  // is enforced here, and discovering it after the pack blob had been written
  // left an object behind for every refused call.
  const rendered = yield* Render.commit(input.segments);

  const view = unqualify(input.pack.view.tree);
  if (view === null) {
    return yield* new Invalid({
      field: "view",
      reason: `'${input.pack.view.tree}' is not an object id`,
    });
  }
  // Read before the edge is written, because a `context/view` entry naming a
  // blob or a commit is a tree git will fetch and then refuse to walk — and
  // the record it is attached to can never be deleted. `readTree` rather than
  // `readObject`: it answers for the well-known empty tree, which is genuinely
  // the view of a repository whose checkout holds nothing.
  yield* repository.readTree(view).pipe(
    Effect.catchTag("ObjectNotFound", () =>
      Effect.fail(
        new Invalid({
          field: "view",
          reason: `view.tree ${input.pack.view.tree} is not a tree this repository holds`,
        }),
      ),
    ),
  );

  // The pack and the view go in whatever a tombstone says, and the asymmetry
  // with the render below is deliberate rather than an oversight.
  //
  // All three are deterministic, so all three can be resurrected the same way:
  // write the blob again and a removed record's tree entry resolves again.
  // What differs is what withholding costs. A render is *retention* — the
  // digest is what the record commits to, `absent` is a state the protocol
  // already models, and `--retain-render=false` reaches it by asking. A pack
  // is the exposure: `audit` reads `context/pack.json` to check a single item,
  // so an exposure without one verifies nothing, and `context/view` is the
  // Git edge §15's sixth criterion requires. Withholding those would make
  // every later identical exposure permanently unauditable because of a
  // removal it had nothing to do with — the wrong trade against a redacted
  // record's path list, which is structure rather than the content the render
  // holds.
  //
  // So they are written, and `resurrected` says which of them a tombstone
  // names, because the reason this was worth fixing at all is that it happened
  // in silence.
  // The last of `Trace.append`'s refusals still asked after the writes.
  // `capture.transport`, `traceId`, `spanId` and `semconv` are unbounded
  // strings, so a caller handing `expose` a large capture wrote the pack blob,
  // the render blob and the `context/` tree and was then refused for payload
  // size — the orphaned objects every other hoisted check here exists to
  // prevent, reached through the one that was not. Hashed rather than written
  // so the bound can be checked before anything lands.
  const packOid = yield* hashObject({ type: "blob", data: bytes });
  const size = encode({
    type: "context-exposure",
    version: 1,
    repo: input.repo,
    session: input.session,
    id: Event.newId(),
    issuedAt: DateTime.formatIso(yield* DateTime.now),
    trustHead: null,
    pack: qualify(packOid),
    renderFormat: Render.FORMAT,
    renderDigest: rendered.digest,
    capture: input.capture ?? null,
  }).length;
  // A margin for the fields the real payload fills in: a trust head is one
  // qualified oid longer than the `null` measured here, and nothing else
  // differs in length.
  if (size + 64 > Trace.MAX_PAYLOAD) {
    return yield* new Invalid({
      field: "capture",
      reason: `a trace record may not exceed ${Trace.MAX_PAYLOAD} bytes; this one is ${size}`,
    });
  }

  const pack = yield* repository.writeBlob(bytes);
  // The overlay only. On a clean checkout `Pack.capture` reproduces the base
  // commit's own tree oid on purpose, and `Maintenance.gc` re-walks the source
  // refs *without* the exclusion — so that tree was never removed and never
  // could be, while `excluded` lists it because it knows nothing about
  // branches. Reported, it was a permanent false alarm on every clean
  // `context for` over a commit whose exposure had been redacted: an object
  // that never left, announced as one that came back.
  const based = yield* repository
    .readCommit(unqualify(input.pack.view.base) ?? view)
    .pipe(Effect.catchTag("ObjectNotFound", () => Effect.succeed(null)));
  const overlay = based === null || based.tree !== view ? [view] : [];
  const resurrected = [pack, ...overlay].filter((oid) => input.removed?.has(oid) === true);

  const attached: Array<TreeEntry> = [
    { mode: BLOB_MODE, name: "pack.json", oid: pack },
    { mode: TREE_MODE, name: "view", oid: view },
  ];
  // Hashed before it is written, because writing it is the whole problem. A
  // ContextRender is deterministic by design, so re-running one task against
  // one unchanged view frames the same bytes and lands on the same blob — and
  // a redacted record's `context/render.bin` entry, kept on purpose because
  // the commit has to stay for the hash chain, resolves again the moment that
  // object is back in the store. So `trace redact` plus `gc` removed the
  // verbatim task string and every exposed file byte, and the next identical
  // `context for` put them back, readable off the ref by anybody with a clone,
  // with nothing anywhere saying so.
  //
  // An operator who removed exactly these bytes has stated the retention
  // policy for them, and `absent` is what the protocol already calls a render
  // the policy did not keep (§11) — a first-class outcome, not a failure. The
  // exposure is unaffected otherwise: its digest is committed, its pack and
  // items verify, and `--retain-render=false` is the same state reached by
  // asking.
  const render = yield* hashObject({ type: "blob", data: rendered.bytes });
  // Only when retention was asked for, because this field's job is to say
  // *which* of the two reasons there is no render. Computed independently, an
  // operator passing `--retain-render=false` on a repository where an earlier
  // identical render happened to be tombstoned was told a redaction caused
  // what their own flag caused.
  const withheld = input.retain !== false && input.removed?.has(render) === true;
  if (input.retain !== false && !withheld) {
    attached.push({
      mode: BLOB_MODE,
      name: "render.bin",
      oid: yield* repository.writeBlob(rendered.bytes),
    });
  }

  const trustHead = yield* repository.resolve(TRUST_LOG);
  const payload: Payload = {
    type: "context-exposure",
    version: 1,
    repo: input.repo,
    session: input.session,
    id: Event.newId(),
    issuedAt: DateTime.formatIso(yield* DateTime.now),
    trustHead,
    pack: qualify(pack),
    renderFormat: Render.FORMAT,
    renderDigest: rendered.digest,
    capture: input.capture ?? null,
  };

  const commit = yield* Trace.append({
    session: input.session,
    type: payload.type,
    id: payload.id,
    payload: encode(payload),
    key: input.key,
    attach: [
      {
        mode: TREE_MODE,
        name: DIRECTORY,
        oid: yield* repository.writeTree(attached),
      },
    ],
  });

  return {
    commit,
    /** The record's canonical identity, which a later record joins on (§3). */
    oid: qualify(commit),
    id: payload.id,
    pack: qualify(pack),
    digest: rendered.digest,
    /**
     * Whether `context/render.bin` is on the record.
     *
     * Both reasons it might not be, because a reader asking "can I recompute
     * the digest from this repository?" gets one answer and `withheld` gave
     * only half of it: `--retain-render=false` leaves no render either, and a
     * caller reading `!withheld` was told a record with no render had one.
     */
    retained: input.retain !== false && !withheld,
    // (`withheld` already implies `retain !== false`, so the two never both
    // explain the same absence.)
    /** And of the two reasons, whether it was the tombstone. */
    withheld,
    /** Objects a tombstone names that this record has put back in the store. */
    resurrected,
  } as const;
});

// -- reading --------------------------------------------------------------------

/**
 * Every exposure on one session's trace ref, oldest first.
 *
 * Filtered by the record's own declared type rather than by whether it decodes:
 * the ref carries runtime telemetry, tool operations and workspace transitions
 * beside these, and reading "this is not a context exposure" as "this record is
 * unreadable" would report a healthy session as a damaged one.
 *
 * The declared type decides which absences are damage, though, not which
 * records are exposures. A payload that decodes as one is one however its
 * commit message is labelled — the message is an unsigned hint that survives
 * redaction, the payload is what somebody put their key to. Selecting on the
 * hint alone let a mislabelled record sit on the ref unaudited.
 */

export const entries = Effect.fn("context.Exposure.entries")(function* (
  session: string,
  taken?: Trace.Walk,
  /**
   * This repository's own id, where the caller knows it.
   *
   * The other half of the binding. Bound to the session and not to the
   * repository, an exposure naming another repo was still audited as this
   * one's — `audit` then reported `binding: no`, `ok: false`, and the CLI
   * turned that into a non-zero exit. Replication is not policy-gated, which
   * is the premise `foreign` is built on, so a peer could plant one and break
   * `context audit S && deploy` for good; a `hub init` that mints a new
   * `repoId` does the same thing benignly. `Invocation.project` filters
   * exactly this case, so without it the two audit surfaces disagreed about
   * one record.
   */
  repo?: string,
) {
  // A caller holding the walk already hands it in; see `Records.entries`.
  const walked = taken ?? (yield* Trace.walk(session));
  const exposures: Array<{
    readonly commit: Oid;
    readonly payload: Payload;
    readonly bytes: Uint8Array;
    readonly signatures: ReadonlyArray<string>;
  }> = [];
  /** Exposures on this ref whose own envelope names another session or repo. */
  const foreign: Array<Oid> = [];
  // A redaction leaves the commit and takes the payload, so the message is all
  // that is left to say whose record it was — which is why `Claim.ownerOf`
  // reads it as the fallback and why this list is seeded through the same
  // function rather than by testing the message here.
  const unreadable: Array<Oid> = walked.unreadable
    .filter((entry) => Claim.ownerOf(entry) === "context")
    .map((entry) => entry.commit);

  for (const record of walked.records) {
    // One question, asked in one place, for both readers. Six variants of the
    // same defect came from each of them deciding for itself which records
    // were theirs — a payload naming the other namespace, a payload naming
    // nothing, a message naming nothing, a message naming something unknown —
    // and each was closed by adding a case here or there. The partition is
    // total now: `ownerOf` returns one of two values, so a record cannot land
    // in neither, and the two readers cannot disagree because they ask the
    // same function.
    if (Claim.ownerOf(record) !== "context") continue;

    const payload = yield* decode(record.payload).pipe(Effect.orElseSucceed(() => null));
    // Ours and unreadable is damage, and it is the one absence this must not
    // swallow: a record that says it is an exposure and will not decode as one
    // is exactly what an audit exists to report.
    if (payload === null) {
      unreadable.push(record.commit);
      continue;
    }

    // Bound to the ref it was read from. Replication is not policy-gated, so a
    // peer can plant a record naming another session or repository; audited as
    // this session's it failed `binding` and drove the non-zero exit, which
    // means a planted record could fail this repository's deploy gate for
    // good. Reported rather than dropped, because an audit surface that
    // silently discards records is the other half of the same problem.
    if (!Claim.bound(payload, { repo, session })) {
      foreign.push(record.commit);
      continue;
    }
    exposures.push({
      commit: record.commit,
      payload,
      bytes: record.payload,
      signatures: record.signatures,
    });
  }

  // The walk itself comes back, so a caller that needs another reader's view
  // of the same ref does not take it again.
  return { exposures, unreadable, foreign, parents: walked.parents, taken: walked } as const;
});

// -- auditing -------------------------------------------------------------------

export type Check = Pack.Check;

const ok: Check = { ok: true };
const bad = (reason: string): Check => ({ ok: false, reason });

/**
 * What a render can be, once retention has had its say.
 *
 * Three states, not two. `absent` is a policy outcome — retention expired, a
 * redaction ran — and reads as one; `unreadable` is the failure, meaning the
 * bytes that are here are not the bytes that were committed to. An audit that
 * reported them the same way would turn every expired retention into an alarm,
 * and §11 asks for the distinction.
 */
export type RenderStatus =
  | { readonly state: "verified"; readonly segments: ReadonlyArray<SegmentSummary> }
  | { readonly state: "absent"; readonly reason: string }
  | { readonly state: "unreadable"; readonly reason: string }
  /**
   * A framing version this reader does not know.
   *
   * Neither of the other two, and it was being reported as `unreadable` —
   * which this type's own docstring defines as "the bytes that are here are
   * not the bytes that were committed to". So the day a producer writes
   * `git+context-render/v2`, an older clone's `context audit <session> &&
   * deploy` would fail permanently on a valid record. Read-path
   * over-strictness of exactly the kind `Capture.stage`'s bare-string schema
   * exists to avoid: an older reader says what it cannot check, rather than
   * calling it damage.
   */
  | { readonly state: "unsupported"; readonly reason: string };

/**
 * What an audit says about one recomputed segment — never its body.
 *
 * The bodies are the repository bytes the render handed over, which §14 calls
 * out as possibly source or secrets, and an audit is not a way to read them.
 * Carried as values, `--json` serialized each `Uint8Array` as a numeric-keyed
 * object: half a megabyte of `{"0":104,"1":101,…}` per exposure, and the full
 * contents of every exposed file printed by a command whose human form
 * deliberately reduces the same thing to a count. A caller that wants the
 * bytes reads `context/render.bin`, where retention policy can reach them.
 */
export interface SegmentSummary {
  readonly placement: string;
  readonly mediaType: string;
  readonly bytes: number;
}

export interface Audit {
  /** The record's canonical identity. */
  readonly exposure: string;
  readonly payload: Payload | null;
  /** Whether at least one signature over the exact payload bytes verifies. */
  readonly signature: Check;
  /** The fingerprints of the keys whose signatures verified. */
  readonly signers: ReadonlyArray<string>;
  /**
   * Whether a signer this repository trusts could have written this record.
   *
   * `null` when the caller handed in no trust projection. Reported apart from
   * `signature` because they fail for different reasons and a reader acts on
   * them differently: bytes nobody signed are a broken record, and bytes signed
   * by somebody this repository has since revoked are a record whose
   * *authority* lapsed while its evidence stayed exactly as valid as it was.
   */
  readonly trust: Check | null;
  /** Whether the record binds the repository and session it was found under. */
  readonly binding: Check;
  /** Whether `context/pack.json` exists and hashes to `payload.pack`. */
  readonly pack: Check;
  /** Whether `context/view` exists and is `pack.view.tree`. */
  readonly retained: Check;
  readonly evidence: Pack.Report | null;
  /**
   * The pack these checks were made against, decoded once.
   *
   * Handed back because every caller that wants more than a verdict — the
   * Invocation projection counting evidence, `context why` explaining it —
   * would otherwise read the same blob and run the same schema a second time,
   * per exposure, per projection.
   */
  readonly decoded: Pack.Pack | null;
  readonly render: RenderStatus;
  /** Runtime correlation, when the producer recorded any. */
  readonly capture: Capture | null;
  /** Every dimension above that can fail, having not failed. */
  readonly ok: boolean;
}

/**
 * Audit one exposure, reporting every dimension independently.
 *
 * Independently is the whole design. Valid repository evidence, a valid render
 * commitment, an unavailable render body and available runtime correlation are
 * four different facts, and an auditor handed one boolean cannot tell which of
 * them they have — which is how "the render expired" and "the evidence
 * drifted" end up sounding the same (§11).
 *
 * `repo` and `session` are what the caller believes it is auditing. Passing
 * them in is what makes the binding check a check: read out of the record
 * itself it would agree with itself every time.
 */
export const audit = Effect.fn("context.Exposure.audit")(function* (input: {
  readonly commit: Oid;
  readonly repo: string;
  readonly session: string;
  /** Absent where the caller has no membership to judge the signer against. */
  readonly trust?: Projection | null;
  /**
   * One trust-log walk, shared across a run of audits.
   *
   * `Verify.authorize` re-walks the whole log per call without it, so a caller
   * auditing every exposure on a session paid O(records × trust log) — the
   * same reason `hub/Projection.ts` builds one and hands it to every event it
   * judges. Absent, each call walks for itself, which is right for a caller
   * auditing exactly one record.
   */
  readonly reach?: ReturnType<typeof trustReach>;
}) {
  const repository = yield* Repository;
  const exposure = qualify(input.commit);

  const record = yield* Record.read(input.commit, Event.RECORD).pipe(
    Effect.catchTags({
      ObjectNotFound: () => Effect.succeed(null),
      Invalid: () => Effect.succeed(null),
    }),
  );
  if (record === null) {
    return {
      exposure,
      payload: null,
      signature: bad("the record carries no readable payload"),
      signers: [],
      // Not `null`, which means "nobody asked". This return is taken when the
      // payload blob is gone or the signatures are malformed, and a caller who
      // supplied membership deserves to know the judgement could not be made
      // rather than that it was never requested.
      trust:
        input.trust == null
          ? null
          : bad("the record carries no readable payload, so it cannot be judged"),
      binding: bad("no payload to bind"),
      pack: bad("no payload naming a pack"),
      retained: bad("no payload naming a view"),
      evidence: null,
      decoded: null,
      render: { state: "unreadable", reason: "no payload naming a render" },
      capture: null,
      ok: false,
    } satisfies Audit;
  }

  const payload = yield* decode(record.payload).pipe(Effect.orElseSucceed(() => null));

  const found: Array<Fingerprint> = [];
  // Capped, as `trust.Verify.signers` caps: nothing bounds the `.sig` blob —
  // `Trace.MAX_PAYLOAD` covers `event.json` alone and the boundary never folds
  // a trace payload — so a `hub.trace` holder could push a record carrying a
  // hundred thousand armored strings and make every later audit of that ref
  // pay for a hundred thousand signature verifications.
  for (const armored of record.signatures.slice(0, MAX_SIGNATURES)) {
    const key = yield* verify(armored, record.payload, NAMESPACE).pipe(
      Effect.catchTag("Invalid", () => Effect.succeed(null)),
    );
    // A signature that does not parse is skipped rather than fatal, for the
    // reason `trust.Verify.signers` skips one: otherwise anybody who may append
    // to this ref could discredit a record by adding junk beside a good
    // signature.
    if (key !== null) found.push(yield* fingerprint(key));
  }
  const signature = found.length > 0 ? ok : bad("no signature over these bytes verifies");

  // Asked of the record's own bytes and signatures rather than of `found`, so
  // that revocation and expiry are judged by the trust log rather than by
  // whether a signature parses.
  // A timestamp that will not parse is judged here, not refused at `decode`.
  // `Verify.Made` takes a `Date`, and `new Date("not-a-date")` is an `Invalid
  // Date` that compares false against everything — so the record would be
  // judged against a moment that does not exist. Refusing it in `decode`
  // instead would have made the whole exposure unreadable over a field only
  // this needs, which is the read-path over-strictness `checkCapture` is
  // written to avoid.
  const dated = payload === null ? Number.NaN : Date.parse(payload.issuedAt);
  const trust =
    input.trust == null
      ? null
      : Number.isNaN(dated) && payload !== null
        ? bad(`the record is dated '${payload.issuedAt}', which is not a date`)
        : payload === null
          ? // Not `null`, which means "nobody asked". A payload that does not
            // decode cannot be dated, so the judgement cannot be made — and
            // collapsing the two left a `--json` reader unable to tell a caller
            // who supplied no membership from a record somebody tampered with.
            bad("the payload does not decode, so it cannot be judged against what its signer held")
          : yield* trusted(
              input.trust,
              record.payload,
              record.signatures,
              { at: new Date(dated), trustHead: headOf(payload.trustHead) },
              found,
              input.reach,
            );

  if (payload === null) {
    return {
      exposure,
      payload: null,
      signature,
      signers: found,
      trust,
      binding: bad("the payload is not a readable context exposure"),
      pack: bad("no payload naming a pack"),
      retained: bad("no payload naming a view"),
      evidence: null,
      decoded: null,
      render: { state: "unreadable", reason: "no payload naming a render" },
      capture: null,
      ok: false,
    } satisfies Audit;
  }

  const binding =
    payload.repo !== input.repo
      ? bad(`the record names repository ${payload.repo}`)
      : payload.session !== input.session
        ? bad(`the record names session ${payload.session}`)
        : ok;

  const info = yield* repository.readCommit(input.commit);
  const at = (path: string) =>
    repository
      .findPath(info.tree, path)
      .pipe(Effect.catchTag("ObjectNotFound", () => Effect.succeed(null)));

  const packEntry = yield* at(PACK);
  const packed =
    packEntry === null
      ? null
      : yield* repository
          .readBlob(packEntry.oid)
          .pipe(Effect.catchTag("ObjectNotFound", () => Effect.succeed(null)));

  // Identity is the oid of the *retained bytes*, compared against what the
  // signed payload names. Re-encoding the decoded pack and hashing that would
  // check this implementation against itself and would pass on a record whose
  // retained bytes had been swapped for equivalent ones.
  const pack =
    packEntry === null
      ? bad(`${PACK} is not retained`)
      : packed === null
        ? bad(`${PACK} is unavailable`)
        : qualify(packEntry.oid) === payload.pack
          ? ok
          : bad(`${PACK} is ${qualify(packEntry.oid)}, not ${payload.pack}`);

  const decoded =
    packed === null ? null : yield* Pack.decode(packed).pipe(Effect.orElseSucceed(() => null));

  const viewEntry = yield* at(VIEW);
  const retained =
    decoded === null
      ? bad("no readable pack naming a view")
      : viewEntry === null
        ? bad(`${VIEW} is not retained, so view.tree is reachable through nothing`)
        : qualify(viewEntry.oid) === decoded.view.tree
          ? ok
          : bad(`${VIEW} is ${qualify(viewEntry.oid)}, not ${decoded.view.tree}`);

  const evidence = decoded === null ? null : yield* Pack.verify(decoded);

  const render: RenderStatus = yield* renderStatus(info.tree, payload);

  return {
    exposure,
    payload,
    signature,
    signers: found,
    trust,
    binding,
    pack,
    retained,
    evidence,
    decoded,
    render,
    capture: payload.capture,
    ok:
      signature.ok &&
      (trust === null || trust.ok) &&
      binding.ok &&
      pack.ok &&
      retained.ok &&
      evidence !== null &&
      evidence.ok &&
      render.state !== "unreadable",
  } satisfies Audit;
});

/**
 * Whether a signer this repository trusts could have written these bytes.
 *
 * Delegated to `trust.Verify.authorize` rather than looked up in the
 * projection's member map, because a membership lookup is the half of the
 * question that does not move: revocation, expiry and stable-identity grants
 * all live in the walk, and a check that skipped them would report a revoked
 * key's record as trusted.
 *
 * `made` is what turns this from a live authorization request into a judgement
 * about a *stored* one, and it is the difference between two opposite answers.
 * Without it, a key that held `hub.trace` on Monday and was revoked on Tuesday
 * makes every exposure it ever signed read as untrusted from Wednesday — a
 * record that was validly authorized when written, reported as one that was
 * not. The payload carries `issuedAt` and the trust head its signer was
 * writing against precisely so this question is answerable, and every other
 * stored-record judgement in the codebase supplies it (`hub/Projection.ts`,
 * `social/Log.ts`). A compromise revocation still reaches backwards; that is
 * `reachesWindows`' decision to make, not this one's.
 */
/**
 * The trust head a payload names, as an oid.
 *
 * Stored raw rather than qualified — `expose` writes `resolve(TRUST_LOG)`
 * straight through, the way every other hub envelope does. Anything else is a
 * head this repository cannot resolve, and `null` reads as "they had seen
 * everything", which is the conservative answer a record that cannot show
 * otherwise deserves.
 */
const headOf = (value: string | null): Oid | null =>
  value !== null && isOid(value) ? value : null;

const trusted = Effect.fn("context.Exposure.trusted")(function* (
  projection: Projection,
  bytes: Uint8Array,
  signatures: ReadonlyArray<string>,
  made: Verify.Made,
  signed: ReadonlyArray<Fingerprint>,
  reach?: ReturnType<typeof trustReach>,
) {
  // `signed` is why this does not verify the same list a second time.
  // `authorize` calls `Verify.signers` for itself otherwise, and the audit has
  // already done exactly that work — over attacker-supplied input, on a path
  // that runs once per record.
  const asked = { projection, bytes, signatures, capability: CAPABILITY, made, signed };
  const decision = yield* reach === undefined
    ? Verify.authorize(asked)
    : Verify.authorize({ ...asked, seen: reach.ancestry, contains: reach.contains });
  return decision.ok ? ok : bad(decision.reason);
});

const renderStatus = Effect.fn("context.Exposure.renderStatus")(function* (
  tree: Oid,
  payload: Payload,
): Effect.fn.Return<RenderStatus, ObjectNotFound | StorageFailure, Repository> {
  if (payload.renderFormat !== Render.FORMAT) {
    return {
      state: "unsupported",
      reason: `'${payload.renderFormat}' is not a framing this version reads`,
    };
  }

  const repository = yield* Repository;
  const entry = yield* repository
    .findPath(tree, RENDER)
    .pipe(Effect.catchTag("ObjectNotFound", () => Effect.succeed(null)));
  if (entry === null) {
    return {
      state: "absent",
      reason: `${RENDER} is not retained; the commitment stands but cannot be recomputed`,
    };
  }

  const bytes = yield* repository
    .readBlob(entry.oid)
    .pipe(Effect.catchTag("ObjectNotFound", () => Effect.succeed(null)));
  if (bytes === null) {
    return { state: "absent", reason: `${RENDER} has been collected or redacted` };
  }

  const checked = yield* Render.recompute(bytes, payload.renderDigest);
  return checked.ok
    ? {
        state: "verified",
        segments: checked.segments.map((segment) => ({
          placement: segment.placement,
          mediaType: segment.mediaType,
          bytes: segment.body.length,
        })),
      }
    : { state: "unreadable", reason: checked.reason };
});

/**
 * The pack an exposure retained, for a reader that wants to explain it.
 *
 * Read out of the record's own tree rather than out of the object database by
 * oid: the retained bytes are the ones the signature commits to, and an object
 * with the same oid reached another way is the same bytes only because the
 * oid says so — which is exactly what the audit checks and this call does not
 * have to repeat.
 */
export const packOf = Effect.fn("context.Exposure.packOf")(function* (commit: Oid) {
  const repository = yield* Repository;
  const info = yield* repository.readCommit(commit);
  const entry = yield* repository
    .findPath(info.tree, PACK)
    .pipe(Effect.catchTag("ObjectNotFound", () => Effect.succeed(null)));
  // The blob as well as the entry, and for the same reason. A redaction is
  // *designed* to leave the tree entry naming a blob that is gone — the commit
  // has to stay for the hash chain — so this is the ordinary post-`gc` state
  // of a removed exposure, not a corrupt repository. Guarded on the lookup and
  // not on the read, `context why <a redacted exposure>` came back with a raw
  // `ObjectNotFound` where every other reader of this data says what happened.
  const bytes =
    entry === null
      ? null
      : yield* repository
          .readBlob(entry.oid)
          .pipe(Effect.catchTag("ObjectNotFound", () => Effect.succeed(null)));
  if (entry === null || bytes === null) {
    return yield* new Invalid({ field: "pack", reason: `${qualify(commit)} retains no ${PACK}` });
  }
  return { oid: entry.oid, bytes } as const;
});

/**
 * The payload one record carries, without auditing anything.
 *
 * For a caller that needs what a record *says* rather than whether it holds —
 * locating which ref a commit belongs to, say. `audit` answers that too, at
 * the cost of a tree walk per evidence item and a SHA-256 over the retained
 * render, which is a great deal of work to read one field.
 */
export const payloadOf = Effect.fn("context.Exposure.payloadOf")(function* (commit: Oid) {
  const read = yield* Record.read(commit, Event.RECORD).pipe(
    Effect.catchTags({
      ObjectNotFound: () => Effect.succeed(null),
      Invalid: () => Effect.succeed(null),
    }),
  );
  if (read === null) return null;
  return yield* decode(read.payload).pipe(Effect.orElseSucceed(() => null));
});

/** `sha1:<hex>` for a record commit, which is an exposure's canonical id. */
export const identify = (commit: Oid): string => qualify(commit);

/** The record commit a qualified exposure id names. */
export const resolve = (value: string): Result.Result<Oid, Invalid> => {
  const oid = unqualify(value);
  return oid === null
    ? Result.fail(
        new Invalid({ field: "exposure", reason: `'${value}' is not a qualified record oid` }),
      )
    : Result.succeed(oid);
};

export type ExposureError = Invalid | ObjectNotFound | StorageFailure;
