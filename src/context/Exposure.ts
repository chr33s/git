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

import { fingerprint, NAMESPACE, type PrivateKey, verify } from "../crypto/SshSignature.ts";
import { Invalid, type ObjectNotFound, type StorageFailure } from "../git/Error.ts";
import type { TreeEntry } from "../git/Format.ts";
import { qualify, unqualify } from "../git/Oid.ts";
import { Repository } from "../git/Repository.ts";
import { TRUST_LOG } from "../git/Refspec.ts";
import type { Oid } from "../git/Store.ts";
import * as Event from "../hub/Event.ts";
import * as Trace from "../hub/Trace.ts";
import type { Projection } from "../trust/Projection.ts";
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
const TREE_MODE = "040000";

/**
 * Runtime correlation, and nothing more.
 *
 * An OTel trace id, a provider request id and a harness event id are all
 * identifiers somebody else minted. They are recorded because an operator
 * debugging a run needs the join, and they are fenced off here because §9 does
 * not let them affect pack identity, render verification, authority or record
 * identity — a record whose identity depended on a provider's id would be a
 * Git record a provider could rename.
 */
export const Capture = Schema.Struct({
  transport: Schema.String,
  traceId: Schema.optional(Schema.String),
  spanId: Schema.optional(Schema.String),
});
export type Capture = typeof Capture.Type;

/**
 * The signed claim.
 *
 * The envelope is the one every hub record carries — repository, session, id,
 * time, trust head — because a payload that did not name its repository is one
 * that can be replayed into another, and cli.md §8 makes binding both the
 * recorder's job.
 */
export const Payload = Schema.Struct({
  type: Schema.tag("context-exposure"),
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
        "traceId",
        "spanId",
      ],
      2,
    )}\n`,
  );

export const decode = Effect.fn("context.Exposure.decode")(function* (bytes: Uint8Array) {
  const json: unknown = yield* Effect.try({
    try: () => JSON.parse(decoder.decode(bytes)),
    catch: () => new Invalid({ field: "exposure", reason: "trace record is not valid JSON" }),
  });
  return yield* decodePayload(json).pipe(
    Effect.mapError(
      (issue) =>
        new Invalid({ field: "exposure", reason: `malformed context exposure: ${issue.message}` }),
    ),
  );
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
  readonly capture?: Capture | null;
}) {
  const repository = yield* Repository;

  const bytes = Pack.encode(input.pack);
  if (bytes.length > Pack.MAX_PAYLOAD) {
    return yield* new Invalid({
      field: "pack",
      reason: `a context pack may not exceed ${Pack.MAX_PAYLOAD} bytes; this one is ${bytes.length}`,
    });
  }
  const pack = yield* repository.writeBlob(bytes);

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

  const rendered = yield* Render.commit(input.segments);

  const attached: Array<TreeEntry> = [
    { mode: BLOB_MODE, name: "pack.json", oid: pack },
    { mode: TREE_MODE, name: "view", oid: view },
  ];
  if (input.retain !== false) {
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

  return { commit, id: payload.id, pack: qualify(pack), digest: rendered.digest } as const;
});

// -- reading --------------------------------------------------------------------

/** Every exposure on one session's trace ref, oldest first. */
export const entries = Effect.fn("context.Exposure.entries")(function* (session: string) {
  const walked = yield* Event.walk(Trace.refOf(session), decode);
  return {
    exposures: walked.records.map((record) => ({
      commit: record.commit,
      payload: record.payload,
      bytes: record.bytes,
      signatures: record.signatures,
    })),
    unreadable: walked.unreadable,
  } as const;
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
  | { readonly state: "verified"; readonly segments: ReadonlyArray<Render.Segment> }
  | { readonly state: "absent"; readonly reason: string }
  | { readonly state: "unreadable"; readonly reason: string };

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
      trust: null,
      binding: bad("no payload to bind"),
      pack: bad("no payload naming a pack"),
      retained: bad("no payload naming a view"),
      evidence: null,
      render: { state: "unreadable", reason: "no payload naming a render" },
      capture: null,
      ok: false,
    } satisfies Audit;
  }

  const payload = yield* decode(record.payload).pipe(Effect.orElseSucceed(() => null));

  const found: Array<string> = [];
  for (const armored of record.signatures) {
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
  const trust =
    input.trust == null ? null : yield* trusted(input.trust, record.payload, record.signatures);

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
 */
const trusted = Effect.fn("context.Exposure.trusted")(function* (
  projection: Projection,
  bytes: Uint8Array,
  signatures: ReadonlyArray<string>,
) {
  const decision = yield* Verify.authorize({
    projection,
    bytes,
    signatures,
    capability: CAPABILITY,
  });
  return decision.ok ? ok : bad(decision.reason);
});

const renderStatus = Effect.fn("context.Exposure.renderStatus")(function* (
  tree: Oid,
  payload: Payload,
): Effect.fn.Return<RenderStatus, ObjectNotFound | StorageFailure, Repository> {
  if (payload.renderFormat !== Render.FORMAT) {
    return {
      state: "unreadable",
      reason: `'${payload.renderFormat}' is not ${Render.FORMAT}`,
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
    ? { state: "verified", segments: checked.segments }
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
  if (entry === null) {
    return yield* new Invalid({ field: "pack", reason: `${qualify(commit)} retains no ${PACK}` });
  }
  return { oid: entry.oid, bytes: yield* repository.readBlob(entry.oid) } as const;
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
