/**
 * `knowledge check`: what a Concept still has behind it, dimension by
 * dimension.
 *
 * ```text
 * selected Repository View + trust snapshot + evaluation clock
 *          ↓
 *   structure · citations · dependencies · temporal · external · retention
 *          ↓
 *   independent outcomes, never one `trusted` boolean
 * ```
 *
 * The dimensions stay apart on purpose (docs/context-pack.knowledge.md §6.1,
 * INV-06). "The signature verifies", "the file it cites has not moved", "the
 * deadline has not passed" and "the snapshot bytes are still retained" are
 * four different facts, and a product that folds them into a confidence score
 * teaches its readers to act on a number none of them means.
 *
 * Three things this deliberately never establishes. A citation that verifies
 * proves provenance, not that the prose is true (§6.2). A changed dependency
 * means *revalidate*, not that the Concept is false (§13). And an offline
 * check says `unknown` about the live web rather than pretending an old
 * snapshot is evidence that a page is unchanged (§6.4) — nothing here opens a
 * socket or runs anything a Concept names.
 */
import { Effect, Predicate } from "effect";

import { type Invalid, type ObjectNotFound, type StorageFailure } from "../git/Error.ts";
import { isGitlink, isTree } from "../git/Format.ts";
import { qualify, unqualify } from "../git/Oid.ts";
import { Repository } from "../git/Repository.ts";
import { TRUST_LOG } from "../git/Refspec.ts";
import { isOid, type Oid } from "../git/Store.ts";
import * as Pack from "../context/Pack.ts";
import * as Event from "../hub/Event.ts";
import * as Session from "../hub/Session.ts";
import * as Tombstone from "../hub/Tombstone.ts";
import { trustReach } from "../hub/Projection.ts";
import type { Projection } from "../trust/Projection.ts";
import * as Record from "../trust/Record.ts";
import * as Verify from "../trust/Verify.ts";
import * as Concept from "./Concept.ts";

/** This checker's own version, recorded in every report and cache stamp. */
export const CHECKER_VERSION = "knowledge-check/1";

/** Host safeguards, not claims that the whole corpus was inspected (§14). */
export const LIMITS = { concepts: 512, citations: 64, evidence: 256 } as const;

export interface Limits {
  readonly concepts?: number;
  readonly citations?: number;
  readonly evidence?: number;
}

// -- dimensions -----------------------------------------------------------------

export type Structure =
  | { readonly state: "valid" }
  | { readonly state: "invalid"; readonly reason: string }
  | { readonly state: "unsupported-profile"; readonly reason: string };

export type CitationState =
  | "accepted"
  | "invalid"
  | "unavailable"
  | "redacted"
  | "unsupported-record";

export interface Citation {
  readonly record: string;
  readonly state: CitationState;
  readonly reason?: string;
  /** The session the accepted record belongs to; display metadata only. */
  readonly session?: string;
}

export type DependencyState = "unchanged" | "changed" | "missing" | "unknown";

export interface Dependency {
  readonly kind: "blob" | "gitlink";
  readonly path: string;
  readonly recorded: string;
  readonly state: DependencyState;
  readonly found?: string;
  readonly reason?: string;
}

export type Temporal =
  | { readonly state: "within-deadline"; readonly deadline: string }
  | { readonly state: "stale"; readonly deadline: string }
  | { readonly state: "no-deadline" }
  | { readonly state: "invalid"; readonly deadline: string; readonly reason: string };

export type ExternalState =
  | "unchanged"
  | "changed"
  | "stale"
  | "unavailable"
  | "unknown"
  | "not-applicable";

export type Retention = "retained" | "missing" | "not-retained" | "unknown";

export interface ExternalSource {
  readonly id: string;
  readonly state: ExternalState;
  readonly retention: Retention;
  readonly reason?: string;
}

export interface Verification {
  readonly record: string;
  readonly state: CitationState;
  readonly reason?: string | undefined;
}

export type Completeness = "complete" | "limited" | "cancelled" | "unavailable";

export interface Eligibility {
  /** Whether this may be folded into the automatic startup Memory (§7). */
  readonly startup: boolean;
  /** Whether normal automatic knowledge recall may include it. */
  readonly recall: boolean;
  readonly reasons: ReadonlyArray<string>;
}

export interface Checked {
  readonly id: string;
  readonly path: string;
  readonly blob: string;
  readonly type: string | null;
  readonly title: string | null;
  readonly structure: Structure;
  readonly lifecycle: Concept.Lifecycle | "unknown";
  readonly citations: ReadonlyArray<Citation>;
  readonly repositoryEvidence: ReadonlyArray<Dependency>;
  readonly temporal: Temporal;
  readonly external: ReadonlyArray<ExternalSource>;
  readonly verificationRecords: ReadonlyArray<Verification>;
  readonly eligibility: Eligibility;
  readonly diagnostics: ReadonlyArray<Concept.Diagnostic>;
  /** The parsed document, for callers that go on to recall or summarize it. */
  readonly concept: Concept.Concept | null;
}

export interface Report {
  readonly version: 1;
  readonly bundle: string;
  readonly bundleAbsent: boolean;
  readonly view: Pack.View;
  readonly evaluatedAt: string;
  readonly profile: {
    readonly okfRevision: string;
    readonly parserVersion: string;
    readonly checkerVersion: string;
  };
  /** What the evaluation actually read; the key any cache of it must use. */
  readonly inputStamp: string;
  readonly complete: boolean;
  readonly completeness: Completeness;
  readonly concepts: ReadonlyArray<Checked>;
  readonly diagnostics: ReadonlyArray<Concept.Diagnostic>;
}

export interface Input {
  readonly view: Pack.View;
  /** `| undefined` throughout, so a caller can pass its own optionals through. */
  readonly bundle?: string | undefined;
  /** A Concept id or repository-relative path; absent checks the bundle. */
  readonly concept?: string | null | undefined;
  /**
   * Only these repository-relative paths, where a caller has already chosen.
   *
   * Recall matches lexically before it verifies, so the signature-checking
   * pass runs over the handful that matched rather than the whole bundle. An
   * empty list checks nothing and says so through `completeness`; a path not
   * in the bundle is simply not there, not an error the way `concept` is.
   */
  readonly paths?: ReadonlyArray<string> | undefined;
  /**
   * The view's files, where a caller has already listed them.
   *
   * The selector lists the whole tree to search it, and listing it again to
   * find the bundle doubled the one full-tree read in a `context for`.
   */
  readonly files?: ReadonlyArray<Listed> | undefined;
  readonly evaluationTime: Date;
  /** This repository's identity, without which a citation cannot be bound. */
  readonly repo?: string | null | undefined;
  /** Membership as it stands; absent means provenance cannot be judged. */
  readonly trust?: Projection | null | undefined;
  readonly limits?: Limits | undefined;
  /**
   * Whether restricted consumers get opaque diagnostics (§6.1).
   *
   * `aggregate` withholds paths, oids and signer detail from the messages, for
   * a caller whose reader may not see the whole view.
   */
  readonly diagnostics?: "path" | "aggregate";
}

const { diagnostic } = Concept;

// -- citations ------------------------------------------------------------------

/** What one check run has learned about a session ref, walked once. */
interface Held {
  /** Every record commit on the ref, decodable or not. */
  readonly onRef: ReadonlySet<Oid>;
  /** What the ref's counted tombstones have removed. */
  readonly removals: ReadonlySet<string>;
}

interface Judge {
  readonly repo: string | null;
  readonly trust: Projection | null;
  readonly reach: ReturnType<typeof trustReach>;
  /**
   * Session refs this run has walked, by id, or `null` for one it does not hold.
   *
   * A citation asked "is this record on the ref it claims?" with a fresh
   * ancestry walk each time, and then walked the same ref again for its
   * tombstones — per citation, across every Concept in the bundle. One walk
   * answers both for every citation naming that session.
   */
  readonly sessions: Map<string, Held | null>;
}

/** One session ref, walked once per check run. */
const heldOf = Effect.fn("knowledge.Check.heldOf")(function* (session: string, judge: Judge) {
  const known = judge.sessions.get(session);
  if (known !== undefined) return known;
  const walked = yield* Session.entries(session);
  const held: Held | null =
    walked.head === null
      ? null
      : {
          onRef: new Set<Oid>([
            walked.head,
            ...walked.events.map((event) => event.commit),
            ...walked.unreadable,
          ]),
          // `Tombstone.removals` is the one fold every reader takes; without
          // membership nothing counts, and the citation has already come back
          // `unavailable` before this is asked.
          removals:
            judge.repo === null
              ? new Set<string>()
              : yield* Tombstone.removals(walked.events, judge.repo, judge.trust),
        };
  judge.sessions.set(session, held);
  return held;
});

/**
 * One canonical record citation, judged under the trust state as it stands.
 *
 * Existence in the object database is not acceptance (§6.2): the record has to
 * decode as a session record, name *this* repository, sit on the session ref
 * it claims, carry a signature that verifies, and its signer has to hold
 * `hub.session` under the membership the caller supplied. A source-only clone
 * that has never fetched the session refs reports `unavailable` — it does not
 * fabricate acceptance, and it does not go and fetch them.
 */
const checkCitation = Effect.fn("knowledge.Check.checkCitation")(function* (
  reference: string,
  judge: Judge,
): Effect.fn.Return<Citation, Invalid | ObjectNotFound | StorageFailure, Repository> {
  const repository = yield* Repository;
  const oid = unqualify(reference);
  if (oid === null) {
    return {
      record: reference,
      state: "unsupported-record",
      reason: "a citation must name a qualified record commit oid",
    };
  }

  const object = yield* repository
    .readObject(oid)
    .pipe(Effect.catchTag("ObjectNotFound", () => Effect.succeed(null)));
  if (object === null) {
    return {
      record: qualify(oid),
      state: "unavailable",
      reason: "this replica does not hold the cited record",
    };
  }
  if (object.type !== "commit") {
    return {
      record: qualify(oid),
      state: "unsupported-record",
      reason: `the citation names a ${object.type}, which is not a record`,
    };
  }

  const record = yield* Record.read(oid, Event.RECORD).pipe(
    Effect.catchTags({
      ObjectNotFound: () => Effect.succeed(null),
      Invalid: () => Effect.succeed(null),
    }),
  );
  if (record === null) {
    return {
      record: qualify(oid),
      state: "unavailable",
      reason: "the record carries no readable payload",
    };
  }

  const payload = yield* Session.decode(record.payload).pipe(Effect.orElseSucceed(() => null));
  if (payload === null) {
    return {
      record: qualify(oid),
      state: "invalid",
      reason: "the payload is not a readable session record",
    };
  }

  if (judge.repo === null) {
    return {
      record: qualify(oid),
      state: "unavailable",
      reason: "this repository has no genesis, so a record's binding cannot be checked",
    };
  }
  if (payload.repo !== judge.repo) {
    return {
      record: qualify(oid),
      state: "invalid",
      reason: `the record names repository ${payload.repo}`,
      session: payload.session,
    };
  }

  // The ref it claims has to actually hold it. A signed object that merely
  // exists in the object database — pushed in, fetched in, or left behind by a
  // dropped ref — is not a record of this repository's session log.
  const held = yield* heldOf(payload.session, judge);
  if (held === null) {
    return {
      record: qualify(oid),
      state: "unavailable",
      reason: `this replica holds no ${Session.refOf(payload.session)}`,
      session: payload.session,
    };
  }
  if (!held.onRef.has(oid)) {
    return {
      record: qualify(oid),
      state: "invalid",
      reason: `the record is not on ${Session.refOf(payload.session)}`,
      session: payload.session,
    };
  }

  if (judge.trust === null) {
    return {
      record: qualify(oid),
      state: "unavailable",
      reason: "no trust projection was supplied, so provenance cannot be judged",
      session: payload.session,
    };
  }

  // A counted redaction outranks the bytes. §6.2: current revocations and
  // tombstones decide, not the presence of a payload.
  if (held.removals.has(qualify(oid)) || held.removals.has(oid)) {
    return {
      record: qualify(oid),
      state: "redacted",
      reason: "a counted tombstone names this record",
      session: payload.session,
    };
  }

  const dated = Date.parse(payload.issuedAt);
  if (Number.isNaN(dated)) {
    return {
      record: qualify(oid),
      state: "invalid",
      reason: `the record is dated '${payload.issuedAt}', which is not a date`,
      session: payload.session,
    };
  }

  const decision = yield* Verify.authorize({
    projection: judge.trust,
    bytes: record.payload,
    signatures: record.signatures,
    capability: payload.type === "event.redacted" ? "hub.redact" : "hub.session",
    made: {
      at: new Date(dated),
      // A declared head that is not an oid is not a head. `Verify.Made` takes
      // one or nothing, and a malformed string must not become a floor.
      trustHead: payload.trustHead !== null && isOid(payload.trustHead) ? payload.trustHead : null,
    },
    seen: judge.reach.ancestry,
    contains: judge.reach.contains,
  });
  return decision.ok
    ? { record: qualify(oid), state: "accepted", session: payload.session }
    : {
        record: qualify(oid),
        state: "invalid",
        reason: decision.reason,
        session: payload.session,
      };
});

// -- repository dependencies ----------------------------------------------------

/**
 * One declared dependency against the selected tree (§6.3).
 *
 * The four outcomes are kept genuinely apart. A path that is absent from a
 * tree this repository can read is `missing`; a tree it *cannot* read is
 * `unknown`, because turning a partial clone into "the file is gone" would
 * report a deletion that never happened.
 */
const checkDependency = Effect.fn("knowledge.Check.checkDependency")(function* (
  tree: Oid,
  item: Concept.Evidence,
): Effect.fn.Return<Dependency, StorageFailure, Repository> {
  const repository = yield* Repository;
  const recorded = item.kind === "blob" ? item.blob : item.commit;
  const oid = unqualify(recorded);
  if (oid === null) {
    return {
      kind: item.kind,
      path: item.path,
      recorded,
      state: "unknown",
      reason: `'${recorded}' is not an object id`,
    };
  }

  const entry = yield* repository
    .findPath(tree, item.path)
    .pipe(Effect.catchTag("ObjectNotFound", () => Effect.succeed("unreadable" as const)));
  if (entry === "unreadable") {
    return {
      kind: item.kind,
      path: item.path,
      recorded,
      state: "unknown",
      reason: "the containing tree could not be read from this replica",
    };
  }
  if (entry === null) {
    return { kind: item.kind, path: item.path, recorded, state: "missing" };
  }

  const gitlink = isGitlink(entry.mode);
  if (item.kind === "gitlink" ? !gitlink : gitlink || isTree(entry.mode)) {
    return {
      kind: item.kind,
      path: item.path,
      recorded,
      state: "missing",
      found: qualify(entry.oid),
      reason: `${item.path} is mode ${entry.mode}, not ${item.kind} evidence`,
    };
  }
  if (entry.oid !== oid) {
    return {
      kind: item.kind,
      path: item.path,
      recorded,
      state: "changed",
      found: qualify(entry.oid),
    };
  }

  // A declared range has to be one the current bytes can satisfy. Symlinks are
  // read as their link-target bytes and never followed (§6.3).
  if (item.kind === "blob" && item.range !== undefined) {
    const bytes = yield* repository
      .readBlob(entry.oid)
      .pipe(Effect.catchTag("ObjectNotFound", () => Effect.succeed(null)));
    if (bytes === null) {
      return {
        kind: item.kind,
        path: item.path,
        recorded,
        state: "unknown",
        reason: "the blob is not available on this replica",
      };
    }
    const [start, end] = item.range;
    if (!Pack.inRange(start, end, bytes.length)) {
      return {
        kind: item.kind,
        path: item.path,
        recorded,
        state: "unknown",
        reason: `[${start}, ${end}) is not a range within ${bytes.length} bytes`,
      };
    }
  }

  return { kind: item.kind, path: item.path, recorded, state: "unchanged" };
});

// -- external sources -----------------------------------------------------------

const SHA256 = /^sha256:[0-9a-f]{64}$/u;

/**
 * A declared external capture, checked offline (§6.4).
 *
 * Nothing here fetches. What can be established locally is whether the
 * declared digest is even well formed and whether a retained snapshot actually
 * exists in this repository — and those are two identities, not one: a Git
 * blob oid and a raw-content SHA-256 digest are computed differently, so a
 * `snapshot` value in YAML is not by itself a retention path.
 */
const checkExternal = Effect.fn("knowledge.Check.checkExternal")(function* (
  source: Concept.External,
): Effect.fn.Return<ExternalSource, StorageFailure, Repository> {
  const repository = yield* Repository;

  if (source.contentDigest !== undefined && !SHA256.test(source.contentDigest)) {
    return {
      id: source.id,
      state: "unknown",
      retention: "unknown",
      reason: `'${source.contentDigest}' is not a sha256 content digest`,
    };
  }
  if (source.retrievedAt !== undefined) {
    const observed = Concept.instantOf(source.retrievedAt);
    if (observed.state === "invalid") {
      return { id: source.id, state: "unknown", retention: "unknown", reason: observed.reason };
    }
  }

  if (source.snapshot === undefined) {
    return { id: source.id, state: "unknown", retention: "not-retained" };
  }
  const oid = unqualify(source.snapshot);
  if (oid === null) {
    // A raw-content digest is not an object address; saying so is the whole
    // point of keeping the two identities apart.
    return {
      id: source.id,
      state: "unknown",
      retention: "not-retained",
      reason: "the snapshot value is a content digest, not a retained object id",
    };
  }
  const held = yield* repository.contains(oid);
  return {
    id: source.id,
    state: "unknown",
    retention: held ? "retained" : "missing",
    reason: held ? undefined : "the declared snapshot object is not in this repository",
  };
});

// -- one Concept ----------------------------------------------------------------

const unreadable = (
  id: string,
  path: string,
  blob: string,
  diagnostics: ReadonlyArray<Concept.Diagnostic>,
  reason: string,
): Checked => ({
  id,
  path,
  blob,
  type: null,
  title: null,
  structure: { state: "invalid", reason },
  lifecycle: "unknown",
  citations: [],
  repositoryEvidence: [],
  temporal: { state: "no-deadline" },
  external: [],
  verificationRecords: [],
  eligibility: { startup: false, recall: false, reasons: ["structure-invalid"] },
  diagnostics,
  concept: null,
});

const checkConcept = Effect.fn("knowledge.Check.checkConcept")(function* (input: {
  readonly bundle: string;
  readonly path: string;
  readonly blob: Oid;
  readonly tree: Oid;
  readonly evaluationTime: Date;
  readonly judge: Judge;
  readonly limits: Required<Limits>;
}): Effect.fn.Return<Checked, Invalid | ObjectNotFound | StorageFailure, Repository> {
  const repository = yield* Repository;
  const qualified = qualify(input.blob);
  const bytes = yield* repository
    .readBlob(input.blob)
    .pipe(Effect.catchTag("ObjectNotFound", () => Effect.succeed(null)));
  if (bytes === null) {
    return unreadable(
      Concept.idOf(input.bundle, input.path) ?? input.path,
      input.path,
      qualified,
      [
        diagnostic("knowledge.unavailable", "error", `${input.path} is not available here`, {
          path: input.path,
        }),
      ],
      "the Concept blob is not available on this replica",
    );
  }

  const parsed = Concept.parse({ bundle: input.bundle, path: input.path, bytes });
  if (!parsed.ok) {
    const first = parsed.diagnostics[0];
    return unreadable(
      Concept.idOf(input.bundle, input.path) ?? input.path,
      input.path,
      qualified,
      parsed.diagnostics,
      first?.message ?? "the document is not a readable Concept",
    );
  }

  const { concept } = parsed;
  const diagnostics: Array<Concept.Diagnostic> = [...parsed.diagnostics];
  const where = { path: input.path };

  const citations: Array<Citation> = [];
  for (const reference of concept.cites.slice(0, input.limits.citations)) {
    const checked = yield* checkCitation(reference, input.judge);
    citations.push(checked);
    if (checked.state !== "accepted") {
      diagnostics.push(
        diagnostic(
          `knowledge.citation.${checked.state}`,
          checked.state === "unavailable" ? "warning" : "error",
          `${checked.record}: ${checked.reason ?? checked.state}`,
          { ...where, field: "gitplus.cites" },
        ),
      );
    }
  }
  if (concept.cites.length > input.limits.citations) {
    diagnostics.push(
      diagnostic(
        "knowledge.limit.citations",
        "warning",
        `only the first ${input.limits.citations} citations were evaluated`,
        where,
      ),
    );
  }

  const repositoryEvidence: Array<Dependency> = [];
  // Truncation is stated, and it disqualifies. Silently checking a prefix
  // would let a Concept whose 300th dependency is missing report fully
  // eligible on support nothing evaluated.
  const overflowed = concept.evidence.length > input.limits.evidence;
  if (overflowed) {
    diagnostics.push(
      diagnostic(
        "knowledge.limit.evidence",
        "error",
        `only the first ${input.limits.evidence} of ${concept.evidence.length} declared dependencies were evaluated`,
        { ...where, field: "gitplus.evidence" },
      ),
    );
  }
  for (const item of concept.evidence.slice(0, input.limits.evidence)) {
    const checked = yield* checkDependency(input.tree, item);
    repositoryEvidence.push(checked);
    if (checked.state !== "unchanged") {
      diagnostics.push(
        diagnostic(
          `knowledge.evidence.${checked.state}`,
          "warning",
          `${checked.path} is ${checked.state}${checked.reason === undefined ? "" : `: ${checked.reason}`}`,
          { ...where, field: "gitplus.evidence" },
        ),
      );
    }
  }

  const temporal = temporalOf(concept, input.evaluationTime);
  if (temporal.state === "invalid") {
    diagnostics.push(
      diagnostic("knowledge.temporal.invalid", "error", temporal.reason, {
        ...where,
        field: "stale_after",
      }),
    );
  } else if (temporal.state === "stale") {
    diagnostics.push(
      diagnostic(
        "knowledge.temporal.stale",
        "warning",
        `the declared deadline ${temporal.deadline} has passed`,
        { ...where, field: "stale_after" },
      ),
    );
  }

  const external: Array<ExternalSource> = [];
  for (const source of concept.external) external.push(yield* checkExternal(source));

  const verificationRecords: Array<Verification> = [];
  for (const reference of concept.verificationRecords.slice(0, input.limits.citations)) {
    const checked = yield* checkCitation(reference, input.judge);
    verificationRecords.push({
      record: checked.record,
      state: checked.state,
      reason: checked.reason,
    });
  }

  // Editorial `verified` metadata is reported and never acted on (INV-05).
  const claimsVerified =
    Predicate.isObject(concept.metadata) &&
    !Array.isArray(concept.metadata) &&
    Object.hasOwn(concept.metadata, "verified");
  if (claimsVerified && verificationRecords.every((record) => record.state !== "accepted")) {
    diagnostics.push(
      diagnostic(
        "knowledge.verified.editorial",
        "info",
        "`verified` is an editorial claim; no accepted verification record establishes it",
        { ...where, field: "verified" },
      ),
    );
  }

  return {
    id: concept.id,
    path: concept.path,
    blob: qualified,
    type: concept.type,
    title: concept.title,
    structure: { state: "valid" },
    lifecycle: concept.lifecycle,
    citations,
    repositoryEvidence,
    temporal,
    external,
    verificationRecords,
    eligibility: eligibilityOf({
      lifecycle: concept.lifecycle,
      citations,
      repositoryEvidence,
      temporal,
      unevaluated: overflowed,
    }),
    diagnostics,
    concept,
  };
});

/** Temporal freshness, comparing instants rather than sorted strings (§5.3). */
export const temporalOf = (concept: Concept.Concept, evaluationTime: Date): Temporal => {
  if (concept.staleAfter === null) return { state: "no-deadline" };
  const deadline = Concept.instantOf(concept.staleAfter);
  if (deadline.state === "invalid") {
    return { state: "invalid", deadline: concept.staleAfter, reason: deadline.reason };
  }
  return evaluationTime.getTime() >= deadline.at
    ? { state: "stale", deadline: deadline.text }
    : { state: "within-deadline", deadline: deadline.text };
};

/**
 * Whether automatic recall may use this, and why not where it may not (§7).
 *
 * Conservative on purpose, and the reasons are the product: "excluded" with no
 * account of which dimension excluded it is the answer that makes a checker
 * useless. Note what is *not* disqualifying — no declared deadline, no
 * citations at all, and an offline-`unknown` external source are conditions to
 * label, not errors.
 */
export const eligibilityOf = (input: {
  readonly lifecycle: Concept.Lifecycle | "unknown";
  readonly citations: ReadonlyArray<Citation>;
  readonly repositoryEvidence: ReadonlyArray<Dependency>;
  readonly temporal: Temporal;
  /** Whether a host limit left some declared support unchecked. */
  readonly unevaluated?: boolean;
}): Eligibility => {
  const reasons: Array<string> = [];
  if (input.unevaluated === true) reasons.push("evidence-unevaluated");
  if (input.lifecycle === "draft") reasons.push("lifecycle-draft");
  if (input.lifecycle === "deprecated") reasons.push("lifecycle-deprecated");
  if (input.lifecycle === "other" || input.lifecycle === "unknown")
    reasons.push("lifecycle-unknown");

  for (const citation of input.citations) {
    if (citation.state !== "accepted") reasons.push(`citation-${citation.state}`);
  }
  for (const dependency of input.repositoryEvidence) {
    if (dependency.state !== "unchanged") reasons.push(`evidence-${dependency.state}`);
  }
  if (input.temporal.state === "stale") reasons.push("temporal-stale");
  if (input.temporal.state === "invalid") reasons.push("temporal-invalid");

  const eligible = reasons.length === 0;
  return { startup: eligible, recall: eligible, reasons };
};

// -- the bundle -----------------------------------------------------------------

/**
 * Every Concept under a bundle root, in a stable byte order.
 *
 * Byte order rather than `localeCompare`, for the reason `Select.candidates`
 * uses one: collation depends on the host's ICU build, and a host-dependent
 * order makes two hosts report different "first 512 Concepts" for one tree.
 */
export const list = Effect.fn("knowledge.Check.list")(function* (
  tree: Oid,
  bundle: string,
  /** The tree's files, where the caller listed them already. */
  listed?: ReadonlyArray<Listed>,
) {
  const repository = yield* Repository;
  const files: ReadonlyArray<Listed> = listed ?? (yield* repository.listFiles(tree));
  return files
    .filter((file) => !isGitlink(file.mode) && Concept.isConceptPath(bundle, file.path))
    .sort((left, right) => (left.path < right.path ? -1 : left.path > right.path ? 1 : 0));
});

/** One file of a listed tree, as `Repository.listFiles` names it. */
export interface Listed {
  readonly path: string;
  readonly oid: Oid;
  readonly mode: string;
}

/**
 * A stable key over everything an evaluation read, for a cache to hang on.
 *
 * `view: null` is a reader with no view at all — repository memory derived
 * from sessions alone — which still needs the same key shape, so the two
 * cannot collide and a memory stamp composes this one rather than re-deriving
 * its fields.
 */
export const stampOf = (input: {
  readonly view: Pack.View | null;
  readonly bundle: string;
  readonly repo: string | null;
  readonly trustHead: string | null;
}): string =>
  [
    `view=${input.view?.tree ?? "none"}`,
    `base=${input.view?.base ?? "none"}`,
    `bundle=${input.bundle}`,
    `repo=${input.repo ?? "none"}`,
    `trust=${input.trustHead ?? "none"}`,
    `parser=${Concept.PARSER_VERSION}`,
    `checker=${CHECKER_VERSION}`,
    `okf=${Concept.OKF_REVISION}`,
  ].join(" ");

/**
 * Check a bundle, or one Concept in it, against one captured view.
 *
 * The evaluation clock is an argument rather than `Date.now()` inside: INV-14
 * makes an ambient clock a way for equivalent pinned inputs to produce
 * different results, and a deadline is exactly the field that would move.
 */
export const check = Effect.fn("knowledge.Check.check")(function* (
  input: Input,
): Effect.fn.Return<Report, Invalid | ObjectNotFound | StorageFailure, Repository> {
  const repository = yield* Repository;
  const bundle = (input.bundle ?? Concept.BUNDLE).replace(/\/+$/u, "");
  const limits: Required<Limits> = {
    concepts: input.limits?.concepts ?? LIMITS.concepts,
    citations: input.limits?.citations ?? LIMITS.citations,
    evidence: input.limits?.evidence ?? LIMITS.evidence,
  };

  const tree = unqualify(input.view.tree);
  const diagnostics: Array<Concept.Diagnostic> = [];
  const evaluatedAt = input.evaluationTime.toISOString();
  const trustHead = yield* repository.resolve(TRUST_LOG);
  const stamp = stampOf({
    view: input.view,
    bundle,
    repo: input.repo ?? null,
    trustHead,
  });
  const base = {
    version: 1,
    bundle,
    view: input.view,
    evaluatedAt,
    profile: {
      okfRevision: Concept.OKF_REVISION,
      parserVersion: Concept.PARSER_VERSION,
      checkerVersion: CHECKER_VERSION,
    },
    inputStamp: stamp,
  } as const;

  if (tree === null) {
    return {
      ...base,
      bundleAbsent: false,
      complete: false,
      completeness: "unavailable",
      concepts: [],
      diagnostics: [
        diagnostic(
          "knowledge.view",
          "error",
          `'${input.view.tree}' is not an object id`,
          undefined,
        ),
      ],
    };
  }

  const found = yield* list(tree, bundle, input.files).pipe(
    Effect.catchTag("ObjectNotFound", () => Effect.succeed(null)),
  );
  if (found === null) {
    return {
      ...base,
      bundleAbsent: false,
      complete: false,
      completeness: "unavailable",
      concepts: [],
      diagnostics: [
        diagnostic("knowledge.view", "error", "the selected view could not be read", undefined),
      ],
    };
  }

  // A repository with no bundle is a successful no-op, never manufactured
  // knowledge (§6.5) — but naming a Concept that does not exist is an error.
  if (found.length === 0 && (input.concept === undefined || input.concept === null)) {
    return {
      ...base,
      bundleAbsent: true,
      complete: true,
      completeness: "complete",
      concepts: [],
      diagnostics: [],
    };
  }

  let selected = found;
  if (input.paths !== undefined) {
    const wanted = new Set(input.paths);
    selected = found.filter((file) => wanted.has(file.path));
  }
  if (input.concept !== undefined && input.concept !== null && input.concept !== "") {
    const asked = input.concept.replace(/^\/+/u, "");
    const wanted = asked.endsWith(".md") ? asked : `${bundle}/${asked}.md`;
    // Prefix arithmetic rather than a regular expression built from a path:
    // a bundle root holds `.` and `/`, which a pattern would read as syntax.
    const withoutSuffix = asked.endsWith(".md") ? asked.slice(0, -3) : asked;
    const id = withoutSuffix.startsWith(`${bundle}/`)
      ? withoutSuffix.slice(bundle.length + 1)
      : withoutSuffix;
    selected = found.filter(
      (file) => file.path === wanted || Concept.idOf(bundle, file.path) === id,
    );
    if (selected.length === 0) {
      return {
        ...base,
        bundleAbsent: found.length === 0,
        complete: false,
        completeness: "unavailable",
        concepts: [],
        diagnostics: [
          diagnostic(
            "knowledge.absent",
            "error",
            `no Concept '${input.concept}' under ${bundle} in this view`,
            undefined,
          ),
        ],
      };
    }
  }

  const judge: Judge = {
    repo: input.repo ?? null,
    trust: input.trust ?? null,
    reach: trustReach(),
    sessions: new Map(),
  };

  const concepts: Array<Checked> = [];
  const limited = selected.length > limits.concepts;
  for (const file of selected.slice(0, limits.concepts)) {
    concepts.push(
      yield* checkConcept({
        bundle,
        path: file.path,
        blob: file.oid,
        tree,
        evaluationTime: input.evaluationTime,
        judge,
        limits,
      }),
    );
  }
  if (limited) {
    // An explicit incomplete outcome, never a clean empty answer (§6.5, K-14).
    diagnostics.push(
      diagnostic(
        "knowledge.limit.concepts",
        "error",
        `this host checks at most ${limits.concepts} Concepts; ${selected.length} are present, so this report is partial`,
        undefined,
      ),
    );
  }
  if (input.trust === undefined || input.trust === null) {
    diagnostics.push(
      diagnostic(
        "knowledge.provenance.unjudged",
        "warning",
        "no trust state was supplied, so declared citations report as unavailable rather than accepted",
        undefined,
      ),
    );
  }

  return {
    ...base,
    bundleAbsent: false,
    complete: !limited,
    completeness: limited ? "limited" : "complete",
    concepts,
    diagnostics: input.diagnostics === "aggregate" ? opaque(diagnostics) : diagnostics,
  };
});

/** Diagnostics for a reader who may not see the view's structure (§6.1). */
const opaque = (
  diagnostics: ReadonlyArray<Concept.Diagnostic>,
): ReadonlyArray<Concept.Diagnostic> =>
  diagnostics.map((entry) => ({
    code: entry.code,
    severity: entry.severity,
    message: entry.code,
  }));

/** Whether a report passes the default gate, or the stricter one (§12.3). */
export const gate = (report: Report, strict: boolean) => {
  const failures: Array<string> = [];
  if (!report.complete) failures.push("the requested scope was not completely evaluated");
  for (const concept of report.concepts) {
    if (concept.structure.state !== "valid") {
      failures.push(`${concept.path}: ${concept.structure.reason}`);
    }
    for (const citation of concept.citations) {
      if (citation.state === "invalid" || citation.state === "unsupported-record") {
        failures.push(`${concept.path}: ${citation.record} ${citation.state}`);
      }
    }
    for (const diagnosed of concept.diagnostics) {
      if (diagnosed.severity === "error" && diagnosed.code === "knowledge.temporal.invalid") {
        failures.push(`${concept.path}: ${diagnosed.message}`);
      }
    }
    if (!strict) continue;
    // Unverified is not verified. The default gate lets `unavailable` pass,
    // because a source-only clone is allowed to say so without fabricating
    // acceptance (§6.2) — but a gate somebody tightened must not exit 0 over
    // a Concept none of whose citations it could check.
    for (const citation of concept.citations) {
      if (citation.state === "unavailable") {
        failures.push(`${concept.path}: ${citation.record} ${citation.state}`);
      }
    }
    for (const dependency of concept.repositoryEvidence) {
      if (dependency.state === "changed" || dependency.state === "missing") {
        failures.push(`${concept.path}: ${dependency.path} is ${dependency.state}`);
      }
    }
    if (concept.temporal.state === "stale") {
      failures.push(`${concept.path}: the deadline ${concept.temporal.deadline} has passed`);
    }
  }
  for (const diagnosed of report.diagnostics) {
    if (diagnosed.severity === "error") failures.push(diagnosed.message);
  }
  return { ok: failures.length === 0, failures } as const;
};
