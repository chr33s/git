/** Accepted immutable session-record support, shared by citations and recall. */
import { Effect } from "effect";
import type { Invalid, ObjectNotFound, StorageFailure } from "../git/Error.ts";
import { qualify, unqualify } from "../git/Oid.ts";
import { Repository } from "../git/Repository.ts";
import { isOid, type Oid } from "../git/Store.ts";
import * as Event from "../hub/Event.ts";
import * as Session from "../hub/Session.ts";
import * as Tombstone from "../hub/Tombstone.ts";
import { trustReach } from "../hub/Projection.ts";
import type { Projection } from "../trust/Projection.ts";
import * as Record from "../trust/Record.ts";
import * as Verify from "../trust/Verify.ts";

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
  readonly session?: string;
}

export const judge = (
  repo: string | null,
  trust: Projection | null,
  heads: ReadonlyMap<string, Oid>,
  evaluatedAt: Date,
): Judge => ({
  repo,
  trust,
  heads,
  evaluatedAt,
  reach: trustReach(),
  sessions: new Map(),
});

/** What one check run has learned about a session ref, walked once. */
interface Held {
  /** Every record commit on the ref, decodable or not. */
  readonly onRef: ReadonlySet<Oid>;
  /** What the ref's counted tombstones have removed. */
  readonly removals: ReadonlySet<string>;
}

export interface Judge {
  readonly evaluatedAt: Date;
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
  readonly heads: ReadonlyMap<string, Oid>;
}

/** One session ref, walked once per check run. */
const heldOf = Effect.fn("knowledge.Provenance.heldOf")(function* (session: string, judge: Judge) {
  const known = judge.sessions.get(session);
  if (known !== undefined) return known;
  const walked = yield* Session.entries(session, judge.heads.get(Session.refOf(session)) ?? null);
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
export const evaluate = Effect.fn("knowledge.Provenance.checkCitation")(function* (
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
    evaluatedAt: judge.evaluatedAt,
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
