/**
 * The trust log folded into "who may do what, now".
 *
 * The fold is also where authority is *checked*. A record only takes effect if
 * the state that precedes it already authorized its signer to write it — which
 * is what makes this a chain reaching the genesis rather than a list of claims.
 * Doing the check anywhere else would mean a projection that could be built
 * from unverified records and then trusted by something that assumed otherwise.
 *
 * Records that fail are skipped, not fatal. A log is replicated, and anybody
 * who can push can append to it; one unauthorized record must not make a
 * repository's entire membership unreadable. What it must do is not count —
 * and `rejected` keeps them, because "why is Bob not a member?" is a question
 * an operator will ask.
 */
import { Effect, Result } from "effect";

import {
  type Fingerprint,
  fingerprint,
  NAMESPACE,
  parsePublicKey,
  verify,
} from "../crypto/SshSignature.ts";
import type { Invalid, ObjectNotFound, StorageFailure } from "../git/Error.ts";
import { Repository } from "../git/Repository.ts";
import type { Oid } from "../git/Store.ts";
import * as Certificate from "./Certificate.ts";
import { MAX_SIGNATURES } from "./Certificate.ts";
import { type Genesis, type RepoId, type RootKey } from "./Genesis.ts";
import * as Log from "./Log.ts";

const decoder = new TextDecoder();

/** How many checkpoints a projection carries from each end; see `project`. */
const CHECKPOINTS = 32;

/** How many copies of one statement contribute signatures; see `readLog`. */
const COPIES = 8;

/**
 * Newest first, ties to the greater oid so every replica agrees.
 */
const byRecency = (left: Attestation, right: Attestation): number =>
  left.at.getTime() !== right.at.getTime()
    ? right.at.getTime() - left.at.getTime()
    : right.commit < left.commit
      ? -1
      : 1;

export interface Member {
  readonly fingerprint: Fingerprint;
  /** The `authorized_keys` line the grant carried. */
  readonly publicKey: string;
  readonly capabilities: ReadonlyArray<string>;
  readonly grantedAt: Date;
  readonly expiresAt: Date | null;
  /** The log commit that granted this, for an operator tracing authority. */
  readonly grant: Oid;
  /**
   * Every grant this key has been given, oldest first.
   *
   * Kept because `grant` alone answers "where do their capabilities come from
   * *now*", and judging a stored event needs "where did they come from *then*".
   * Collapsing the two meant an ordinary renewal — a second `hub grant` to the
   * same key — retroactively un-authorized every event that member had ever
   * signed, since the only grant on record post-dated all of them.
   */
  readonly history: ReadonlyArray<GrantRecord>;
}

/** One grant, as the fold recorded it. */
export interface GrantRecord {
  readonly commit: Oid;
  readonly capabilities: ReadonlyArray<string>;
}

export interface Revocation {
  readonly subject: Fingerprint;
  readonly reason: Certificate.Revoke["reason"];
  /**
   * When a compromise is taken to have begun, for the retroactive class.
   *
   * `null` for every other reason — those are forward-only, and a date on
   * them would invite the reading that they reach backwards too.
   */
  readonly compromisedFrom: Date | null;
  readonly commit: Oid;
  /**
   * The grant that put this key back, if one has.
   *
   * The revocation is *not* removed when a key is re-granted, because it is
   * still true about the window it covers: deleting it made every signature
   * the key made while revoked authorized again — at the old grant's
   * capabilities, not the narrower ones it was let back in with, and
   * including under a `compromised` revocation, the one class meant to reach
   * backwards. What a re-grant ends is the window, not the record.
   */
  readonly supersededBy: Oid | null;
}

/**
 * The earlier of two moments, either of which may be absent.
 *
 * A revocation can only ever be *strengthened* while it is open, so where two
 * statements about when a compromise began meet, the one reaching further back
 * is the one that survives.
 */
const earliest = (left: Date | null, right: Date | null): Date | null => {
  if (left === null) return right;
  if (right === null) return left;
  return left <= right ? left : right;
};

/**
 * The window a key is currently out on, if it is out.
 *
 * A key can be revoked, let back in, and revoked again, so what it has is a
 * *list* of disjoint windows rather than one. Only the last of them can still
 * be open — a revocation of a key already out strengthens the window it is in
 * rather than opening a second — so this is the whole of "is this key revoked
 * right now", and every earlier window stays on the record because it is still
 * true about its own interval.
 */
export const openWindow = (
  revocations: ReadonlyArray<Revocation> | undefined,
): Revocation | null => {
  const last = revocations?.at(-1);
  return last !== undefined && last.supersededBy === null ? last : null;
};

export interface Attestation {
  readonly commit: Oid;
  readonly at: Date;
  readonly frontier: ReadonlyArray<string>;
}

export interface Rejected {
  readonly commit: Oid;
  readonly reason: string;
}

export interface Projection {
  readonly repoId: RepoId;
  /** The log head this state was folded from; `null` when the log is empty. */
  readonly head: Oid | null;
  readonly members: ReadonlyMap<Fingerprint, Member>;
  /**
   * What revoked members held before they were revoked.
   *
   * Kept because a forward-only revocation does not unmake history: a review
   * signed before its author could see their own revocation is still a review
   * this repository authorized, and judging it needs the capabilities they had
   * at the time. Dropping them from `members` alone would silently rewrite
   * every past event by anyone who ever left.
   */
  readonly former: ReadonlyMap<Fingerprint, Member>;
  /**
   * Every window each key has been out on, oldest first.
   *
   * A list rather than a record, because a key can be revoked, let back in and
   * revoked again — and each window is separately true. Kept as one, a second
   * revocation overwrote the first, and with it the retroactive reach of a
   * `compromised` one: an attacker who had a key revoked for compromise could
   * have it revoked again for any ordinary reason and then re-granted, and
   * every signature they made during the compromise was authorized again.
   */
  readonly revoked: ReadonlyMap<Fingerprint, ReadonlyArray<Revocation>>;
  readonly roots: ReadonlyArray<RootKey>;
  readonly threshold: number;
  /** The most recent checkpoint, for callers that only want to show one. */
  readonly checkpoint: Attestation | null;
  /**
   * The newest checkpoints, newest first, for callers that bound staleness.
   *
   * More than one because `at` is written by whoever signed the checkpoint and
   * the fold has no clock to disbelieve it with. Keeping only the greatest `at`
   * meant a single attestation dated ahead — a malicious admin, or a CI box
   * with a fast clock — became the only checkpoint on record, and a verifier
   * that refuses a future date then refused *every* write on a repository with
   * `maxTrustAgeSeconds` set, including the write to `refs/meta/policy` that
   * would lift the bound. `Verify.fresh` holds the clock, so it can skip past
   * one and find an honest checkpoint behind it.
   */
  readonly checkpoints: ReadonlyArray<Attestation>;
  readonly rejected: ReadonlyArray<Rejected>;
}

/** Which distinct keys signed these bytes, out of a set we care about. */
const signersOf = Effect.fn("trust.Projection.signersOf")(function* (
  bytes: Uint8Array,
  signatures: ReadonlyArray<string>,
) {
  const signers = new Set<Fingerprint>();
  for (const armored of signatures.slice(0, MAX_SIGNATURES)) {
    const key = yield* verify(armored, bytes, NAMESPACE).pipe(
      // A signature that does not parse cannot add authority, and refusing the
      // whole record for it would let anyone break a grant by appending junk.
      Effect.catchTag("Invalid", () => Effect.succeed(null)),
    );
    if (key === null) continue;
    signers.add(yield* fingerprint(key));
  }
  return signers;
});

const rootQuorum = (
  signers: ReadonlySet<Fingerprint>,
  roots: ReadonlyArray<RootKey>,
  threshold: number,
): boolean => {
  let met = 0;
  for (const root of roots) if (signers.has(root.fingerprint)) met++;
  return met >= threshold;
};

/**
 * Whether a member's grant had expired by the time a record claims to have
 * been issued.
 *
 * The fold still has no *clock* — the comparison is against the record's own
 * `issuedAt`, so the answer is a pure function of the log and two hosts folding
 * the same history reach the same state. Ignoring expiry here entirely meant an
 * expired `member.invite` holder's pre-signed grants took effect on every
 * replica forever, which is an expiry that expires nothing.
 *
 * `issuedAt` is written by the issuer, so a determined one can backdate it —
 * but only into a window in which they genuinely held the capability, which is
 * the same bound `Verify` accepts for every other stored statement.
 */
const expired = (member: Member, at: Date): boolean =>
  member.expiresAt !== null && member.expiresAt.getTime() <= at.getTime();

/** Every signer who may invite members — all of them, not the first found. */
const inviters = (
  members: ReadonlyMap<Fingerprint, Member>,
  revoked: ReadonlyMap<Fingerprint, ReadonlyArray<Revocation>>,
  signers: ReadonlySet<Fingerprint>,
  at: Date,
): ReadonlyArray<Fingerprint> => {
  const found: Fingerprint[] = [];
  for (const signer of signers) {
    if (openWindow(revoked.get(signer)) !== null) continue;
    const member = members.get(signer);
    if (member === undefined || expired(member, at)) continue;
    if (Certificate.permits(member.capabilities, "member.invite")) found.push(signer);
  }
  return found;
};

/** Whether any signer holds a capability *within the fold*. */
const holds = (
  members: ReadonlyMap<Fingerprint, Member>,
  revoked: ReadonlyMap<Fingerprint, ReadonlyArray<Revocation>>,
  signers: ReadonlySet<Fingerprint>,
  capability: string,
  at: Date,
): Fingerprint | null => {
  for (const signer of signers) {
    if (openWindow(revoked.get(signer)) !== null) continue;
    const member = members.get(signer);
    if (member === undefined || expired(member, at)) continue;
    if (Certificate.permits(member.capabilities, capability)) return signer;
  }
  return null;
};

/**
 * Fold the log.
 *
 * The genesis supplies the initial roots and threshold; every later
 * `trust.root-change` replaces them, so a repository can rotate its roots
 * without its identity — which is over the genesis bytes — ever changing.
 */
export const project = Effect.fn("trust.Projection.project")(function* (genesis: Genesis) {
  const repository = yield* Repository;
  const { endorsed, entries, keyOf, winner } = yield* readLog();

  // The ref, not the last record the fold reached. A join carries no record,
  // so after `Replication.reconcile` the two differ — and this field is what
  // a caller keys a memo on, which would then miss the join that changed
  // nothing and hit for two states that are not the same.
  const head = yield* repository.resolve(Log.LOG_REF);

  const members = new Map<Fingerprint, Member>();
  const former = new Map<Fingerprint, Member>();
  const revoked = new Map<Fingerprint, ReadonlyArray<Revocation>>();
  const rejected: Rejected[] = [];

  let roots: ReadonlyArray<RootKey> = genesis.roots;
  let threshold = genesis.document.threshold;
  const checkpoints: Attestation[] = [];
  for (const entry of entries) {
    const invalid = yield* Certificate.validate(entry.payload, genesis.repoId).pipe(
      Effect.as(null),
      Effect.catchTag("Invalid", (error) => Effect.succeed(error.reason)),
    );
    if (invalid !== null) {
      rejected.push({ commit: entry.commit, reason: invalid });
      continue;
    }

    const payload = entry.payload;
    const key = keyOf(entry);

    // Which of several commits carrying one statement is *the* one, decided by
    // descent rather than by fold order. The id is inside the signed bytes, so
    // a replay carries the same one — and fold order breaks ties by raw oid,
    // which anybody who may write the ref can grind. Losing that race moved
    // `grant`, `history[].commit` and `Revocation.commit` onto a commit honest
    // trust heads cannot reach, which stops every stored event by that member
    // counting and stops a revocation applying at all.
    //
    // Asked *before* the signatures are verified, which is the whole cost of
    // this loop. Asked after, N copies of one record — a single push, and
    // append-only containment does not stop it — each paid for the union's
    // worth of Ed25519 verifications before being dropped as duplicates, so
    // the bound on the union was multiplied by the very thing it bounded.
    if (winner.get(key) !== entry.commit) {
      rejected.push({ commit: entry.commit, reason: `${payload.id} has already been applied` });
      continue;
    }

    // Every signature any copy of this statement carried. They all sign the
    // same bytes, so they are all endorsements of the same thing, and which
    // commit one arrived in says nothing about it. Requiring them to be in the
    // winning copy would hand a replay that *drops* the signatures a way to
    // strip a revocation's authority simply by winning descent.
    const endorsements = endorsed.get(key) ?? [entry.signatures];
    const signers = new Set<Fingerprint>();
    for (const copy of endorsements) {
      for (const signer of yield* signersOf(entry.bytes, copy)) signers.add(signer);
    }
    const quorum = rootQuorum(signers, roots, threshold);
    // What the record says about when it was made, which is what every
    // expiry below is judged against.
    const claimedAt = new Date(payload.issuedAt);

    if (payload.type === "trust.root-change") {
      // Only the current roots may replace themselves. A member with
      // `repo.admin` must not be able to rewrite the set of keys that granted
      // them `repo.admin` — that is the one loop authority cannot survive.
      if (!quorum) {
        rejected.push({ commit: entry.commit, reason: "root change needs the root quorum" });
        continue;
      }
      const changed = yield* rootsOf(payload.rootKeys);
      roots = changed;
      threshold = payload.threshold;
      continue;
    }

    if (payload.type === "trust.grant") {
      // Every signer who may invite, not merely the first one found: a record
      // co-signed by an inviter and an admin must not be accepted or refused
      // on the order the signatures happen to be in.
      const issuers = quorum ? [] : inviters(members, revoked, signers, claimedAt);
      if (!quorum && issuers.length === 0) {
        rejected.push({ commit: entry.commit, reason: "issuer may not invite members" });
        continue;
      }
      // Nobody may grant what they do not hold. Without this, one member with
      // `member.invite` could mint themselves an admin by granting it to a key
      // they also control. Held capabilities are pooled across the signers,
      // because they all signed the same record.
      if (!quorum) {
        const held = issuers.flatMap((issuer) => members.get(issuer)?.capabilities ?? []);
        const excess = payload.capabilities.filter(
          (capability) => !Certificate.permits(held, capability),
        );
        if (excess.length > 0) {
          rejected.push({
            commit: entry.commit,
            reason: `issuer cannot grant ${excess.join(", ")}`,
          });
          continue;
        }
      }

      // SAFETY: `Certificate.validate` has checked that `subject` is the
      // fingerprint of `publicKey`, which is what a `Fingerprint` names.
      const subject = payload.subject as Fingerprint;

      // A grant over a revocation re-instates — rotating a key back in is an
      // ordinary thing to do, and requiring a new fingerprint for it would
      // mean a compromised-then-recovered key could never be used again. But
      // it takes the authority that *made* the revocation, not merely the
      // authority to add members: `member.invite` clearing a revocation made
      // `revoke` undoable by anybody who could `grant`, retroactive compromise
      // revocations included, and left nothing in `revoked` to say so.
      if (
        openWindow(revoked.get(subject)) !== null &&
        !quorum &&
        holds(members, revoked, signers, "member.revoke", claimedAt) === null
      ) {
        rejected.push({
          commit: entry.commit,
          reason: `issuer may not re-instate ${subject}; that needs member.revoke`,
        });
        continue;
      }

      // Appended to whatever this key already had — including what it held
      // before a revocation, since re-instating is a continuation rather than
      // a fresh start, and the events signed under the old grant were
      // authorized when they were made.
      const previous = members.get(subject) ?? former.get(subject);
      members.set(subject, {
        fingerprint: subject,
        publicKey: payload.publicKey,
        capabilities: payload.capabilities,
        grantedAt: new Date(payload.issuedAt),
        expiresAt: payload.expiresAt === null ? null : new Date(payload.expiresAt),
        grant: entry.commit,
        history: [
          ...(previous?.history ?? []),
          { commit: entry.commit, capabilities: payload.capabilities },
        ],
      });
      // Closed, not erased: everything the key signed between the revocation
      // and this grant stays refused.
      // Only a revocation that is still *open* is closed by this grant. An
      // ordinary renewal would otherwise move an already-closed window's end
      // forward, and every event signed after the first re-instatement — which
      // named the old end as its trust head — would stop reaching it and be
      // refused: approvals vanishing and protected-branch merges failing for a
      // key that had been a member the whole time.
      const windows = revoked.get(subject) ?? [];
      const ending = openWindow(windows);
      if (ending !== null) {
        revoked.set(subject, [...windows.slice(0, -1), { ...ending, supersededBy: entry.commit }]);
      }
      // `former` is what a forward-only revocation is judged against, and this
      // key is a current member again: leaving a stale entry there would let a
      // later revocation be measured against capabilities they no longer hold.
      former.delete(subject);
      continue;
    }

    if (payload.type === "trust.revoke") {
      if (!quorum && holds(members, revoked, signers, "member.revoke", claimedAt) === null) {
        rejected.push({ commit: entry.commit, reason: "issuer may not revoke members" });
        continue;
      }
      // SAFETY: `Certificate.validate` has checked the subject is a fingerprint.
      const subject = payload.subject as Fingerprint;
      const compromised = payload.reason === "compromised";
      const from = !compromised
        ? null
        : payload.compromisedAt !== null
          ? new Date(payload.compromisedAt)
          : // A compromise of unknown age is not a compromise of no age: fall
            // back to the grant, so everything the key signed is suspect. Read
            // from `former` too, because a second revocation arrives after the
            // first already moved them there.
            ((members.get(subject) ?? former.get(subject))?.grantedAt ?? new Date(0));

      // A key already out is not put out twice. Appending a second open window
      // left the first one open forever — a re-grant closes the *last* window,
      // so every stored event by that key stayed refused while live pushes were
      // waved through, on a ref that can never be rewound. What a revocation of
      // an already-revoked key does is *strengthen* the window it is already
      // in: learning afterwards that a key was compromised is the reason to
      // send one, and the window keeps its original start, since that is when
      // the key stopped being trusted.
      const windows = revoked.get(subject) ?? [];
      const already = openWindow(windows);
      const record: Revocation = {
        subject,
        supersededBy: null,
        // The *stronger* reason survives, as the dates do. Overwritten, a
        // compromise relabelled itself the moment anybody revoked the same key
        // again for an ordinary reason: `compromisedFrom` still reached
        // backwards, so authorization was unaffected, but `hub members` told
        // an operator the key had merely left.
        reason:
          already?.reason === "compromised" || payload.reason === "compromised"
            ? "compromised"
            : payload.reason,
        compromisedFrom: earliest(already?.compromisedFrom ?? null, from),
        commit: already?.commit ?? entry.commit,
      };
      // Appended when the key is in, replacing when it is already out: a key
      // revoked, re-instated and revoked again was out for two separate
      // intervals, and the events it signed in the first are not made
      // authorized by the second ending.
      revoked.set(
        subject,
        already === null ? [...windows, record] : [...windows.slice(0, -1), record],
      );
      const held = members.get(subject);
      if (held !== undefined) former.set(subject, held);
      members.delete(subject);
      continue;
    }

    if (!quorum && holds(members, revoked, signers, "repo.admin", claimedAt) === null) {
      rejected.push({ commit: entry.commit, reason: "issuer may not checkpoint" });
      continue;
    }

    // Kept in fold order here and sorted by `at` below. Taking simply the last
    // one folded would let two checkpoints made concurrently on two replicas
    // and then joined leave the older in force — and a repository that had set
    // `maxTrustAgeSeconds` would then refuse every push against an attestation
    // it already had a fresher replacement for.
    checkpoints.push({
      commit: entry.commit,
      at: new Date(payload.issuedAt),
      frontier: payload.frontier,
    });
  }

  // Bounded, because a checkpoint costs one commit to write and this list is
  // walked on every gated write, so an unbounded one is a cost anybody holding
  // `repo.admin` could impose.
  //
  // Kept from *both* ends: the newest few by `at`, and the last few the fold
  // reached. `at` alone is written by the signer, so a run of forward-dated
  // attestations — one admin host with a fast clock, checkpointing hourly —
  // filled the whole list and evicted every credible one, and `Verify.fresh`
  // skipping past them then found nothing behind. Keeping the tail of the log
  // as well means the recovery is the obvious one: push a checkpoint. A new
  // one lands at the head, so it is always retained however it is dated.
  const newest = [...checkpoints].sort(byRecency).slice(0, CHECKPOINTS);
  const latest = checkpoints.slice(-CHECKPOINTS);
  const kept = new Map(
    [...newest, ...latest].map((attestation) => [attestation.commit, attestation]),
  );
  const retained = [...kept.values()].sort(byRecency);

  return {
    repoId: genesis.repoId,
    head,
    members,
    former,
    revoked,
    roots,
    threshold,
    checkpoint: retained[0] ?? null,
    checkpoints: retained,
    rejected,
  };
});

/**
 * The log, plus which commit owns each record id.
 *
 * The rest of the log descends from a genuine record; a replay grafted on
 * later is descended from by nothing, however its oid sorts. Ties — where
 * descent cannot separate them — go to the lower oid, so every replica still
 * agrees.
 */
const readLog = Effect.fn("trust.Projection.readLog")(function* () {
  const { parents, records } = yield* Log.entries();

  // Grouped by id *and* by the exact bytes — the *statement*, and nothing else.
  // Two records with one id but different content are two different claims,
  // and each is answered on its own authority rather than by whichever reached
  // the fold first. What this decides is the other case: the same statement
  // committed more than once, where the commit is what `grant` and
  // `Revocation.commit` are read from later.
  //
  // One key, not two. An earlier version put the signatures in the key here
  // and left the duplicate check keyed on the payload alone — so a copy
  // carrying one extra junk signature was a group of its own, escaped this
  // resolution entirely, and was decided by fold order, whose tie-break for
  // parentless commits is a grindable oid. That is the exact failure this
  // mechanism exists to prevent, walked around by appending a byte.
  const claimed = new Map<string, Oid[]>();
  // Every signature any copy of the statement carried, since they are all
  // signatures over the same bytes and so all endorsements of the same thing.
  // Which copy an endorsement was committed in says nothing about it — and
  // requiring them to be in the *winning* copy would let a replay that drops
  // the signatures strip a revocation's authority by winning descent.
  const endorsed = new Map<string, ReadonlyArray<ReadonlyArray<string>>>();
  const signatures = new Map<Oid, ReadonlyArray<string>>();
  const keyOf = (entry: (typeof records)[number]) =>
    `${entry.payload.id}\u0000${decoder.decode(entry.bytes)}`;
  for (const entry of records) {
    const key = keyOf(entry);
    claimed.set(key, [...(claimed.get(key) ?? []), entry.commit]);
    signatures.set(entry.commit, entry.signatures);
  }

  const winner = new Map<string, Oid>();
  const disputed = [...claimed].filter(([, commits]) => commits.length > 1);
  for (const [id, commits] of claimed) {
    if (commits.length !== 1) continue;
    winner.set(id, commits[0]!);
    endorsed.set(id, [signatures.get(commits[0]!) ?? []]);
  }

  // The reverse edges, so "how much descends from this commit?" is a walk from
  // it rather than a lookup into a closure.
  //
  // A transitive ancestor closure — a set per commit holding every commit
  // before it — is quadratic in the log's length, and the log only grows on a
  // ref that cannot be rewound. Building it only when a statement is disputed
  // was not the guard it looked like: appending one duplicate record is a push
  // anybody holding `source.push` can make, and it would have made every fold
  // afterwards quadratic forever, on a path (`Policy.gate`,
  // `Redaction.excluded`, every `gc`) that runs un-memoised inside a 128 MiB
  // worker. Counted per candidate instead, the cost is one walk each and the
  // memory is one set at a time.
  const children = new Map<Oid, Oid[]>();
  if (disputed.length > 0) {
    for (const [oid, of] of parents) {
      for (const parent of of) children.set(parent, [...(children.get(parent) ?? []), oid]);
    }
  }

  /** How many commits this one reaches, following `edges`; itself excluded. */
  const reach = (start: Oid, edges: ReadonlyMap<Oid, ReadonlyArray<Oid>>): number => {
    const seen = new Set<Oid>();
    const stack = [start];
    while (stack.length > 0) {
      for (const next of edges.get(stack.pop()!) ?? []) {
        if (seen.has(next)) continue;
        seen.add(next);
        stack.push(next);
      }
    }
    return seen.size;
  };

  for (const [id, commits] of disputed) {
    const descendants = (commit: Oid) => reach(commit, children);
    // Depth breaks what descent cannot. Append-only containment forces a replay
    // to be published as a join over both copies, and a join descends from
    // both — so where the targeted record is the current head, the counts came
    // out equal and the decision fell to the oid, which whoever writes the
    // replay can grind. The genuine record hangs off the log's history; a copy
    // grafted in beside it does not, and cannot be given that history without
    // being made a descendant of the record it is trying to displace.
    const rank = (commit: Oid): readonly [number, number, Oid] => [
      descendants(commit),
      reach(commit, parents),
      commit,
    ];
    let best = rank(commits[0]!);
    for (const commit of commits.slice(1)) {
      const candidate = rank(commit);
      const better =
        candidate[0] !== best[0]
          ? candidate[0] > best[0]
          : candidate[1] !== best[1]
            ? candidate[1] > best[1]
            : candidate[2] < best[2];
      if (better) best = candidate;
    }
    winner.set(id, best[2]);

    // The endorsements come from the *best-ranked* copies, not the first few
    // the walk reached. Verifying every copy is what the union costs, and the
    // copies are written by whoever may push — so a cap taken in walk order
    // was a way to spend it: eight parentless, unsigned replays of a record
    // sort early, fill the list with empty signature sets, and the genuine
    // commit — which still wins `winner` — is folded with no signers at all.
    // The quorum then fails and the record is rejected, which turns any
    // `source.push` holder into somebody who can nullify a revocation.
    // Ranked, the copies that fill the list are the ones grafting cannot
    // displace, for the same reason the winner is.
    endorsed.set(
      id,
      commits
        .map((commit) => [rank(commit), commit] as const)
        .sort(([left], [right]) =>
          left[0] !== right[0]
            ? right[0] - left[0]
            : left[1] !== right[1]
              ? right[1] - left[1]
              : left[2] < right[2]
                ? -1
                : 1,
        )
        .slice(0, COPIES)
        .map(([, commit]) => signatures.get(commit) ?? []),
    );
  }

  return { entries: records, winner, keyOf, endorsed };
});

/**
 * Key lines to root keys.
 *
 * A line that does not parse is dropped rather than failing the fold:
 * `Certificate.validate` has already refused any root change whose keys are
 * unreadable, so reaching here with one means the record was accepted by an
 * older version, and losing the whole projection over it would be worse than
 * losing the key.
 */
const rootsOf = Effect.fn("trust.Projection.rootsOf")(function* (lines: ReadonlyArray<string>) {
  const roots: RootKey[] = [];
  for (const line of lines) {
    const parsed = parsePublicKey(line);
    if (Result.isFailure(parsed)) continue;
    roots.push({ key: parsed.success, fingerprint: yield* fingerprint(parsed.success) });
  }
  return roots;
});

export type ProjectionError = Invalid | ObjectNotFound | StorageFailure;
