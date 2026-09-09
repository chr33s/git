/** Deterministic projection of one anchored note's signed event DAG. */
import { Effect } from "effect";

import {
  type Fingerprint as SignerFingerprint,
  fingerprint as fingerprintKey,
  NAMESPACE,
  verify,
} from "../crypto/SshSignature.ts";
import type { Oid } from "../git/Store.ts";
import * as Note from "./Note.ts";

export interface NoteProjection {
  readonly state: "ready";
  readonly id: string;
  readonly path: string;
  readonly anchor: string;
  readonly text: string;
  readonly createdAt: string;
  readonly createdBy: string;
  readonly updatedAt: string;
  readonly updatedBy: string;
  readonly active: boolean;
  readonly pinned: boolean;
  readonly baseline: Note.Baseline | null;
}

export interface ConflictedNote {
  readonly state: "conflicted";
  readonly id: string;
  readonly path: string;
  readonly anchor: string;
  readonly text: string;
  readonly createdAt: string;
  readonly createdBy: string;
  readonly updatedAt: string;
  readonly updatedBy: string;
  readonly active: boolean;
  readonly pinned: boolean;
  readonly baseline: Note.Baseline | null;
  readonly competing: ReadonlyArray<{
    readonly commit: Oid;
    readonly event: string;
    readonly id: string;
  }>;
}

export type Projection = NoteProjection | ConflictedNote;

const signerOf = Effect.fn("hub.NoteProjection.signerOf")(function* (
  bytes: Uint8Array,
  signatures: ReadonlyArray<string>,
) {
  for (const armored of signatures) {
    const key = yield* verify(armored, bytes, NAMESPACE).pipe(
      Effect.catchTag("Invalid", () => Effect.succeed(null)),
    );
    if (key !== null) return yield* fingerprintKey(key);
  }
  return null;
});

const ancestorSets = (
  parents: ReadonlyMap<Oid, ReadonlyArray<Oid>>,
  ordered: ReadonlyArray<Oid>,
): ReadonlyMap<Oid, ReadonlySet<Oid>> => {
  const found = new Map<Oid, ReadonlySet<Oid>>();
  for (const commit of ordered) {
    const ancestors = new Set<Oid>();
    for (const parent of parents.get(commit) ?? []) {
      ancestors.add(parent);
      for (const ancestor of found.get(parent) ?? []) ancestors.add(ancestor);
    }
    found.set(commit, ancestors);
  }
  return found;
};

interface Accepted extends Note.Entry {
  readonly signer: SignerFingerprint;
}

const semantic = (event: Note.NotePayload): boolean =>
  event.type === "note.confirmed" ||
  event.type === "note.replaced" ||
  event.type === "note.retired" ||
  event.type === "note.restored";

/**
 * What competing creations agree on, where they agree at all.
 *
 * Two replicas that began the same note id may have written different paths
 * and different text. Reporting one of them as *the* path would be the fold
 * choosing, so the earliest by issue time is reported and every candidate
 * stays visible in `competing` for whoever settles it.
 */
const byIssue = (creations: ReadonlyArray<Accepted>): ReadonlyArray<Accepted> =>
  [...creations].sort((left, right) => {
    const time = left.payload.issuedAt.localeCompare(right.payload.issuedAt);
    // Two replicas can issue at the same instant, and the commit is the only
    // tie-break both of them compute the same way. Left to the walk's order it
    // was not a tie-break at all: that order varies with the object ids, so the
    // same two records projected a different author on a different replica.
    return time !== 0 ? time : left.commit.localeCompare(right.commit);
  });

const beginning = (creations: ReadonlyArray<Accepted>) => {
  const first = byIssue(creations)[0];
  if (first === undefined || first.payload.type !== "note.created") {
    return { path: "", anchor: "", text: "", createdAt: "", createdBy: "" };
  }
  return {
    path: first.payload.path,
    anchor: first.payload.anchor,
    text: first.payload.text,
    createdAt: first.payload.issuedAt,
    createdBy: first.signer,
  };
};

/** The latest creation, for the same reason and by the same ordering. */
const ending = (creations: ReadonlyArray<Accepted>) => {
  const ordered = byIssue(creations);
  return ordered[ordered.length - 1];
};

/** `null` means no valid creation exists; malformed and unsigned records do not project. */
export const project = Effect.fn("hub.NoteProjection.project")(function* (note: string) {
  const walked = yield* Note.entries(note);
  const accepted: Accepted[] = [];
  for (const entry of walked.events) {
    const signer = yield* signerOf(entry.bytes, entry.signatures);
    if (signer === null || entry.payload.note !== note) continue;
    const valid = yield* Note.validate(entry.payload).pipe(
      Effect.as(true),
      Effect.catchTag("Invalid", () => Effect.succeed(false)),
    );
    if (valid) accepted.push({ ...entry, signer });
  }

  const ancestors = ancestorSets(walked.parents, walked.ordered);
  const creations = accepted.filter((entry) => entry.payload.type === "note.created");
  // The creation every other one descends: the note's single beginning.
  const creation = creations.find((candidate) =>
    creations.every(
      (other) =>
        other.commit === candidate.commit ||
        ancestors.get(other.commit)?.has(candidate.commit) === true,
    ),
  );
  if (creation === undefined || creation.payload.type !== "note.created") {
    // No creation at all is a ref carrying nothing this replica can read, and
    // `null` says so. Several that do not descend one another is a different
    // answer: two replicas each began the same note id, which is a
    // disagreement about what the note *is* — path, anchor and text at once —
    // and returning `null` for it made the note vanish from every listing
    // while two signed creations sat on the ref. §8 wants the disagreement
    // surfaced, so it surfaces as the conflict it is.
    const rival = creations[0];
    if (creations.length < 2 || rival === undefined || rival.payload.type !== "note.created") {
      return null;
    }
    const earliest = beginning(creations);
    const latest = ending(creations) ?? rival;
    return {
      state: "conflicted",
      id: note,
      // The earliest by issue time, so two replicas describe one note the same
      // way; nothing is authoritative here and a reader is told as much.
      ...earliest,
      // Every one of these four now names a record chosen by issue time, and
      // that is the whole point. They were taken from `creations[0]` — first in
      // *topological* order — which is not a function of the records at all:
      // it moves with the object ids, so two replicas holding the same two
      // creations reported different authors and different timestamps for
      // them, and `createdAt` and `createdBy` could name different events.
      createdBy: earliest.createdBy,
      updatedAt: latest.payload.issuedAt,
      updatedBy: latest.signer,
      active: true,
      pinned: false,
      baseline: null,
      competing: creations.map((entry) => ({
        commit: entry.commit,
        event: entry.payload.type,
        id: entry.payload.id,
      })),
    } satisfies ConflictedNote;
  }

  const created = creation.payload;
  const descends = (entry: Accepted, of: Accepted): boolean =>
    ancestors.get(entry.commit)?.has(of.commit) === true;

  /**
   * The lifecycle events this note actually holds, oldest first.
   *
   * An event that does not descend the creation is not part of this note's
   * history — a stray commit reachable from the ref, which the walk hands
   * over and the fold declines.
   */
  const lifecycle = accepted.filter(
    (entry) =>
      entry.commit !== creation.commit && descends(entry, creation) && semantic(entry.payload),
  );

  /**
   * The judgments nothing later supersedes. More than one means two replicas
   * said incompatible things about one note and neither has been answered.
   *
   * Read from the DAG rather than accumulated while folding: a fold that
   * mutates state as it walks has already applied one branch by the time it
   * meets the other, and which branch that was depends on the order the walk
   * happened to emit them in. The same two events would then converge to two
   * different states on two replicas — which is the one thing a projection of
   * an append-only log may not do.
   */
  const tips = lifecycle.filter(
    (entry) => !lifecycle.some((other) => other.commit !== entry.commit && descends(other, entry)),
  );

  // With a single tip, its own ancestry is the history that led to it. With
  // several, only what every side agrees on is settled; each competing branch
  // stays visible in `competing` rather than being folded into a state that
  // would read as somebody's decision.
  const settled =
    tips.length === 1
      ? lifecycle.filter((entry) =>
          tips.some((tip) => entry.commit === tip.commit || descends(tip, entry)),
        )
      : lifecycle.filter((entry) => tips.every((tip) => descends(tip, entry)));

  let text = created.text;
  let baseline = created.baseline;
  let active = true;
  let updatedAt = created.issuedAt;
  let updatedBy: string = creation.signer;

  for (const entry of settled) {
    switch (entry.payload.type) {
      case "note.confirmed":
        baseline = entry.payload.baseline;
        break;
      case "note.replaced":
        text = entry.payload.text;
        baseline = entry.payload.baseline;
        break;
      case "note.retired":
        active = false;
        break;
      case "note.restored":
        active = true;
        baseline = entry.payload.baseline;
        break;
      case "note.created":
        break;
    }
    updatedAt = entry.payload.issuedAt;
    updatedBy = entry.signer;
  }

  // Pinning is projection metadata: §7.7 keeps it out of drift semantics, so
  // it never contests a judgment and the last one written simply wins.
  let pinned = created.pinned;
  for (const entry of accepted) {
    if (entry.commit === creation.commit || !descends(entry, creation)) continue;
    if (entry.payload.type === "note.pinned" || entry.payload.type === "note.unpinned") {
      pinned = entry.payload.type === "note.pinned";
    }
  }

  const common = {
    id: note,
    path: created.path,
    anchor: created.anchor,
    text,
    createdAt: created.issuedAt,
    createdBy: creation.signer,
    updatedAt,
    updatedBy,
    active,
    pinned,
    baseline,
  };
  if (tips.length > 1) {
    return {
      state: "conflicted",
      ...common,
      competing: tips.map((entry) => ({
        commit: entry.commit,
        event: entry.payload.type,
        id: entry.payload.id,
      })),
    } satisfies ConflictedNote;
  }
  return { state: "ready", ...common } satisfies NoteProjection;
});

export const all = Effect.fn("hub.NoteProjection.all")(function* () {
  const projected: Projection[] = [];
  for (const id of yield* Note.notes()) {
    const note = yield* project(id);
    if (note !== null) projected.push(note);
  }
  return projected;
});
