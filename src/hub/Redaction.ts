/**
 * Which blobs a redaction tombstone has taken out of circulation.
 *
 * A tombstone is a statement, and deleting the loose copy of the blob it names
 * is only half of acting on it. Git stores objects twice over — loose, and
 * inside packs — and a pack cannot give up one object without being rewritten,
 * so `deleteObject` on a packed blob is a no-op that returns success. A
 * redaction that reported success while the payload stayed clonable would be
 * the worst possible shape for this feature: the operator believes the
 * credentials are gone, and they are still being served.
 *
 * So the removal is finished where pack rewriting already happens — in `gc`.
 * §21 and §27 of the spec say exactly this: a blob covered by a valid
 * tombstone is excluded from reachability protection and pruned. This module
 * computes "covered by a valid tombstone", and `gc` takes it as the set of
 * objects the walk must not protect.
 *
 * ## The four sets, and which way each one fails
 *
 * This module exports four answers over one question, and the difference
 * between them is *polarity* — which direction is the safe one when the walk
 * is short or the trust graph is silent. Reading one where another was meant
 * has been the single most repeated defect here, so they are named together:
 *
 * | set | the question | a *smaller* answer means |
 * | --- | --- | --- |
 * | `excluded` | may `gc` destroy this? | keeping bytes a removal asked for |
 * | `covered` | is this absence explained? | clones and deepening fetches failing, permanently |
 * | `removed` | did somebody accountable say to remove this? | re-retaining removed bytes on the next exposure |
 * | `withheld` | what did a removal ask for that `gc` will keep? | telling an operator a removal completed when it did not |
 *
 * So `excluded` withholds when it cannot see the whole ref, and `covered`
 * must not; `excluded` and `removed` require a tombstone that is bound and
 * counted, and `covered` deliberately requires neither, because a fetch has to
 * explain an absence whether or not the removal was authorized. A conservative
 * answer also has to stay *recomputable*: every memo here refuses to remember
 * a short walk, because the refs do not move when the missing objects arrive
 * and a pinned subset would never recover.
 *
 * "Valid" is load-bearing and is why this reads the projection rather than the
 * raw events. A tombstone only counts if it decoded, if its signer held
 * `hub.redact`, and if that signer was authorized when they signed it —
 * otherwise anybody who can push would be able to delete another member's
 * review by naming it.
 */
import { Context, Effect, Option } from "effect";

import type { Fingerprint } from "../crypto/SshSignature.ts";
import * as Dag from "../git/Dag.ts";
import type { Invalid, ObjectNotFound, StorageFailure } from "../git/Error.ts";
import { Repository } from "../git/Repository.ts";
import { type Oid, storageOf } from "../git/Store.ts";
import * as Record from "../trust/Record.ts";
import * as Verify from "../trust/Verify.ts";
import { GENESIS_REF, type Genesis, readGenesis } from "../trust/Genesis.ts";
import { LOG_REF } from "../trust/Log.ts";
import {
  project as projectTrust,
  type Projection as TrustProjection,
} from "../trust/Projection.ts";
import * as Event from "./Event.ts";
import { project } from "./Projection.ts";
import * as Exposure from "../context/Exposure.ts";
import * as Records from "../telemetry/Records.ts";
import * as Session from "./Session.ts";
import * as Claim from "./Claim.ts";
import * as Trace from "./Trace.ts";
import * as Task from "./Task.ts";
import * as Tombstone from "./Tombstone.ts";

/**
 * Every payload blob a valid tombstone covers, across every pull request.
 *
 * Returned as oids rather than deleted here, because whether an object can
 * actually be removed depends on whether it is packed — a question `gc` owns
 * and this module has no business answering.
 */
export const blobs = Effect.fn("hub.Redaction.blobs")(function* (
  genesis: Genesis,
  trust: TrustProjection,
  only?: ReadonlyArray<string>,
) {
  const repository = yield* Repository;

  const found = new Set<Oid>();
  for (const pr of only ?? (yield* tombstoned(yield* Event.pullRequests()))) {
    // One pull request this host cannot fold is one pull request. `tombstoned`
    // above already treats an unwalkable history that way, and leaving this
    // one unguarded put the whole repository's collection behind a single
    // pull request — including the purge that is how a redacted payload
    // actually leaves the object store.
    const state = yield* project(genesis, trust, pr).pipe(
      Effect.catchTag("Invalid", () => Effect.succeed(null)),
    );
    if (state === null || state.redacted.size === 0) continue;

    // By commit, not by event id: an id belongs to its author here, and a
    // tombstone matched by bare id would take every same-id event with it —
    // which is how somebody holding only `hub.comment` could have an approval's
    // payload deleted along with their own duplicate.
    for (const entry of state.redacted) {
      // The blob by the name its own tree gives it. A tombstone whose target
      // has already been collected finds nothing, which is the state it was
      // asking for.
      const info = yield* repository
        .readCommit(entry)
        .pipe(Effect.catchTag("ObjectNotFound", () => Effect.succeed(null)));
      if (info === null) continue;
      // The tree too. Refs are applied without a connectivity check, so a
      // replica can hold a commit whose tree never arrived — and this is `gc`,
      // which must not stop collecting a whole repository over one object it
      // was going to be told to drop anyway.
      const path = yield* repository
        .findPath(info.tree, `${Event.RECORD}.json`)
        .pipe(Effect.catchTag("ObjectNotFound", () => Effect.succeed(null)));
      if (path !== null) found.add(path.oid);
    }
  }
  return found;
});

// -- sessions and tasks ------------------------------------------------------------
//
// The same tombstone on a namespace with no fold. A pull request's redaction
// is judged by `Projection.ts`, which a pull request needs anyway; a session's
// projection reads what is there and authorizes nothing, so the two questions
// a tombstone raises — may this key write one, does a written one count — are
// asked directly against the trust graph. See `Tombstone.ts`.

/** Every session, task and trace ref on this repository. */
const recordRefs = Effect.fn("hub.Redaction.recordRefs")(function* () {
  return [
    ...(yield* Session.sessions()).map(Session.refOf),
    ...(yield* Task.tasks()).map(Task.refOf),
    // Traces last and by the same rule. A retained render carries the task
    // string verbatim and the exact bytes of every exposed file, so leaving
    // this namespace out made `session redact` a removal that removed the
    // prompt from the account of the work and left it in the account of the
    // runtime.
    ...(yield* Trace.traces()).map(Trace.refOf),
  ];
});

/**
 * The tombstones on one session or task ref: what each names, and who signed.
 *
 * Signers come out with the target because whether the tombstone counts is a
 * question about them, and re-reading the record to ask it later would mean
 * verifying every signature twice on a path `gc` takes for every repository.
 *
 * A ref this host will not walk — above the ceiling, or missing an object a
 * replica never received — contributes nothing, the same reading `tombstoned`
 * gives a pull request. Failing instead would take `gc` out for the whole host
 * over one ref that arrived by replication without passing the boundary.
 */
/** This repository's own id, or `""` where it has no genesis to state one. */
/**
 * The genesis, or `null` where this repository has none *this host can read*.
 *
 * Refs are applied without an ordering or connectivity check — which is the
 * premise most of this file rests on — so `refs/meta/trust/genesis` can name
 * an object that never arrived, or one that is not a genesis at all. Failing
 * there takes `gc` and every deepening fetch of the repository down with it,
 * over a state the rest of this module tolerates everywhere else.
 */
const genesisOf = Effect.fnUntraced(function* () {
  return yield* readGenesis().pipe(
    Effect.catchTags({
      Invalid: () => Effect.succeed(null),
      ObjectNotFound: () => Effect.succeed(null),
    }),
  );
});

const repoOf = Effect.fnUntraced(function* () {
  const stored = yield* genesisOf();
  return stored === null ? "" : stored.genesis.repoId;
});

/** One tombstone: what it names, who signed it, and whether it belongs here. */
export interface Mark {
  readonly target: Oid;
  /** Whether the payload names this ref's session and this repository. */
  readonly bound: boolean;
  readonly signers: ReadonlyArray<Fingerprint>;
}

/**
 * The `type` a payload gives itself, without running any schema.
 *
 * Enough to skip the decode for a record that is not a tombstone, and correct
 * where a byte search is not: an escaped `type` parses to the tag and does not
 * contain it literally.
 */

const tombstonesOn = Effect.fn("hub.Redaction.tombstonesOn")(function* (ref: string) {
  const repository = yield* Repository;
  const found: Array<Mark> = [];
  // Every record commit this walk reached, handed back so that the question
  // "does anything still live name this blob?" does not need a second walk of
  // the same refs. `covered` runs on the deepening-fetch path, which is
  // anonymous and can be driven in a loop, so walking these twice was twice
  // the cost on exactly the path the namespace split exists to protect.
  const commits: Array<Oid> = [];

  const head = yield* repository.resolve(ref);
  if (head === null) return { found, commits, complete: true } as const;

  const kind =
    Session.sessionOf(ref) !== null ? "session" : Trace.traceOf(ref) !== null ? "trace" : "task";
  // Each namespace's own ceiling. Trace refs are deliberately bounded higher
  // than a pull request's fold (docs/telemetry.md §13), so asking `Event`'s
  // made every tombstone on a trace ref longer than 4096 records vanish: the
  // walk failed, `parents` came back null, and this returned nothing at all —
  // a redaction that pushed fine and was then never honoured.
  const ceiling = kind === "trace" ? yield* Trace.ceilingOf() : yield* Event.ceilingOf();
  const parents = yield* Dag.reachable(head, null, Event.isHubCommit, ceiling).pipe(
    Effect.catchTags({
      Invalid: () => Effect.succeed(null),
      ObjectNotFound: () => Effect.succeed(null),
    }),
  );
  // A ref this host will not walk is a ref whose live records cannot be
  // enumerated, and that is not a detail — see `stillNamed`.
  if (parents === null) return { found, commits, complete: false } as const;

  // And an *empty* walk of a ref that has a head is the same thing wearing a
  // different face. `Event.isHubCommit` deliberately answers `false` when the
  // commit's tree is missing — the partial-replication state it is written for
  // — so a replica holding a head without its tree got an empty map and no
  // error. Read as a complete walk of a ref with no live records, that handed
  // every blob its exposures shared with another session's to `gc`, silently.
  if (!parents.has(head)) return { found, commits, complete: false } as const;

  // And a walk that stopped *part way* is the same thing again, one level
  // down. `Dag.reachable` stops traversing at any commit `Event.isHubCommit`
  // rejects, and it rejects a commit whose tree is missing — so a ref whose
  // head is intact but whose older trees never arrived came back truncated
  // and called itself whole. Two things follow, and both are what this flag
  // was added to prevent: `tombstonesFor` pins the short answer against a head
  // that does not move when a deepening fetch delivers the objects, so
  // tombstones behind the gap are never honoured again; and `stillNamed` never
  // sees the live records behind it, so a deterministic pack or render shared
  // with a redacted one is not subtracted and `gc` takes a blob a live
  // exposure still needs.
  //
  // A parent nobody walked is the signal: hub history is self-contained, so
  // every parent of a reached commit is itself reachable unless the traversal
  // stopped there.
  const whole = [...parents.values()].every((named) =>
    named.every((parent) => parents.has(parent)),
  );
  if (!whole) return { found, commits, complete: false } as const;

  // Hoisted: it cannot change during the walk, and inside the loop it was a
  // ref resolve per record on every session, task and trace ref.
  const repo = yield* repoOf();
  for (const commit of parents.keys()) {
    // One commit read for both facts. This ran `carries` and then `read` —
    // two commit reads, three path lookups and two blob reads — for every
    // record on every session, task and trace ref, to answer a question that
    // discards all but a handful. `removed()` takes this walk on
    // `context for --session`, once per model invocation, in a fresh process
    // where the per-ref memo is always cold; a trace ref gains a record per
    // invocation, so the cost of invocation N grew with every record written
    // before it.
    //
    // What is left is still linear in the repository's records. Making it less
    // than that means keeping the answer somewhere a new process can read,
    // which is a decision about durable derived state rather than a change to
    // this walk.
    const held = yield* Record.payloadOf(commit, Event.RECORD);
    if (!held.carried) continue;
    commits.push(commit);
    if (held.bytes === null) continue;

    // The `type` this payload gives itself, read by parsing rather than by
    // searching the bytes. A byte search was wrong in the direction that
    // matters: JSON permits `\uXXXX` inside a string, so
    // `{"type":"event.redact\u0065d"}` parses to the tag and contains no
    // literal `"event.redacted"` — and this codebase's own writer never emits
    // that, but another implementation replicating in is the threat model the
    // whole file is written against. Missed here, `gc` never collects the
    // payload or the render while `session show --audit` reports the removal
    // as done.
    //
    // Still a filter, because the schema decode below is the expensive part
    // and this skips it for everything that is not a tombstone. `removed()`
    // runs on `context for --session`, once per model invocation, in a fresh
    // process where the per-ref memo is always cold — so this walk is the
    // pre-call path's cost and it grows with the repository's age.
    // The signature blobs are read only for the records that turn out to be
    // tombstones, which is what makes the saving hold: everything above is one
    // blob, and `Record.read` below is two more plus a parse.
    if (Claim.declaredType(held.bytes) !== Tombstone.TAG) continue;
    const record = yield* Record.read(commit, Event.RECORD).pipe(
      Effect.catchTags({
        ObjectNotFound: () => Effect.succeed(null),
        Invalid: () => Effect.succeed(null),
      }),
    );
    if (record === null) continue;

    // Decoded as what the ref *is*: the two unions share this member and
    // nothing else, and a task record read as a session decodes as nothing.
    //
    // Two branches rather than one decoder chosen by a conditional, and it is
    // not a style preference: an `Effect` whose success type is the union of
    // both payload unions was more than inference would follow, and it gave up
    // by widening — `excluded` came out with `unknown` for its errors and its
    // context, and every caller of it downstream inherited that. Each branch
    // yields a `string | null` and the shapes never meet.
    // Bound to the ref it was read from and to this repository, in all three
    // namespaces. The write paths guard themselves, but replication is not
    // policy-gated: a tombstone from another repository or naming another
    // session was folded in and honoured, so `gc` collected the payload — and
    // for a trace record the pack, the render and the view with it — while
    // both readers *do* bind, so `session show --audit` called the result
    // "record(s) could not be read here" and `context audit` counted it
    // unreadable and failed for good. An origin and a mirror sharing a trust
    // graph is all it takes: a tombstone written on one names a commit that
    // lives on the other's ref, replicates, and destroys a payload nobody
    // there asked to remove, on a ref nothing can rewind.
    let named: string | null = null;
    let bound = true;
    if (kind === "session") {
      const payload = yield* Session.decode(record.payload).pipe(Effect.orElseSucceed(() => null));
      if (payload?.type === Tombstone.TAG) {
        named = payload.targetCommit;
        bound = Claim.bound(payload, { repo, session: Session.sessionOf(ref) ?? "" });
      }
    } else if (kind === "trace") {
      const payload = yield* Records.decode(record.payload).pipe(Effect.orElseSucceed(() => null));
      if (payload?.type === Tombstone.TAG) {
        named = payload.targetCommit;
        bound = Claim.bound(payload, { repo, session: Trace.traceOf(ref) ?? "" });
      }
    } else {
      const payload = yield* Task.decode(record.payload).pipe(Effect.orElseSucceed(() => null));
      if (payload?.type === Tombstone.TAG) {
        named = payload.targetCommit;
        // A task ref names its subject `task` rather than `session`; the test
        // is the same one.
        bound = Claim.bound(
          { repo: payload.repo, session: payload.task },
          { repo, session: Task.taskOf(ref) ?? "" },
        );
      }
    }
    if (named === null) continue;
    const target = Event.unqualify(named);
    if (target === null) continue;
    found.push({
      target,
      bound,
      signers: yield* Verify.signers(record.payload, record.signatures),
    });
  }
  return { found, commits, complete: true } as const;
});

/**
 * The blobs one record commit carries, if this host still holds them, in two
 * halves.
 *
 * The halves differ in whether another record can be holding the same object.
 * `event.json` is a signed statement over its own bytes: two records share one
 * only by being byte-identical, which makes them the same statement, so a
 * tombstone naming one names exactly the bytes it meant. A Context Pack, a
 * ContextRender and a `context/view` overlay are deterministic instead —
 * different records making different statements land on the same object — so
 * one record's tombstone must never decide those on its own. `excluded` gates
 * the two on different conditions; see there.
 *
 * A tombstone whose target has already been collected finds nothing, which is
 * the state it was asking for; a target whose tree never arrived is a replica
 * that applied a ref without a connectivity check, and neither is a reason to
 * stop collecting the repository.
 */
const payloadOf = Effect.fn("hub.Redaction.payloadOf")(function* (commit: Oid) {
  const repository = yield* Repository;
  const payload: Array<Oid> = [];
  const attached: Array<Oid> = [];
  const info = yield* repository
    .readCommit(commit)
    .pipe(Effect.catchTag("ObjectNotFound", () => Effect.succeed(null)));
  if (info === null) return { payload, attached } as const;

  const at = (path: string) =>
    repository
      .findPath(info.tree, path)
      .pipe(Effect.catchTag("ObjectNotFound", () => Effect.succeed(null)));

  const record = yield* at(`${Event.RECORD}.json`);
  if (record !== null) payload.push(record.oid);

  // The `context/` half only where there is a `context/` — one lookup instead
  // of three that always miss, on every session and task record `gc` walks.
  if ((yield* at(DIRECTORY)) === null) return { payload, attached } as const;
  for (const path of ATTACHED) {
    const entry = yield* at(path);
    if (entry !== null) attached.push(entry.oid);
  }
  return { payload, attached } as const;
});

/** Both halves, for the callers that draw no distinction between them. */
const blobsOf = Effect.fn("hub.Redaction.blobsOf")(function* (commit: Oid) {
  const found = yield* payloadOf(commit);
  return [...found.payload, ...found.attached];
});

/**
 * Every blob a live record on these refs still needs.
 *
 * Content-addressing is the whole hazard. `event.json` carries a unique `id`
 * and `issuedAt`, so no two records share one — but a Context Pack and a
 * ContextRender are deterministic by design: identity *is* the blob oid of the
 * exact bytes, so running `context for` twice against an unchanged tree and
 * one unchanged task writes two records whose `context/pack.json` and
 * `context/render.bin` are the same git object. Excluding one record's blobs
 * without asking who else names them deleted the other record's content:
 * `Maintenance.gc` re-walks only the source refs, so nothing here rescues a
 * blob a hub ref alone reaches, and the surviving exposure's audit reported
 * its own pack unavailable forever on a ref that cannot be rewound.
 *
 * So the exclusion set is what the tombstones name *minus* what anything still
 * live names. `ATTACHED` covers `context/view` for the opposite reason — a
 * dirty overlay is reachable through its record and nothing else — and both
 * decisions rest on the same fact: `Maintenance.gc` re-walks the source refs
 * without the exclusion, so what a branch reaches is never in danger, and what
 * only a hub ref reaches needs asking about here.
 */
const stillNamed = Effect.fn("hub.Redaction.stillNamed")(function* (
  commits: ReadonlyArray<Oid>,
  redacted: ReadonlySet<Oid>,
) {
  const kept = new Set<Oid>();
  for (const commit of commits) {
    if (redacted.has(commit)) continue;
    for (const blob of yield* blobsOf(commit)) kept.add(blob);
  }
  return kept;
});

/**
 * Every blob a tombstone is asking to have collected.
 *
 * The payload, and — for a Context Exposure — the two blobs the record itself
 * owns beside it. `context/render.bin` is where the leak actually is: it holds
 * the task string verbatim and the exact bytes of every exposed file, so a
 * redaction that removed only `event.json` removed the account of the exposure
 * and left the exposure itself readable and clonable forever. That is the
 * failure trace redaction exists to close, and removing one blob did not close
 * it.
 *
 * `context/view` is here too, and leaving it out was a hole rather than a
 * kindness. For a *dirty* checkout `Pack.capture` writes an overlay tree that
 * no commit reaches — the record's own edge is the only thing holding it — and
 * it contains the exact bytes of every tracked file, the edited one included.
 * So a secret typed into a file, exposed, redacted and collected still read
 * back out of `<record>^{tree}:context/view/<path>` forever, on a ref nothing
 * can rewind. The removal reported success and left behind the thing it was
 * asked to remove.
 *
 * The *tree* oid, not the blobs under it, and that is the whole trick: `skip`
 * in `Maintenance.gc`'s walk stops traversal at the object it names, so one
 * entry makes everything below unreachable — one `findPath` per record instead
 * of enumerating a subtree per record on a path that already walks too much.
 *
 * The paths come from `context/Exposure.ts` rather than being spelled again
 * here. Re-declared, a rename there would silently stop `payloadOf` returning
 * the render blob — `gc` would keep it, `trace redact` would report success,
 * and the verbatim prompt would stay clonable forever on an append-only ref —
 * with no compile error anywhere to catch it.
 *
 * Nothing a branch reaches is endangered by any of this: `Maintenance.gc`
 * re-walks the source refs *without* the exclusion, so a blob that is also a
 * file in some commit survives. And an overlay two live exposures share is
 * protected by `stillNamed`, which collects exactly these entries for records
 * no tombstone names.
 */
const DIRECTORY = Exposure.DIRECTORY;
const ATTACHED = [Exposure.PACK, Exposure.RENDER, Exposure.VIEW];

/**
 * Every tombstone across every session and task, in one walk.
 *
 * Flat rather than per-ref because both callers want it that way, and walked
 * once because both of the questions asked of it — what a tombstone names, and
 * whether it counts — are answered from the same two fields. Whether the trust
 * graph is folded at all is then decided by whether this came back empty,
 * which is what keeps a repository with no redactions in it from paying for a
 * fold on every `gc`.
 */
/**
 * One ref's tombstones, remembered against that ref's own head.
 *
 * Per ref rather than per repository, and it is the difference between a memo
 * that helps and one that does not. `covered` runs on the anonymous deepening
 * fetch path and its whole-repository key holds every record ref's oid — so on
 * a repository with active agents, one `trace record` invalidated the answer
 * for every session at once and the next fetch re-walked all of them. Keyed on
 * `(storage, ref, head)` the cost is proportional to what actually moved, and
 * staleness is impossible because the head is in the key.
 *
 * The commit list is deliberately not remembered. Only `excluded` reads it,
 * `gc` is where `excluded` runs, and holding an Oid array per ref would put
 * the whole repository's record history in a memo inside a worker with a fixed
 * memory ceiling.
 */
/**
 * How many *refs* the per-ref memos remember.
 *
 * Separate from `MEMO`, which bounds one entry per repository. These hold one
 * entry per ref, and `recordRefs()` yields a session ref and a trace ref per
 * session — so sharing the repository-sized bound made a repository with more
 * than a couple of hundred sessions evict its own entries inside a single
 * `marks()` pass, giving the memo a zero hit rate in exactly the
 * "fleet of active agents" case it was added for.
 */
const REFS = 4096;

const onRef = new Map<
  string,
  {
    /** The head the answer was computed from; see `tombstonesFor`. */
    readonly head: Oid | null;
    readonly found: ReadonlyArray<Mark>;
    readonly complete: boolean;
  }
>();

/**
 * Derived-state port, per ref, for the walk a fresh process would repeat.
 *
 * The maps above live for the life of a process, which is right for a server
 * and useless for the CLI: `context for --session` asks `removed()` once per
 * model invocation, each time in a new process, so the walk is redone from
 * cold and its cost grows with every record the repository has ever written.
 * §13.1 of docs/telemetry.md has the measurements.
 *
 * Per *ref*, and that is the whole of why this works. The first shape of this
 * kept one answer for the repository, keyed on every record ref's head — and
 * `context for --session S` appends to `refs/hub/trace/S` immediately after
 * asking, so the next invocation computed a different key and missed, every
 * time, while still paying a write. Keyed per ref, an append invalidates the
 * one ref it moved and the other however-many are read rather than walked,
 * which is the cost that actually grows with the repository.
 *
 * It is also the level with no trust in it. `tombstonesOn` decodes tombstones
 * and decides `bound` from the genesis; whether a tombstone *counts* is
 * `removed()`'s question, folded fresh each time against the trust log. So a
 * grant arriving after the tombstone it authorises cannot be served a stale
 * answer, because the part that depends on the trust log was never kept.
 *
 * The shape is `git/Search.SearchIndex`'s, and deliberately: that is this
 * codebase's answer to "derived state a host may want to keep", and inventing
 * a second one — a cache ref, a file under the git directory — would have
 * dragged in the advertisement, `hub disable` and reachability questions a
 * port does not raise. Where the answer is *kept* is a deployment decision, so
 * it is the host's to make; the default keeps nothing and behaves exactly as
 * this module did before the port existed.
 *
 * ## The contract, and it is the whole of the risk
 *
 * `read` MUST answer `null` for anything it is not certain of — a key it does
 * not hold, a store it cannot read, a value it cannot parse. An implementation
 * that guesses "nothing was removed" re-opens the resurrection this lookup
 * exists to prevent: the next identical `context for` retains a render whose
 * bytes an operator removed, under the same oid, where the redacted record's
 * surviving tree entry resolves them again — and it does that silently, since
 * a retained render is the ordinary case and raises no notice.
 *
 * The key carries everything this answer depends on: the storage identity, the
 * genesis, the namespace's ceiling, the ref, and its head. A stale answer is
 * therefore only possible if an implementation ignores part of it, which is
 * why `read` takes the whole key and not its parts.
 */
export class Answers extends Context.Service<
  Answers,
  {
    readonly read: (key: string) => Effect.Effect<ReadonlyArray<Mark> | null>;
    readonly write: (key: string, found: ReadonlyArray<Mark>) => Effect.Effect<void>;
  }
>()("hub/Redaction.Answers") {}

const tombstonesFor = Effect.fn("hub.Redaction.tombstonesFor")(function* (ref: string) {
  const repository = yield* Repository;
  const head = yield* repository.resolve(ref);
  // The ceiling belongs in the key for the reason it belongs in the
  // repository-wide one: a host that will not walk a ref this long reports no
  // tombstones on it, so two ceilings are two answers under what would
  // otherwise be one key — and the stale one is a redaction silently not
  // honoured.
  const ceiling = Trace.traceOf(ref) !== null ? yield* Trace.ceilingOf() : yield* Event.ceilingOf();
  // The head is *validated*, not keyed on. In the key, every append left the
  // previous entry behind permanently dead — and `REFS` is one global map
  // shared by every repository this process serves, so a fleet of agents on
  // one repository minted a new entry per `context for` and evicted every
  // other repository's answer, putting the anonymous deepening fetch back on a
  // full walk of every session, task and trace ref. That is the failure the
  // repository-wide memo below documents and avoids the same way.
  // The genesis is in the key because the answer depends on it: every `Mark`'s
  // `bound` is computed against this repository's own id, and `repoOf` returns
  // `""` when there is no genesis yet — so a process that touched a ref before
  // the genesis arrived cached every tombstone on it as unbound, and the
  // genesis landing moves no record ref. `excluded` recomputes then, its own
  // key naming `GENESIS_REF`, but `marks` hands back the stale entries: every
  // removal skipped for the life of the process, `gc` never dropping the
  // redacted payload or render, and `removed()` empty so the next identical
  // `context for` re-retains it. The mirrored case is a `hub init` that mints
  // a new `repoId`, which flips `bound` the other way and has `gc` act on
  // tombstones that no longer bind.
  const genesis = yield* repository.resolve(GENESIS_REF);
  const key = `${yield* storageOf()}\u0000${ceiling}\u0000${genesis}\u0000${ref}`;

  const known = onRef.get(key);
  if (known !== undefined && known.head === head) {
    onRef.delete(key);
    onRef.set(key, known);
    return known;
  }

  // Then the host's, if it kept one. The head goes in the key here rather than
  // being validated against it: a durable answer has no place to hold the
  // "which head was this?" field that the in-process map validates, and two
  // heads are two answers either way. Stale entries left behind are harmless —
  // nothing can read them, since the key names the head they were made from.
  // Asked for, not required. A cache must not change the type of every caller
  // that reads this module — the server's paths, the client's push planner and
  // their tests all reach `covered`/`excluded`, and none of them wants an
  // opinion about durable state. A host that has somewhere to keep an answer
  // provides the layer; everything else is exactly as it was.
  const answers = Option.getOrNull(yield* Effect.serviceOption(Answers));
  const durable = `${key}\u0000${head}`;
  const kept = answers === null ? null : yield* answers.read(durable);
  if (kept !== null) {
    // Whole by construction: only a complete walk is ever written.
    const answer = { head, found: kept, complete: true } as const;
    onRef.delete(key);
    onRef.set(key, answer);
    return answer;
  }

  const walked = yield* tombstonesOn(ref);
  const answer = { head, found: walked.found, complete: walked.complete } as const;
  // Cached only when the walk saw the whole ref. `tombstonesOn` reports
  // `complete: false` for a head whose tree never arrived — the ordinary
  // partial-replication state this file is written for — and the head does not
  // move when a later fetch delivers the missing objects. Remembered against
  // the head alone, that "no tombstones here" stood for the life of the
  // process: `marks` reported none, `excluded` short-circuited, `covered`
  // omitted the blobs, and `gc` kept a redacted `render.bin` that a deepening
  // fetch could no longer explain. Before this memo existed, any hub ref
  // moving invalidated the repository-wide one and it recovered on the next
  // append.
  if (walked.complete) {
    onRef.delete(key);
    onRef.set(key, answer);
    // The same rule for the durable copy, and for the same reason: the refs do
    // not move when the missing objects arrive, so a kept short walk would
    // never recover — and on disk it would outlive the process that made it.
    if (answers !== null) yield* answers.write(durable, walked.found);
  }
  while (onRef.size > REFS) {
    const oldest = onRef.keys().next();
    if (oldest.done === true) break;
    onRef.delete(oldest.value);
  }
  return answer;
});

/**
 * The payload blobs one pull request's tombstones name, remembered per ref.
 *
 * The other half of the per-ref memo, and the half that actually cost
 * something. `covered`'s outer memo is keyed on *every* record ref's head, so
 * one `trace record` invalidated it wholesale and the next anonymous deepening
 * fetch re-walked every pull request in the repository — and a trace ref is
 * appended to once per model invocation rather than once per session event, so
 * a fleet of active agents kept that memo permanently cold. Keyed here on this
 * ref's own head, a trace append costs nothing on this side.
 *
 * One walk per miss, and none per hit. The old shape ran `tombstoned` over
 * every pull request and then `Event.entries` again over the survivors; this
 * walks once and remembers the answer against the ref's own head, which is
 * cheaper on a miss and free on a hit.
 */
const onPullRequest = new Map<string, { readonly head: Oid | null; readonly found: Set<Oid> }>();

const namedByPullRequest = Effect.fn("hub.Redaction.namedByPullRequest")(function* (pr: string) {
  const repository = yield* Repository;
  const ref = Event.refOf(pr);
  // Validated rather than keyed on, for the reason `tombstonesFor` gives.
  const head = yield* repository.resolve(ref);
  const key = `${yield* storageOf()}\u0000${yield* Event.ceilingOf()}\u0000${ref}`;

  const known = onPullRequest.get(key);
  if (known !== undefined && known.head === head) {
    onPullRequest.delete(key);
    onPullRequest.set(key, known);
    return { found: known.found, complete: true } as const;
  }

  const found = new Set<Oid>();
  // A pull request this replica will not walk is one candidate missing, the
  // same reading every other caller gives it. The cost of failing instead is
  // this set escaping into the fetch planner — every deepening fetch and every
  // hub-ref push on the replica failing over one over-sized pull request that
  // arrived by replication without passing the boundary that would have
  // refused it.
  const walked = yield* Event.entries(pr).pipe(
    Effect.catchTag("Invalid", () => Effect.succeed(null)),
  );
  // Whether this answer is the whole answer, which decides only whether it is
  // worth remembering. The walk can fail, and a tombstone's target commit or
  // tree can be an object this replica has not received — both are ordinary,
  // both are handled by skipping, and both make the result a *subset*. Cached
  // anyway against a head that does not move when a later deepening fetch
  // delivers the missing objects, that subset was pinned for the process
  // lifetime — and this set has the opposite polarity to `excluded`'s, so a
  // subset is clones and deepening fetches failing on an absence nothing can
  // explain. `tombstonesFor` immediately above guards exactly this, and its
  // docstring notes the old repository-wide memo recovered on its own because
  // any hub ref moving invalidated it; the per-ref memo is what removed that.
  let complete = walked !== null;
  for (const entry of walked?.events ?? []) {
    if (entry.payload?.type !== Tombstone.TAG || entry.payload.pr !== pr) continue;
    const target = Event.unqualify(entry.payload.targetCommit);
    if (target === null) continue;
    const info = yield* repository
      .readCommit(target)
      .pipe(Effect.catchTag("ObjectNotFound", () => Effect.succeed(null)));
    if (info === null) {
      complete = false;
      continue;
    }
    // As above, and here it matters more: this set is what a *fetch* takes, so
    // a failure is every deepening fetch of the repository failing over a
    // tombstone whose target is exactly the object that did not arrive.
    const path = yield* repository
      .findPath(info.tree, `${Event.RECORD}.json`)
      .pipe(Effect.catchTag("ObjectNotFound", () => Effect.succeed(null)));
    if (path === null) complete = false;
    else found.add(path.oid);
  }

  if (complete) {
    onPullRequest.delete(key);
    onPullRequest.set(key, { head, found });
    // Bounded like its three siblings. `REFS` is one global map shared by
    // every repository this process serves, and `covered` — on the anonymous
    // deepening-fetch path — now asks this for *every* pull request rather
    // than only the tombstoned ones, so an unbounded map is a front-end
    // holding a `Set` per pull request per repository it has ever served,
    // against a worker with a fixed memory ceiling. The eviction was dropped
    // when the write moved inside this guard.
    while (onPullRequest.size > REFS) {
      const oldest = onPullRequest.keys().next();
      if (oldest.done === true) break;
      onPullRequest.delete(oldest.value);
    }
  }
  return { found, complete } as const;
});

/**
 * The `complete` flag is for the *memo*, not for the set.
 *
 * `excluded` honours it as a gate — it says "gc may delete this", so an
 * unwalkable ref means the shared `context/` blobs on it were never enumerated
 * and must be held back. `covered` must not: it says "this absence is
 * explained", where a smaller set is how a fetch starts failing with nothing
 * to recover from, so it takes whatever was found. What it does with the flag
 * is decline to *remember* a short answer, since the refs do not move when the
 * missing objects arrive and a pinned subset would never recover.
 */
const marks = Effect.fn("hub.Redaction.marks")(function* () {
  const found: Array<Mark> = [];
  let complete = true;
  for (const ref of yield* recordRefs()) {
    const walked = yield* tombstonesFor(ref);
    found.push(...walked.found);
    complete &&= walked.complete;
  }
  return { found, complete } as const;
});

/**
 * The tombstones *and* the commits, in one walk of each ref.
 *
 * What `gc` needs, and only `gc`: it wants both halves, and asking `marks()`
 * for one and `recordCommits()` for the other walked and decoded every session,
 * task and trace ref twice per collection. `covered` still takes the memoized
 * half alone, because it never looks at the commits.
 */
const marksAndCommits = Effect.fn("hub.Redaction.marksAndCommits")(function* () {
  const found: Array<Mark> = [];
  const commits: Array<Oid> = [];
  let complete = true;
  for (const ref of yield* recordRefs()) {
    const walked = yield* tombstonesOn(ref);
    found.push(...walked.found);
    commits.push(...walked.commits);
    complete &&= walked.complete;
  }
  return { found, commits, complete } as const;
});

/**
 * The same set, read from the repository, for a caller that has no trust state
 * in hand.
 *
 * `gc` runs on every repository and most of them have no genesis, so "nothing
 * is redacted" is the ordinary answer rather than an error. Both `gc` entry
 * points want exactly this one line.
 */
export const excluded = Effect.fn("hub.Redaction.excluded")(function* () {
  const repository = yield* Repository;

  // The memo is consulted *before* the walk it exists to save. Keyed on
  // anything the walk produced, it saved only the fold — and the walk reads
  // every event of every pull request, on a path (a deepening fetch) an
  // anonymous reader can drive in a loop. Ref names and their oids are what a
  // ref store already has, and the answer is a pure function of them, so a
  // moved ref changes the key and a stale answer is not possible.
  const refs = yield* Event.pullRequests();
  const written = yield* recordRefs();
  // The storage as well as the genesis: an origin and its mirror under one
  // host share the genesis oid, and right after a replication the hub ref oids
  // too — while what they can read need not agree, since refs are applied
  // without a connectivity check. See `Storage`.
  const identity = `${yield* storageOf()}\u0000${yield* repository.resolve(GENESIS_REF)}`;
  // Names as well as oids. A repository with no genesis keys under `null`
  // along with every other one, so the state is all that separates them — and
  // oids alone made a fork and its parent, whose pull requests point at the
  // same commits under different names, one entry.
  // The repository's own identity keys the entry. Left out, the key was the
  // hub and trust refs alone: two repositories on one host whose heads happen
  // to match — the same trust log under a *different* genesis, which is a
  // different membership and so a different answer — shared an entry, and one
  // of them collected with an exclusion set computed for the other.
  const state = [
    // The ceiling is part of the answer, not of the repository: a host that
    // will not walk a pull request that size reports nothing redacted in it,
    // so two hosts with the same refs and different ceilings hold two
    // different answers under what would otherwise be one key.
    `ceiling ${yield* Event.ceilingOf()}/${yield* Trace.ceilingOf()}`,
    yield* repository.resolve(LOG_REF),
    ...(yield* Effect.forEach(refs, (pr) =>
      repository.resolve(Event.refOf(pr)).pipe(Effect.map((oid) => `${Event.refOf(pr)} ${oid}`)),
    )),
    // The session and task refs too: a tombstone on one of those removes a
    // payload exactly as a pull request's does, and a key that did not name
    // them served a stale answer to every `gc` after the first.
    ...(yield* Effect.forEach(written, (ref) =>
      repository.resolve(ref).pipe(Effect.map((oid) => `${ref} ${oid}`)),
    )),
  ].join("\u0000");

  const known = memo.get(identity);
  if (known !== undefined && known.state === state) {
    // Re-inserted so iteration order is least-recently-used first.
    memo.delete(identity);
    memo.set(identity, known);
    return known.found;
  }

  // Walked before anything is folded, and the trust log is not folded at all
  // unless a tombstone turned up. A fold verifies a signature per record and
  // per event; any *valid* tombstone is first of all a decoded `event.redacted`
  // payload, so a walk that finds none rules the repository out without a
  // single verification. The filter can produce a false positive, which costs
  // one fold; it cannot produce a false negative, which is what would matter.
  const candidates = yield* tombstoned(refs);
  // The memoized half first, and the commit walk only if it turns anything up.
  // `marksAndCommits` walks and decodes every record on every session, task
  // and trace ref *uncached* — and this now runs on the exposure path, where
  // `context for --session` moves that session's trace head and so misses this
  // memo every time. A repository with no tombstones in it is the ordinary
  // case and paid the whole walk for an answer that is always empty; with
  // `marks` first it pays the per-ref memo, which only the ref that actually
  // moved invalidates.
  const marked = yield* marks();
  const stored = candidates.length === 0 && marked.found.length === 0 ? null : yield* genesisOf();
  const empty: ReadonlyArray<Oid> = [];
  // And only when *this* namespace has one. `stored !== null` also covers a
  // repository whose single tombstone is on a pull-request ref, where the one
  // thing this walk adds — `commits`, for `stillNamed` — is never read,
  // because `written` and `shared` come back empty. A repository with one
  // redacted comment and three hundred sessions paid an uncached walk and
  // decode of every record on every session, task and trace ref and threw the
  // answer away, on `gc` and on every `context for --session`.
  const walked =
    stored === null || marked.found.length === 0
      ? // `marked.complete`, not `true`. `tombstonesOn` reports a ref it could
        // not walk as `{found: [], complete: false}` — head present, tree never
        // received, the ordinary partial-replication state — so no tombstones
        // found is *exactly* the case where an unwalkable ref goes unnoticed.
        // Hardcoded, this branch called the answer whole, the memo below stored
        // it against heads that do not move when a deepening fetch delivers the
        // missing objects, and every later `gc` in the process returned the
        // stale set: the trace ref's tombstone never honoured, its
        // `context/render.bin` still in the store and still clonable.
        { found: marked.found, commits: empty, complete: marked.complete }
      : yield* marksAndCommits();
  const removals = walked.found;
  const found = new Set<Oid>();
  if (stored !== null) {
    const trust = yield* projectTrust(stored.genesis);
    for (const oid of yield* blobs(stored.genesis, trust, candidates)) found.add(oid);

    // The record namespaces' contribution, kept apart from the pull requests'.
    // Valid by `Tombstone.counts`: a signer the trust graph knows who ever
    // held `hub.redact`. Without that, anybody who may append to a session —
    // which is anybody holding `hub.session` — could name another agent's
    // prompt and have `gc` destroy it.
    const written = new Set<Oid>();
    const shared = new Set<Oid>();
    const targets = new Set<Oid>();
    for (const removal of removals) {
      // Bound and counted. `covered` deliberately asks neither — it says which
      // absences are *explained*, where a smaller set is how a fetch starts
      // failing with nothing to recover from — but this one says what `gc` may
      // destroy, and there the safe direction is the other way.
      if (!removal.bound) continue;
      if (!Tombstone.counts(trust, removal.signers)) continue;
      targets.add(removal.target);
      const blobs = yield* payloadOf(removal.target);
      for (const blob of blobs.payload) written.add(blob);
      for (const blob of blobs.attached) shared.add(blob);
    }

    // What anything still live names comes back out. A pack and a render are
    // deterministic, so two exposures of the same view and task share both
    // blobs — and excluding one record's without asking who else names them
    // deleted the other record's content. See `stillNamed`.
    if (written.size > 0 || shared.size > 0) {
      for (const blob of yield* stillNamed(walked.commits, targets)) {
        written.delete(blob);
        shared.delete(blob);
      }
    }

    // The payload blobs go regardless of what this host could walk. A ref it
    // could not walk hides live records, and the subtraction above is what
    // those records would have contributed — but only a *shared* object can be
    // held by a record other than the one the tombstone names, and a payload
    // is not one. Two records share an `event.json` only by carrying the same
    // bytes, which makes them the same signed statement; a tombstone over
    // those bytes covers every copy of them by construction.
    for (const blob of written) found.add(blob);

    // The `context/` blobs wait for a complete walk, because those *are*
    // shared: a Pack and a ContextRender are deterministic, so two exposures
    // of one view and one task are two different records naming one object.
    // The set is *subtracted* from, so an unwalkable ref is the destructive
    // direction here, not the cautious one — and `Maintenance.gc` re-walks
    // only the source refs, so nothing rescues a blob a hub ref alone reaches.
    //
    // Held apart from the payloads for the blast radius rather than the
    // pedantry. Gating both took every session and task redaction in the
    // repository down over one trace ref whose head tree a replica never
    // received — the ordinary partial-replication state, and the namespace
    // most likely to be in it, since a trace record is the only one carrying a
    // `context/` subtree. Sessions and tasks have no shared half at all, so
    // they are now unaffected by it.
    //
    // Narrowed once before, from clearing everything to this namespace's own
    // contribution: that took the pull requests' tombstones down too, while
    // `tombstoned`/`blobs` were carefully written to treat one unwalkable pull
    // request as one pull request rather than a broken repository.
    if (walked.complete) for (const blob of shared) found.add(blob);
  }

  // Remembered only when the walk saw everything, exactly as `tombstonesFor`,
  // `namedByPullRequest` and `covered` are. The `shared` half above is
  // *withheld* when the walk came back short — the safe direction — but stored
  // unconditionally that withholding was permanent: none of the heads in the
  // key move when a deepening fetch delivers the objects that made the walk
  // incomplete, so every later `gc` in the process kept withholding, and the
  // verbatim task string and every exposed file byte stayed clonable off the
  // ref. A conservative answer has to stay recomputable or it is not
  // conservative, it is the wrong answer with a longer life.
  if (walked.complete) {
    memo.delete(identity);
    memo.set(identity, { state, found });
  }
  while (memo.size > MEMO) {
    const oldest = memo.keys().next();
    if (oldest.done === true) break;
    memo.delete(oldest.value);
  }
  return found;
});

/**
 * Every blob a counted, bound tombstone names — with no completeness gate.
 *
 * `excluded` withholds the shared `context/` half whenever any record ref came
 * back short, which is the safe direction *for `gc`*: a blob it cannot prove
 * unused must not be deleted. `context for` reuses the answer for the opposite
 * purpose — deciding whether to *retain* a render whose bytes a removal named
 * — and there the withholding inverts. On a replica holding one partially
 * replicated ref, `excluded` omitted every render oid, `withheld` came back
 * false, and the next identical `context for` wrote the removed bytes back
 * into the store under the same oid, where the redacted record's surviving
 * tree entry resolves them again. That is the resurrection `withheld` exists
 * to close, re-opened by an unrelated ref's replication state, silently.
 *
 * So retention asks this instead: what somebody accountable said to remove,
 * whether or not this host can act on it yet. Nothing is subtracted — a blob
 * a live record also names is still a blob a removal named, and re-retaining
 * it is still putting removed bytes back.
 */
export const removed = Effect.fn("hub.Redaction.removed")(function* () {
  // Computed every time, and deliberately. What is expensive here is the walk
  // underneath `marks()`, and that is where the host's answer is kept — one
  // entry per ref, so an append invalidates the ref it moved and no other.
  // What is *cheap* is this: a trust fold and a tree read per tombstone, which
  // most repositories do not have. Keeping it would mean keeping something
  // that depends on the trust log, and a grant arriving after the tombstone it
  // authorises moves no record ref — so the kept answer would stand while the
  // removal it should have honoured went unhonoured.
  const found = new Set<Oid>();
  const stored = yield* genesisOf();
  if (stored === null) return found;

  const marked = yield* marks();
  if (marked.found.length === 0) return found;

  const trust = yield* projectTrust(stored.genesis);
  for (const removal of marked.found) {
    if (!removal.bound) continue;
    if (!Tombstone.counts(trust, removal.signers)) continue;
    for (const blob of yield* blobsOf(removal.target)) found.add(blob);
  }
  return found;
});

/**
 * Everything an answer here depends on, in one string.
 *
 * The storage identity and the genesis separate two repositories a host serves
 * — including an origin and its mirror, whose refs may match while what they
 * can read does not. The ceilings are part of the answer rather than of the
 * repository: a host that will not walk a ref that long reports nothing
 * redacted on it. And every record ref's head, because a tombstone on any of
 * them changes the result.
 *
 * Built here rather than inside a caller so that a host implementing `Answers`
 * has nothing to derive: the string it is handed is the whole of what it must
 * key on.
 */

/**
 * What a tombstone asked to have removed and `gc` will keep anyway.
 *
 * A Context Pack and a ContextRender are deterministic: two exposures of one
 * view and one task are two records naming one object. So a tombstone on the
 * first cannot take the object with it — the second is live, nobody asked for
 * it to go, and `Maintenance.gc` re-walks only the source refs, so deleting it
 * would leave that exposure's audit reporting its own render unavailable
 * forever on a ref nothing can rewind. `excluded` subtracts exactly this, and
 * it is right to.
 *
 * What was wrong is that nothing said so. `trace redact` printed an oid, `gc`
 * reported a count, and the verbatim task string and every exposed file byte
 * stayed readable at `<record>^{tree}:context/render.bin` and clonable off the
 * ref — while the one guarantee the feature exists to make is that they are
 * gone. A removal that cannot complete has to be a removal that says it did
 * not, and it has to name what else holds the bytes, because redacting those
 * too is the only thing that finishes the job.
 *
 * Asked of `excluded` itself rather than recomputed, so this cannot drift from
 * what the collector will actually do.
 */
export const withheld = Effect.fn("hub.Redaction.withheld")(function* (target: Oid) {
  const holders: Array<Oid> = [];
  const own = yield* payloadOf(target);
  const mine = new Set<Oid>([...own.payload, ...own.attached]);
  if (mine.size === 0) return { blobs: [], holders };

  const removable = yield* excluded();
  const blobs = [...mine].filter((blob) => !removable.has(blob));
  if (blobs.length === 0) return { blobs, holders };

  // Only the ones still live, judged the way `excluded` judges them. A record
  // an accepted tombstone already names is holding nothing — `stillNamed`
  // skips exactly those — so counting it here told the operator to redact
  // something already redacted, and got "other live record(s)" wrong.
  //
  // "Accepted" is the whole of it: `excluded` subtracts only tombstones that
  // pass `Tombstone.counts`, so a record named by a tombstone from a signer
  // who never held `hub.redact` is still live and still protecting its blobs.
  // Skipped here on the bare tombstone, that record went unnamed and the
  // caller reported "a record ref this host cannot walk may name them" — a
  // cause that was not the cause, in place of the live record an operator
  // could have acted on.
  const claimed = new Set<Oid>();
  const stored = yield* genesisOf();
  const graph = stored === null ? null : yield* projectTrust(stored.genesis);
  if (graph !== null) {
    for (const removal of (yield* marks()).found) {
      // `bound` as well as counted, because `excluded` requires both. Judged on
      // `counts` alone, a counted-but-unbound tombstone took its target out of
      // the holder search while `excluded` went on treating that record as
      // live and protecting its `context/` blobs — so this came back with
      // objects held and nobody to blame, and the caller printed "a record ref
      // this host cannot walk may name them", which is the wrong-cause message
      // this function exists to avoid, reached through `bound` instead of
      // through `counts`.
      if (removal.bound && Tombstone.counts(graph, removal.signers)) claimed.add(removal.target);
    }
  }

  // Stopped as soon as every kept object has something to blame. The walk is
  // a `readCommit` plus up to five `findPath` calls per record commit in the
  // repository, and the answer an operator needs is "which live records to
  // redact next" — one per object is that answer. Unbounded, `trace redact`
  // paid the whole repository to name a holder it had already found, on top of
  // the walks `excluded` above it makes.
  const kept = new Set(blobs);
  const blamed = new Set<Oid>();
  for (const ref of yield* recordRefs()) {
    if (blamed.size === kept.size) break;
    for (const commit of (yield* tombstonesOn(ref)).commits) {
      if (blamed.size === kept.size) break;
      if (commit === target || claimed.has(commit)) continue;
      const found = yield* payloadOf(commit);
      const holds = [...found.payload, ...found.attached].filter((blob) => kept.has(blob));
      if (holds.length === 0) continue;
      for (const blob of holds) blamed.add(blob);
      holders.push(commit);
    }
  }
  return { blobs, holders };
});

/**
 * Every payload blob a tombstone *names*, whether or not it still counts.
 *
 * A different question from `excluded`, and the one a fetch asks: "is this
 * absence explained?" rather than "may this be removed?". Authorization
 * decides whether a removal happens; it must not decide whether a removal that
 * already happened is explicable, because the removal is irreversible and the
 * answer moves. `Verify.authorize` judges expiry against the clock, and a
 * `compromised` revocation reaches backwards — so a tombstone valid on Monday
 * can be invalid on Friday, and with the strict set the bytes stay gone while
 * nothing accounts for them: every fetch of `refs/hub/*` fails from then on,
 * permanently, and two hosts whose memos turned over at different moments
 * disagree about the same request.
 */
export const covered = Effect.fn("hub.Redaction.covered")(function* () {
  const repository = yield* Repository;

  // Memoised for the same reason `excluded` is, and more urgently: this is the
  // set a *deepening fetch* takes, and a deepening fetch is a request an
  // anonymous reader makes. Unmemoised, every one of them walked every pull
  // request's event DAG twice — once to find the tombstones and once to read
  // them — which is a walk of the whole hub history per request, driveable in
  // a loop by anybody who can reach the repository.
  //
  // Sound because the answer is a pure function of the refs in the key: this
  // set asks only what a tombstone *names*, so no trust state and no clock
  // enters it, and a moved ref changes the key.
  const refs = yield* Event.pullRequests();
  const written = yield* recordRefs();
  // The storage as well as the genesis: an origin and its mirror under one
  // host share the genesis oid, and right after a replication the hub ref oids
  // too — while what they can read need not agree, since refs are applied
  // without a connectivity check. See `Storage`.
  const identity = `${yield* storageOf()}\u0000${yield* repository.resolve(GENESIS_REF)}`;
  const state = [
    // As above: the ceiling decides which pull requests were walked at all.
    `ceiling ${yield* Event.ceilingOf()}/${yield* Trace.ceilingOf()}`,
    ...(yield* Effect.forEach(refs, (pr) =>
      repository.resolve(Event.refOf(pr)).pipe(Effect.map((oid) => `${Event.refOf(pr)} ${oid}`)),
    )),
    ...(yield* Effect.forEach(written, (ref) =>
      repository.resolve(ref).pipe(Effect.map((oid) => `${ref} ${oid}`)),
    )),
  ].join("\u0000");

  const known = names.get(identity);
  if (known !== undefined && known.state === state) {
    names.delete(identity);
    names.set(identity, known);
    return known.found;
  }

  const found = new Set<Oid>();
  // Whether this answer is the whole answer, threaded up so the memo below can
  // refuse to pin a subset. Everything under it can come back short — a walk
  // that failed, an object a replica has not received — and this set has the
  // opposite polarity to `excluded`'s: a subset is clones and deepening
  // fetches failing on an absence nothing can explain, permanently, because
  // the refs do not move when the missing objects finally arrive.
  let complete = true;
  for (const pr of refs) {
    const named = yield* namedByPullRequest(pr);
    complete &&= named.complete;
    for (const oid of named.found) found.add(oid);
  }

  // And what a session's or a task's tombstone names, on the same terms: what
  // it *names*, not what still counts. Authorization decides whether a removal
  // happens; it must not decide whether one that already happened can be
  // accounted for, or a fetch starts failing on the day a redactor's grant
  // lapses and the bytes are already gone.
  const marked = yield* marks();
  complete &&= marked.complete;
  for (const removal of marked.found) {
    // The target's own tree, because `blobsOf` reads it and a tree this
    // replica has not received makes the answer short in the same way an
    // unwalkable ref does. A *collected* payload does not look like this — the
    // tree entry survives redaction on purpose, so `findPath` still names the
    // blob — which is what makes the absence of the tree a clean signal.
    const held = yield* (yield* Repository)
      .readCommit(removal.target)
      .pipe(Effect.catchTag("ObjectNotFound", () => Effect.succeed(null)));
    const reached =
      held === null
        ? null
        : yield* (yield* Repository)
            .readTree(held.tree)
            .pipe(Effect.catchTag("ObjectNotFound", () => Effect.succeed(null)));
    if (reached === null) complete = false;
    for (const blob of yield* blobsOf(removal.target)) found.add(blob);
  }

  // Neither of `excluded`'s two narrowings applies here, and applying them was
  // the worst thing done to this file. The two sets have opposite polarity:
  // `excluded` says "gc may delete this", so a smaller set is the safe
  // direction; this one says "this absence is explained", so a smaller set is
  // how a fetch starts failing with no recovery.
  //
  // So a blob a live record also names stays. It is only ever *used* when the
  // object is genuinely missing — `absent` filters on `contains` — and if
  // another replica's collection already removed it, an unexplained absence
  // is a clone that cannot complete rather than an object nobody needed.
  //
  // And an unwalkable ref does not empty this. Emptying it discarded the pull
  // request coverage gathered above along with everything else, so one
  // over-ceiling trace ref that arrived by replication made every clone and
  // every deepening fetch of the repository fail permanently.

  if (complete) {
    names.delete(identity);
    names.set(identity, { state, found });
  }
  while (names.size > MEMO) {
    const oldest = names.keys().next();
    if (oldest.done === true) break;
    names.delete(oldest.value);
  }
  return found;
});

/**
 * What a tombstone covers *and this repository no longer holds*.
 *
 * The exclusion `gc` takes says "stop protecting this"; this one says "this is
 * not here, walk past it". They are not the same set, because git dedupes by
 * content: a redacted payload can be the very object a branch's tree names,
 * and `gc` keeps that one. Handing the whole set to a pack walk would drop an
 * object the pack genuinely needs, and the other end would rebuild a tree
 * pointing at nothing.
 *
 * Both ends of a transfer need it — the server serving a fetch, and a client
 * packing a hub ref for a push — so it lives beside the set it filters rather
 * than in either of them.
 */
export const absent = Effect.fn("hub.Redaction.absent")(function* () {
  const repository = yield* Repository;
  // Every payload a tombstone *names*, not only those a tombstone that still
  // counts names; see `covered`.
  const gone = new Set<Oid>();
  for (const oid of yield* covered()) if (!(yield* repository.contains(oid))) gone.add(oid);
  return gone;
});

/** The pull requests carrying anything that *might* be a tombstone. */
/**
 * Which pull requests hold a tombstone at all, remembered per ref.
 *
 * Memoized for the reason its neighbours are, and more urgently since
 * `excluded` moved onto the exposure write path: this runs a full
 * `Event.entries` — `Dag.reachable`, `Dag.topological`, and a
 * `Record.carries`/read/decode per event — for *every* pull request in the
 * repository, and `excluded`'s own memo misses by construction on that path,
 * because `context for --session S` appends to S's trace ref and every record
 * ref's head is in that key. A harness doing N invocations against a
 * repository with M open pull requests was paying N × M full folds on its
 * pre-call path. Keyed on this ref's own head, an append to a trace ref costs
 * nothing here.
 *
 * A short walk is not remembered, for the reason the others are not: the head
 * does not move when the objects that were missing arrive.
 */
const onTombstoned = new Map<string, { readonly head: Oid | null; readonly held: boolean }>();

const tombstonedOn = Effect.fn("hub.Redaction.tombstonedOn")(function* (pr: string) {
  const repository = yield* Repository;
  const ref = Event.refOf(pr);
  const head = yield* repository.resolve(ref);
  const key = `${yield* storageOf()}\u0000${yield* Event.ceilingOf()}\u0000${ref}`;

  const known = onTombstoned.get(key);
  if (known !== undefined && known.head === head) {
    onTombstoned.delete(key);
    onTombstoned.set(key, known);
    return known.held;
  }

  // One pull request this cannot walk is one pull request, not a broken
  // repository. The fold's ceiling is enforced where a push crosses it, so a
  // history that arrived by replication may sit above it — and failing here
  // would take out `gc` for every repository on the host and every deepening
  // fetch of this one, on a walk that visits pull requests with nothing
  // redacted in them at all.
  const walked = yield* Event.entries(pr).pipe(
    Effect.catchTag("Invalid", () => Effect.succeed(null)),
  );
  if (walked === null) return false;

  const held = walked.events.some((entry) => entry.payload?.type === "event.redacted");
  onTombstoned.delete(key);
  onTombstoned.set(key, { head, held });
  while (onTombstoned.size > REFS) {
    const oldest = onTombstoned.keys().next();
    if (oldest.done === true) break;
    onTombstoned.delete(oldest.value);
  }
  return held;
});

const tombstoned = Effect.fn("hub.Redaction.tombstoned")(function* (refs: ReadonlyArray<string>) {
  const found: string[] = [];
  for (const pr of refs) if (yield* tombstonedOn(pr)) found.push(pr);
  return found;
});

/**
 * Answers already worked out, one per repository.
 *
 * Keyed by the repository and *validated* against the refs the answer was
 * worked out from, rather than keyed by those refs. Keyed by them, every hub
 * event minted a new entry: one busy repository filled the whole map with its
 * own history and evicted every other repository the host serves, reinstating
 * the walk-every-pull-request cost this exists to remove — the same defect
 * `Auth.folds` was reshaped to avoid, for the same reason. Nothing ever wants
 * two answers for one repository, so a moved ref replaces rather than adds.
 *
 * Least-recently-used, not first-in: a host with more repositories than the
 * bound serves them in some order, and evicting by insertion evicts whichever
 * happens to be oldest rather than whichever is idle.
 */
const MEMO = 256;
type Memo = Map<string, { readonly state: string; readonly found: ReadonlySet<Oid> }>;
const memo: Memo = new Map();

/**
 * The same, for `covered`.
 *
 * A separate map rather than a wider value, because the two are asked on
 * different paths — `excluded` by `gc`, `covered` by every deepening fetch —
 * and sharing one entry would make either question pay for the other's walk.
 */
const names: Memo = new Map();

export type RedactionError = Invalid | ObjectNotFound | StorageFailure;
