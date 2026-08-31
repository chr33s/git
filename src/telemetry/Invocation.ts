/**
 * One Invocation, joined from the records that were written separately.
 *
 * ```text
 * Invocation sha1:c3…
 *   Context     the exact tree, its evidence, the render commitment
 *   Runtime     model / usage / outcome / finish
 *   Attempts    only when instrumentation observed them
 *   Workspace   tree A → tree B
 *   Capture     transport, stage, coverage
 * ```
 *
 * A user should never have to know that the pre-call and post-call halves are
 * two signed records (docs/telemetry.md §19.14). They are two records because
 * they happen at different times and fail independently; they are *one* row
 * because that is the thing a person is asking about.
 *
 * The join is by Git record OID and only by that (§3). Timestamp proximity is
 * not a join here and never becomes one: two invocations a millisecond apart
 * on two fibers would pair with each other's exposures, and the resulting row
 * would be a confident account of something that did not happen. A runtime
 * record that names no exposure stays a runtime-only row, and an exposure with
 * no runtime record stays a context-only row — a harness that crashed
 * mid-call produces exactly that, and inventing the missing half would be the
 * one failure this whole plane exists to make visible.
 *
 * Everything the server owes the browser and the CLI is computed here (§14):
 * trust verification, the DAG fold, the joins, coverage and derived
 * diagnostics. Clients receive a projected Invocation; they do not rebuild
 * protocol state.
 */
import { Effect } from "effect";

import * as Exposure from "../context/Exposure.ts";
import * as Pack from "../context/Pack.ts";
import type { ObjectNotFound, StorageFailure, Invalid } from "../git/Error.ts";
import { qualify } from "../git/Oid.ts";
import type { Oid } from "../git/Store.ts";
import { trustReach } from "../hub/Projection.ts";
import * as Tombstone from "../hub/Tombstone.ts";
import * as Trace from "../hub/Trace.ts";
import * as Verify from "../trust/Verify.ts";
import * as Records from "./Records.ts";

/**
 * What is known about how completely this session was captured (§12).
 *
 * `complete` requires a `trace-health` record that says nothing was sampled,
 * transformed or dropped. Its absence is `unknown`, never `complete`: §12.1
 * says an absence in the trace is meaningful only alongside known capture
 * capability and health, so a pipeline that never reported on itself has told
 * us nothing about what it swallowed.
 */
export type Coverage = "complete" | "degraded" | "unknown";

export interface ContextView {
  /** The exposure record's own identity. */
  readonly exposure: string;
  readonly view: Pack.View | null;
  readonly blobs: number;
  readonly gitlinks: number;
  readonly renderFormat: string | null;
  readonly renderDigest: string | null;
  /** `verified` / `absent` / `unreadable`, as `Exposure.audit` reports it. */
  readonly render: string;
  /** Whether every dimension of the exposure audit held. */
  readonly verified: boolean;
}

export interface Runtime {
  /** The telemetry record's own identity. */
  readonly record: string;
  readonly operation: string | null;
  readonly model: Records.InvocationTelemetry["model"];
  readonly usage: Records.Usage | null;
  readonly outcome: Records.InvocationTelemetry["outcome"];
  readonly finishReasons: ReadonlyArray<string>;
  readonly context: Records.ContextFacts | null;
  readonly agent: Records.InvocationTelemetry["agent"];
  readonly conversation: string | null;
  /**
   * The exposure this record named, whether or not it was found.
   *
   * Carried so that "this invocation exposed nothing" and "this invocation
   * named an exposure this replica does not hold" are two answers rather than
   * one. `Records.check` validates the *shape* of the oid and nothing more, so
   * a partially replicated ref — or a record naming another session's
   * exposure — produced a row with no Context section, from which an operator
   * concludes the model saw no repository context. That is the opposite of
   * the truth, in a module whose rule is that distinctions stay distinct.
   */
  readonly exposure: string | null;
  /** Absent when attempts were not instrumented; never a manufactured `[1]`. */
  readonly attempts: ReadonlyArray<Records.Attempt> | null;
  /**
   * Whether a signer this repository trusted then wrote this record.
   *
   * `null` when the caller handed in no trust projection. Reported for the
   * same reason the Context half reports it: one command should not verify
   * half of what it prints and take the other half on faith.
   */
  readonly trust: Exposure.Check | null;
}

export interface Invocation {
  /**
   * The row's canonical identity: the runtime record's OID when there is one,
   * otherwise the exposure's. Whichever it is, it is a Git record OID (§19.12).
   */
  readonly id: string;
  readonly commit: Oid;
  /** The trace records this row follows, so lanes survive the projection. */
  readonly parents: ReadonlyArray<Oid>;
  readonly context: ContextView | null;
  readonly runtime: Runtime | null;
  /**
   * The transition this invocation was followed by, with the same verdict
   * every other attached record carries.
   *
   * Replication is not policy-gated, so a `workspace-transition` signed by a
   * key that never held `hub.trace` can sit on a fetched ref — and printed as
   * bare fact under a trusted invocation it asserts a repository change nobody
   * accountable claimed.
   */
  readonly workspace: {
    readonly before: string;
    readonly after: string;
    readonly trust: Exposure.Check | null;
  } | null;
  readonly capture: Records.Capture | null;
  readonly coverage: Coverage;
  /**
   * `inputTokens / effectiveInputLimitTokens`, when both are known (§9).
   *
   * `null` whenever the denominator is missing, because a pressure ratio over
   * an incompatible or guessed limit is a number that reads as a measurement
   * and is not one. Derived, and labelled as such wherever it is shown.
   */
  readonly inputPressure: number | null;
}

export interface Projection {
  readonly session: string;
  readonly invocations: ReadonlyArray<Invocation>;
  /** Compaction and other lifecycle records, in the order they were written. */
  readonly lifecycle: ReadonlyArray<{
    readonly record: string;
    readonly payload: Records.ContextCompaction;
    readonly trust: Exposure.Check | null;
  }>;
  /**
   * Tool operations, kept beneath the invocation each names when it names one.
   *
   * With the same verdict every other record here carries. Replication is not
   * policy-gated, so a trace ref fetched from a peer can hold a tool operation
   * signed by a key that never held `hub.trace` — and without this the audit
   * printed `trust no` under the invocation and, directly beneath it, that
   * peer's `write_file · ok · 4096 bytes` with nothing to tell them apart.
   */
  readonly tools: ReadonlyArray<{
    readonly record: string;
    readonly payload: Records.ToolOperation;
    readonly trust: Exposure.Check | null;
  }>;
  readonly health: ReadonlyArray<Records.TraceHealth>;
  /**
   * Health records this projection would not take a coverage claim from.
   *
   * Carried rather than dropped, for the reason `tools` and `lifecycle` carry
   * a verdict: a record signed by a key that never held `hub.trace` is still a
   * record on the ref, and a reader who cannot see it cannot tell an
   * unreported capture path from one somebody unaccountable reported on. It
   * keeps `coverage` off `complete`; it never makes it `degraded`.
   */
  readonly unjudged: ReadonlyArray<{
    readonly record: string;
    readonly payload: Records.TraceHealth;
    readonly trust: Exposure.Check | null;
  }>;
  /**
   * Workspace transitions no invocation claimed.
   *
   * A transition is attached to the invocation it followed, by tree and by
   * position — and one that matches none is still a signed record saying the
   * workspace changed. Dropped, the audit reported no workspace change while
   * the ref said there was one: a call with no repository context writes an
   * invocation with `exposure: null`, so nothing has a `view.tree` to match,
   * and the transition after it vanished from every section of the output.
   */
  readonly transitions: ReadonlyArray<{
    readonly record: string;
    readonly before: string;
    readonly after: string;
    readonly trust: Exposure.Check | null;
  }>;
  /** Records this session says were removed, by the commit each named. */
  readonly redacted: ReadonlyArray<string>;
  readonly coverage: Coverage;
  /**
   * Whether the trace DAG branches.
   *
   * Reported rather than resolved: §15 forbids manufacturing a single causal
   * order when concurrent parents exist, so a renderer that sees this true
   * must show lanes rather than a list.
   */
  readonly concurrent: boolean;
  /** Records this replica holds but could not read — a redaction, usually. */
  readonly unreadable: ReadonlyArray<Oid>;
  /**
   * Records on this session's ref whose own payload names a different session.
   *
   * Replication is not policy-gated, so a peer can land one. Reported rather
   * than dropped, for the reason `context/Exposure.entries` reports its own:
   * an audit surface that silently discards records is the other half of the
   * problem it is trying to solve.
   */
  readonly foreign: ReadonlyArray<Oid>;
}

/**
 * What the capture path admits to having lost.
 *
 * The strictest health record wins, because coverage is a claim about the
 * weakest link: one collector that sampled makes the session's audit
 * incomplete however clean every other stage was (§12).
 */
export const coverageOf = (health: ReadonlyArray<Records.TraceHealth>): Coverage => {
  if (health.length === 0) return "unknown";
  const clean = health.every(
    (entry) => entry.sampling === "none" && !entry.transformed && entry.dropped === 0,
  );
  return clean ? "complete" : "degraded";
};

/**
 * Input pressure, when the two numbers mean compatible things (§9).
 *
 * The numerator is whole-invocation input, which includes far more than the
 * repository render — §7.2 forbids describing it as ContextRender tokens — and
 * the denominator is the input budget believed usable for this call. Both have
 * to be present, and the denominator has to be positive, or there is no ratio
 * to report.
 */
export const pressureOf = (telemetry: Records.InvocationTelemetry): number | null => {
  const input = telemetry.usage?.inputTokens;
  const limit = telemetry.context?.effectiveInputLimitTokens;
  if (input === undefined || limit === undefined || limit <= 0) return null;
  // A count that cannot be negative, checked here as well as at the door.
  // `Records.check` runs from `record` and nothing else — `decode` deliberately
  // does not call it, and the boundary does not validate payload numerics — so
  // the `counting` guard covers records this host wrote and no others. A peer
  // holding `hub.trace` appending `inputTokens: -5` replicated in and rendered
  // as `pressure -5% of the effective input limit (derived)`, which is the
  // output `counting`'s own docstring says it exists to prevent, reached
  // through replication instead of `--event`.
  if (input < 0) return null;
  return input / limit;
};

/**
 * The context half of a row: what the exposure says, and whether it holds.
 *
 * Audited rather than merely read, because §19.8 keeps Context Exposure the
 * authoritative repository-context boundary — a projection that printed the
 * pack's own claims without checking them would present a drifted view as a
 * verified one, which is precisely the confusion the audit exists to prevent.
 */
const contextOf = Effect.fn("telemetry.Invocation.contextOf")(function* (input: {
  readonly commit: Oid;
  readonly repo: string;
  readonly session: string;
  readonly trust: Parameters<typeof Exposure.audit>[0]["trust"];
  readonly reach?: Parameters<typeof Exposure.audit>[0]["reach"];
}) {
  const audited = yield* Exposure.audit(input);
  // The pack the audit has already read and decoded. Reading it back through
  // `packOf` cost a second `readCommit`, `findPath`, `readBlob` and schema
  // decode per exposure, per projection — purely to count blobs and gitlinks.
  const pack = audited.decoded;

  return {
    exposure: audited.exposure,
    view: pack?.view ?? null,
    blobs: pack?.items.filter((item) => item.kind === "blob").length ?? 0,
    gitlinks: pack?.items.filter((item) => item.kind === "gitlink").length ?? 0,
    renderFormat: audited.payload?.renderFormat ?? null,
    renderDigest: audited.payload?.renderDigest ?? null,
    render: audited.render.state,
    verified: audited.ok,
  } satisfies ContextView;
});

/**
 * Fold one session's trace ref into the Invocations a person reads.
 *
 * `trust` is threaded through to the exposure audit rather than resolved here,
 * for the reason `Exposure.audit` takes it: a caller with no membership to
 * judge against gets an honest `null` dimension instead of a check that
 * silently passed.
 */
export const project = Effect.fn("telemetry.Invocation.project")(function* (input: {
  readonly session: string;
  readonly repo: string;
  readonly trust?: Parameters<typeof Exposure.audit>[0]["trust"];
}) {
  // One walk, read twice. Both `entries` calls do nothing but filter the
  // walked records by type, so taking the ref three times was three
  // `Dag.reachable` passes and three payload reads per record.
  const walked = yield* Trace.walk(input.session);
  const telemetry = yield* Records.entries(input.session, walked, input.repo);
  const exposures = yield* Exposure.entries(input.session, walked, input.repo);

  // Verified once per record, when there is membership to verify against.
  // `coverage` in particular is a completeness claim, and a claim derived from
  // an unverified record is a claim anybody who can append to the ref can make.
  const untrusted = new Map<Oid, string>();
  // One walk of the trust log for the whole projection, handed to every
  // judgement it makes. Without it each record re-walked the log from
  // scratch, so a long session was O(records × trust log) — the same reason
  // the membership itself is built once per command rather than per record.
  const reach = trustReach();
  if (input.trust != null) {
    for (const entry of telemetry.records) {
      const refusal = yield* Records.verified({
        entry,
        bytes: entry.bytes,
        projection: input.trust,
        reach,
      });
      if (refusal !== null) untrusted.set(entry.commit, refusal);
    }
  }
  // Who could have removed something, by the capability that removes it. The
  // same test `cli/context.ts` makes for the same decision.
  const removable = new Set<Oid>();
  if (input.trust != null) {
    for (const entry of telemetry.records) {
      if (entry.payload.type !== Records.REDACTED) continue;
      const signers = yield* Verify.signers(entry.bytes, entry.signatures);
      if (Tombstone.counts(input.trust, signers)) removable.add(entry.commit);
    }
  }

  const verdict = (commit: Oid): Exposure.Check | null => {
    if (input.trust == null) return null;
    const refusal = untrusted.get(commit);
    return refusal === undefined ? { ok: true } : { ok: false, reason: refusal };
  };

  const health: Array<Records.TraceHealth> = [];
  const lifecycle: Array<{
    record: string;
    payload: Records.ContextCompaction;
    trust: Exposure.Check | null;
  }> = [];
  const tools: Array<{
    record: string;
    payload: Records.ToolOperation;
    trust: Exposure.Check | null;
  }> = [];
  const workspaces: Array<{ commit: Oid; payload: Records.WorkspaceTransition }> = [];
  const runtimes: Array<{ commit: Oid; payload: Records.InvocationTelemetry }> = [];
  const redacted: Array<string> = [];
  /** The same set, for membership: `includes` on a growing array inside a loop
   * over every record is quadratic on a ref bounded at 16 384 of them. */
  const removals = new Set<string>();
  /** Health records this projection could not attribute; see the gate below. */
  const unjudged: Array<{
    record: string;
    payload: Records.TraceHealth;
    trust: Exposure.Check | null;
  }> = [];

  // The tombstones first, in a pass of their own. Every other kind is filtered
  // by what they name, and a record's removal can be stated after the record
  // it removes — the tombstone is always *later* on the ref — so a single pass
  // decided each record before it knew whether it had been removed.
  // `Records.entries` does both halves of the binding now, and reports what it
  // drops. Done here as a bare filter, the rejects went nowhere: a record
  // naming another repository was in no section of the audit at all, while the
  // identical mismatch on the session was named.
  const bound = telemetry.records;

  for (const entry of bound) {
    const payload = entry.payload;
    if (payload.type !== Records.REDACTED) continue;
    // Recorded, not applied to the bytes: the record it names usually reads as
    // unreadable already, once its payload is gone. What this adds is the
    // account of *why* — an absence with a tombstone beside it is a removal,
    // and one without is a replica that has not caught up.
    //
    // And only from a signer who could have removed anything — judged on
    // `hub.redact`, which is what writes a tombstone, not on the `hub.trace`
    // every other record here is judged by. `permits` gives no implication
    // between the two, so a dedicated redactor holding only `hub.redact` had
    // their removal reported as a record nobody could read: the exact
    // distinction the comment above says this is making, inverted.
    //
    // Recorded once per record removed, not once per tombstone naming it.
    // `redact` deliberately permits a tombstone for an already-unreadable
    // record — a replica whose payload another host collected still needs to
    // be able to write the local one — so two tombstones for one removal are
    // ordinary, and `N record(s) removed by a signed redaction` counted them
    // as two removals. Every other consumer already wraps this in a `Set`.
    if (removable.has(entry.commit) && !removals.has(payload.targetCommit)) {
      removals.add(payload.targetCommit);
      redacted.push(payload.targetCommit);
    }
  }

  // What a counted tombstone names is gone from every reader, whatever this
  // replica still happens to hold. Applied to exposures and to nothing else,
  // `trace redact --target <an invocation, a tool call, a compaction, a
  // transition>` reported success and `session show --audit` went on printing
  // the removed record in full — here and on every replica, until some `gc`
  // collected the blob, and forever on a replica that never collects. The
  // statement outranks the bytes for all six kinds or for none of them.
  const removed = new Set(redacted);
  const gone = (commit: Oid) => removed.has(qualify(commit));

  // A health record that was removed said something about the capture path
  // that this projection can no longer read. Dropped with everything else, a
  // `hub.redact` holder could redact the record admitting twelve dropped
  // records and leave the clean one behind — and the session then reported
  // `coverage: complete` while its own ref carries a signed statement that it
  // is not. A removal accounts for bytes; it does not settle a question.
  let removedHealth = false;

  // Read from the walk, not from the decoded records: after `gc` drops a
  // redacted record's `event.json` the tree entry survives, so `Trace.walk`
  // puts it in `unreadable` and it never reaches `bound` at all. Set only from
  // the decoded side, this flag was true in the window between `trace redact`
  // and the collection that acts on it, and false forever after — so `gc`
  // silently re-enabled the exact claim the flag exists to prevent. The commit
  // message survives redaction and is the one thing left that says what kind
  // of record it was.
  for (const entry of walked.unreadable) {
    if (entry.type === Records.HEALTH && removals.has(qualify(entry.commit))) {
      removedHealth = true;
    }
  }

  for (const entry of bound) {
    const payload = entry.payload;
    if (payload.type === Records.REDACTED) continue;
    if (gone(entry.commit)) {
      if (payload.type === Records.HEALTH) removedHealth = true;
      continue;
    }
    // Only a health record somebody accountable wrote counts toward coverage.
    if (payload.type === Records.HEALTH) {
      // A *positive* verdict, not the absence of a negative one. `verdict`
      // returns `null` when the caller supplied no membership, and
      // `null?.ok !== false` is true — so a projection asked without trust
      // reported `coverage: complete` on the word of a health record anybody
      // who can append to the ref could have written, which is the one claim
      // this gate exists to hold. `trust` is optional on `project`, and
      // "nobody judged this" has to read as `unknown`, not as `complete`.
      // A record nobody accountable wrote cannot *raise* the claim, and must
      // not silently lower the bar either. Dropped outright, a trusted
      // `sdk-export` record saying nothing was lost yielded `coverage
      // complete` for a session whose own ref carries a signed statement from
      // another stage that twelve records were dropped — the §12 "MUST NOT be
      // presented as complete" case, reached by discarding the evidence
      // against it. So an unjudged health record is remembered, kept out of
      // the strictest-wins fold, and stops the answer being `complete`.
      if (verdict(entry.commit)?.ok === true) health.push(payload);
      else unjudged.push({ record: qualify(entry.commit), payload, trust: verdict(entry.commit) });
    } else if (payload.type === Records.COMPACTION) {
      lifecycle.push({ record: qualify(entry.commit), payload, trust: verdict(entry.commit) });
    } else if (payload.type === Records.TOOL) {
      tools.push({ record: qualify(entry.commit), payload, trust: verdict(entry.commit) });
    } else if (payload.type === Records.WORKSPACE) {
      workspaces.push({ commit: entry.commit, payload });
    } else if (payload.type === Records.INVOCATION) {
      runtimes.push({ commit: entry.commit, payload });
    }
  }

  // `unknown` rather than `complete` while anything on this ref says otherwise
  // and nobody can vouch for it. `degraded` already outranks both, so a
  // trusted record that admits a loss still wins.
  const judged = coverageOf(health);
  const coverage =
    judged === "complete" && (unjudged.length > 0 || removedHealth) ? "unknown" : judged;

  // A workspace transition belongs to the invocation whose exposure names the
  // tree it started from *and* which precedes it in the history: §11's capture
  // pattern remembers tree A before the work and materializes tree B before
  // the next invocation, so A and the walk order together are what link them.
  // Nothing here reads a clock.
  //
  // Both halves are needed. Keyed on `beforeTree` alone, two invocations that
  // began from the same clean tree — the ordinary case — collapsed to one
  // transition, and both rows were then rendered with it: a fabricated claim
  // about what the first invocation changed. Each transition is claimed once.
  const position = new Map(walked.records.map((entry, index) => [entry.commit, index]));
  // Position is `Dag.topological`'s linearization, which orders *concurrent*
  // lanes by oid rather than by causality — and `project` explicitly supports
  // that shape, since it reports `concurrent`. Two fibers appending `Ia → Ta`
  // and `Ib → Tb` from the same tree can linearize as `Ia, Ib, Ta, Tb`: `Ia`'s
  // window closes at `Ib` and claims nothing, `Ib` claims `Ta`, and the row
  // for `Ib` asserts a change lane A made. So the window is a filter and
  // descent is the rule — a transition belongs to an invocation it actually
  // descends from, which is the fabricated attribution the two-part key was
  // supposed to prevent, reached through the ordering instead of the key.
  // Asked as "which invocation is nearest above this transition?" rather than
  // "is this one of its ancestors?". The first shape memoized a whole ancestor
  // *set* per transition: on the ordinary capture pattern — invocation,
  // transition, next invocation — that is one set per transition on the ref,
  // and set k on a linear trace holds about 2k oids, so a session with four
  // thousand pairs (well inside `Trace.MAX_RECORDS`) retained on the order of
  // sixteen million entries, live at once, in one `project` call. It traded
  // the join's quadratic *time* for quadratic *space*.
  //
  // The nearest runtime record above a transition is also the better answer:
  // it is the invocation on that transition's own lane, which is what the
  // attribution means. One oid remembered per commit visited, so the whole
  // walk is linear in the ref.
  // Every invocation the ref carries, not only the ones this replica will
  // render. A redacted or unreadable invocation is still a boundary in the
  // history: dropped from this set, `ownerOf` walked straight past it to the
  // previous live one and `starts` no longer closed that one's window, so
  // `I1 → I2 → W` with `I2` redacted put `I2`'s workspace change on `I1`'s row
  // — the fabricated attribution the two-part key and the descent rule were
  // both written to prevent, reached through the record set instead of the
  // ordering. The same happens on a clone where `I2`'s payload never arrived.
  const runtimeCommits = new Set([
    ...runtimes.map((runtime) => runtime.commit),
    ...telemetry.records
      .filter((entry) => entry.payload.type === Records.INVOCATION)
      .map((entry) => entry.commit),
    ...walked.unreadable
      .filter((entry) => entry.type === Records.INVOCATION)
      .map((entry) => entry.commit),
    // The foreign ones too. `Records.entries` diverts a record whose signed
    // payload names another session or repository, so it reaches neither
    // `runtimes` nor `telemetry.records` — and a peer can land one, which is
    // this module's own premise. Left out, `starts` never closed the previous
    // invocation's window at it and `ownerOf` walked straight through, so a
    // transition the foreign record produced was reported under the genuine
    // invocation before it: a repository change that invocation did not make.
    ...telemetry.foreign
      .filter((entry) => entry.type === Records.INVOCATION)
      .map((entry) => entry.commit),
  ]);
  const owners = new Map<Oid, Oid | null>();
  // Each commit's *own* answer, computed from its parents' — not a breadth-first
  // search whose whole frontier is stamped with the result. That shape put the
  // wrong owner on every sibling it passed: with lanes `Ia → … → Ta → J` and
  // `Ib → J`, asking about a child of `J` pushed `Ta` onto the visited path,
  // found `Ib` at the same level, and recorded `Ta`'s owner as `Ib` — so lane
  // A's genuine transition dropped out of its own row, and if lane B's window
  // and tree matched it was claimed there instead, asserting that B made A's
  // repository change. The fabricated attribution the descent rule exists to
  // prevent, reached through the memo rather than the ordering.
  //
  // A join whose lanes disagree owns nothing. There is no single invocation a
  // transition after a merge descends from, and saying so leaves it in the
  // unclaimed section, which is the honest place for it.
  const ownerOf = (start: Oid): Oid | null => {
    const stack: Array<Oid> = [start];
    const opened = new Set<Oid>();
    while (stack.length > 0) {
      const at = stack[stack.length - 1]!;
      if (owners.has(at)) {
        stack.pop();
        continue;
      }
      if (runtimeCommits.has(at)) {
        owners.set(at, at);
        stack.pop();
        continue;
      }
      const parents: ReadonlyArray<Oid> = walked.parents.get(at) ?? [];
      // A parent already on the stack would be a cycle. A commit graph has
      // none, and a malformed one must not make this spin.
      const pending = parents.filter((parent) => !owners.has(parent) && !opened.has(parent));
      if (pending.length > 0) {
        opened.add(at);
        stack.push(...pending);
        continue;
      }
      const answers = new Set(parents.map((parent) => owners.get(parent) ?? null));
      owners.set(at, answers.size === 1 ? ([...answers][0] ?? null) : null);
      stack.pop();
    }
    return owners.get(start) ?? null;
  };
  const descends = (from: Oid, to: Oid): boolean => ownerOf(to) === from;
  const unclaimed = workspaces.map((entry) => ({ ...entry, claimed: false }));
  // Where the *next* invocation begins. A transition after that point belongs
  // to it, not to this one: bounded only below, an invocation whose own run
  // wrote nothing claimed the transition its successor wrote, and the audit
  // then showed a change under the invocation that did not make it and nothing
  // under the one that did.
  // Only the ones this walk can place. `position` comes from `walked.records`,
  // which a redacted or unreadable invocation never reaches — so mapping those
  // to `0` did not close a window at them, it added a spurious zero. The
  // ownership rule is what actually holds the line there: `runtimeCommits`
  // includes them, so `ownerOf` stops at a removed invocation even though this
  // window cannot.
  const starts = [...runtimeCommits]
    .map((commit) => position.get(commit))
    .filter((at) => at !== undefined)
    .sort((left, right) => left - right);
  const nextAfter = (at: number) => starts.find((start) => start > at) ?? Number.MAX_SAFE_INTEGER;

  const transitionAfter = (from: Oid, before: string) => {
    const at = position.get(from) ?? 0;
    const until = nextAfter(at);
    const fits = (entry: (typeof unclaimed)[number]) => {
      if (entry.claimed || entry.payload.beforeTree !== before) return false;
      const where = position.get(entry.commit) ?? 0;
      if (where < at || where >= until) return false;
      return descends(from, entry.commit);
    };
    // A record somebody accountable signed wins the claim. Taking the first in
    // walk order let an injected transition displace the genuine one, which was
    // then demoted to the unclaimed section — so the row an operator reads
    // showed the forgery and the real change was reported as belonging to
    // nobody.
    const found =
      unclaimed.find((entry) => fits(entry) && verdict(entry.commit)?.ok !== false) ??
      unclaimed.find((entry) => fits(entry));
    if (found === undefined) return null;
    found.claimed = true;
    return { payload: found.payload, trust: verdict(found.commit) } as const;
  };

  // Keyed once. Scanned per runtime record, the join was O(runtimes ×
  // exposures) on a session long enough for anyone to want this projection.
  // Without the ones a counted tombstone names. Their payload is usually gone
  // already and they arrive here as `unreadable` — but a Pack and a
  // ContextRender are deterministic, so a later identical `context for`
  // rewrites those blobs under the same oids and the removed record's tree
  // entries resolve again. Read as whatever resolves, the exposure came back
  // as an intact context row, and the audit said the run had seen bytes an
  // operator had removed. The tombstone is the statement; the bytes are a fact
  // about this replica at this moment, and the statement outranks them.
  const byOid = new Map(
    exposures.exposures
      .filter((exposure) => !removed.has(qualify(exposure.commit)))
      .map((exposure) => [qualify(exposure.commit), exposure]),
  );
  const paired = new Set<string>();
  const invocations: Array<Invocation> = [];

  for (const runtime of runtimes) {
    const payload = runtime.payload;
    const exposure = payload.exposure === null ? null : (byOid.get(payload.exposure) ?? null);
    if (exposure !== null) paired.add(qualify(exposure.commit));

    const context =
      exposure === null
        ? null
        : yield* contextOf({
            commit: exposure.commit,
            repo: input.repo,
            session: input.session,
            trust: input.trust,
            reach,
          });

    const before = context?.view?.tree;
    const transition = before === undefined ? null : transitionAfter(runtime.commit, before);

    invocations.push({
      id: qualify(runtime.commit),
      commit: runtime.commit,
      parents: walked.parents.get(runtime.commit) ?? [],
      context,
      runtime: {
        record: qualify(runtime.commit),
        operation: payload.operation?.name ?? null,
        model: payload.model,
        usage: payload.usage ?? null,
        outcome: payload.outcome,
        finishReasons: payload.response?.finishReasons ?? [],
        context: payload.context ?? null,
        agent: payload.agent,
        conversation: payload.conversation?.externalId ?? null,
        exposure: payload.exposure,
        attempts: payload.attempts ?? null,
        trust: verdict(runtime.commit),
      },
      workspace:
        transition === null
          ? null
          : {
              before: transition.payload.beforeTree,
              after: transition.payload.afterTree,
              trust: transition.trust,
            },
      // The exposure's capture where the runtime record wrote none. Both
      // halves carry one — `expose` takes it and `InvocationTelemetry` has its
      // own — and the exposure-only branch below already falls back this way.
      // Without it, a harness that recorded transport and stage on the
      // pre-call exposure and omitted them afterwards produced `capture: null`
      // and no Capture section at all: the *more* complete trace showed less
      // than one whose call never came back.
      capture: payload.capture ?? exposure?.payload.capture ?? null,
      coverage,
      inputPressure: pressureOf(payload),
    });
  }

  // Exposures nothing claimed. A context-only row rather than a dropped one:
  // "the harness showed the model this and then never came back" is a fact an
  // audit exists to be able to state.
  for (const exposure of exposures.exposures) {
    if (paired.has(qualify(exposure.commit))) continue;
    if (removed.has(qualify(exposure.commit))) continue;
    invocations.push({
      id: qualify(exposure.commit),
      commit: exposure.commit,
      parents: walked.parents.get(exposure.commit) ?? [],
      context: yield* contextOf({
        commit: exposure.commit,
        repo: input.repo,
        session: input.session,
        trust: input.trust,
        // The same memo the paired branch uses. Left out, a session whose
        // harness crashed mid-call — the case that produces these rows, and
        // the case the projection exists to make visible — re-walked the whole
        // trust log once per exposure.
        reach,
      }),
      runtime: null,
      workspace: null,
      capture: exposure.payload.capture,
      coverage,
      inputPressure: null,
    });
  }

  // Ordered by the DAG, never by `issuedAt`: a clock is not a causal join, and
  // this is the order the walk already established from the edges themselves.
  // `position` is that same map, built above for the workspace join.
  invocations.sort(
    (left, right) =>
      (position.get(left.commit) ?? 0) - (position.get(right.commit) ?? 0) ||
      (left.id < right.id ? -1 : left.id > right.id ? 1 : 0),
  );

  return {
    session: input.session,
    invocations,
    lifecycle,
    tools,
    health,
    unjudged,
    foreign: [...telemetry.foreign.map((entry) => entry.commit), ...exposures.foreign],
    // Whatever no row took. `claimed` is set as each is attached, so this is
    // exactly the remainder.
    transitions: unclaimed
      .filter((entry) => !entry.claimed)
      .map((entry) => ({
        record: qualify(entry.commit),
        before: entry.payload.beforeTree,
        after: entry.payload.afterTree,
        trust: verdict(entry.commit),
      })),
    redacted,
    coverage,
    concurrent: Trace.concurrent(walked.parents),
    // Every reader's unreadable set, exposures included. `Exposure.entries`
    // calls a record that claims to be a context exposure and then fails to
    // decode "the one case this filter must not swallow" — and dropping it
    // here swallowed it one level up, so a damaged run projected as a clean
    // one with a row simply missing.
    // Every reader's, plus every commit on the ref whose payload is gone —
    // the projection is the one place that reports the whole shape rather
    // than one namespace's share of it.
    unreadable: [
      ...new Set([
        ...walked.unreadable.map((entry) => entry.commit),
        ...telemetry.unreadable,
        ...exposures.unreadable,
      ]),
    ],
  } satisfies Projection;
});

export type InvocationError = Invalid | ObjectNotFound | StorageFailure;
