/**
 * `git+ context …` — what a harness exposed, and whether it can prove it.
 *
 * Three verbs and no more (docs/context-pack.md §13). `for` builds a pack with
 * whatever selector is compiled in; `why` explains one that already exists,
 * keeping verified Git facts apart from the selector's account of itself; and
 * `audit` checks a historical exposure dimension by dimension. Everything
 * else — scores, indexes, graph paths — is a diagnostic these commands may
 * print and a verifier never needs.
 *
 * All three discover the current checkout by default, because the repository a
 * person is asking about is almost always the one they are standing in.
 */
import * as fs from "node:fs";

import { Console, Effect, Predicate } from "effect";
import { Argument, Command, Flag } from "effect/unstable/cli";

import * as Exposure from "../context/Exposure.ts";
import * as Pack from "../context/Pack.ts";
import * as Records from "../telemetry/Records.ts";
import * as Tombstone from "../hub/Tombstone.ts";
import * as Verify from "../trust/Verify.ts";
import * as Render from "../context/Render.ts";
import * as Select from "../context/Select.ts";
import { Invalid } from "../git/Error.ts";
import { Repository } from "../git/Repository.ts";
import type { Oid } from "../git/Store.ts";
import * as Event from "../hub/Event.ts";
import * as Record from "../trust/Record.ts";
import * as Redaction from "../hub/Redaction.ts";
import * as Secrets from "../hub/Secrets.ts";
import * as Claim from "../hub/Claim.ts";
import * as Trace from "../hub/Trace.ts";
import { trustReach } from "../hub/Projection.ts";
import { readGenesis } from "../trust/Genesis.ts";
import { project as projectTrust } from "../trust/Projection.ts";
import {
  mustResolve,
  readPrivateKey,
  repoFlag,
  resolveRev,
  rootFlag,
  withDiscovered,
  withWork,
  workFlag,
} from "./shared.ts";

const jsonFlag = Flag.boolean("json").pipe(
  Flag.withDefault(false),
  Flag.withDescription("Structured output, for machine consumers"),
);

/**
 * The repository's own identity, which every exposure is bound to.
 *
 * Refused rather than defaulted, for the reason `session.ts` refuses: a record
 * names the repository inside its signed bytes so that it cannot be replayed
 * into another one, and a repository with no genesis has no identity to name.
 */
const identityOf = Effect.fn("context.identityOf")(function* () {
  const stored = yield* readGenesis();
  if (stored === null) {
    return yield* new Invalid({
      field: "repo",
      reason: "this repository has no genesis; run `git+ hub init` first",
    });
  }
  return stored.genesis.repoId;
});

/**
 * The repository's identity and its membership, for an audit that judges both.
 *
 * The projection is built once per command rather than per record: it is a
 * walk of the trust log, and `audit` over a session's whole trace ref would
 * otherwise rebuild it for every exposure that session recorded.
 */
const membership = Effect.fn("context.membership")(function* () {
  const stored = yield* readGenesis();
  if (stored === null) {
    return yield* new Invalid({
      field: "repo",
      reason: "this repository has no genesis, so its exposures cannot be judged",
    });
  }
  return { repo: stored.genesis.repoId, trust: yield* projectTrust(stored.genesis) };
});

/**
 * What Git can be held to about one item: its kind, its path, its object.
 *
 * The selector's account of the same item is printed by the caller, and
 * separately, because they are two different kinds of claim — one is a hash
 * comparison and the other is a scoring heuristic's opinion (§13).
 */
const describe = (item: Pack.Item): string => {
  const detail =
    item.kind === "blob"
      ? `${item.blob}${item.range === undefined ? "" : ` [${item.range[0]}, ${item.range[1]})`}`
      : `${item.commit} (mode 160000)`;
  return `${item.kind} ${item.path}\n  ${detail}`;
};

/** The selector's own notes, or a plain statement that it recorded none. */
const explanation = (item: Pack.Item): string => {
  const notes = [item.role, item.reason, item.symbol].filter((note) => note !== undefined);
  return notes.length === 0 ? "no explanation recorded" : notes.join(" / ");
};

const printPack = (pack: Pack.Pack) =>
  Effect.gen(function* () {
    yield* Console.log(`view.base ${pack.view.base}`);
    yield* Console.log(`view.tree ${pack.view.tree}`);
    if (pack.selector !== undefined) {
      yield* Console.log(`selector  ${pack.selector.name}@${pack.selector.version}`);
    }
    yield* Console.log("");
    for (const item of pack.items) {
      yield* Console.log(`${describe(item)}\n  ${explanation(item)}`);
    }
    for (const omission of pack.omissions ?? []) {
      // An omission with neither a path nor a count is the selector saying it
      // does not know the extent — the search stopped at its match cap, so
      // everything past the cut-off went unreached. Printing `0 items` claimed
      // the opposite of what the record says.
      const what =
        omission.path ??
        (omission.count === undefined ? "an unknown number of items" : `${omission.count} items`);
      yield* Console.log(`omitted   ${what}: ${omission.reason}`);
    }
  });

// -- for ------------------------------------------------------------------------

const forCommand = Command.make(
  "for",
  {
    work: workFlag,
    json: jsonFlag,
    task: Flag.string("task").pipe(Flag.withDescription("What the context is being built for")),
    rev: Flag.string("rev").pipe(
      Flag.withDefault("HEAD"),
      Flag.withDescription("The commit the view is anchored to"),
    ),
    maxItems: Flag.integer("max-items").pipe(
      Flag.withDefault(Select.MAX_ITEMS),
      Flag.withDescription("How many evidence items the selector may choose"),
    ),
    maxBytes: Flag.integer("max-bytes").pipe(
      Flag.withDefault(Select.MAX_BYTES),
      Flag.withDescription("How many evidence bytes the selector may choose"),
    ),
    session: Flag.string("session").pipe(
      Flag.withDefault(""),
      Flag.withDescription("Record an exposure on this session's trace ref"),
    ),
    key: Flag.string("key").pipe(
      Flag.withDefault(""),
      Flag.withDescription("Path to the SSH private key the exposure is signed with"),
    ),
    retain: Flag.boolean("retain-render").pipe(
      Flag.withDefault(true),
      Flag.withDescription("Keep the exact render bytes so the digest can be recomputed"),
    ),
  },
  ({ json, key, maxBytes, maxItems, retain, rev, session, task, work }) =>
    Effect.gen(function* () {
      // Read before the repository layer is built: a private key is the one
      // input no command takes as an argument, and the file is on this
      // machine, not in the repository.
      //
      // Refused by name rather than by path: `--key` defaults to empty, so a
      // caller who asked to record an exposure and forgot it got `cannot read`
      // with nothing after it — a file-not-found error naming no file.
      if (session !== "" && key === "") {
        return yield* new Invalid({
          field: "key",
          reason: "recording an exposure needs --key; a record nobody signed is not a record",
        });
      }
      // And the mirror, refused for the reason `trace record` refuses its own:
      // a key passed with no session records nothing, exits zero, and looks
      // exactly like a run that recorded something.
      // Checked here, because `Exposure.expose` checks it after `Pack.capture`
      // has written a blob per dirty tracked file and the whole overlay tree —
      // the same ordering defect the checks around it exist to prevent.
      if (session !== "" && !Trace.isTraceId(session)) {
        return yield* new Invalid({
          field: "session",
          reason: `'${session}' cannot name a trace; it must be one ref path component`,
        });
      }
      if (session === "" && key !== "") {
        return yield* new Invalid({
          field: "session",
          reason: "--key only signs an exposure; name the session to record one with --session",
        });
      }
      // Held to what the protocol will accept, before the selection runs. A
      // render carries one segment per item plus the task, so anything past
      // `MAX_SEGMENTS - 1` read every candidate blob and then died inside
      // `Render.commit` with the work already done.
      const ceiling = Math.min(Pack.MAX_ITEMS, Render.MAX_SEGMENTS - 1);
      if (maxItems < 1 || maxItems > ceiling) {
        return yield* new Invalid({
          field: "max-items",
          reason: `--max-items must be between 1 and ${ceiling}`,
        });
      }
      // Bounded too, rather than degrading quietly into "instructions only,
      // everything else omitted as budget" — which is what a zero or negative
      // budget produced.
      // Bounded above as well as below. Nothing else caps the render: a
      // gigabyte budget wrote a gigabyte of `context/render.bin` onto an
      // append-only trace ref this version cannot delete, and replicated it.
      if (maxBytes < 1 || maxBytes > Select.MAX_EVIDENCE) {
        return yield* new Invalid({
          field: "max-bytes",
          reason: `--max-bytes must be between 1 and ${Select.MAX_EVIDENCE}`,
        });
      }

      // The task, before a single object exists. `Exposure.expose` scans it
      // too and refuses correctly — but `Pack.capture` runs first inside
      // `withWork`, and on a dirty checkout it has already written a blob per
      // tracked file and the whole overlay tree by then. This is the refusal
      // an operator is most likely to hit, and it was the one that left the
      // most behind: the same ordering defect the two checks above were
      // hoisted here to prevent.
      const leaked = Secrets.scan(task);
      if (leaked.length > 0) {
        return yield* new Invalid({
          field: "task",
          reason: `this task looks like it carries ${leaked
            .map((finding) => `a ${finding.kind} (${finding.hint})`)
            .join(", ")}; it is written verbatim into the render`,
        });
      }

      const signer = session === "" ? null : yield* readPrivateKey(key);

      const result = yield* withWork(
        work,
        Effect.gen(function* () {
          const repository = yield* Repository;
          const base = yield* mustResolve(repository, rev);
          const head = yield* repository.resolve(yield* repository.head);

          // Captured once, and handed to both the selector and the record. A
          // second capture between them would let the work tree move and
          // produce a pack whose items belong to a tree nothing retains.
          //
          // Which capture depends on whether the named commit is the one the
          // work tree is on. An overlay is the *checkout's* content; labelling
          // it with somebody else's commit signs a false ancestry claim onto
          // an append-only ref, and nothing downstream would catch it —
          // `Pack.verify` resolves `view.tree` and never reads `base`. So a
          // revision that is not HEAD gets its own committed tree, which is
          // the honest reading of "build context against that commit".
          // The repository's own identity before anything is written. It is
          // the last refusal on this path that was not hoisted: in a
          // repository with no genesis, `context for --session S --key K`
          // wrote a blob per dirty tracked file and the whole overlay tree and
          // *then* said the repository is not hub-enabled, leaving exactly the
          // orphaned objects the checks above `withWork` exist to prevent.
          const repo = signer === null ? null : yield* identityOf();

          const view = base === head ? yield* Pack.capture(base) : yield* Pack.committed(base);
          const pack = yield* Select.select({ task, view, maxItems, maxBytes });
          if (signer === null) return { pack, exposure: null } as const;

          const segments = yield* Select.render(pack, task);
          // What somebody accountable said to remove, so a deterministic
          // render cannot put those bytes back in the store. `removed`, not
          // `excluded`: the second withholds its shared half whenever any
          // record ref came back short, which is the safe direction for `gc`
          // and the wrong one here — one partially replicated ref anywhere in
          // the repository and every render reads as never-removed, so the
          // next identical `context for` retains it again. And not `covered`
          // either: that one is deliberately un-authorized, so anybody who can
          // append to a trace ref could stop every later exposure retaining
          // its render. Asked here rather than inside `expose`, because
          // `hub/Redaction` imports that module for the `context/` path names
          // and reading it back would close a cycle.
          const exposed = yield* Exposure.expose({
            repo: repo ?? (yield* identityOf()),
            session,
            key: signer,
            pack,
            segments,
            retain,
            task,
            removed: yield* Redaction.removed(),
          });
          return { pack, exposure: exposed } as const;
        }),
      );

      // A removal a later exposure partly undoes is the counterpart of a
      // removal that could not complete, and both used to happen in silence.
      // In the document on the `--json` path and on stderr otherwise: a reader
      // that asked for JSON gets JSON, and one that did not gets the notice
      // where a notice belongs.
      if (json) {
        return yield* Console.log(
          JSON.stringify(
            {
              pack: result.pack,
              exposure: result.exposure === null ? null : Exposure.identify(result.exposure.commit),
              renderDigest: result.exposure?.digest ?? null,
              renderRetained: result.exposure?.retained ?? null,
              // Which of the two reasons, because `renderRetained: false`
              // conflates "a signed removal names those exact bytes" with
              // "`--retain-render=false` was passed". The human path prints
              // the distinction; the comment above promised both notices land
              // here and only one did.
              renderWithheld: result.exposure?.withheld ?? null,
              resurrected: (result.exposure?.resurrected ?? []).map((oid) => Pack.qualify(oid)),
            },
            null,
            2,
          ),
        );
      }

      if (result.exposure !== null) {
        if (result.exposure.withheld) {
          yield* Console.error(
            "! the render was not retained: a signed removal names those exact bytes",
          );
        }
        for (const oid of result.exposure.resurrected) {
          yield* Console.error(
            `! ${Pack.qualify(oid)} is named by a signed removal and is back in this repository; the record it was removed from can read it again`,
          );
        }
      }

      yield* printPack(result.pack);
      if (result.exposure !== null) {
        yield* Console.log("");
        yield* Console.log(`exposure  ${Exposure.identify(result.exposure.commit)}`);
        yield* Console.log(`render    ${result.exposure.digest}`);
      }
    }),
);

// -- why ----------------------------------------------------------------------

/**
 * The pack bytes an argument names.
 *
 * Four spellings, because four callers are natural: a qualified pack oid out
 * of an exposure payload, an exposure's own record id — which is what a person
 * actually has after `context for` printed one — an ordinary revision, and a
 * file for a pack that was never persisted. CLI input takes any revision this
 * repository can resolve; serialized records keep the qualified form (§4.3).
 *
 * `resolveRev`, not `repository.resolve`: the latter takes full ref names
 * only, which is why `shared.ts` has the former at all. Given `main` this
 * resolved to nothing, fell through to the filesystem, and reported that a
 * branch was "neither an object in this repository nor a readable file".
 *
 * Which of the two object kinds it is comes from the object itself rather than
 * from the spelling: both are `sha1:<hex>`, and guessing by shape sent a
 * perfectly good exposure id to `readBlob`.
 */
const packBytes = Effect.fn("context.packBytes")(function* (reference: string) {
  const repository = yield* Repository;
  const named = Pack.unqualify(reference) ?? (yield* resolveRev(repository, reference));

  if (named !== null) {
    const object = yield* repository
      .readObject(named)
      .pipe(Effect.catchTag("ObjectNotFound", () => Effect.succeed(null)));
    if (object?.type === "blob") return object.data;
    // A record: the pack it retains is the one its signature commits to.
    if (object?.type === "commit") {
      // Unless a counted tombstone names it. `context audit` honours one in
      // both of its branches and `session show --audit` does too — because a
      // Pack is deterministic, so reading "whatever resolves" flips a removed
      // record back to intact the moment anybody repeats the work, and when a
      // live exposure shares that blob `stillNamed` keeps it forever. `why`
      // read the record's tree with no such check and printed `view.tree`,
      // every selected path and every blob oid of a record an operator had
      // removed.
      const { repo, trust } = yield* membership();
      const held = yield* locate(named);
      if (held !== null) {
        const removals = yield* removalsOn(held.session, repo, trust);
        if (removals.has(Pack.qualify(named))) {
          return yield* new Invalid({
            field: "pack",
            reason: `${reference} was removed by a signed redaction`,
          });
        }
      }
      return (yield* Exposure.packOf(named)).bytes;
    }
    if (object !== null) {
      return yield* new Invalid({
        field: "pack",
        reason: `${reference} is a ${object.type}, which is neither a pack nor an exposure`,
      });
    }
  }

  const contents = yield* Effect.try({
    try: () => fs.readFileSync(reference),
    catch: () =>
      new Invalid({
        field: "pack",
        reason: `'${reference}' is neither an object in this repository nor a readable file`,
      }),
  });
  return new Uint8Array(contents);
});

/**
 * `why` and `audit` read objects and nothing else.
 *
 * So they take `--root`/`--repo` rather than `--work`: both are read-only, and
 * requiring a checkout made them unusable in exactly the case §13 says to keep
 * available — a bare repository on a server, where an operator auditing a
 * pushed exposure has objects and no files. `for` keeps `--work`, because
 * capturing a Repository View genuinely needs the work tree.
 */
const why = Command.make(
  "why",
  {
    root: rootFlag,
    repo: repoFlag,
    json: jsonFlag,
    pack: Argument.string("pack"),
    item: Argument.string("item").pipe(Argument.optional),
  },
  ({ item, json, pack, repo, root }) =>
    withDiscovered(
      root,
      repo,
      Effect.gen(function* () {
        const decoded = yield* Pack.decode(yield* packBytes(pack));
        const report = yield* Pack.verify(decoded);
        const wanted = item._tag === "Some" ? item.value : null;

        const rows = decoded.items
          .map((entry, index) => ({ entry, checked: report.items[index] }))
          .filter(({ entry }) => wanted === null || entry.path === wanted);

        if (json) {
          return yield* Console.log(
            JSON.stringify({ view: decoded.view, verified: report.view, items: rows }, null, 2),
          );
        }

        yield* Console.log(`view.tree ${decoded.view.tree}`);
        yield* Console.log(report.view.ok ? "  verified" : `  unverified: ${report.view.reason}`);
        for (const { checked, entry } of rows) {
          yield* Console.log("");
          yield* Console.log(describe(entry));
          // The two halves are printed apart on purpose: the first is what Git
          // can be made to agree with, the second is the selector's own
          // account of itself, and an audit that ran them together would give
          // a scoring heuristic the standing of a hash comparison.
          yield* Console.log(
            checked?.evidence.ok === true
              ? "  git:      resolves under view.tree"
              : `  git:      ${checked?.evidence.ok === false ? checked.evidence.reason : "not checked"}`,
          );
          if (checked?.authority != null) {
            yield* Console.log(
              checked.authority.ok
                ? "  authority: verified against view.tree"
                : `  authority: unverified claim — ${checked.authority.reason}`,
            );
          }
          yield* Console.log(`  selector: ${explanation(entry)}`);
        }
      }),
    ),
);

// -- audit --------------------------------------------------------------------

/**
 * The records a session's own tombstones account for.
 *
 * Verified, not merely present. `Records.entries` is the *nothing verified*
 * walk, and replication is deliberately not policy-gated — so a fetched-in
 * commit whose payload claims the tombstone tag would otherwise reclassify a
 * tampered exposure as "redacted", which does not feed the non-zero exit. That
 * turns a failing `context audit S && deploy` into a deploying one, which is
 * the whole guarantee that exit exists to provide. Judged on `hub.redact`,
 * which is the capability that writes one; `Redaction.excluded` makes the same
 * test for the same decision.
 */
const removalsOn = Effect.fn("context.removalsOn")(function* (
  session: string,
  /** This repository's own id: the half of the binding `entries` cannot check. */
  repo: string,
  trust: Parameters<typeof Exposure.audit>[0]["trust"],
  /** The walk the caller already took; see `Records.entries`. */
  taken?: Trace.Walk,
) {
  const removals = new Set<string>();
  if (trust == null) return removals;
  for (const entry of (yield* Records.entries(session, taken)).records) {
    // Bound to this repository, which `Records.entries` deliberately leaves to
    // its caller — `Invocation.project` does the same filter for the same
    // reason. Traces are transferable by explicit refspec and replication is
    // not gated on payload contents, so a key holding `hub.redact` in two
    // repositories could redact an exposure in one and have the record reach
    // the other: the exposure it names then landed in `redacted` here, which
    // is excluded from `audits`, from `unreadable` and so from the non-zero
    // exit — and `context audit <session> && deploy` deployed having never
    // checked that exposure's signature, trust, binding or evidence.
    if (!Claim.bound(entry.payload, { repo })) continue;
    if (entry.payload.type !== Records.REDACTED) continue;
    const signers = yield* Verify.signers(entry.bytes, entry.signatures);
    if (Tombstone.counts(trust, signers)) removals.add(entry.payload.targetCommit);
  }
  return removals;
});

/**
 * Which session's trace ref holds this record, and whether it is an exposure.
 *
 * The record's own payload names a session, and that is used as a *hint* —
 * checked against the ref it names, never believed. Scanning every trace ref
 * in the repository read all of them end to end to find one commit, which for
 * a repository with a few hundred recorded sessions is a full walk and decode
 * of each. The fallback scan stays for a record whose payload is unreadable or
 * whose claim does not hold, which is exactly when the answer matters most.
 *
 * Both halves come back because both are asked. `trace record` prints an
 * `invocation-telemetry` oid to stdout and the docs call this verb's argument
 * "a qualified Git record OID", so one arriving here is the ordinary case —
 * and matching only exposures reported it as "on no trace ref in this
 * repository", which is false: it is on one, and saying which is more use than
 * denying it is there.
 */
/**
 * The session a record's own payload names, whatever kind of record it is.
 *
 * Sniffed from the JSON rather than decoded, because the envelope is shared by
 * every hub namespace and each namespace's schema is not. A hint, never
 * believed: `locate` checks it against the ref it names.
 */
const sessionHint = Effect.fn("context.sessionHint")(function* (commit: Oid) {
  const read = yield* Record.read(commit, Event.RECORD).pipe(
    Effect.catchTags({
      ObjectNotFound: () => Effect.succeed(null),
      Invalid: () => Effect.succeed(null),
    }),
  );
  if (read === null) return null;
  const named = yield* Effect.try({
    try: (): string | null => {
      const json: unknown = JSON.parse(new TextDecoder().decode(read.payload));
      if (!Predicate.isObject(json)) return null;
      if (!Predicate.hasProperty(json, "session")) return null;
      const session: unknown = json.session;
      return Predicate.isString(session) ? session : null;
    },
    catch: () => null,
  }).pipe(Effect.orElseSucceed(() => null));
  return named;
});

const locate = Effect.fn("context.locate")(function* (commit: Oid) {
  const on = Effect.fn("context.on")(function* (session: string) {
    // A ref this host will not walk is one ref, not a broken repository. The
    // scan runs whenever the payload's own hint does not hold, so an
    // over-ceiling trace ref anywhere failed `context audit` for every
    // unrelated record — the reading every comparable walk here already gives
    // (`Redaction.tombstonesOn`, `Redaction.tombstoned`).
    const walked = yield* Trace.walk(session).pipe(
      Effect.catchTag("Invalid", () => Effect.succeed(null)),
    );
    if (walked === null) return null;

    // `unreadable` counts. A redacted record is still on its ref — the commit
    // stays so the hash chain holds — and reporting "is on no trace ref" for
    // one whose payload was deliberately removed says the opposite of what
    // happened, on the one path an operator uses to confirm a removal.
    const read = walked.records.find((entry) => entry.commit === commit);
    if (read !== undefined) {
      // From the signed payload, the way `Exposure.entries` decides it. On the
      // commit-message hint alone the two audit surfaces disagreed about one
      // record: `context audit <session>` enumerated and audited a mislabelled
      // exposure, while `context audit sha1:<oid>` refused it as "not a
      // context exposure". Claiming to be one and not decoding still counts —
      // `entries` reports that as a damaged exposure rather than as something
      // else, and so should this.
      const payload = yield* Exposure.decode(read.payload).pipe(Effect.orElseSucceed(() => null));
      return { session, exposure: payload !== null || read.type === Exposure.TYPE } as const;
    }
    const gone = walked.unreadable.find((entry) => entry.commit === commit);
    return gone === undefined
      ? null
      : ({ session, exposure: gone.type === Exposure.TYPE } as const);
  });

  // The payload, not an audit. `audit` answers this too, and pays a tree walk
  // per evidence item and a SHA-256 over the retained render to do it — all of
  // which the real audit then repeats on the same commit a moment later.
  //
  // And read as a bare field rather than through an exposure decoder. Every
  // hub record carries `session` in the same envelope, but decoding as an
  // exposure gave `null` for all five other kinds — so a `tool-operation` or
  // an unbound `invocation-telemetry` oid, which the docstring above calls an
  // ordinary input, fell through to the scan below and walked and decoded
  // every record on every trace ref in the repository to produce a one-line
  // error. Whatever it says is checked against the ref it names, so a
  // sniffed field is a hint exactly as the decoded one was.
  const claimed = yield* sessionHint(commit);
  if (claimed !== null && Trace.isTraceId(claimed)) {
    const here = yield* on(claimed);
    if (here !== null) return here;
  }

  for (const session of yield* Trace.traces()) {
    const here = yield* on(session);
    if (here !== null) return here;
  }
  return null;
});

const printAudit = (audit: Exposure.Audit) =>
  Effect.gen(function* () {
    const line = (label: string, check: Exposure.Check) =>
      Console.log(`${label.padEnd(10)}${check.ok ? "ok" : `no — ${check.reason}`}`);

    yield* Console.log(`exposure  ${audit.exposure}`);
    yield* line("signature", audit.signature);
    if (audit.signers.length > 0) yield* Console.log(`signers   ${audit.signers.join(", ")}`);
    if (audit.trust !== null) yield* line("trust", audit.trust);
    yield* line("binding", audit.binding);
    yield* line("pack", audit.pack);
    yield* line("view", audit.retained);

    if (audit.evidence !== null) {
      // The view first. `Pack.verify` returns it as its own `Check`, and when
      // it fails there are no item lines at all — so every visible line read
      // `ok` and the command ended on a bare `not verified` with the reason
      // nowhere in the output. `context why` prints this; `audit` dropped it.
      yield* line("view.tree", audit.evidence.view);
      for (const checked of audit.evidence.items) {
        yield* Console.log(
          `  ${checked.kind} ${checked.path}: ${checked.evidence.ok ? "verified" : checked.evidence.reason}`,
        );
        if (checked.authority?.ok === false) {
          yield* Console.log(`    authority: unverified claim — ${checked.authority.reason}`);
        }
      }
    }

    // Three render states, printed as three: a retention that has expired is
    // not an audit failure, and reporting it as one would teach an operator to
    // stop reading these (§11).
    yield* Console.log(
      audit.render.state === "verified"
        ? `render    ok — ${audit.render.segments.length} segments recomputed`
        : `render    ${audit.render.state} — ${audit.render.reason}`,
    );
    if (audit.capture !== null) {
      // The stage and the semconv revision too. §12 makes *where* a signal was
      // picked off the thing that decides whether a completeness claim is
      // available at all, and `cli/audit.ts` prints it for the same record —
      // so an operator checking a pushed exposure on a server saw the fields
      // that qualify the claim only through `--json`.
      const parts = [
        audit.capture.transport,
        audit.capture.stage,
        audit.capture.semconv === undefined
          ? undefined
          : `semconv ${audit.capture.semconv.revision}`,
        audit.capture.traceId,
        audit.capture.spanId,
      ].filter((part) => part !== undefined && part !== "");
      yield* Console.log(`capture   ${parts.join(" · ")}`);
    }
    yield* Console.log(audit.ok ? "verified" : "not verified");
  });

/**
 * The exposure an invocation record names, if the target is one.
 *
 * context-pack.md §13 calls this verb's argument
 * `<invocation-or-exposure>`, and `trace record` prints an
 * `invocation-telemetry` oid — so one arriving here is the ordinary case, not
 * a mistake. An invocation names the exposure it was given in its own signed
 * payload; following that is the same audit reached by the other spelling,
 * where refusing it made the documented command fail on the oid the
 * neighbouring command had just printed.
 *
 * Only an invocation, and only one that names an exposure. A tool operation, a
 * workspace transition or an invocation that used no context still gets the
 * refusal and the pointer at `session show --audit`, because for those there
 * is no exposure to audit and saying so is more use than auditing something
 * else.
 */
const boundExposure = Effect.fn("context.boundExposure")(function* (commit: Oid) {
  const read = yield* Record.read(commit, Event.RECORD).pipe(
    Effect.catchTags({
      ObjectNotFound: () => Effect.succeed(null),
      Invalid: () => Effect.succeed(null),
    }),
  );
  if (read === null) return null;
  const payload = yield* Records.decode(read.payload).pipe(Effect.orElseSucceed(() => null));
  if (payload?.type !== Records.INVOCATION || payload.exposure === null) return null;
  return Pack.unqualify(payload.exposure);
});

const auditCommand = Command.make(
  "audit",
  {
    root: rootFlag,
    repo: repoFlag,
    json: jsonFlag,
    target: Argument.string("invocation-or-exposure"),
  },
  ({ json, repo, root, target }) =>
    withDiscovered(
      root,
      repo,
      Effect.gen(function* () {
        const { repo, trust } = yield* membership();
        const repository = yield* Repository;
        // One trust-log walk for the whole command; see `Exposure.audit`.
        const reach = trustReach();
        // Resolution decides, not shape. `Trace.isTraceId` is true of any
        // single ref-safe component — `main`, `HEAD`, `v1.0` are all sessions
        // by that test — so asking it first sent every ordinary revision to
        // the session branch, which walked a `refs/hub/trace/main` that does
        // not exist and reported "has no exposures to audit".
        const named = Pack.unqualify(target) ?? (yield* resolveRev(repository, target));
        // Followed before anything is looked up, so every step below sees the
        // exposure and none of them needs to know the argument named an
        // invocation.
        const oid = named === null ? null : ((yield* boundExposure(named)) ?? named);
        const found = oid === null ? null : yield* locate(oid);
        const holder = found?.exposure === true ? found.session : null;

        // A revision that resolves but sits on no trace ref, whose name could
        // also be a session, is a session. Resolving first is right — deciding
        // by shape sent every revision to the session branch — but it is not
        // the last word: a branch named after the run it was worked on is the
        // ordinary case, and it shadowed that run's audit outright.
        // `found`, not `holder`. `holder` is null for any record that is not a
        // context exposure, so a bare 40-hex oid of an `invocation-telemetry`
        // record — which `resolveRev` returns as-is and which `isTraceId`
        // accepts, since it is a legal ref component — fell into the session
        // branch, walked a `refs/hub/trace/<40 hex>` that does not exist, and
        // reported "has no exposures to audit" instead of saying what the
        // record actually is. The qualified spelling escaped only because `:`
        // is a reserved ref character.
        const asSession = oid === null || (found === null && Trace.isTraceId(target));

        const audits: Array<Exposure.Audit> = [];
        const unreadable: Array<string> = [];
        const redacted: Array<string> = [];
        const foreign: Array<Oid> = [];
        if (asSession) {
          // A session id: every exposure it holds, oldest first, which is what
          // an operator asking "what did this run see?" actually wants.
          if (!Trace.isTraceId(target)) {
            return yield* new Invalid({
              field: "exposure",
              reason: `'${target}' is neither a qualified record oid nor a session id`,
            });
          }
          const walked = yield* Exposure.entries(target, undefined, repo);
          // The tombstone outranks the bytes, and the single-record branch
          // below already read it that way — this one did not, so the two
          // forms of the command gave different accounts of one record.
          //
          // Which matters because the bytes can come back. A Pack and a
          // ContextRender are deterministic, so re-running the same task
          // against an unchanged view writes blobs with the same oids, and the
          // redacted record's tree entries — kept on purpose, since the commit
          // has to stay for the hash chain — resolve again. Auditing whatever
          // resolves, this flipped a redacted record back to a full `verified`
          // audit the moment anybody repeated the work, silently. The bytes
          // being recreated by a later, legitimate exposure is not something
          // this version can prevent; presenting the removed record as intact
          // is.
          const removals = yield* removalsOn(target, repo, trust, walked.taken);
          for (const entry of walked.exposures) {
            const oid = Pack.qualify(entry.commit);
            if (removals.has(oid)) {
              redacted.push(oid);
              continue;
            }
            audits.push(
              yield* Exposure.audit({ commit: entry.commit, repo, session: target, trust, reach }),
            );
          }
          // A record that declared itself a context exposure and then failed
          // to decode is the one absence `Exposure.entries` refuses to
          // swallow — and dropping it here swallowed it anyway, so a session
          // holding one good exposure and one tampered payload printed one
          // verified audit and exited zero.
          //
          // A *redacted* one is not that. An absence with a tombstone beside
          // it is a removal, and one without is a record this replica could
          // not read: counting them together made every later
          // `context audit S && deploy` fail permanently the first time
          // anybody used the removal path the CLI documents.
          // Said, not counted against the gate: a record on this ref naming
          // another session is that session's business, and a peer able to
          // plant one must not be able to fail this repository's deploy gate
          // with it.
          foreign.push(...walked.foreign);
          for (const commit of walked.unreadable) {
            const oid = Pack.qualify(commit);
            if (removals.has(oid)) redacted.push(oid);
            else unreadable.push(oid);
          }
        } else {
          // Found by the ref it lives under, not by what it says about itself:
          // read out of the payload, the binding check would agree with itself
          // every time.
          const session = holder;
          if (session === null) {
            // On a trace ref, just not as an exposure — a runtime record, a
            // tool operation, a workspace transition.
            if (found !== null) {
              return yield* new Invalid({
                field: "exposure",
                reason: `${target} is on ${Trace.refOf(found.session)} but is not a context exposure; read it with \`git+ session show ${found.session} --audit\``,
              });
            }
            return yield* new Invalid({
              field: "exposure",
              reason: `${target} is on no trace ref in this repository`,
            });
          }
          // The same split the session branch makes. Without it the two forms
          // of the command disagreed about one record: `context audit <session>`
          // exited zero on a legitimately redacted exposure while
          // `context audit <that record>` failed forever, on the same
          // repository, for the same removal.
          if ((yield* removalsOn(session, repo, trust)).has(Pack.qualify(oid))) {
            redacted.push(Pack.qualify(oid));
          } else {
            // And the same split for a record this repository has already
            // decided is somebody else's. The session form routes one into
            // `foreign` and leaves the exit code alone; this one handed the
            // ref-derived session and repo to `audit`, whose `binding` check
            // then failed — so `context audit <session> && deploy` kept
            // deploying while `context audit sha1:<that oid> && deploy` was
            // broken for good over the same record. A peer holding `hub.trace`
            // in two repositories is enough to land one.
            const held = (yield* Exposure.entries(session, undefined, repo)).foreign;
            if (held.includes(oid)) foreign.push(oid);
            else audits.push(yield* Exposure.audit({ commit: oid, repo, session, trust, reach }));
          }
        }

        // Asked after the unreadable records are known, not before. A session
        // whose exposures have all been redacted has none to audit *and* has
        // records it could not read, and reporting only the first says "this
        // run exposed nothing" — the opposite of what happened, in the one
        // case an operator most needs the distinction.
        if (
          audits.length === 0 &&
          unreadable.length === 0 &&
          redacted.length === 0 &&
          // A record this repository has decided is somebody else's is still
          // something the ref holds, and refusing over it is the non-zero exit
          // the `foreign` routing exists to avoid.
          foreign.length === 0
        ) {
          return yield* new Invalid({
            field: "exposure",
            reason: `${target} has no exposures to audit`,
          });
        }
        // The unreadable ids go *inside* the document on the JSON path.
        // Printed after it as bare lines, the output stopped being JSON the
        // moment a session held one, and there was no machine-readable way to
        // learn which records they were.
        if (json) {
          // Inside the document, for the reason the unreadable ids are: printed
          // after it as bare lines the output stopped being JSON, and a machine
          // consumer had no way to learn which records they were.
          yield* Console.log(
            JSON.stringify(
              { audits, unreadable, redacted, foreign: foreign.map(Pack.qualify) },
              null,
              2,
            ),
          );
        } else {
          for (const commit of foreign) {
            yield* Console.error(
              `! ${Pack.qualify(commit)} is on this ref but names another session or repository`,
            );
          }
          for (const [index, audit] of audits.entries()) {
            if (index > 0) yield* Console.log("");
            yield* printAudit(audit);
          }
          for (const commit of redacted) yield* Console.log(`redacted   ${commit}`);
          for (const commit of unreadable) yield* Console.log(`unreadable ${commit}`);
        }

        // The report is printed either way, and then the command fails. This
        // is the one verb in the tree whose whole purpose is verification, and
        // exiting zero on a record whose signature, trust, binding or evidence
        // did not check makes `git+ context audit … && deploy` deploy.

        const failed = audits.filter((audit) => !audit.ok);
        if (failed.length > 0 || unreadable.length > 0) {
          return yield* new Invalid({
            field: "exposure",
            reason: [
              failed.length > 0
                ? `${failed.length} of ${audits.length} exposure(s) did not verify`
                : null,
              unreadable.length > 0 ? `${unreadable.length} record(s) could not be read` : null,
            ]
              .filter((part) => part !== null)
              .join("; "),
          });
        }
      }),
    ),
);

export const contextCommand = Command.make("context", {}, () =>
  Console.log("Usage: git+ context <for|why|audit>"),
).pipe(
  Command.withSubcommands([
    forCommand.pipe(
      Command.withDescription("Build a Context Pack for a task and print the evidence"),
    ),
    why.pipe(Command.withDescription("Explain a pack's evidence and how it was selected")),
    auditCommand.pipe(Command.withDescription("Verify a historical Context Exposure")),
  ]),
);
