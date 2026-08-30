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

import { Console, Effect } from "effect";
import { Argument, Command, Flag } from "effect/unstable/cli";

import * as Exposure from "../context/Exposure.ts";
import * as Pack from "../context/Pack.ts";
import * as Select from "../context/Select.ts";
import { Invalid } from "../git/Error.ts";
import { Repository } from "../git/Repository.ts";
import { isOid, type Oid } from "../git/Store.ts";
import * as Trace from "../hub/Trace.ts";
import { readGenesis } from "../trust/Genesis.ts";
import { project as projectTrust } from "../trust/Projection.ts";
import { mustResolve, readPrivateKey, withWork, workFlag } from "./shared.ts";

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
      yield* Console.log(
        `omitted   ${omission.path ?? `${omission.count ?? 0} items`}: ${omission.reason}`,
      );
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
      const signer = session === "" ? null : yield* readPrivateKey(key);

      const result = yield* withWork(
        work,
        Effect.gen(function* () {
          const repository = yield* Repository;
          const base = yield* mustResolve(repository, rev);
          // Captured once, and handed to both the selector and the record. A
          // second capture between them would let the work tree move and
          // produce a pack whose items belong to a tree nothing retains.
          const view = yield* Pack.capture(base);
          const pack = yield* Select.select({ task, view, maxItems, maxBytes });
          if (signer === null) return { pack, exposure: null } as const;

          const segments = yield* Select.render(pack, task);
          const exposed = yield* Exposure.expose({
            repo: yield* identityOf(),
            session,
            key: signer,
            pack,
            segments,
            retain,
          });
          return { pack, exposure: exposed } as const;
        }),
      );

      if (json) {
        return yield* Console.log(
          JSON.stringify(
            {
              pack: result.pack,
              exposure: result.exposure === null ? null : Exposure.identify(result.exposure.commit),
              renderDigest: result.exposure?.digest ?? null,
            },
            null,
            2,
          ),
        );
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
 * actually has after `context for` printed one — an abbreviated oid or ref, and
 * a file for a pack that was never persisted. CLI input takes any revision this
 * repository can resolve; serialized records keep the qualified form (§4.3).
 *
 * Which of the two object kinds it is comes from the object itself rather than
 * from the spelling: both are `sha1:<hex>`, and guessing by shape sent a
 * perfectly good exposure id to `readBlob`.
 */
const packBytes = Effect.fn("context.packBytes")(function* (reference: string) {
  const repository = yield* Repository;
  const named =
    Pack.unqualify(reference) ??
    (isOid(reference) ? reference : yield* repository.resolve(reference));

  if (named !== null) {
    const object = yield* repository
      .readObject(named)
      .pipe(Effect.catchTag("ObjectNotFound", () => Effect.succeed(null)));
    if (object?.type === "blob") return object.data;
    // A record: the pack it retains is the one its signature commits to.
    if (object?.type === "commit") return (yield* Exposure.packOf(named)).bytes;
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

const why = Command.make(
  "why",
  {
    work: workFlag,
    json: jsonFlag,
    pack: Argument.string("pack"),
    item: Argument.string("item").pipe(Argument.optional),
  },
  ({ item, json, pack, work }) =>
    withWork(
      work,
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

/** Which session's trace ref holds this record, if any of them do. */
const sessionOf = Effect.fn("context.sessionOf")(function* (commit: Oid) {
  for (const session of yield* Trace.traces()) {
    const walked = yield* Exposure.entries(session);
    if (walked.exposures.some((exposure) => exposure.commit === commit)) return session;
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
      yield* Console.log(
        `capture   ${audit.capture.transport} ${audit.capture.traceId ?? ""} ${audit.capture.spanId ?? ""}`.trimEnd(),
      );
    }
    yield* Console.log(audit.ok ? "verified" : "not verified");
  });

const auditCommand = Command.make(
  "audit",
  { work: workFlag, json: jsonFlag, target: Argument.string("exposure-or-session") },
  ({ json, target, work }) =>
    withWork(
      work,
      Effect.gen(function* () {
        const { repo, trust } = yield* membership();
        const oid = Pack.unqualify(target) ?? (isOid(target) ? target : null);

        const audits: Array<Exposure.Audit> = [];
        if (oid === null) {
          // A session id: every exposure it holds, oldest first, which is what
          // an operator asking "what did this run see?" actually wants.
          if (!Trace.isTraceId(target)) {
            return yield* new Invalid({
              field: "exposure",
              reason: `'${target}' is neither a qualified record oid nor a session id`,
            });
          }
          for (const entry of (yield* Exposure.entries(target)).exposures) {
            audits.push(
              yield* Exposure.audit({ commit: entry.commit, repo, session: target, trust }),
            );
          }
        } else {
          // Found by the ref it lives under, not by what it says about itself:
          // read out of the payload, the binding check would agree with itself
          // every time.
          const session = yield* sessionOf(oid);
          if (session === null) {
            return yield* new Invalid({
              field: "exposure",
              reason: `${target} is on no trace ref in this repository`,
            });
          }
          audits.push(yield* Exposure.audit({ commit: oid, repo, session, trust }));
        }

        if (audits.length === 0) {
          return yield* new Invalid({
            field: "exposure",
            reason: `${target} has no exposures to audit`,
          });
        }
        if (json) return yield* Console.log(JSON.stringify(audits, null, 2));
        for (const [index, audit] of audits.entries()) {
          if (index > 0) yield* Console.log("");
          yield* printAudit(audit);
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
