/** `git+ note …` and `git+ why`: code-anchored repository memory. */
import { Console, Effect } from "effect";
import { Argument, Command, Flag } from "effect/unstable/cli";

import * as Checkout from "../git/Checkout.ts";
import { Invalid } from "../git/Error.ts";
import { Repository } from "../git/Repository.ts";
import { WorkTree } from "../git/Work.ts";
import * as Anchor from "../hub/Anchor.ts";
import { syntax as anchorSyntax } from "../hub/Anchor.syntax.ts";
import * as Memory from "../hub/Memory.ts";
import * as Note from "../hub/Note.ts";
import * as Audit from "../hub/NoteAudit.ts";
import * as Candidate from "../hub/NoteCandidate.ts";
import * as Index from "../hub/NoteIndex.ts";
import * as Notes from "../hub/NoteProjection.ts";
import { readGenesis } from "../trust/Genesis.ts";
import { mustResolve, readPrivateKey } from "./shared.ts";
import { withWork, workFlag } from "./work.ts";

const keyFlag = Flag.string("key").pipe(
  Flag.withDescription("Path to the SSH private key to sign with"),
);
const jsonFlag = Flag.boolean("json").pipe(Flag.withDefault(false));
const pathArgument = Argument.string("path");
const noteArgument = Argument.string("note");

const identity = Effect.fn("note.identity")(function* () {
  const stored = yield* readGenesis();
  if (stored === null) {
    return yield* new Invalid({
      field: "repository",
      reason: "this work tree has no genesis; run `git+ hub init` first",
    });
  }
  return stored.genesis.repoId;
});

const one = Effect.fn("note.one")(function* (id: string) {
  const projected = yield* Notes.project(id);
  if (projected === null) {
    return yield* new Invalid({ field: "note", reason: `no anchored note '${id}'` });
  }
  if (projected.state === "conflicted") {
    return yield* new Invalid({
      field: "note",
      reason: `${id} has conflicting lifecycle events and must be reconciled before mutation`,
    });
  }
  return projected;
});

const currentBaseline = Effect.fn("note.currentBaseline")(function* (
  projected: Notes.NoteProjection,
) {
  const work = yield* WorkTree;
  if ((yield* work.stat(projected.path)) === null) {
    return yield* new Invalid({
      field: "path",
      reason: `${projected.path} no longer exists in the work tree`,
    });
  }
  const resolver = yield* Anchor.AnchorResolver;
  const resolved = yield* resolver.resolve(
    projected.path,
    yield* work.read(projected.path),
    projected.anchor,
  );
  if (resolved._tag !== "Found") {
    return yield* new Invalid({
      field: "anchor",
      reason: `${projected.path}#${projected.anchor} does not currently resolve (${resolved._tag})`,
    });
  }
  return resolved.fingerprint;
});

const add = Command.make(
  "add",
  {
    work: workFlag,
    key: keyFlag,
    anchor: Flag.string("anchor").pipe(Flag.withDefault(Anchor.FILE_ANCHOR)),
    pinned: Flag.boolean("pinned").pipe(Flag.withDefault(false)),
    path: pathArgument,
    text: Argument.string("text"),
  },
  ({ anchor, key, path, pinned, text, work }) =>
    Effect.gen(function* () {
      const signer = yield* readPrivateKey(key);
      const id = yield* withWork(
        work,
        Effect.gen(function* () {
          const tree = yield* WorkTree;
          if ((yield* tree.stat(path)) === null) {
            // The same diagnostic `confirm` and `replace` give for the same
            // mistake; without it a mistyped path surfaced as a raw store error.
            return yield* new Invalid({
              field: "path",
              reason: `${path} does not exist in the work tree`,
            });
          }
          const source = yield* tree.read(path);
          const resolved = yield* (yield* Anchor.AnchorResolver).resolve(path, source, anchor);
          if (resolved._tag === "Ambiguous") {
            return yield* new Invalid({
              field: "anchor",
              reason: `'${anchor}' is ambiguous: ${resolved.candidates.map((item) => item.value).join(", ")}`,
            });
          }
          if (resolved._tag !== "Found") {
            return yield* new Invalid({
              field: "anchor",
              reason: `'${anchor}' does not resolve in ${path} (${resolved._tag})`,
            });
          }
          const created = yield* Note.create({
            repo: yield* identity(),
            path,
            anchor: resolved.anchor.value,
            text,
            baseline: resolved.fingerprint,
            pinned,
            key: signer,
          });
          yield* Index.refresh();
          return created.note;
        }).pipe(Effect.provide(anchorSyntax)),
      );
      yield* Console.log(id);
    }),
);

const anchors = Command.make(
  "anchors",
  { work: workFlag, json: jsonFlag, path: pathArgument },
  ({ json, path, work }) =>
    withWork(
      work,
      Effect.gen(function* () {
        const resolver = yield* Anchor.AnchorResolver;
        const found = yield* resolver.anchors(path, yield* (yield* WorkTree).read(path));
        if (json) {
          yield* Console.log(
            JSON.stringify(
              { path, resolver: `${resolver.name}@${resolver.version}`, anchors: found },
              null,
              2,
            ),
          );
          return;
        }
        for (const item of found) {
          yield* Console.log(`${item.value.padEnd(28)} ${item.startLine}-${item.endLine}`);
        }
      }).pipe(Effect.provide(anchorSyntax)),
    ),
);

/**
 * A revision named on the command line, resolved as every other command here
 * resolves one — an oid, a full ref, or a bare branch or tag name.
 *
 * `--base`/`--head` exist so a merge check can ask what *this change* put at
 * risk. A repository-wide audit still answers "what is stale here", but that
 * is a different question from "did this pull request invalidate anything",
 * and a required check that conflates them blocks unrelated work.
 */
const commitOf = Effect.fn("note.commitOf")(function* (name: string) {
  return yield* mustResolve(yield* Repository, name);
});

/**
 * Every path this work tree has moved away from HEAD.
 *
 * Staged and unstaged both, and untracked files too: a note whose anchor was
 * just deleted and rewritten under a new name is exactly the one worth
 * checking, and it is invisible to a staged-only reading.
 */
const touchedPaths = Effect.fn("note.touchedPaths")(function* () {
  const current = yield* Checkout.status();
  return new Set([
    ...current.staged.map((entry) => entry.path),
    ...current.unstaged.map((entry) => entry.path),
    ...current.untracked,
  ]);
});

const check = Command.make(
  "check",
  {
    work: workFlag,
    json: jsonFlag,
    base: Flag.string("base").pipe(
      Flag.optional,
      Flag.withDescription("Check only notes on paths this range changed"),
    ),
    head: Flag.string("head").pipe(
      Flag.optional,
      Flag.withDescription("The revision whose source the range is checked against"),
    ),
    touched: Flag.boolean("touched").pipe(
      Flag.withDefault(false),
      Flag.withDescription("Check only notes on paths this work tree has changed"),
    ),
    path: Argument.string("path").pipe(Argument.optional),
  },
  ({ base, head, json, path, touched, work }) =>
    withWork(
      work,
      Effect.gen(function* () {
        const query = path._tag === "Some" ? path.value : undefined;
        const ranged = base._tag === "Some" || head._tag === "Some";
        if ((base._tag === "Some") !== (head._tag === "Some")) {
          return yield* new Invalid({
            field: "base",
            reason: "--base and --head name a range and are given together",
          });
        }
        // Refused rather than ignored: a range already says which paths to
        // check, and the JSON output reported the work tree's dirty paths
        // beside range-scoped results — which a CI consumer reads as a filter
        // that was never applied.
        if (ranged && touched) {
          return yield* new Invalid({
            field: "touched",
            reason: "--touched scopes to this work tree and --base/--head to a range; pick one",
          });
        }
        // What this work tree has actually changed, which is what §26 asks an
        // agent to resolve before it submits: everything else in the
        // repository may well be stale, and blaming this change for it is how
        // a required check stops being read.
        const changed = touched ? yield* touchedPaths() : null;
        const source = yield* Audit.workTree();
        // A range answers renames from its own diff, so the extra listing that
        // lets a moved note match a scoped query is only worth its walk on the
        // work-tree read.
        const notes = yield* Index.select(
          query,
          ranged ? undefined : yield* Audit.present(source, query),
        );
        const results =
          base._tag === "Some" && head._tag === "Some"
            ? yield* Audit.auditRange(
                notes,
                yield* commitOf(base.value),
                yield* commitOf(head.value),
                query,
              )
            : yield* Audit.auditAll(
                changed === null ? notes : notes.filter((note) => changed.has(note.path)),
                source,
                query,
              );
        if (json) {
          yield* Console.log(
            JSON.stringify(
              {
                query: query ?? null,
                base: base._tag === "Some" ? base.value : null,
                head: head._tag === "Some" ? head.value : null,
                touched: changed === null ? null : [...changed].sort(),
                notes: results,
              },
              null,
              2,
            ),
          );
        } else {
          yield* Console.log(`notes: ${results.length} checked`);
          const counts = new Map<string, number>();
          for (const item of results) counts.set(item.status, (counts.get(item.status) ?? 0) + 1);
          for (const [status, count] of counts) yield* Console.log(`${count} ${status}`);
          for (const item of results.filter((entry) => entry.status !== "fresh")) {
            const moved = item.pathMovedFrom === null ? "" : ` (moved from ${item.pathMovedFrom})`;
            yield* Console.log(
              `${item.status.padEnd(20)} ${item.path}#${item.anchor}${moved} (${item.id})`,
            );
          }
        }
        if (results.some((item) => Audit.actionable(item.status))) process.exitCode = 2;
      }).pipe(Effect.provide(anchorSyntax)),
    ),
);

const confirm = Command.make(
  "confirm",
  { work: workFlag, key: keyFlag, note: noteArgument },
  ({ key, note, work }) =>
    Effect.gen(function* () {
      const signer = yield* readPrivateKey(key);
      yield* withWork(
        work,
        Effect.gen(function* () {
          const projected = yield* one(note);
          yield* Note.confirm({
            repo: yield* identity(),
            note,
            baseline: yield* currentBaseline(projected),
            key: signer,
          });
          yield* Index.refresh();
        }).pipe(Effect.provide(anchorSyntax)),
      );
      yield* Console.log(`Confirmed ${note}`);
    }),
);

const replace = Command.make(
  "replace",
  { work: workFlag, key: keyFlag, note: noteArgument, text: Argument.string("text") },
  ({ key, note, text, work }) =>
    Effect.gen(function* () {
      const signer = yield* readPrivateKey(key);
      yield* withWork(
        work,
        Effect.gen(function* () {
          const projected = yield* one(note);
          yield* Note.replace({
            repo: yield* identity(),
            note,
            text,
            baseline: yield* currentBaseline(projected),
            key: signer,
          });
          yield* Index.refresh();
        }).pipe(Effect.provide(anchorSyntax)),
      );
      yield* Console.log(`Replaced ${note}`);
    }),
);

const retire = Command.make(
  "retire",
  {
    work: workFlag,
    key: keyFlag,
    reason: Flag.string("reason").pipe(Flag.withDefault("")),
    note: noteArgument,
  },
  ({ key, note, reason, work }) =>
    Effect.gen(function* () {
      const signer = yield* readPrivateKey(key);
      yield* withWork(
        work,
        Effect.gen(function* () {
          yield* one(note);
          yield* Note.retire({
            repo: yield* identity(),
            note,
            reason: reason === "" ? null : reason,
            key: signer,
          });
          yield* Index.refresh();
        }),
      );
      yield* Console.log(`Retired ${note}`);
    }),
);

const restore = Command.make(
  "restore",
  { work: workFlag, key: keyFlag, note: noteArgument },
  ({ key, note, work }) =>
    Effect.gen(function* () {
      const signer = yield* readPrivateKey(key);
      yield* withWork(
        work,
        Effect.gen(function* () {
          const projected = yield* one(note);
          const workTree = yield* WorkTree;
          const baseline =
            (yield* workTree.stat(projected.path)) === null
              ? null
              : yield* currentBaseline(projected);
          yield* Note.restore({ repo: yield* identity(), note, baseline, key: signer });
          yield* Index.refresh();
        }).pipe(Effect.provide(anchorSyntax)),
      );
      yield* Console.log(`Restored ${note}`);
    }),
);

/**
 * Rebuild the path index by hand.
 *
 * Every mutation refreshes it already, so this is for the replica that
 * received notes by fetching rather than by writing them — where nothing
 * local ever ran to rebuild it, and every `why` pays the full fold until
 * something does.
 */
const index = Command.make("index", { work: workFlag }, ({ work }) =>
  withWork(
    work,
    Effect.gen(function* () {
      const built = yield* Index.refresh();
      const notes = Object.keys(built.refs).length;
      const paths = Object.keys(built.paths).length;
      yield* Console.log(`indexed ${notes} note(s) across ${paths} path(s)`);
    }),
  ),
);

/** As `task redact`, on the namespace whose text §28 makes untrusted data. */
const redact = Command.make(
  "redact",
  {
    work: workFlag,
    key: keyFlag,
    target: Flag.string("target").pipe(Flag.withDescription("The record's event id")),
    reason: Flag.string("reason").pipe(Flag.withDescription("Why it is being removed")),
    note: noteArgument,
  },
  ({ key, note, reason, target, work }) =>
    Effect.gen(function* () {
      const signer = yield* readPrivateKey(key);
      yield* withWork(
        work,
        Effect.gen(function* () {
          yield* Note.redact({
            repo: yield* identity(),
            note,
            target,
            reason,
            key: signer,
          });
          yield* Index.refresh();
        }),
      );
      yield* Console.log(`Redacted ${target}; the payload goes at the next gc`);
    }),
);

/**
 * What sessions learned that no note yet says, and where it could go.
 *
 * Prints, never writes. §27 draws the line here and this command is the line:
 * a suggestion becomes a constraint only when somebody signs a `note.created`
 * for it, which is what the `git+ note add` line under each candidate is for.
 */
const candidates = Command.make(
  "candidates",
  { work: workFlag, json: jsonFlag },
  ({ json, work }) =>
    withWork(
      work,
      Effect.gen(function* () {
        const found = yield* Candidate.candidates();
        if (json) {
          yield* Console.log(JSON.stringify({ candidates: found }, null, 2));
          return;
        }
        if (found.length === 0) {
          yield* Console.log("no uncaptured session observations with somewhere to anchor them");
          return;
        }
        yield* Console.log(
          `${found.length} candidate constraint(s); none of this is recorded yet\n`,
        );
        for (const candidate of found) {
          yield* Console.log(`  ${candidate.kind}: ${candidate.text}`);
          yield* Console.log(`    session ${candidate.session}`);
          for (const path of candidate.paths) {
            yield* Console.log(
              `    git+ note add ${path} "${candidate.text.replaceAll('"', "'")}"`,
            );
          }
          yield* Console.log("");
        }
      }),
    ),
);

export const noteCommand = Command.make("note", {}, () =>
  Console.log(
    "git+ note <add|anchors|check|confirm|replace|retire|restore|redact|index|candidates> — see --help",
  ),
).pipe(
  Command.withSubcommands([
    add.pipe(Command.withDescription("Attach a durable constraint to source")),
    anchors.pipe(Command.withDescription("Discover canonical anchors in a source file")),
    check.pipe(Command.withDescription("Audit anchored constraints for source drift")),
    confirm.pipe(Command.withDescription("Confirm that a constraint still applies")),
    replace.pipe(Command.withDescription("Replace a constraint and its baseline")),
    retire.pipe(Command.withDescription("Retire a constraint without deleting history")),
    restore.pipe(Command.withDescription("Restore a retired constraint")),
    redact.pipe(Command.withDescription("Remove one record's content, needing hub.redact")),
    index.pipe(Command.withDescription("Rebuild the disposable path index over notes")),
    candidates.pipe(Command.withDescription("Suggest constraints from sessions; records nothing")),
  ]),
);

/**
 * `git+ why [path]` — the read before the edit.
 *
 * With no path it is the read before the *session*: every pinned constraint,
 * plus repository memory. That is what pinning has always been for — §7.6
 * calls it projection metadata and keeps it out of drift semantics, which
 * leaves prominence as the only thing it can mean — and it is the one query
 * an agent can usefully make before it knows which files it will touch.
 */
export const whyCommand = Command.make(
  "why",
  { work: workFlag, json: jsonFlag, path: Argument.string("path").pipe(Argument.optional) },
  ({ json, path, work }) =>
    withWork(
      work,
      Effect.gen(function* () {
        const query = path._tag === "Some" ? path.value : undefined;
        const source = yield* Audit.workTree();
        const selected = yield* Index.select(query, yield* Audit.present(source, query));
        const scoped = query === undefined ? selected.filter((note) => note.pinned) : selected;
        const notes = yield* Audit.auditAll(scoped, source, query);
        const memory = (yield* Memory.distill()).entries;
        if (json) {
          yield* Console.log(
            JSON.stringify(
              { query: query ?? null, pinned: query === undefined, notes, memory },
              null,
              2,
            ),
          );
          return;
        }
        yield* Console.log(query ?? "(pinned constraints)");
        yield* Console.log("\nAnchored constraints\n");
        if (notes.length === 0) yield* Console.log("  (none)");
        for (const note of notes) {
          const where = query === undefined ? `${note.path}#${note.anchor}` : note.anchor;
          yield* Console.log(
            `  ${where}\n    ${note.text}\n    status: ${note.status}\n    note: ${note.id}`,
          );
        }
        yield* Console.log("\nRepository memory\n");
        if (memory.length === 0) yield* Console.log("  (none)");
        for (const entry of memory) yield* Console.log(`  ${entry.kind}: ${entry.text}`);
      }).pipe(Effect.provide(anchorSyntax)),
    ),
);
