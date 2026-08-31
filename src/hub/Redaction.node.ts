/**
 * Keeping the removed-blob answer across a process boundary.
 *
 * `hub/Redaction.Answers` is the port; this is the implementation a CLI wants.
 * Every `git+` verb is its own process, so the in-process memos in `Redaction`
 * never get a second reader, and `context for --session` — which runs once per
 * model invocation — redid the whole walk each time. docs/telemetry.md §13.1
 * has what that cost and how it grows.
 *
 * A file beside the repository rather than a ref, and that is the whole reason
 * this is a separate module: a ref would have to be hidden from the
 * advertisement, cleaned up by `hub disable`, and kept out of `gc`'s
 * reachability, and none of those questions have anything to do with the
 * answer. Git keeps its own derived state — the commit-graph, the index — as
 * files in the git directory for the same reason, and treats every one of them
 * as disposable. So does this: delete it and the next run recomputes.
 *
 * Node-only, by the `.node.ts` convention, because it reads and writes files.
 *
 * ## Failing closed
 *
 * `read` answers `null` for anything it is not certain of — no file, an
 * unreadable one, one that does not parse, one whose key is not this key. The
 * cost of `null` is one walk; the cost of a wrong "nothing was removed" is a
 * render whose bytes an operator removed being retained again under the same
 * oid, where the redacted record's surviving tree entry resolves them, and no
 * notice raised because a retained render is the ordinary case.
 *
 * Nothing here validates beyond the key, and nothing needs to: `Redaction`
 * builds it from the storage identity, the genesis, both namespace ceilings
 * and every record ref's head, which is everything the answer depends on.
 */
import * as fs from "node:fs";
import * as path from "node:path";

import { Effect, Layer, Schema } from "effect";

import { isFingerprint } from "../crypto/SshSignature.ts";
import { isOid } from "../git/Store.ts";
import { Answers, type Mark } from "./Redaction.ts";

/** Where the answer sits, under the directory Git keeps its own state in. */
const FILE = "redaction.json";
const DIRECTORY = "gitplus";

/**
 * The file's own shape, parsed at the boundary rather than asserted past it.
 *
 * A schema rather than a hand-written check, because this is exactly an I/O
 * boundary: the bytes came off a disk that anything could have written to.
 */
/**
 * One ref's tombstones, as they sit on disk.
 *
 * `bound` and `signers` are kept beside the target because whether a tombstone
 * *counts* is decided fresh on every read — that part depends on the trust log
 * and is deliberately not kept — but who signed it and whether it names this
 * ref are settled by the walk that found it.
 */
const Marks = Schema.Array(
  Schema.Struct({
    target: Schema.String,
    bound: Schema.Boolean,
    signers: Schema.Array(Schema.String),
  }),
);

/**
 * A decoded entry turned into marks, or nothing.
 *
 * The schema says these are strings; `isOid` and `isFingerprint` say what kind.
 * Checked rather than asserted, so a file that has been edited, truncated or
 * written by something else reads as absent — the refusal every other
 * malformation here gets — instead of putting a value the rest of the module
 * believes is an object id into `gc`'s reach.
 */
const asMarks = (kept: typeof Marks.Type): ReadonlyArray<Mark> | null => {
  const marks: Array<Mark> = [];
  for (const entry of kept) {
    if (!isOid(entry.target)) return null;
    if (!entry.signers.every(isFingerprint)) return null;
    marks.push({
      target: entry.target,
      bound: entry.bound,
      signers: entry.signers.filter(isFingerprint),
    });
  }
  return marks;
};

/**
 * One entry per key, which is one per ref.
 *
 * A map, not a single entry — and getting that wrong is the second half of the
 * same mistake as keying the answer repository-wide. The port is asked per
 * ref, so a file holding one answer has each ref's write clobber the last, and
 * a repository with any history caches one ref out of however many it has.
 */
const Kept = Schema.fromJsonString(Schema.Struct({ entries: Schema.Record(Schema.String, Marks) }));
const decodeKept = Schema.decodeUnknownEffect(Kept);

/**
 * How many refs' answers to keep.
 *
 * Far above the record refs of any one repository, and this file belongs to
 * one repository. Entries whose key no longer matches are dead rather than
 * wrong — nothing can read them, because the key names the head they were made
 * from — so the bound is about the file not growing without limit, not about
 * correctness.
 */
const ENTRIES = 4096;

/**
 * The answers kept beside one repository, one per ref.
 */
/** What is on disk, or nothing at all — every failure reads as nothing. */
const held = Effect.fnUntraced(function* (gitDir: string) {
  const raw = yield* Effect.try({
    try: () => fs.readFileSync(path.join(gitDir, DIRECTORY, FILE), "utf8"),
    catch: () => null,
  }).pipe(Effect.orElseSucceed(() => null));
  if (raw === null) return null;

  // The schema parses the string itself, so no `unknown` is ever handled here:
  // the bytes go in and a domain value comes out, or nothing does.
  return yield* decodeKept(raw).pipe(Effect.orElseSucceed(() => null));
});

export const beside = (gitDir: string): Layer.Layer<Answers> =>
  Layer.succeed(
    Answers,
    Answers.of({
      read: (key) =>
        Effect.gen(function* () {
          const kept = yield* held(gitDir);
          const marks = kept?.entries[key];
          return marks === undefined ? null : asMarks(marks);
        }),

      write: (key, found) =>
        Effect.gen(function* () {
          const kept = yield* held(gitDir);
          const entries = { ...kept?.entries, [key]: found };

          // Oldest first, which for a string-keyed object is insertion order.
          // Two processes writing at once can lose one of their entries, since
          // this reads and writes rather than merging under a lock — and a lost
          // entry is a recompute, which is the same answer more slowly.
          for (const stale of Object.keys(entries).slice(0, -ENTRIES)) delete entries[stale];

          yield* Effect.sync(() => {
            try {
              const directory = path.join(gitDir, DIRECTORY);
              fs.mkdirSync(directory, { recursive: true });
              // Written whole and renamed into place, so a reader never sees a
              // half-written file.
              const staging = path.join(directory, `${FILE}.${process.pid}`);
              fs.writeFileSync(staging, JSON.stringify({ entries }));
              fs.renameSync(staging, path.join(directory, FILE));
            } catch {
              // A repository this process cannot write to is one that
              // recomputes, which is the same answer, slower. Never a failure:
              // `context for` has real work to do and this is an optimisation.
            }
          });
        }),
    }),
  );
