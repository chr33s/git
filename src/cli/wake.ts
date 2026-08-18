/**
 * `chr33s-git wake` — run something when hub state moves.
 *
 * An agent fleet is event-driven: an agent should start work when a review
 * lands on its pull request, not when somebody remembers to start it. This
 * design needs no message bus for that, because every event it has is a ref
 * update — a review is a fast-forward of `refs/hub/pr/<id>` — so the question
 * "what happened since I last looked" is a walk, and the answer is already in
 * the repository.
 *
 * Pull rather than push, deliberately. A post-receive hook can call this, and
 * so can a timer, and so can a person: it processes everything between its
 * cursor and each ref's tip and then advances, which makes a missed hook a
 * late wake rather than a lost one. The notification is a hint; the refs are
 * the truth.
 *
 * Rules are local to this replica and are not replicated. Each host decides
 * who it wakes, exactly as each host decides what it serves — and a rule that
 * travelled with the repository would be one replica arranging to run commands
 * on another.
 */
import { spawn } from "node:child_process";
import * as fs from "node:fs";
import * as path from "node:path";

import { Console, Effect, Schema } from "effect";
import { Command, Flag } from "effect/unstable/cli";

import * as Dag from "../git/Dag.ts";
import { Invalid } from "../git/Error.ts";
import { Repository } from "../git/Repository.ts";
import { isOid, type Oid } from "../git/Store.ts";
import * as Event from "../hub/Event.ts";
import * as Record from "../trust/Record.ts";
import { repoArgument, rootFlag, withRepo } from "./shared.ts";

/** What this replica runs, and for what. */
const RuleDocument = Schema.Struct({
  /** A ref name, or a prefix ending in `*`. */
  ref: Schema.String,
  /** Event types this rule answers to; absent or `["*"]` is every one. */
  on: Schema.optional(Schema.Array(Schema.String)),
  /** argv, run without a shell. */
  run: Schema.Array(Schema.String),
});

const RulesDocument = Schema.Struct({ rules: Schema.Array(RuleDocument) });

/** Each watched ref, mapped to the last commit this replica processed. */
const CursorDocument = Schema.Record(Schema.String, Schema.String);

type Rule = (typeof RuleDocument)["Type"];

const decodeRules = Schema.decodeUnknownEffect(RulesDocument);
const decodeCursors = Schema.decodeUnknownEffect(CursorDocument);

const RULES_FILE = "wake.json";
const CURSOR_FILE = "wake.cursor.json";

/** A first run, and what an unreadable bookmark is read as. */
const NO_CURSORS: Record<string, string> = {};

/**
 * Where a replica keeps its bookmark.
 *
 * A file beside the repository rather than a ref inside it. A ref would
 * replicate — turning one replica's progress into everybody's — and would have
 * to answer to the append-only rules and the advertisement besides. None of
 * that is what a bookmark is: it is local, it moves backwards when an operator
 * wants a replay, and losing it costs a re-run rather than a fact.
 *
 * An unreadable or unparseable bookmark is read as no bookmark, which replays
 * rather than skips. This is the one place that trade is obviously right: a
 * woken command re-reads the refs anyway, so a replay costs a wasted start
 * while a skip costs the work.
 */
const cursorsOf = (directory: string): Effect.Effect<Record<string, string>> =>
  Effect.gen(function* () {
    const contents = yield* Effect.try(() =>
      fs.readFileSync(path.join(directory, CURSOR_FILE), "utf8"),
    ).pipe(Effect.orElseSucceed(() => null));
    if (contents === null) return {};

    const parsed = yield* Effect.try(() => JSON.parse(contents)).pipe(
      Effect.flatMap(decodeCursors),
      Effect.orElseSucceed(() => NO_CURSORS),
    );
    return { ...parsed };
  });

const rulesOf = (location: string): Effect.Effect<ReadonlyArray<Rule>, Invalid> =>
  Effect.gen(function* () {
    const contents = yield* Effect.try({
      try: () => fs.readFileSync(location, "utf8"),
      catch: () => new Invalid({ field: "wake", reason: `cannot read ${location}` }),
    });
    const json = yield* Effect.try({
      try: () => JSON.parse(contents),
      catch: () => new Invalid({ field: "wake", reason: `${location} is not valid JSON` }),
    });

    // Refused rather than skipped, the way a policy document that will not
    // parse is: rules nobody can read are rules nobody is running, and the
    // operator who wrote them believes otherwise.
    const document = yield* decodeRules(json).pipe(
      Effect.mapError(
        (issue) =>
          new Invalid({ field: "wake", reason: `malformed ${RULES_FILE}: ${issue.message}` }),
      ),
    );

    for (const rule of document.rules) {
      if (rule.run.length === 0) {
        return yield* new Invalid({
          field: "wake",
          reason: `the rule for ${rule.ref} has nothing to run`,
        });
      }
    }
    return document.rules;
  });

/** The same prefix rule the ref namespaces use elsewhere: a name, or `…*`. */
const matchesRef = (pattern: string, ref: string): boolean =>
  pattern.endsWith("*") ? ref.startsWith(pattern.slice(0, -1)) : ref === pattern;

const matchesEvent = (rule: Rule, type: string): boolean => {
  const on = rule.on ?? [];
  return on.length === 0 || on.includes("*") || on.includes(type);
};

/**
 * One woken command.
 *
 * Event fields reach it as environment variables and never as arguments. What
 * a hub event says is chosen by whoever may append to the ref — the lowest hub
 * capability there is — so an event that could reach a shell would make
 * `hub.comment` a way to run commands on every replica that watches. No shell,
 * and nothing interpolated: the payload names what happened, and the command
 * decides what to do about it.
 */
const run = (
  rule: Rule,
  environment: Record<string, string>,
): Effect.Effect<{ readonly ok: boolean; readonly code: number }> =>
  Effect.promise(
    () =>
      new Promise((resolve) => {
        const child = spawn(rule.run[0]!, rule.run.slice(1), {
          env: { ...process.env, ...environment },
          stdio: "inherit",
          shell: false,
        });
        // A command that cannot start is a rule that did not fire, which is
        // the same outcome as one that failed: reported, and the cursor stays
        // where it was so the next run tries again.
        child.on("error", () => resolve({ ok: false, code: -1 }));
        child.on("close", (code) => resolve({ ok: code === 0, code: code ?? -1 }));
      }),
  );

/** Every hub event between a ref's cursor and its tip, oldest first. */
const since = Effect.fn("wake.since")(function* (ref: string, tip: Oid, cursor: Oid | null) {
  const parents = yield* Dag.reachable(tip, cursor, Event.isHubCommit, Event.MAX_EVENTS);
  const found: Array<{ readonly commit: Oid; readonly type: string }> = [];
  const unreadable: Array<Oid> = [];

  for (const commit of Dag.topological(parents)) {
    // Joins carry nothing — they are how two histories became one — and a
    // redacted payload is gone by design. Neither is an event to wake for, and
    // neither is a failure.
    if (!(yield* Record.carries(commit, Event.RECORD))) continue;
    const record = yield* Record.read(commit, Event.RECORD).pipe(
      Effect.catchTags({
        ObjectNotFound: () => Effect.succeed(null),
        Invalid: () => Effect.succeed(null),
      }),
    );
    if (record === null) continue;

    // One event this version cannot read must not stop the walk — the rest of
    // the history is still worth waking for — but it is reported rather than
    // skipped in silence. A rule that never fires looks exactly like a rule
    // with nothing to do, and the operator who wrote it believes it works.
    const payload = yield* Event.decode(record.payload).pipe(Effect.orElseSucceed(() => null));
    if (payload === null) {
      unreadable.push(commit);
      continue;
    }
    found.push({ commit, type: payload.type });
  }

  return { ref, found, unreadable };
});

const wake = Command.make(
  "wake",
  {
    root: rootFlag,
    dry: Flag.boolean("dry-run").pipe(
      Flag.withDescription("Say what would run, run nothing, and leave the cursor alone"),
    ),
    repo: repoArgument,
  },
  ({ dry, repo, root }) =>
    Effect.gen(function* () {
      const directory = path.join(root, repo);
      const rules = yield* rulesOf(path.join(directory, RULES_FILE));
      if (rules.length === 0) {
        return yield* Console.log(`no rules in ${path.join(directory, RULES_FILE)}; nothing to do`);
      }

      const cursors = yield* cursorsOf(directory);
      const advanced: Record<string, string> = { ...cursors };
      let fired = 0;
      let failed = 0;

      const heads = yield* withRepo(
        root,
        repo,
        Effect.gen(function* () {
          const repository = yield* Repository;
          const refs = yield* repository.refs;
          return refs.filter(([name]) => name.startsWith("refs/hub/"));
        }),
      );

      for (const [ref, tip] of heads) {
        const watching = rules.filter((rule) => matchesRef(rule.ref, ref));
        if (watching.length === 0) continue;

        // A bookmark that is not an object id is one no walk can stop at, so
        // it is read as no bookmark and the ref replays.
        const recorded = cursors[ref];
        const cursor = recorded !== undefined && isOid(recorded) ? recorded : null;
        if (cursor === tip) continue;

        const walked = yield* withRepo(root, repo, since(ref, tip, cursor));
        for (const commit of walked.unreadable) {
          yield* Console.error(`! ${ref}: no rule can be matched against ${commit}, unreadable`);
        }

        // Every rule for one event before the next event, and the cursor moves
        // only if all of them worked. A rule that failed is one an operator
        // has to see run again — and a woken command re-reads the refs anyway,
        // so arriving twice costs a wasted start, while never arriving costs
        // the work.
        let clean = true;
        for (const event of walked.found) {
          for (const rule of watching) {
            if (!matchesEvent(rule, event.type)) continue;
            fired++;
            if (dry) {
              yield* Console.log(`would run ${rule.run.join(" ")} for ${event.type} on ${ref}`);
              continue;
            }
            const outcome = yield* run(rule, {
              CHR33S_GIT_REPO: repo,
              CHR33S_GIT_REF: ref,
              CHR33S_GIT_EVENT: event.type,
              CHR33S_GIT_COMMIT: event.commit,
            });
            if (!outcome.ok) {
              failed++;
              clean = false;
              yield* Console.error(
                `! ${rule.run[0]} exited ${outcome.code} for ${event.type} on ${ref}`,
              );
            }
          }
        }

        if (clean && !dry) advanced[ref] = tip;
      }

      if (!dry) {
        fs.writeFileSync(
          path.join(directory, CURSOR_FILE),
          `${JSON.stringify(advanced, null, 2)}\n`,
        );
      }

      yield* Console.log(
        dry ? `${fired} rule(s) would run` : `${fired} rule(s) run, ${failed} failed`,
      );
      // A non-zero exit, so whatever called this — a hook, a timer, a person —
      // learns that something did not run without reading the output.
      if (failed > 0) {
        return yield* new Invalid({ field: "wake", reason: `${failed} woken command(s) failed` });
      }
    }),
);

export const wakeCommand = wake.pipe(
  Command.withDescription("Run local rules for hub events since the last run"),
);
