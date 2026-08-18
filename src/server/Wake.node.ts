/**
 * Running something when hub state moves.
 *
 * An agent fleet is event-driven: an agent should start work when a review
 * lands on its pull request, not when somebody remembers to start it. This
 * design needs no message bus for that, because every event it has is a ref
 * update — a review is a fast-forward of `refs/hub/pr/<id>` — so the question
 * "what happened since I last looked" is a walk, and the answer is already in
 * the repository.
 *
 * Pull rather than push, deliberately. A post-receive hook drives this, and so
 * can a timer, and so can a person at a terminal: each run processes
 * everything between its bookmark and each ref's tip and then advances, which
 * makes a missed hook a late wake rather than a lost one. The notification is
 * a hint; the refs are the truth.
 *
 * Node-only, by the `.node.ts` convention: it spawns processes and reads files
 * beside the repository, neither of which a Worker has.
 */
import { spawn } from "node:child_process";
import * as fs from "node:fs";
import * as path from "node:path";

import { Console, Effect, Layer, Schema } from "effect";

import * as Dag from "../git/Dag.ts";
import { Invalid } from "../git/Error.ts";
import { stores } from "../git/Node.ts";
import * as GitRepository from "../git/Repository.ts";
import { Hooks, Repository } from "../git/Repository.ts";
import { isOid, type Oid } from "../git/Store.ts";
import * as Event from "../hub/Event.ts";
import * as Record from "../trust/Record.ts";

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

/** The one namespace a wake walks; see `dispatch`. */
const HUB = "refs/hub/";

const RULES_FILE = "wake.json";
const CURSOR_FILE = "wake.cursor.json";

/** A first run, and what an unreadable bookmark is read as. */
const NO_CURSORS: Record<string, string> = {};

/**
 * How long one woken command may take before it is killed.
 *
 * Generous, because what these start is an agent: a review answered properly
 * takes minutes, and a bound that cut that short would be worse than none. It
 * exists for the command that will *never* finish, not the slow one.
 */
const TIMEOUT = 30 * 60 * 1000;

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
      // Hub refs are the only ones this walks, so a rule for anything else is
      // one that can never fire. Refused rather than accepted and ignored: a
      // typo like `refs/hub/prs/*` otherwise reported "0 rule(s) would run"
      // and exited 0, which reads exactly like a repository with nothing to
      // do — the one answer that must not be silently wrong.
      if (!matchesRef(rule.ref, HUB) && !rule.ref.startsWith(HUB)) {
        return yield* new Invalid({
          field: "wake",
          reason: `the rule for ${rule.ref} watches a ref this never walks; patterns start with ${HUB}`,
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
          // Output is inherited so an operator sees what a rule said, but
          // *input* is not: a woken command inheriting a server's stdin can
          // read from it and block there for good, and a pass that never
          // finishes leaves this repository's wake switched off for as long as
          // the process lives.
          stdio: ["ignore", "inherit", "inherit"],
          shell: false,
        });

        // The same hazard from the other side. A rule that hangs on a socket
        // or a prompt is indistinguishable from one still working, so it is
        // given a bound rather than trusted: killed, reported as a failure,
        // and — because a failure holds the bookmark — tried again next pass.
        const bound = setTimeout(() => {
          child.kill("SIGKILL");
        }, TIMEOUT);
        const settle = (outcome: { readonly ok: boolean; readonly code: number }) => {
          clearTimeout(bound);
          resolve(outcome);
        };

        // A command that cannot start is a rule that did not fire, which is
        // the same outcome as one that failed: reported, and the bookmark
        // stays where it was so the next run tries again.
        child.on("error", () => settle({ ok: false, code: -1 }));
        child.on("close", (code) => settle({ ok: code === 0, code: code ?? -1 }));
      }),
  );

/** Every hub event between a ref's cursor and its tip, oldest first. */
const since = Effect.fn("wake.since")(function* (ref: string, tip: Oid, cursor: Oid | null) {
  // The ceiling this repository is held to, which an operator may have raised
  // or lowered — reading the constant behind it meant a wake walked further
  // than the fold that judges the same refs.
  const parents = yield* Dag.reachable(tip, cursor, Event.isHubCommit, yield* Event.ceilingOf());
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

  return { ref, found, unreadable, failed: false };
});

/** What one run did, for a caller that reports or fails on it. */
export interface Summary {
  readonly fired: number;
  readonly failed: number;
}

export const RULES = RULES_FILE;

/**
 * One pass: from each watched ref's bookmark to its tip, and no further.
 *
 * Every rule for one event before the next event, and a ref's bookmark moves
 * only if all of them worked. A rule that failed is one an operator has to see
 * run again — and a woken command re-reads the refs anyway, so arriving twice
 * costs a wasted start, while never arriving costs the work.
 */
export const dispatch = Effect.fn("Wake.dispatch")(function* (input: {
  readonly directory: string;
  readonly repo: string;
  readonly dryRun?: boolean;
}) {
  const dry = input.dryRun === true;
  const rules = yield* rulesOf(path.join(input.directory, RULES_FILE));
  if (rules.length === 0) return { fired: 0, failed: 0 } satisfies Summary;

  const repository = yield* Repository;
  const cursors = yield* cursorsOf(input.directory);
  const advanced: Record<string, string> = { ...cursors };
  let fired = 0;
  let failed = 0;

  const heads = (yield* repository.refs).filter(([name]) => name.startsWith(HUB));
  const matched = new Set<string>();

  for (const [ref, tip] of heads) {
    const watching = rules.filter((rule) => matchesRef(rule.ref, ref));
    if (watching.length === 0) continue;
    for (const rule of watching) matched.add(rule.ref);

    // A bookmark that is not an object id is one no walk can stop at, so it is
    // read as no bookmark and the ref replays.
    const recorded = cursors[ref];
    const cursor = recorded !== undefined && isOid(recorded) ? recorded : null;
    if (cursor === tip) continue;

    // One ref's walk failing is one ref that does not advance, not a pass that
    // throws away what the others earned: the bookmark is written once at the
    // end, so an escaping failure discarded advances already made and those
    // refs then re-fired their rules on every wake, for good.
    const walked = yield* since(ref, tip, cursor).pipe(
      Effect.catchCause((cause) =>
        Console.error(`! ${ref}: could not be walked: ${String(cause)}`).pipe(
          Effect.as({ ref, found: [], unreadable: [], failed: true } as const),
        ),
      ),
    );
    if (walked.failed === true) {
      failed++;
      continue;
    }
    for (const commit of walked.unreadable) {
      yield* Console.error(`! ${ref}: no rule can be matched against ${commit}, unreadable`);
    }

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
          CHR33S_GIT_REPO: input.repo,
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

  // A pattern that matches nothing this repository holds — `refs/hub/prs/*`
  // for `refs/hub/pr/…` — is accepted by the rules file and then fires for
  // nothing, which reads exactly like a repository with nothing to do. Said
  // out loud instead, but only once there is something it could have matched:
  // before the first pull request, matching nothing is simply the truth.
  if (heads.length > 0) {
    for (const rule of rules) {
      if (matched.has(rule.ref)) continue;
      yield* Console.error(`! no ref matches ${rule.ref}; that rule watches nothing here`);
    }
  }

  if (!dry) {
    fs.writeFileSync(
      path.join(input.directory, CURSOR_FILE),
      `${JSON.stringify(advanced, null, 2)}\n`,
    );
  }

  return { fired, failed } satisfies Summary;
});

/**
 * Runs in flight, and the runs asked for while one was.
 *
 * Two pushes landing together would otherwise start two walks over the same
 * range, and both would spawn for the same event — the duplicate the bookmark
 * exists to prevent. Serialized per repository instead, with a single re-run
 * remembered rather than queued: a walk that has not started yet will see
 * everything that arrived before it does, so more than one pending run is more
 * than one walk over the same refs.
 */
const running = new Map<string, boolean>();
const pending = new Set<string>();

/**
 * The repository a wake reads, which is deliberately not the one that woke it.
 *
 * `hooksNoop`, so a wake that writes cannot wake itself: the hooks that fire
 * this belong to the host's repository, and handing them to the walk's own
 * repository would make one appended event a loop.
 */
const alone = (directory: string) =>
  GitRepository.layer.pipe(
    Layer.provide(GitRepository.hooksNoop),
    Layer.provide(stores(directory)),
  );

const once = (directory: string, repo: string): Effect.Effect<void> =>
  Effect.gen(function* () {
    if (running.get(directory) === true) {
      pending.add(directory);
      return;
    }
    running.set(directory, true);
    try {
      // A wake is background work on somebody else's push: a rules file that
      // will not parse, a repository that moved out from under it, a command
      // that cannot start — none of them may take down the response that
      // triggered this, so the whole pass is reported and swallowed.
      do {
        pending.delete(directory);
        yield* dispatch({ directory, repo }).pipe(
          Effect.provide(alone(directory)),
          Effect.catchCause((cause) => Console.error(`! wake ${repo}: ${String(cause)}`)),
        );
      } while (pending.has(directory));
    } finally {
      running.set(directory, false);
    }
  });

/**
 * A hook that wakes, for the set a host installs after a push.
 *
 * Forked rather than awaited: `postReceive` runs inside the push, and a push
 * must not wait on whatever a rule decides to start. It is also why this needs
 * no payload — the walk reads the refs the push just wrote.
 */
export const service = (directory: string, repo: string): Hooks["Service"] =>
  Hooks.of({
    preReceive: () => Effect.void,
    update: () => Effect.void,
    postReceive: () => Effect.forkDetach(once(directory, repo)).pipe(Effect.asVoid),
  });
