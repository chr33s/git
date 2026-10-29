import assert from "node:assert/strict";
import { it } from "@effect/vitest";
import { Effect, Layer } from "effect";
import { fingerprint, formatPublicKey, generate } from "../crypto/SshSignature.ts";
import { stores } from "../git/Memory.ts";
import { qualify } from "../git/Oid.ts";
import * as GitRepository from "../git/Repository.ts";
import { Repository } from "../git/Repository.ts";
import * as Certificate from "../trust/Certificate.ts";
import * as Genesis from "../trust/Genesis.ts";
import * as Log from "../trust/Log.ts";
import * as Session from "../hub/Session.ts";
import * as PR from "../hub/PullRequest.ts";
import * as Event from "../hub/Event.ts";
import * as Inputs from "./Inputs.ts";
import * as Outcomes from "./Outcomes.ts";

const world = GitRepository.layer.pipe(
  Layer.provide(GitRepository.hooksNoop),
  Layer.provide(stores),
);
const author = {
  name: "Agent",
  email: "agent@example.com",
  at: new Date("2026-10-28T00:00:00Z"),
  offset: 0,
};
const fixture = Effect.fn("test.outcomeFixture")(function* () {
  const repository = yield* Repository;
  const root = yield* generate("root@example.com");
  const agent = yield* generate("agent@example.com");
  const genesis = yield* Genesis.create([formatPublicKey(root.publicKey)], 1);
  yield* Genesis.writeGenesis(genesis, [yield* Genesis.signGenesis(genesis, root)]);
  for (const [key, capabilities] of [
    [root, ["hub.merge", "hub.review", "hub.create-pr"]],
    [agent, ["hub.session", "hub.create-pr"]],
  ] as const) {
    yield* Log.issue(
      yield* Certificate.grant({
        repo: genesis.repoId,
        publicKey: formatPublicKey(key.publicKey),
        capabilities,
        id: Log.newId(),
      }),
      [root],
    );
  }
  const tree = yield* repository.writeFiles({
    changes: [
      { path: "src/auth.ts", mode: "100644", content: new TextEncoder().encode("before\n") },
    ],
  });
  const base = yield* repository.commitTree({ tree, parents: [], message: "base\n", author });
  yield* repository.setRef({ name: "refs/heads/main", to: base, expected: null });
  const opened = yield* Session.open({
    repo: genesis.repoId,
    agent: { kind: "test-agent", model: "", harness: "test" },
    prompt: "fix auth",
    key: agent,
  });
  const patch = yield* repository.writeFiles({
    base: tree,
    changes: [{ path: "src/auth.ts", mode: "100644", content: new TextEncoder().encode("fix\n") }],
  });
  const original = yield* repository.commitTree({
    tree: patch,
    parents: [base],
    message: `fix auth\n\nSession: ${opened.session}\n`,
    author,
  });
  const { pr } = yield* PR.open({
    repo: genesis.repoId,
    title: "Fix auth",
    base: "refs/heads/main",
    head: original,
    key: agent,
  });
  yield* Session.produced({
    repo: genesis.repoId,
    session: opened.session,
    key: agent,
    commits: [original],
    refs: ["refs/heads/agent-work"],
    pulls: [pr],
    note: null,
  });
  return { root, agent, genesis, base, original, patch, pr, session: opened.session };
});
const capture = () =>
  Inputs.capture({
    sourceRef: "refs/heads/main",
    evaluatedAt: "2026-10-28T23:00:00Z",
    limits: { concepts: 25, sessions: 512, bytes: 1048576 },
    configuration: { version: 1 },
  });

it.effect("verifies a merged native outcome and counts repeated summaries as one example", () =>
  Effect.gen(function* () {
    const f = yield* fixture();
    const repository = yield* Repository;
    yield* repository.setRef({ name: "refs/heads/main", to: f.original, expected: f.base });
    yield* PR.merged({
      repo: f.genesis.repoId,
      pr: f.pr,
      head: f.original,
      mergeCommit: f.original,
      key: f.root,
    });
    const first = yield* Outcomes.evaluate({
      manifest: yield* capture(),
      ref: Event.refOf(f.pr),
      actors: [],
    });
    assert.equal(first.eligible, true);
    assert.equal(first.independentExamples, 1);
    assert.deepEqual(first.paths, ["src/auth.ts"]);
    assert.equal(first.sources[0]?.originalHead, qualify(f.original));
    yield* Session.produced({
      repo: f.genesis.repoId,
      session: f.session,
      key: f.agent,
      commits: [f.original],
      pulls: [f.pr],
      note: "summarized again",
    });
    const next = yield* Outcomes.evaluate({
      manifest: yield* capture(),
      ref: Event.refOf(f.pr),
      actors: [],
    });
    assert.equal(next.key, first.key);
    assert.equal(next.independentExamples, 1);
    assert.equal(next.sources.length, 2);
  }).pipe(Effect.provide(world)),
);

it.effect(
  "an accepted merge assertion without the corresponding source history teaches nothing",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      yield* PR.merged({
        repo: f.genesis.repoId,
        pr: f.pr,
        head: f.original,
        mergeCommit: f.base,
        key: f.root,
      });
      const result = yield* Outcomes.evaluate({
        manifest: yield* capture(),
        ref: Event.refOf(f.pr),
        actors: [],
      });
      assert.equal(result.disposition, "merged");
      assert.equal(result.eligible, false);
      assert.equal(result.linkage, "insufficient");
      assert.match(result.diagnostics.join(" "), /base history/u);
    }).pipe(Effect.provide(world)),
);

it.effect("a closure without explanation produces no negative preference", () =>
  Effect.gen(function* () {
    const f = yield* fixture();
    yield* PR.close({ repo: f.genesis.repoId, pr: f.pr, key: f.agent });
    const result = yield* Outcomes.evaluate({
      manifest: yield* capture(),
      ref: Event.refOf(f.pr),
      actors: [],
    });
    assert.equal(result.disposition, "closed");
    assert.equal(result.eligible, false);
    assert.equal(result.independentExamples, 0);
    assert.match(result.diagnostics.join(" "), /does not establish a negative preference/u);
  }).pipe(Effect.provide(world)),
);

it.effect("does not label a bot or an unclassified reviewing actor as human", () =>
  Effect.gen(function* () {
    const f = yield* fixture();
    yield* PR.review({
      repo: f.genesis.repoId,
      pr: f.pr,
      head: f.original,
      decision: "reject",
      body: "Use constant time comparison here.",
      key: f.root,
    });
    const manifest = yield* capture();
    const unknown = yield* Outcomes.evaluate({ manifest, ref: Event.refOf(f.pr), actors: [] });
    assert.equal(
      unknown.records.find((record) => record.type === "review.submitted")?.actorKind,
      "unknown",
    );
    const bot = yield* Outcomes.evaluate({
      manifest,
      ref: Event.refOf(f.pr),
      actors: [{ fingerprint: yield* fingerprint(f.root.publicKey), kind: "bot" }],
    });
    assert.equal(
      bot.records.find((record) => record.type === "review.submitted")?.actorKind,
      "bot",
    );
  }).pipe(Effect.provide(world)),
);

it.effect("a rewrite with insufficient original-to-accepted linkage stays inconclusive", () =>
  Effect.gen(function* () {
    const f = yield* fixture();
    const repository = yield* Repository;
    const rewritten = yield* repository.commitTree({
      tree: f.patch,
      parents: [f.base],
      message: "squashed differently\n",
      author,
    });
    yield* PR.update({ repo: f.genesis.repoId, pr: f.pr, head: rewritten, key: f.root });
    yield* repository.setRef({ name: "refs/heads/main", to: rewritten, expected: f.base });
    yield* PR.merged({
      repo: f.genesis.repoId,
      pr: f.pr,
      head: rewritten,
      mergeCommit: rewritten,
      key: f.root,
    });
    const result = yield* Outcomes.evaluate({
      manifest: yield* capture(),
      ref: Event.refOf(f.pr),
      actors: [],
    });
    assert.equal(result.eligible, false);
    assert.ok(result.contradictions.some((reason) => reason.includes("cannot be correlated")));
  }).pipe(Effect.provide(world)),
);
