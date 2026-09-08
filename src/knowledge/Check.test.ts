/**
 * What a Concept still has behind it, one dimension at a time.
 *
 * The K-series of docs/context-pack.knowledge.md §16. The cases that matter most
 * are the ones where a wrong answer would be *convenient*: a record that
 * exists but names another repository is not accepted provenance (K-05), a
 * tree this replica cannot read is `unknown` rather than a deleted file
 * (K-07), a changed dependency is a revalidation finding rather than a
 * falsified Concept (K-08), and a host limit reached midway is an explicitly
 * partial report rather than a clean empty one (K-14).
 */
import assert from "node:assert/strict";
import { describe, it } from "@effect/vitest";

import { Effect, Layer } from "effect";

import { formatPublicKey, generate, type PrivateKey } from "../crypto/SshSignature.ts";
import { type Signature } from "../git/Format.ts";
import { stores } from "../git/Memory.ts";
import { qualify } from "../git/Oid.ts";
import * as GitRepository from "../git/Repository.ts";
import { Repository } from "../git/Repository.ts";
import type { Oid } from "../git/Store.ts";
import * as Pack from "../context/Pack.ts";
import * as Session from "../hub/Session.ts";
import * as Certificate from "../trust/Certificate.ts";
import { create, signGenesis, writeGenesis } from "../trust/Genesis.ts";
import * as Log from "../trust/Log.ts";
import { project as projectTrust, type Projection } from "../trust/Projection.ts";
import * as Check from "./Check.ts";
import * as Concept from "./Concept.ts";

const author: Signature = {
  name: "Runner",
  email: "runner@example.com",
  at: new Date(1_700_000_000_000),
  offset: 0,
};

const NOW = new Date("2026-09-08T00:00:00Z");
const encode = (text: string) => new TextEncoder().encode(text);

const world = GitRepository.layer.pipe(
  Layer.provide(GitRepository.hooksNoop),
  Layer.provideMerge(stores),
);

const scenario = <A, E>(effect: Effect.Effect<A, E, Repository>): Promise<A> =>
  Effect.runPromise(effect.pipe(Effect.provide(world)));

/** A hub-enabled repository whose agent may write session records. */
const enabled = Effect.fn("test.enabled")(function* () {
  const root = yield* generate("root@example.com");
  const agent = yield* generate("agent@example.com");
  const genesis = yield* create([formatPublicKey(root.publicKey)], 1);
  yield* writeGenesis(genesis, [yield* signGenesis(genesis, root)]);
  yield* Log.issue(
    yield* Certificate.grant({
      repo: genesis.repoId,
      publicKey: formatPublicKey(agent.publicKey),
      capabilities: ["repo.read", "hub.session"],
      id: Log.newId(),
    }),
    [root],
  );
  return { genesis, root, agent } as const;
});

/** One commit holding exactly these files, and the view it names. */
const viewOf = Effect.fn("test.viewOf")(function* (files: Record<string, string>) {
  const repository = yield* Repository;
  const entries: Array<{ path: string; oid: Oid; mode: string }> = [];
  for (const [path, contents] of Object.entries(files)) {
    entries.push({ path, oid: yield* repository.writeBlob(encode(contents)), mode: "100644" });
  }
  const tree = yield* repository.writePaths(entries);
  const commit = yield* repository.commitTree({ tree, parents: [], message: "files\n", author });
  return yield* Pack.committed(commit);
});

/** A signed `session.produced` record whose commit oid a Concept can cite. */
const learning = Effect.fn("test.learning")(function* (
  repo: string,
  key: PrivateKey,
  note: string,
) {
  const session = Session.newId();
  yield* Session.open({
    repo,
    session,
    agent: { kind: "test", model: "", harness: "" },
    prompt: "do the thing",
    key,
  });
  const commit = yield* Session.produced({ repo, session, key, note });
  return { session, commit } as const;
});

const conceptFor = (input: {
  readonly cites?: ReadonlyArray<string>;
  readonly evidence?: string;
  readonly status?: string;
  readonly staleAfter?: string;
  readonly extra?: string;
}) =>
  [
    "---",
    "type: Gotcha",
    "title: Worker auth needs the production fixture",
    "description: Use the production policy fixture when changing worker authorization.",
    ...(input.status === undefined ? [] : [`status: ${input.status}`]),
    ...(input.staleAfter === undefined ? [] : [`stale_after: "${input.staleAfter}"`]),
    ...(input.extra === undefined ? [] : [input.extra]),
    "gitplus:",
    ...(input.cites === undefined
      ? []
      : ["  cites:", ...input.cites.map((record) => `    - record: ${record}`)]),
    ...(input.evidence === undefined ? [] : ["  evidence:", input.evidence]),
    "---",
    "The discovery session reported a dependency on this fixture.",
    "",
  ].join("\n");

const check = (input: {
  readonly view: Pack.View;
  readonly repo?: string | null;
  readonly trust?: Projection | null;
  readonly concept?: string;
  readonly at?: Date;
  readonly limits?: Check.Limits;
}) =>
  Check.check({
    view: input.view,
    evaluationTime: input.at ?? NOW,
    repo: input.repo ?? null,
    trust: input.trust ?? null,
    concept: input.concept,
    limits: input.limits,
  });

describe("knowledge check", () => {
  it.effect("§6.5: a repository with no bundle is a successful no-op", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const view = yield* viewOf({ "src/a.ts": "export const a = 1\n" });
          const report = yield* check({ view });
          assert.equal(report.bundleAbsent, true);
          assert.equal(report.complete, true);
          assert.deepEqual(report.concepts, []);
          // No manufactured knowledge, and no invented gate failure either.
          assert.equal(Check.gate(report, true).ok, true);
        }),
      ),
    ),
  );

  it.effect("naming a Concept that does not exist is an error", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const view = yield* viewOf({ ".gitplus/knowledge/index.md": "# index\n" });
          const report = yield* check({ view, concept: "gotchas/nope" });
          assert.equal(report.complete, false);
          assert.equal(report.diagnostics[0]?.code, "knowledge.absent");
          assert.equal(Check.gate(report, false).ok, false);
        }),
      ),
    ),
  );

  it.effect("accepts a citation to a signed session record on its own ref", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const { agent, genesis } = yield* enabled();
          const learned = yield* learning(genesis.repoId, agent, "gotcha: use the fixture");
          const trust = yield* projectTrust(genesis);

          const view = yield* viewOf({
            "tests/worker/auth.test.ts": "test('auth', () => {})\n",
            ".gitplus/knowledge/gotchas/worker-auth.md": conceptFor({
              cites: [qualify(learned.commit)],
            }),
          });
          const report = yield* check({ view, repo: genesis.repoId, trust });
          const [concept] = report.concepts;
          assert.equal(concept?.structure.state, "valid");
          assert.equal(concept?.citations[0]?.state, "accepted");
          assert.equal(concept?.citations[0]?.session, learned.session);
          assert.equal(concept?.eligibility.recall, true);
          assert.equal(Check.gate(report, true).ok, true);
        }),
      ),
    ),
  );

  it.effect("K-05: a record naming another repository is not accepted", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const { agent, genesis } = yield* enabled();
          // Signed by an authorized key, bound to a repository that is not
          // this one: a valid signature is not a valid citation.
          const learned = yield* learning("some-other-repository", agent, "gotcha: elsewhere");
          const trust = yield* projectTrust(genesis);

          const view = yield* viewOf({
            ".gitplus/knowledge/gotchas/worker-auth.md": conceptFor({
              cites: [qualify(learned.commit)],
            }),
          });
          const report = yield* check({ view, repo: genesis.repoId, trust });
          const citation = report.concepts[0]?.citations[0];
          assert.equal(citation?.state, "invalid");
          assert.match(citation?.reason ?? "", /names repository some-other-repository/u);
          // §7: an invalid declared citation excludes it from automatic recall.
          assert.equal(report.concepts[0]?.eligibility.recall, false);
          assert.equal(Check.gate(report, false).ok, false);
        }),
      ),
    ),
  );

  it.effect("K-07: a citation this replica does not hold is unavailable", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const { genesis } = yield* enabled();
          const trust = yield* projectTrust(genesis);
          const view = yield* viewOf({
            ".gitplus/knowledge/gotchas/worker-auth.md": conceptFor({
              cites: ["sha1:1111111111111111111111111111111111111111"],
            }),
          });
          const report = yield* check({ view, repo: genesis.repoId, trust });
          const citation = report.concepts[0]?.citations[0];
          // Not "invalid": nothing about a signature was established here.
          assert.equal(citation?.state, "unavailable");
          assert.equal(report.concepts[0]?.eligibility.recall, false);
          // Unavailable provenance is a warning, not a default-gate failure;
          // it is the *excluded from recall* half that matters.
          assert.equal(Check.gate(report, false).ok, true);
          // But not a passed gate under `--strict`: nothing was verified.
          assert.equal(Check.gate(report, true).ok, false);
        }),
      ),
    ),
  );

  it.effect("K-06: a citation with no trust state to judge it is unavailable", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const { agent, genesis } = yield* enabled();
          const learned = yield* learning(genesis.repoId, agent, "gotcha: use the fixture");
          const view = yield* viewOf({
            ".gitplus/knowledge/gotchas/worker-auth.md": conceptFor({
              cites: [qualify(learned.commit)],
            }),
          });
          // Same bytes, no membership supplied: the payload is readable and
          // acceptance is still not established.
          const report = yield* check({ view, repo: genesis.repoId, trust: null });
          assert.equal(report.concepts[0]?.citations[0]?.state, "unavailable");
          assert.equal(
            report.diagnostics.some((entry) => entry.code === "knowledge.provenance.unjudged"),
            true,
          );
        }),
      ),
    ),
  );

  it.effect("K-08: blob evidence is unchanged, changed or missing, independently", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const recorded = yield* repository.writeBlob(encode("test('auth', () => {})\n"));
          const evidence = [
            "    - kind: blob",
            "      path: tests/worker/auth.test.ts",
            `      blob: ${qualify(recorded)}`,
          ].join("\n");

          const unchanged = yield* viewOf({
            "tests/worker/auth.test.ts": "test('auth', () => {})\n",
            ".gitplus/knowledge/gotchas/worker-auth.md": conceptFor({ evidence }),
          });
          assert.equal(
            (yield* check({ view: unchanged })).concepts[0]?.repositoryEvidence[0]?.state,
            "unchanged",
          );

          const changed = yield* viewOf({
            "tests/worker/auth.test.ts": "test('auth', () => { rewritten() })\n",
            ".gitplus/knowledge/gotchas/worker-auth.md": conceptFor({ evidence }),
          });
          const drifted = (yield* check({ view: changed })).concepts[0]?.repositoryEvidence[0];
          assert.equal(drifted?.state, "changed");
          // The recorded oid is reported beside what is there now; neither
          // replaces the other (§9.2).
          assert.equal(drifted?.recorded, qualify(recorded));
          assert.notEqual(drifted?.found, qualify(recorded));

          const gone = yield* viewOf({
            ".gitplus/knowledge/gotchas/worker-auth.md": conceptFor({ evidence }),
          });
          const report = yield* check({ view: gone });
          assert.equal(report.concepts[0]?.repositoryEvidence[0]?.state, "missing");
          // A changed or missing dependency is a revalidation finding: the
          // default gate passes and `--strict` does not.
          assert.equal(Check.gate(report, false).ok, true);
          assert.equal(Check.gate(report, true).ok, false);
        }),
      ),
    ),
  );

  it.effect("K-08: a path that became another item kind is missing, not changed", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const recorded = yield* repository.writeBlob(encode("x\n"));
          const view = yield* viewOf({
            "vendor/policy-engine/readme.md": "a directory now\n",
            ".gitplus/knowledge/gotchas/worker-auth.md": conceptFor({
              evidence: [
                "    - kind: blob",
                "      path: vendor/policy-engine",
                `      blob: ${qualify(recorded)}`,
              ].join("\n"),
            }),
          });
          const report = yield* check({ view });
          assert.equal(report.concepts[0]?.repositoryEvidence[0]?.state, "missing");
        }),
      ),
    ),
  );

  it.effect("K-04: an expired deadline excludes recall; an invalid one is an error", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const stale = yield* viewOf({
            ".gitplus/knowledge/gotchas/worker-auth.md": conceptFor({
              staleAfter: "2026-01-01T00:00:00Z",
            }),
          });
          const expired = yield* check({ view: stale });
          assert.equal(expired.concepts[0]?.temporal.state, "stale");
          assert.equal(expired.concepts[0]?.eligibility.recall, false);
          assert.equal(Check.gate(expired, false).ok, true);
          assert.equal(Check.gate(expired, true).ok, false);

          const dateOnly = yield* viewOf({
            ".gitplus/knowledge/gotchas/worker-auth.md": conceptFor({ staleAfter: "2026-12-31" }),
          });
          const report = yield* check({ view: dateOnly });
          // Not "fresh until some midnight": an unknown temporal assessment.
          assert.equal(report.concepts[0]?.temporal.state, "invalid");
          assert.equal(Check.gate(report, false).ok, false);
        }),
      ),
    ),
  );

  it.effect("§7: no declared deadline is `no-deadline`, and does not disqualify", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const view = yield* viewOf({
            ".gitplus/knowledge/gotchas/worker-auth.md": conceptFor({}),
          });
          const report = yield* check({ view });
          assert.equal(report.concepts[0]?.temporal.state, "no-deadline");
          assert.equal(report.concepts[0]?.eligibility.recall, true);
        }),
      ),
    ),
  );

  it.effect("§7: draft and deprecated Concepts are excluded from automatic recall", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          for (const status of ["draft", "deprecated"]) {
            const view = yield* viewOf({
              ".gitplus/knowledge/gotchas/worker-auth.md": conceptFor({ status }),
            });
            const report = yield* check({ view });
            assert.equal(report.concepts[0]?.eligibility.recall, false, status);
            assert.deepEqual(report.concepts[0]?.eligibility.reasons, [`lifecycle-${status}`]);
            // Excluded from recall, and still not a gate failure: it is a
            // lifecycle state, not a syntax error.
            assert.equal(Check.gate(report, true).ok, true, status);
          }
        }),
      ),
    ),
  );

  it.effect("K-11: portable `verified` grants nothing and is reported as editorial", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const view = yield* viewOf({
            ".gitplus/knowledge/gotchas/worker-auth.md": conceptFor({
              extra: 'verified:\n  - by: human:root\n    at: "2026-08-22T11:00:00Z"',
            }),
          });
          const report = yield* check({ view });
          assert.equal(
            report.concepts[0]?.diagnostics.some(
              (entry) => entry.code === "knowledge.verified.editorial",
            ),
            true,
          );
          assert.deepEqual(report.concepts[0]?.verificationRecords, []);
        }),
      ),
    ),
  );

  it.effect("K-12: a snapshot digest is not a retention path", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const view = yield* viewOf({
            ".gitplus/knowledge/gotchas/worker-auth.md": [
              "---",
              "type: Gotcha",
              "gitplus:",
              "  external:",
              "    vendor-contract:",
              '      retrieved_at: "2026-08-22T09:10:00Z"',
              `      content_digest: sha256:${"a".repeat(64)}`,
              `      snapshot: sha256:${"b".repeat(64)}`,
              "---",
              "Prose.",
              "",
            ].join("\n"),
          });
          const report = yield* check({ view });
          const external = report.concepts[0]?.external[0];
          // Offline: the live state is `unknown`, never "revalidated".
          assert.equal(external?.state, "unknown");
          assert.equal(external?.retention, "not-retained");
        }),
      ),
    ),
  );

  it.effect("K-14: a host limit reached midway is an explicitly partial report", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const files: Record<string, string> = {};
          for (let at = 0; at < 5; at += 1) {
            files[`.gitplus/knowledge/gotchas/c${at}.md`] = conceptFor({});
          }
          const view = yield* viewOf(files);
          const report = yield* check({ view, limits: { concepts: 2 } });
          assert.equal(report.concepts.length, 2);
          assert.equal(report.complete, false);
          assert.equal(report.completeness, "limited");
          // Not a clean empty answer, and not a clean *full* one either.
          assert.equal(Check.gate(report, false).ok, false);
        }),
      ),
    ),
  );

  it.effect("§14: truncated dependency checking is stated and disqualifies", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const recorded = qualify(yield* repository.writeBlob(encode("x\n")));
          const evidence = Array.from({ length: 4 }, (_, at) =>
            ["    - kind: blob", `      path: a${at}.ts`, `      blob: ${recorded}`].join("\n"),
          ).join("\n");
          const view = yield* viewOf({
            "a0.ts": "x\n",
            "a1.ts": "x\n",
            "a2.ts": "x\n",
            "a3.ts": "x\n",
            ".gitplus/knowledge/gotchas/worker-auth.md": conceptFor({ evidence }),
          });

          const report = yield* check({ view, limits: { evidence: 2 } });
          const concept = report.concepts[0];
          assert.equal(concept?.repositoryEvidence.length, 2);
          // Not eligible on support nothing evaluated.
          assert.equal(concept?.eligibility.recall, false);
          assert.equal(concept?.eligibility.reasons.includes("evidence-unevaluated"), true);
          assert.equal(
            concept?.diagnostics.some((entry) => entry.code === "knowledge.limit.evidence"),
            true,
          );
        }),
      ),
    ),
  );

  it.effect("K-02: navigation documents are not checked as Concepts", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const view = yield* viewOf({
            ".gitplus/knowledge/index.md": "# corpus\n",
            ".gitplus/knowledge/gotchas/log.md": "# history\n",
            ".gitplus/knowledge/gotchas/worker-auth.md": conceptFor({}),
          });
          const report = yield* check({ view });
          assert.deepEqual(
            report.concepts.map((concept) => concept.id),
            ["gotchas/worker-auth"],
          );
        }),
      ),
    ),
  );

  it.effect("K-03: a malformed Concept is a diagnostic, not a failed bundle read", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const view = yield* viewOf({
            ".gitplus/knowledge/gotchas/bad.md": "---\ntype: A\ntype: B\n---\n",
            ".gitplus/knowledge/gotchas/worker-auth.md": conceptFor({}),
          });
          const report = yield* check({ view });
          assert.equal(report.concepts.length, 2);
          const bad = report.concepts.find((concept) => concept.path.endsWith("bad.md"));
          assert.equal(bad?.structure.state, "invalid");
          assert.equal(bad?.eligibility.recall, false);
          // The good one is still read: one bad document costs one document.
          const good = report.concepts.find((concept) => concept.id === "gotchas/worker-auth");
          assert.equal(good?.structure.state, "valid");
          assert.equal(Check.gate(report, false).ok, false);
        }),
      ),
    ),
  );

  it.effect("the report identifies the view, profile and evaluation it describes", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const view = yield* viewOf({
            ".gitplus/knowledge/gotchas/worker-auth.md": conceptFor({}),
          });
          const report = yield* check({ view });
          assert.equal(report.view.tree, view.tree);
          assert.equal(report.profile.okfRevision, Concept.OKF_REVISION);
          assert.equal(report.profile.checkerVersion, Check.CHECKER_VERSION);
          assert.equal(report.evaluatedAt, NOW.toISOString());
          assert.match(report.inputStamp, /view=sha1:[0-9a-f]{40}/u);
        }),
      ),
    ),
  );
});
