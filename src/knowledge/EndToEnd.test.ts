/**
 * The release fixture: two sessions, a learning, and an auditable recall.
 *
 * docs/context-pack.knowledge.md §16.1, with a fake invocation sink in place of a
 * provider — no paid call is needed to prove the boundary, only that the exact
 * bytes handed over are the bytes the exposure commits to.
 *
 * ```text
 * session A   records a discovery, publishes a Concept citing it
 *             startup Memory is deliberately too small to hold it
 * session B   retrieves it anyway, validates the evidence, builds a pack,
 *             hands the segments over, and the sink's own digest matches
 *             the signed exposure
 * then        the dependency changes  → not presented as current
 *             the discovery is redacted → not resurrected from the old note
 * ```
 *
 * What each step is allowed to claim is the whole point. A recorded learning
 * is not a published Concept; a published Concept is not accepted provenance;
 * accepted provenance is not selection; and selection is not exposure — only
 * the render digest is (INV-10).
 */
import assert from "node:assert/strict";
import { describe, it } from "@effect/vitest";

import { Effect, Layer } from "effect";

import { formatPublicKey, generate } from "../crypto/SshSignature.ts";
import { type Signature } from "../git/Format.ts";
import { stores } from "../git/Memory.ts";
import { qualify } from "../git/Oid.ts";
import * as GitRepository from "../git/Repository.ts";
import { Repository } from "../git/Repository.ts";
import type { Oid } from "../git/Store.ts";
import * as Exposure from "../context/Exposure.ts";
import * as Pack from "../context/Pack.ts";
import * as Render from "../context/Render.ts";
import * as Select from "../context/Select.ts";
import * as Memory from "../hub/Memory.ts";
import * as Session from "../hub/Session.ts";
import * as Records from "../telemetry/Records.ts";
import * as Invocation from "../telemetry/Invocation.ts";
import * as Certificate from "../trust/Certificate.ts";
import { create, signGenesis, writeGenesis } from "../trust/Genesis.ts";
import * as Log from "../trust/Log.ts";
import { project as projectTrust } from "../trust/Projection.ts";
import * as Check from "./Check.ts";

const author: Signature = {
  name: "Runner",
  email: "runner@example.com",
  at: new Date(1_700_000_000_000),
  offset: 0,
};

const NOW = new Date("2026-09-08T00:00:00Z");
const FIXTURE = "test('auth', () => { productionPolicy() })\n";
const encode = (text: string) => new TextEncoder().encode(text);

const world = GitRepository.layer.pipe(
  Layer.provide(GitRepository.hooksNoop),
  Layer.provideMerge(stores),
);

const scenario = <A, E>(effect: Effect.Effect<A, E, Repository>): Promise<A> =>
  Effect.runPromise(effect.pipe(Effect.provide(world)));

const enabled = Effect.fn("test.enabled")(function* () {
  const root = yield* generate("root@example.com");
  const agent = yield* generate("agent@example.com");
  const genesis = yield* create([formatPublicKey(root.publicKey)], 1);
  yield* writeGenesis(genesis, [yield* signGenesis(genesis, root)]);
  yield* Log.issue(
    yield* Certificate.grant({
      repo: genesis.repoId,
      publicKey: formatPublicKey(agent.publicKey),
      capabilities: ["repo.read", "hub.session", "hub.trace", "hub.redact"],
      id: Log.newId(),
    }),
    [root],
  );
  return { genesis, agent } as const;
});

const commitFiles = Effect.fn("test.commitFiles")(function* (files: Record<string, string>) {
  const repository = yield* Repository;
  const entries: Array<{ path: string; oid: Oid; mode: string }> = [];
  for (const [path, contents] of Object.entries(files)) {
    entries.push({ path, oid: yield* repository.writeBlob(encode(contents)), mode: "100644" });
  }
  const tree = yield* repository.writePaths(entries);
  const commit = yield* repository.commitTree({ tree, parents: [], message: "files\n", author });
  return yield* Pack.committed(commit);
});

/** The Concept session A publishes through the ordinary source path. */
const conceptFor = (input: { record: string; blob: string }) =>
  [
    "---",
    "type: Gotcha",
    "title: Worker auth tests need the production policy fixture",
    "description: The worker authorization suite depends on the production policy fixture.",
    "sources:",
    "  - id: discovery",
    `    resource: gitplus:record:${input.record}`,
    "gitplus:",
    "  cites:",
    `    - record: ${input.record}`,
    "  evidence:",
    "    - kind: blob",
    "      path: tests/worker/auth.test.ts",
    `      blob: ${input.blob}`,
    "---",
    "The discovery session reported a dependency on this fixture. This document",
    "records that observation; it does not itself prove all tests require it.",
    "",
  ].join("\n");

/**
 * A fake invocation sink.
 *
 * It does what a provider adapter does and nothing else: it takes the exact
 * ordered segments and computes their framing digest for itself. Recomputing
 * independently is the point — a sink that read the digest out of the exposure
 * would agree with it by construction.
 */
const sink = Effect.fn("test.sink")(function* (segments: ReadonlyArray<Render.Segment>) {
  const committed = yield* Render.commit(segments);
  return { digest: committed.digest, bytes: committed.bytes.length } as const;
});

describe("two-session learning and recall", () => {
  it.effect("records a discovery, recalls it later, and proves what crossed", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const { agent, genesis } = yield* enabled();
          const repo = genesis.repoId;
          const repository = yield* Repository;

          // -- session A: discover, record, publish ---------------------------
          const sessionA = Session.newId();
          yield* Session.open({
            repo,
            session: sessionA,
            agent: { kind: "test", model: "", harness: "" },
            prompt: "make the worker auth suite pass",
            key: agent,
          });
          const discovery = yield* Session.produced({
            repo,
            session: sessionA,
            key: agent,
            note: "gotcha: the worker auth suite needs the production policy fixture",
          });

          const fixture = yield* repository.writeBlob(encode(FIXTURE));
          const view = yield* commitFiles({
            "tests/worker/auth.test.ts": FIXTURE,
            "src/worker/auth.ts": "export const authorize = () => true\n",
            ".gitplus/knowledge/gotchas/worker-auth.md": conceptFor({
              record: qualify(discovery),
              blob: qualify(fixture),
            }),
          });
          const trust = yield* projectTrust(genesis);

          // Published *and* accepted: the citation verifies against the trust
          // state, which is a different claim from "a file was committed".
          const report = yield* Check.check({
            view,
            evaluationTime: NOW,
            repo,
            trust,
          });
          assert.equal(report.concepts[0]?.citations[0]?.state, "accepted");
          assert.equal(report.concepts[0]?.repositoryEvidence[0]?.state, "unchanged");
          assert.equal(report.concepts[0]?.eligibility.recall, true);

          // A deliberately starved startup Memory: this Concept does not fit.
          const startup = yield* Memory.derive({
            view,
            repo,
            trust,
            evaluationTime: NOW,
            limits: { bytes: 260 },
          });
          assert.equal(startup.text.includes("production policy fixture"), false);
          yield* Memory.write(startup.text);

          // -- session B: retrieve it anyway, and hand it over ----------------
          const sessionB = Session.newId();
          yield* Session.open({
            repo,
            session: sessionB,
            agent: { kind: "test", model: "", harness: "" },
            prompt: "why is the worker authorization test failing",
            key: agent,
          });

          const task = "why is the worker authorization test failing";
          const pack = yield* Select.select({
            task,
            view,
            knowledge: { repo, trust, evaluationTime: NOW },
          });
          // Retrieved despite its absence from startup Memory (INV-09, R-01).
          const concept = pack.items.find((item) => item.path.startsWith(".gitplus/knowledge/"));
          assert.equal(concept?.role, "knowledge");
          // …with the current bytes of what it declares (R-02).
          assert.equal(
            pack.items.some(
              (item) =>
                item.path === "tests/worker/auth.test.ts" &&
                item.kind === "blob" &&
                item.blob === qualify(fixture),
            ),
            true,
          );
          assert.equal((yield* Pack.verify(pack)).ok, true);

          const segments = yield* Select.render(pack, task);
          const handed = yield* sink(segments);
          const exposed = yield* Exposure.expose({
            repo,
            session: sessionB,
            key: agent,
            pack,
            segments,
            retain: true,
            task,
          });
          // The sink's independently computed digest is the exposure's own:
          // this, and only this, is the claim that these bytes crossed the
          // boundary (INV-10).
          assert.equal(handed.digest, exposed.digest);

          // The runtime half, joined by the exposure's qualified record oid.
          yield* Records.record(
            {
              ...(yield* Records.context(repo, sessionB)),
              type: Records.INVOCATION,
              exposure: exposed.oid,
              capture: { transport: "otel", stage: "sdk-export" },
              operation: { name: "chat" },
              usage: { source: "provider", inputTokens: 1200, outputTokens: 42 },
              outcome: { status: "ok" },
            },
            agent,
          );

          const joined = yield* Invocation.project({ session: sessionB, repo, trust });
          assert.equal(joined.invocations.length, 1);
          const row = joined.invocations[0]!;
          // One row from two records: the context half and the runtime half,
          // joined by the exposure's qualified record oid and by nothing else.
          assert.equal(row.context?.exposure, exposed.oid);
          assert.equal(row.context?.render, "verified");
          assert.equal(row.runtime?.operation, "chat");
          assert.equal(row.runtime?.usage?.inputTokens, 1200);

          // The historical audit still verifies, with no Memory or index in
          // the way: a redistillation cannot be a prerequisite for reading a
          // past exposure.
          const audited = yield* Exposure.audit({
            commit: exposed.commit,
            repo,
            session: sessionB,
            trust,
          });
          assert.equal(audited.ok, true);
          assert.equal(audited.render.state, "verified");
        }),
      ),
    ),
  );

  it.effect("stops presenting the learning when its declared dependency changes", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const { agent, genesis } = yield* enabled();
          const repo = genesis.repoId;
          const repository = yield* Repository;

          const session = Session.newId();
          yield* Session.open({
            repo,
            session,
            agent: { kind: "test", model: "", harness: "" },
            prompt: "work",
            key: agent,
          });
          const discovery = yield* Session.produced({
            repo,
            session,
            key: agent,
            note: "gotcha: the worker auth suite needs the production policy fixture",
          });
          const fixture = yield* repository.writeBlob(encode(FIXTURE));
          const concept = conceptFor({
            record: qualify(discovery),
            blob: qualify(fixture),
          });
          const trust = yield* projectTrust(genesis);

          const rewritten = yield* commitFiles({
            // The fixture the Concept cited is not what this tree holds.
            "tests/worker/auth.test.ts": "test('auth', () => { somethingElse() })\n",
            ".gitplus/knowledge/gotchas/worker-auth.md": concept,
          });

          const pack = yield* Select.select({
            task: "worker authorization fixture",
            view: rewritten,
            knowledge: { repo, trust, evaluationTime: NOW },
          });
          // Not silently presented as current, and not silently re-pointed at
          // the new bytes either (R-04, §9.2).
          assert.equal(
            pack.items.some((item) => item.path.startsWith(".gitplus/knowledge/")),
            false,
          );
          assert.equal(
            (pack.omissions ?? []).some(
              (omission) => omission.path === ".gitplus/knowledge/gotchas/worker-auth.md",
            ),
            true,
          );

          // And the checker says which dimension moved, without calling the
          // prose false.
          const report = yield* Check.check({
            view: rewritten,
            evaluationTime: NOW,
            repo,
            trust,
          });
          assert.equal(report.concepts[0]?.repositoryEvidence[0]?.state, "changed");
          assert.equal(report.concepts[0]?.citations[0]?.state, "accepted");
        }),
      ),
    ),
  );

  it.effect("a redacted discovery cannot be resurrected by an old Memory note", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const { agent, genesis } = yield* enabled();
          const repo = genesis.repoId;
          const repository = yield* Repository;

          const session = Session.newId();
          yield* Session.open({
            repo,
            session,
            agent: { kind: "test", model: "", harness: "" },
            prompt: "work",
            key: agent,
          });
          const discovery = yield* Session.produced({
            repo,
            session,
            key: agent,
            note: "gotcha: the worker auth suite needs the production policy fixture",
          });
          const trust = yield* projectTrust(genesis);
          const fixture = yield* repository.writeBlob(encode(FIXTURE));
          const view = yield* commitFiles({
            "tests/worker/auth.test.ts": FIXTURE,
            ".gitplus/knowledge/gotchas/worker-auth.md": conceptFor({
              record: qualify(discovery),
              blob: qualify(fixture),
            }),
          });

          // A note written while the discovery was still good.
          const before = yield* Memory.derive({ view, repo, trust, evaluationTime: NOW });
          assert.equal(before.text.includes("production policy fixture"), true);
          yield* Memory.write(before.text);

          const walked = yield* Session.entries(session);
          const target = walked.events.find((entry) => entry.commit === discovery)!;
          yield* Session.redact({
            repo,
            session,
            target: target.payload.id,
            reason: "the note carried something it should not have",
            key: agent,
          });

          // The stored note still holds the old text — a cache is not a
          // deletion (§8.5) — and automatic recall re-derives, so the entry
          // does not come back (INV-07).
          assert.equal((yield* Memory.read())?.includes("production policy fixture"), true);
          const after = yield* Memory.derive({ view, repo, trust, evaluationTime: NOW });
          assert.equal(
            after.entries.some((entry) => entry.origin === "session"),
            false,
          );

          // The Concept's citation is now redacted rather than accepted, so it
          // is out of automatic recall too — and the file is still there to
          // re-curate.
          const report = yield* Check.check({ view, evaluationTime: NOW, repo, trust });
          assert.equal(report.concepts[0]?.citations[0]?.state, "redacted");
          assert.equal(report.concepts[0]?.eligibility.recall, false);
          assert.equal(report.concepts[0]?.structure.state, "valid");
        }),
      ),
    ),
  );
});
