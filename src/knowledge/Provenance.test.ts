import assert from "node:assert/strict";
import { it } from "@effect/vitest";
import { Effect, Layer } from "effect";
import { fingerprint } from "../crypto/SshSignature.ts";
import { stores } from "../git/Memory.ts";
import * as GitRepository from "../git/Repository.ts";
import { Repository } from "../git/Repository.ts";
import { qualify } from "../git/Oid.ts";
import * as Event from "../hub/Event.ts";
import * as Session from "../hub/Session.ts";
import * as Memory from "../hub/Memory.ts";
import * as Record from "../trust/Record.ts";
import * as Trust from "../trust/Projection.ts";
import * as Certificate from "../trust/Certificate.ts";
import * as Log from "../trust/Log.ts";
import { knowledgeFixture } from "../testing/Knowledge.ts";
import * as Provenance from "./Provenance.ts";
import * as Inputs from "./Inputs.ts";
import * as Maintenance from "./Maintenance.ts";

const world = GitRepository.layer.pipe(
  Layer.provide(GitRepository.hooksNoop),
  Layer.provide(stores),
);

for (const scenario of [
  "valid",
  "foreign",
  "unreachable",
  "unsigned",
  "revoked",
  "missing-trust",
] as const) {
  it.effect(`direct observation provenance: ${scenario}`, () =>
    Effect.gen(function* () {
      const fixture = yield* knowledgeFixture();
      const repository = yield* Repository;
      const original = yield* Session.produced({
        repo: fixture.genesis.repoId,
        session: fixture.session,
        key: fixture.agent,
        note: "gotcha: Keep a bounded auth comparison.",
      });
      let target = original;
      if (scenario === "foreign" || scenario === "unsigned") {
        const record = yield* Record.read(original, Event.RECORD);
        const payload = yield* Session.decode(record.payload);
        target = yield* Record.write({
          name: Event.RECORD,
          payload: Session.encode({
            ...payload,
            repo: scenario === "foreign" ? "foreign-repository" : payload.repo,
          }),
          signatures: scenario === "unsigned" ? [] : record.signatures,
          parents: [original],
          message: "unaccepted observation\n",
        });
        yield* repository.setRef({
          name: Session.refOf(fixture.session),
          to: target,
          expected: original,
        });
      }
      if (scenario === "revoked")
        yield* Log.issue(
          Certificate.revoke({
            repo: fixture.genesis.repoId,
            subject: yield* fingerprint(fixture.agent.publicKey),
            reason: "compromised",
            id: Log.newId(),
          }),
          [fixture.root],
        );
      const trust = scenario === "missing-trust" ? null : yield* Trust.project(fixture.genesis);
      const heads = new Map(yield* repository.refs);
      if (scenario === "unreachable") {
        const parent = (yield* repository.readCommit(original)).parents[0];
        assert.ok(parent);
        heads.set(Session.refOf(fixture.session), parent);
      }
      const judged = yield* Provenance.evaluate(
        qualify(target),
        Provenance.judge(fixture.genesis.repoId, trust, heads, new Date("2026-10-29T12:00:00Z")),
      );
      const reasons = {
        valid: /./u,
        foreign: /names repository/u,
        unreachable: /not on refs/u,
        unsigned: /sign/u,
        revoked: /revok/u,
        "missing-trust": /no trust projection/u,
      };
      assert.equal(
        judged.state,
        scenario === "valid"
          ? "accepted"
          : scenario === "missing-trust"
            ? "unavailable"
            : "invalid",
      );
      if (scenario !== "valid") assert.match(judged.reason ?? "", reasons[scenario]);
      if (scenario === "valid" || scenario === "revoked" || scenario === "missing-trust") {
        const recalled = yield* Memory.derive({
          repo: fixture.genesis.repoId,
          trust,
          evaluationTime: new Date("2026-10-29T12:00:00Z"),
        });
        assert.equal(
          recalled.entries.some((entry) => entry.origin === "session"),
          scenario === "valid",
        );
      }
    }).pipe(Effect.provide(world)),
  );
}

it.effect(
  "a captured manifest retains its original session head and deterministic evidence after a live append",
  () =>
    Effect.gen(function* () {
      const fixture = yield* knowledgeFixture();
      const manifest = yield* Inputs.capture({
        sourceRef: "refs/heads/main",
        evaluatedAt: "2026-10-29T00:00:00Z",
        limits: { concepts: 25, sessions: 512, bytes: 1048576 },
        configuration: { version: 1 },
      });
      const first = yield* Maintenance.plan({ manifest, history: [], maximum: 25 });
      const selected = first.chosen[0];
      assert.ok(selected);
      const before = yield* Maintenance.check(manifest, selected);
      yield* Session.produced({
        repo: fixture.genesis.repoId,
        session: fixture.session,
        key: fixture.agent,
        note: "gotcha: A later observation outside the captured view.",
      });
      const second = yield* Maintenance.plan({ manifest, history: [], maximum: 25 });
      assert.deepEqual(second, first);
      assert.deepEqual(yield* Maintenance.check(manifest, selected), before);
      assert.equal(yield* Inputs.current(manifest), false);
    }).pipe(Effect.provide(world)),
);
