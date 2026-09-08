import assert from "node:assert/strict";
import { describe, it } from "@effect/vitest";
import { Effect, Layer } from "effect";

import { fromSeed, NAMESPACE, sign } from "../crypto/SshSignature.ts";
import { stores } from "../git/Memory.ts";
import * as GitRepository from "../git/Repository.ts";
import type { Oid } from "../git/Store.ts";
import * as Record from "../trust/Record.ts";
import * as Event from "./Event.ts";
import * as Session from "./Session.ts";

const repository = GitRepository.layer.pipe(
  Layer.provide(GitRepository.hooksNoop),
  Layer.provide(stores),
);

describe("session decisions", () => {
  it.effect("resolves concurrent answers by event ID and later answers by causality", () =>
    Effect.gen(function* () {
      const repo = yield* GitRepository.Repository;
      const key = yield* fromSeed(new Uint8Array(32).fill(1), "session@test");
      const base = {
        version: 1,
        repo: "test",
        session: "session",
        trustHead: null,
        issuedAt: "2026-01-01T00:00:00.000Z",
      } as const;
      const write = Effect.fn("test.sessionRecord")(function* (
        payload: Session.SessionPayload,
        parents: ReadonlyArray<Oid>,
      ) {
        const bytes = Session.encode(payload);
        return yield* Record.write({
          name: Event.RECORD,
          payload: bytes,
          signatures: [yield* sign(key, bytes, NAMESPACE)],
          parents,
          message: `${payload.type} ${payload.id}\n`,
          at: new Date(0),
        });
      });
      const opened = yield* write(
        {
          ...base,
          id: "opening",
          type: "session.opened",
          agent: { kind: "test", model: "", harness: "" },
          prompt: "choose a plan",
          role: "user",
          context: null,
        },
        [],
      );
      const asked = yield* write(
        {
          ...base,
          id: "question",
          type: "decision.requested",
          question: "Which plan?",
          options: ["first", "second"],
          refs: [],
        },
        [opened],
      );
      const answer = (id: string, chose: string, parents: ReadonlyArray<Oid>) =>
        write(
          {
            ...base,
            id,
            type: "decision.resolved",
            decision: "question",
            chose,
            note: null,
          },
          parents,
        );
      const first = yield* answer("00000000-0000-7000-8000-000000000003", "first", [asked]);
      const second = yield* answer("00000000-0000-7000-8000-000000000002", "second", [asked]);
      assert.ok(first < second, "fixture orders commit IDs opposite to event IDs");
      const join = Effect.fn("test.sessionJoin")(function* (parents: ReadonlyArray<Oid>) {
        const commit = yield* repo.commitTree({
          tree: yield* repo.writeTree([]),
          parents,
          message: "join\n",
          author: Record.identityAt(new Date(0)),
        });
        yield* repo.setRef({ name: Session.refOf(base.session), to: commit });
        return commit;
      });
      yield* join([first, second]);
      assert.equal((yield* Session.project(base.session)).decisions[0]?.chose, "first");

      // A clock can move backwards. Superseded answers must leave the set
      // before choosing the greatest ID among the remaining concurrent ones.
      const revised = yield* answer("00000000-0000-7000-8000-000000000001", "revised", [first]);
      const merged = yield* join([revised, second]);
      assert.equal((yield* Session.project(base.session)).decisions[0]?.chose, "second");
      const final = yield* answer("00000000-0000-7000-8000-000000000000", "final", [merged]);
      yield* repo.setRef({ name: Session.refOf(base.session), to: final });
      assert.equal((yield* Session.project(base.session)).decisions[0]?.chose, "final");
    }).pipe(Effect.provide(repository)),
  );
});
