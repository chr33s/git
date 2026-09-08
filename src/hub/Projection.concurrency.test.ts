import assert from "node:assert/strict";
import { describe, it } from "@effect/vitest";
import { Effect, Layer } from "effect";

import { formatPublicKey, fromSeed, NAMESPACE, sign } from "../crypto/SshSignature.ts";
import { stores } from "../git/Memory.ts";
import * as GitRepository from "../git/Repository.ts";
import type { Oid } from "../git/Store.ts";
import * as Certificate from "../trust/Certificate.ts";
import { create, signGenesis, writeGenesis } from "../trust/Genesis.ts";
import * as Log from "../trust/Log.ts";
import { project as projectTrust } from "../trust/Projection.ts";
import * as Record from "../trust/Record.ts";
import * as Event from "./Event.ts";
import { project } from "./Projection.ts";

const repository = GitRepository.layer.pipe(
  Layer.provide(GitRepository.hooksNoop),
  Layer.provide(stores),
);

describe("concurrent PR revisions", () => {
  for (const type of ["pr.updated", "pr.opened"] as const) {
    it.effect(`${type} discards superseded events before comparing concurrent IDs`, () =>
      Effect.gen(function* () {
        const repo = yield* GitRepository.Repository;
        const key = yield* fromSeed(new Uint8Array(32).fill(1), "revision@test");
        const genesis = yield* create(
          [formatPublicKey(key.publicKey)],
          1,
          "00000000-0000-4000-8000-000000000001",
        );
        yield* writeGenesis(genesis, [yield* signGenesis(genesis, key)]);
        yield* Log.issue(
          yield* Certificate.grant({
            repo: genesis.repoId,
            publicKey: formatPublicKey(key.publicKey),
            capabilities: ["hub.create-pr", "hub.merge", "hub.approve"],
            id: "grant",
            at: new Date("2025-01-01T00:00:00.000Z"),
          }),
          [key],
        );
        const trust = yield* projectTrust(genesis);
        const pr = "concurrent";
        const envelope = {
          version: 1,
          repo: genesis.repoId,
          pr,
          trustHead: null,
          issuedAt: "2026-01-01T00:00:00.000Z",
        } as const;
        const write = Effect.fn("test.revisionRecord")(function* (
          payload: Event.HubPayload,
          parents: ReadonlyArray<Oid>,
        ) {
          const bytes = Event.encode(payload);
          return yield* Record.write({
            name: Event.RECORD,
            payload: bytes,
            signatures: [yield* sign(key, bytes, NAMESPACE)],
            parents,
            message: `${payload.type} ${payload.id}\n`,
            // Fixed dates make the concurrent event fold before the lower-ID
            // successor in both fixtures, exercising the running-winner bug.
            at: new Date(type === "pr.updated" ? 1000 : 0),
          });
        });
        const content = (revision: string) => ({
          title: `Revision ${revision}`,
          description: `Description ${revision}`,
          base: `refs/heads/base-${revision}`,
          head: `sha1:${revision.repeat(40)}`,
        });
        const opening = yield* write(
          { ...envelope, ...content("a"), type: "pr.opened", id: "opening" },
          [],
        );
        const revision = (id: string, head: string, parents: ReadonlyArray<Oid>) =>
          write({ ...envelope, ...content(head), type, id }, parents);
        const first = yield* revision("00000000-0000-7000-8000-000000000003", "b", [opening]);
        const concurrent = yield* revision("00000000-0000-7000-8000-000000000002", "c", [opening]);
        const revised = yield* revision("00000000-0000-7000-8000-000000000001", "d", [first]);
        const join = yield* repo.commitTree({
          tree: yield* repo.writeTree([]),
          parents: [revised, concurrent],
          message: "join\n",
          author: Record.identityAt(new Date(0)),
        });
        yield* repo.setRef({ name: Event.refOf(pr), to: join });
        const walked = yield* Event.entries(pr);
        assert.ok(walked.ordered.indexOf(concurrent) < walked.ordered.indexOf(revised));
        const state = yield* project(genesis, trust, pr);
        assert.deepEqual(state.rejected, []);
        assert.equal(state.head, "c".repeat(40));
        if (type === "pr.opened") {
          assert.equal(state.title, "Revision c");
          assert.equal(state.description, "Description c");
          assert.equal(state.base, "refs/heads/base-c");
        }
        const reviewed = yield* write(
          {
            ...envelope,
            type: "review.submitted",
            id: "review",
            head: content("c").head,
            decision: "approve",
            body: "Reviewed after synchronizing both devices",
          },
          [join],
        );
        yield* repo.setRef({ name: Event.refOf(pr), to: reviewed });
        const withReview = yield* project(genesis, trust, pr);
        assert.deepEqual(withReview.rejected, []);
        assert.equal(withReview.reviews[0]?.base, state.base);
        assert.equal(withReview.reviews[0]?.stale, false);
        const final = yield* revision("00000000-0000-7000-8000-000000000000", "e", [reviewed]);
        yield* repo.setRef({ name: Event.refOf(pr), to: final });
        assert.equal((yield* project(genesis, trust, pr)).head, "e".repeat(40));
      }).pipe(Effect.provide(repository)),
    );
  }
});
