import assert from "node:assert/strict";
import { describe, it } from "@effect/vitest";

import { Effect } from "effect";

import type { Fingerprint } from "../crypto/SshSignature.ts";
import type { Oid } from "../git/Store.ts";
import type { RepoId } from "../trust/Genesis.ts";
import { principalId, type PrincipalId } from "../trust/Principal.ts";
import type { VerifiedLog, VerifiedStatement } from "./Log.ts";
import { confidence, fresh, pathsTo, project } from "./Projection.ts";
import {
  checkpoint,
  encode,
  follow,
  mirrors,
  revoke,
  vouch,
  type SocialStatement,
} from "./Statement.ts";

/** SAFETY: forty-three base64 characters after `SHA256:`, which is the shape. */
const repoId = (seed: string): RepoId => `SHA256:${seed.repeat(43).slice(0, 43)}` as RepoId;
const principal = (seed: string): PrincipalId => principalId(repoId(seed));

const alice = principal("a");
const bob = principal("b");
const carol = principal("c");
const dave = principal("d");
const eve = principal("e");
const now = new Date("2026-08-20T12:00:00Z");

const oid = (value: number): Oid => {
  // SAFETY: exactly forty lowercase hexadecimal characters.
  return value.toString(16).padStart(40, "0") as Oid;
};

const signer = (seed: string): Fingerprint => {
  // SAFETY: the same fingerprint spelling validated by the crypto module.
  return `SHA256:${seed.repeat(43).slice(0, 43)}` as Fingerprint;
};

const record = (payload: SocialStatement, index: number): VerifiedStatement => ({
  commit: oid(index),
  parents: index === 1 ? [] : [oid(index - 1)],
  payload,
  bytes: encode(payload),
  signatures: [],
  signer: signer(payload.author.slice("SHA256:".length, "SHA256:".length + 1)),
});

const log = (owner: PrincipalId, statements: ReadonlyArray<SocialStatement>): VerifiedLog => ({
  principal: owner,
  head: statements.length === 0 ? null : oid(statements.length),
  statements: statements.map(record),
  rejected: [],
});

const context = (author: PrincipalId, index: number) => ({
  author,
  id: `018bcfe5-6800-7000-8000-${index.toString().padStart(12, "0")}`,
  socialHead: index === 1 ? null : oid(index - 1),
  trustHead: null,
  at: new Date(now.getTime() + index),
});

describe("social projection", () => {
  it.effect("attenuates scope and stops when an intermediate vouch cannot extend", () =>
    Effect.sync(() => {
      const projection = project({
        roots: [alice],
        logs: [
          log(alice, [
            vouch({
              ...context(alice, 1),
              subject: bob,
              scope: ["introduce.repo", "vouch"],
              depth: 1,
            }),
          ]),
          log(bob, [
            vouch({
              ...context(bob, 1),
              subject: carol,
              scope: ["introduce.repo"],
              depth: 1,
            }),
          ]),
          log(carol, [
            vouch({
              ...context(carol, 1),
              subject: dave,
              scope: ["introduce.repo"],
              depth: 0,
            }),
          ]),
        ],
        at: now,
      });

      assert.equal(pathsTo(projection, bob, "introduce.repo").length, 1);
      assert.equal(pathsTo(projection, bob, "review").length, 0, "scope only narrows");
      assert.equal(pathsTo(projection, carol, "introduce.repo").length, 1);
      assert.equal(
        pathsTo(projection, dave, "introduce.repo").length,
        0,
        "a path without vouch scope cannot be extended",
      );
    }),
  );

  it.effect("counts independent paths rather than vouch statements", () =>
    Effect.sync(() => {
      const projection = project({
        roots: [alice],
        logs: [
          log(alice, [
            vouch({
              ...context(alice, 1),
              subject: bob,
              scope: ["review", "vouch"],
              depth: 1,
            }),
            vouch({
              ...context(alice, 2),
              subject: carol,
              scope: ["review", "vouch"],
              depth: 1,
            }),
          ]),
          log(bob, [
            vouch({
              ...context(bob, 1),
              subject: dave,
              scope: ["review"],
              depth: 0,
            }),
          ]),
          log(carol, [
            vouch({
              ...context(carol, 1),
              subject: dave,
              scope: ["review"],
              depth: 0,
            }),
          ]),
        ],
        at: now,
      });

      assert.equal(pathsTo(projection, dave, "review").length, 2);
      assert.equal(confidence(projection, dave, "review"), 2);
    }),
  );

  it.effect("never reads follows as trust edges", () =>
    Effect.sync(() => {
      const projection = project({
        roots: [alice],
        logs: [log(alice, [follow({ ...context(alice, 1), subject: eve, petname: "eve" })])],
        at: now,
      });

      assert.equal(pathsTo(projection, eve, "introduce.repo").length, 0);
      assert.equal(projection.petnames.get(alice)?.get(eve), "eve");
    }),
  );

  it.effect("withdraws a vouch and restores it when the withdrawal is itself revoked", () =>
    Effect.sync(() => {
      const vouched = vouch({
        ...context(alice, 1),
        subject: bob,
        scope: ["review"],
        depth: 0,
      });
      const withdrawn = revoke({
        ...context(alice, 2),
        target: vouched.id,
        reason: "withdrawn",
      });

      const without = project({
        roots: [alice],
        logs: [log(alice, [vouched, withdrawn])],
        at: now,
      });
      assert.equal(confidence(without, bob, "review"), 0);

      const restored = project({
        roots: [alice],
        logs: [
          log(alice, [
            vouched,
            withdrawn,
            revoke({ ...context(alice, 3), target: withdrawn.id, reason: "superseded" }),
          ]),
        ],
        at: now,
      });
      assert.equal(confidence(restored, bob, "review"), 1);
    }),
  );

  it.effect("projects only the newest active mirror list for a repository", () =>
    Effect.sync(() => {
      const graph = project({
        roots: [alice],
        logs: [
          log(alice, [
            mirrors({
              ...context(alice, 1),
              repo: "self",
              urls: [{ url: "https://old.example/alice", mode: "read" }],
            }),
            mirrors({
              ...context(alice, 2),
              repo: "self",
              urls: [{ url: "https://new.example/alice", mode: "write" }],
            }),
          ]),
        ],
        at: now,
      });
      const locations = graph.active.filter(
        (statement) => statement.payload.type === "social.mirrors",
      );
      assert.equal(locations.length, 1);
      assert.equal(locations[0]?.payload.type, "social.mirrors");
      if (locations[0]?.payload.type === "social.mirrors") {
        assert.equal(locations[0].payload.urls[0]?.url, "https://new.example/alice");
      }
    }),
  );

  it.effect("bounds a reachable vouch path by its author's social checkpoint", () =>
    Effect.sync(() => {
      const vouched = vouch({
        ...context(alice, 1),
        subject: bob,
        scope: ["review"],
        depth: 0,
      });
      const without = project({ roots: [alice], logs: [log(alice, [vouched])], at: now });
      assert.equal(fresh(without, 60_000, now).ok, false);

      const recent = checkpoint({
        ...context(alice, 2),
        frontier: [oid(1)],
        at: new Date(now.getTime() - 30_000),
      });
      const bounded = project({
        roots: [alice],
        logs: [log(alice, [vouched, recent])],
        at: now,
      });
      assert.equal(fresh(bounded, 60_000, now).ok, true);
      assert.equal(fresh(bounded, 10_000, now).ok, false);
    }),
  );
});
