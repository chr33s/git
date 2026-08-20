import assert from "node:assert/strict";
import { describe, it } from "@effect/vitest";

import { Effect } from "effect";

import type { Fingerprint } from "../crypto/SshSignature.ts";
import type { Oid } from "../git/Store.ts";
import type { RepoId } from "../trust/Genesis.ts";
import { principalId, type PrincipalId } from "../trust/Principal.ts";
import { decide, repositories } from "./Introduce.ts";
import type { VerifiedLog, VerifiedStatement } from "./Log.ts";
import { project } from "./Projection.ts";
import { attestRepo, encode, vouch, type SocialStatement } from "./Statement.ts";

/** SAFETY: forty-three base64 characters after `SHA256:`, which is the shape. */
const repo = (seed: string): RepoId => `SHA256:${seed.repeat(43).slice(0, 43)}` as RepoId;
const principal = (seed: string): PrincipalId => principalId(repo(seed));
const alice = principal("a");
const bob = principal("b");
const carol = principal("c");
const projectRepo = repo("p");
const impostor = repo("x");
const url = "https://git.example.com/acme/project";

const oid = (value: number): Oid => {
  // SAFETY: exactly forty lowercase hexadecimal characters.
  return value.toString(16).padStart(40, "0") as Oid;
};

const entry = (payload: SocialStatement, index: number): VerifiedStatement => ({
  commit: oid(index),
  parents: index === 1 ? [] : [oid(index - 1)],
  payload,
  bytes: encode(payload),
  signatures: [],
  // SAFETY: forty-three base64 characters after the prefix.
  signer: `SHA256:${payload.author.slice(-1).repeat(43)}` as Fingerprint,
});

const log = (owner: PrincipalId, statements: ReadonlyArray<SocialStatement>): VerifiedLog => ({
  principal: owner,
  head: statements.length === 0 ? null : oid(statements.length),
  statements: statements.map(entry),
  rejected: [],
});

const context = (author: PrincipalId, index: number) => ({
  author,
  id: `018bcfe5-6800-7000-8000-${index.toString().padStart(12, "0")}`,
  socialHead: index === 1 ? null : oid(index - 1),
  trustHead: null,
  at: new Date(`2026-08-20T00:00:0${index}Z`),
});

describe("social introduction", () => {
  it.effect("replaces blind TOFU when enough independent attesters name the presented RepoID", () =>
    Effect.gen(function* () {
      const projection = project({
        roots: [alice],
        logs: [
          log(alice, [
            vouch({
              ...context(alice, 1),
              subject: bob,
              scope: ["introduce.repo"],
              depth: 0,
            }),
            attestRepo({
              ...context(alice, 2),
              repo: projectRepo,
              urls: [url],
              role: "origin",
            }),
          ]),
          log(bob, [
            attestRepo({
              ...context(bob, 1),
              repo: projectRepo,
              urls: [url],
              role: "origin",
            }),
          ]),
        ],
      });

      const decision = yield* decide({
        projection,
        url: `${url}.git/`,
        presented: projectRepo,
        minPaths: 2,
      });
      assert.equal(decision.kind, "introduced");
      assert.equal(decision.kind === "introduced" ? decision.paths : 0, 2);
      assert.equal(repositories(projection)[0]?.repo, projectRepo);
    }),
  );

  it.effect("surfaces a split view instead of picking one identity", () =>
    Effect.gen(function* () {
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
            vouch({
              ...context(alice, 2),
              subject: carol,
              scope: ["introduce.repo"],
              depth: 0,
            }),
          ]),
          log(bob, [
            attestRepo({
              ...context(bob, 1),
              repo: projectRepo,
              urls: [url],
              role: "origin",
            }),
          ]),
          log(carol, [
            attestRepo({
              ...context(carol, 1),
              repo: impostor,
              urls: [url],
              role: "origin",
            }),
          ]),
        ],
      });

      const decision = yield* decide({ projection, url, presented: projectRepo, minPaths: 1 });
      assert.equal(decision.kind, "split");
      assert.deepEqual(
        decision.kind === "split" ? decision.claims.map(({ repo }) => repo).sort() : [],
        [impostor, projectRepo].sort(),
      );
    }),
  );

  it.effect("surfaces one trusted claim that conflicts with the presented identity", () =>
    Effect.gen(function* () {
      const projection = project({
        roots: [alice],
        logs: [
          log(alice, [
            attestRepo({
              ...context(alice, 1),
              repo: projectRepo,
              urls: [url],
              role: "origin",
            }),
          ]),
        ],
      });

      const decision = yield* decide({ projection, url, presented: impostor });
      assert.equal(decision.kind, "split");
      assert.deepEqual(decision.kind === "split" ? decision.claims.map(({ repo }) => repo) : [], [
        projectRepo,
      ]);
    }),
  );

  it.effect("falls back to TOFU when the verifier's own roots do not reach an attester", () =>
    Effect.gen(function* () {
      const projection = project({
        roots: [alice],
        logs: [
          log(bob, [
            attestRepo({
              ...context(bob, 1),
              repo: projectRepo,
              urls: [url],
              role: "origin",
            }),
          ]),
        ],
      });
      const decision = yield* decide({ projection, url, presented: projectRepo });
      assert.equal(decision.kind, "tofu");
    }),
  );
});
