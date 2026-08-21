import assert from "node:assert/strict";
import { describe, it } from "@effect/vitest";

import { Effect } from "effect";

import type { Fingerprint } from "../crypto/SshSignature.ts";
import type { Oid } from "../git/Store.ts";
import type { RepoId } from "../trust/Genesis.ts";
import { principalId, type PrincipalId } from "../trust/Principal.ts";
import type { VerifiedLog, VerifiedStatement } from "./Log.ts";
import { project } from "./Projection.ts";
import { follow, mirrors, type SocialStatement } from "./Statement.ts";
import { followedBy, locationOf } from "./Sync.node.ts";

const repo = (seed: string): RepoId => {
  // SAFETY: exactly forty-three base64 characters after the required prefix.
  return `SHA256:${seed.repeat(43).slice(0, 43)}` as RepoId;
};
const principal = (seed: string): PrincipalId => principalId(repo(seed));
const oid = (index: number): Oid => {
  // SAFETY: exactly forty lowercase hexadecimal characters.
  return index.toString(16).padStart(40, "0") as Oid;
};
const entry = (payload: SocialStatement, index: number): VerifiedStatement => ({
  commit: oid(index),
  parents: index === 1 ? [] : [oid(index - 1)],
  payload,
  bytes: new Uint8Array(),
  signatures: [],
  // SAFETY: exactly forty-three base64 characters after the required prefix.
  signer: `SHA256:${"s".repeat(43)}` as Fingerprint,
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

describe("social synchronization", () => {
  it.effect("crawls follow edges and prefers the subject's newest writable mirror", () =>
    Effect.sync(() => {
      const alice = principal("a");
      const bob = principal("b");
      const projection = project({
        roots: [alice],
        logs: [
          log(alice, [follow({ ...context(alice, 1), subject: bob, petname: "bob" })]),
          log(bob, [
            mirrors({
              ...context(bob, 1),
              repo: "self",
              urls: [{ url: "https://old.example/bob", mode: "read" }],
            }),
            mirrors({
              ...context(bob, 2),
              repo: "self",
              urls: [
                { url: "https://read.example/bob", mode: "read" },
                { url: "https://write.example/bob", mode: "write" },
              ],
            }),
          ]),
        ],
      });

      assert.deepEqual(followedBy(projection, new Set([alice])), [bob]);
      assert.equal(
        locationOf(projection, bob, [
          { url: "https://pinned.example/bob", repoId: bob, provenance: { kind: "tofu" } },
        ]),
        "https://write.example/bob",
      );
    }),
  );
});
