import assert from "node:assert/strict";
import { describe, it } from "@effect/vitest";

import { Effect } from "effect";

import type { RepoId } from "../trust/Genesis.ts";
import { principalId } from "../trust/Principal.ts";
import {
  attestExternalIdentity,
  attestPrincipalKey,
  attestRepo,
  checkpoint,
  decode,
  encode,
  follow,
  label,
  mirrors,
  revoke,
  validate,
  vouch,
} from "./Statement.ts";

/** SAFETY: forty-three base64 characters after `SHA256:`, which is the shape. */
const repoId = (seed: string): RepoId => `SHA256:${seed.repeat(43).slice(0, 43)}` as RepoId;

const alice = principalId(repoId("a"));
const bob = principalId(repoId("b"));
const project = repoId("c");
const id = "018bcfe5-6800-7000-8000-000000000001";
const at = new Date("2026-08-20T00:00:00.000Z");

describe("social statements", () => {
  it.effect("round-trips a canonical repository attestation", () =>
    Effect.promise(async () => {
      const statement = attestRepo({
        author: alice,
        id,
        socialHead: null,
        trustHead: null,
        repo: project,
        urls: ["https://git.example.com/acme/project"],
        role: "origin",
        forkOf: null,
        lineage: `sha1:${"a1".repeat(20)}`,
        inbox: "https://git.example.com/acme/project/inbox",
        at,
      });

      const bytes = encode(statement);
      const decoded = await Effect.runPromise(
        Effect.gen(function* () {
          const payload = yield* decode(bytes);
          yield* validate(payload, alice);
          return payload;
        }),
      );

      assert.deepEqual(decoded, statement);
      assert.equal(
        new TextDecoder().decode(bytes),
        `${JSON.stringify(
          {
            version: 1,
            type: "social.attest.repo",
            author: alice,
            id,
            issuedAt: at.toISOString(),
            socialHead: null,
            trustHead: null,
            repo: project,
            urls: ["https://git.example.com/acme/project"],
            role: "origin",
            forkOf: null,
            lineage: `sha1:${"a1".repeat(20)}`,
            inbox: "https://git.example.com/acme/project/inbox",
          },
          null,
          2,
        )}\n`,
      );
    }),
  );

  it.effect("binds every statement to the identity repository that carries it", () =>
    Effect.promise(async () => {
      const statement = attestRepo({
        author: alice,
        id,
        socialHead: null,
        trustHead: null,
        repo: project,
        urls: ["https://git.example.com/acme/project"],
        role: "origin",
        at,
      });

      const failure = await Effect.runPromise(validate(statement, bob).pipe(Effect.flip));
      assert.match(failure.reason, /author.*identity repository/i);
    }),
  );

  it.effect("keeps external identities as proof-backed claims, not principal names", () =>
    Effect.promise(async () => {
      const statement = attestExternalIdentity({
        author: alice,
        id,
        socialHead: null,
        trustHead: null,
        subject: bob,
        identity: "github:bob",
        proof: "https://gist.github.com/bob/proof",
        at,
      });

      const decoded = await Effect.runPromise(decode(encode(statement)));
      assert.equal(decoded.type, "social.attest.principal");
      assert.equal(decoded.claim, "external-identity");
      assert.equal(decoded.subject, `principal:${bob}`);
    }),
  );

  it.effect("refuses a vouch that can widen into an unknown scope", () =>
    Effect.promise(async () => {
      const statement = {
        ...vouch({
          author: alice,
          id,
          socialHead: null,
          trustHead: null,
          subject: bob,
          scope: ["review"],
          depth: 1,
          at,
        }),
        scope: ["review", "repo.admin"],
      };

      const failure = await Effect.runPromise(validate(statement, alice).pipe(Effect.flip));
      assert.match(failure.reason, /unknown social scope/);
    }),
  );
  it("puts the envelope first and the variant's own fields after it, for every type", () => {
    // `encode` derives this order structurally rather than restating each
    // variant's field list, so what is signed is whatever the constructor
    // built. These are the orders that produces — a change to one is a change
    // to the bytes every existing signature was made over.
    const context = { author: alice, id, socialHead: null, trustHead: null, at } as const;
    const envelope = ["version", "type", "author", "id", "issuedAt", "socialHead", "trustHead"];
    const keysOf = (bytes: Uint8Array) => Object.keys(JSON.parse(new TextDecoder().decode(bytes)));

    const variants = [
      [
        attestRepo({ ...context, repo: project, urls: [], role: "origin" }),
        ["repo", "urls", "role", "forkOf", "lineage", "inbox"],
      ],
      [
        attestPrincipalKey({ ...context, subject: bob, publicKey: "ssh-ed25519 AAAA" }),
        ["subject", "claim", "publicKey"],
      ],
      [
        attestExternalIdentity({ ...context, subject: bob, identity: "i", proof: "p" }),
        ["subject", "claim", "identity", "proof"],
      ],
      [mirrors({ ...context, repo: "self", urls: [] }), ["repo", "urls"]],
      [
        vouch({ ...context, subject: bob, scope: ["review"], depth: 1 }),
        ["subject", "scope", "depth", "expiresAt"],
      ],
      [follow({ ...context, subject: bob, petname: "bob" }), ["subject", "petname"]],
      [
        label({ ...context, subject: "s", namespace: "n", label: "l" }),
        ["subject", "namespace", "label"],
      ],
      [revoke({ ...context, target: "t" }), ["target", "reason", "compromisedAt"]],
      [checkpoint({ ...context, frontier: [] }), ["frontier"]],
    ] as const;

    for (const [statement, fields] of variants) {
      assert.deepEqual(keysOf(encode(statement)), [...envelope, ...fields], statement.type);
    }
  });
});
