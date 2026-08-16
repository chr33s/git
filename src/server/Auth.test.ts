import assert from "node:assert/strict";
import { describe, it } from "@effect/vitest";

import { Effect, Layer } from "effect";

import { fingerprint, formatPublicKey, generate, NAMESPACE, sign } from "../crypto/SshSignature.ts";
import { stores } from "../git/Memory.ts";
import * as GitRepository from "../git/Repository.ts";
import { Repository } from "../git/Repository.ts";
import * as Certificate from "../trust/Certificate.ts";
import { create, signGenesis, writeGenesis } from "../trust/Genesis.ts";
import * as Log from "../trust/Log.ts";
import {
  authenticate,
  credentialOf,
  encodeDelegation,
  guard,
  MAX_DELEGATION_SECONDS,
  mintDelegation,
  Nonces,
  noncesInMemory,
  openDelegation,
  requiredCapability,
  signEnvelope,
} from "./Auth.ts";

const scenario = <A, E>(effect: Effect.Effect<A, E, Repository | Nonces>) =>
  Effect.runPromise(
    effect.pipe(
      Effect.provide(
        Layer.merge(
          GitRepository.layer.pipe(
            Layer.provide(GitRepository.hooksNoop),
            Layer.provideMerge(stores),
          ),
          noncesInMemory,
        ),
      ),
    ),
  );

const request = (url: string, init?: RequestInit) => new Request(`http://host/${url}`, init);

/** A repository with a genesis and one member holding `capabilities`. */
const hub = Effect.fn("test.hub")(function* (capabilities: ReadonlyArray<string>) {
  const root = yield* generate("root@example.com");
  const member = yield* generate("member@example.com");
  const genesis = yield* create([formatPublicKey(root.publicKey)], 1);
  yield* writeGenesis(genesis, [yield* signGenesis(genesis, root)]);

  yield* Log.issue(
    yield* Certificate.grant({
      repo: genesis.repoId,
      publicKey: formatPublicKey(member.publicKey),
      capabilities,
      id: Log.newId(),
    }),
    [root],
  );
  return { genesis, root, member };
});

/** The encoding a delegated credential travels in. */
const base64url = (bytes: Uint8Array): string => {
  let binary = "";
  for (const byte of bytes) binary += String.fromCharCode(byte);
  return btoa(binary).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
};

const basic = (credential: string) => ({
  authorization: `Basic ${btoa(`x:${credential}`)}`,
});

describe("Auth", () => {
  describe("what an operation costs", () => {
    it("charges a fetch a read and a push a push", () => {
      assert.equal(
        requiredCapability(request("r/git-upload-pack", { method: "POST" })),
        "repo.read",
      );
      assert.equal(
        requiredCapability(request("r/git-receive-pack", { method: "POST" })),
        "source.push",
      );
    });

    it("charges the receive-pack advertisement from the advertisement on", () => {
      assert.equal(
        requiredCapability(request("r/info/refs?service=git-receive-pack")),
        "source.push",
        "a client that cannot push should not learn the ref layout through the push endpoint",
      );
    });

    it("charges the LFS batch endpoint a read, POST or not", () => {
      // A reader must be able to clone a repository that uses LFS; the upload
      // it may negotiate is a separate PUT, charged separately.
      assert.equal(
        requiredCapability(request("r/info/lfs/objects/batch", { method: "POST" })),
        "repo.read",
      );
      assert.equal(
        requiredCapability(request("r/info/lfs/objects/abc", { method: "PUT" })),
        "source.push",
      );
    });
  });

  describe("reading the credential off a request", () => {
    it("takes a Basic password, which is how git sends one", () => {
      const presented = credentialOf(request("r", { headers: basic("hub1.x.y") }));
      assert.equal(presented.kind, "delegated");
      assert.equal(presented.kind === "delegated" ? presented.credential : "", "hub1.x.y");
    });

    it("takes a Basic username when the password is empty", () => {
      // `http://<credential>@host/repo` arrives this way, and missing it is
      // how you get a server where curl works and `git clone` does not.
      const presented = credentialOf(
        request("r", { headers: { authorization: `Basic ${btoa("hub1.x.y:")}` } }),
      );
      assert.equal(presented.kind === "delegated" ? presented.credential : "", "hub1.x.y");
    });

    it("takes a Bearer token", () => {
      const presented = credentialOf(request("r", { headers: { authorization: "Bearer abc" } }));
      assert.equal(presented.kind === "delegated" ? presented.credential : "", "abc");
    });

    it("takes the native scheme as a payload and a signature", () => {
      const presented = credentialOf(
        request("r", { headers: { authorization: "Hub-SSH-v1 cGF5.c2ln" } }),
      );
      assert.equal(presented.kind, "native");
    });

    it("reports nothing when there is no header", () => {
      assert.equal(credentialOf(request("r")).kind, "none");
    });
  });

  describe("a repository with no genesis", () => {
    it("serves anonymously, because it is an ordinary git repository", async () => {
      const denied = await scenario(guard(request("r/git-upload-pack", { method: "POST" })));
      assert.equal(denied.denied, null);
    });

    it("serves a push anonymously too — nothing has claimed it", async () => {
      const denied = await scenario(guard(request("r/git-receive-pack", { method: "POST" })));
      assert.equal(denied.denied, null);
    });
  });

  describe("delegated credentials", () => {
    it("lets a member present one for what they hold", async () => {
      const outcome = await scenario(
        Effect.gen(function* () {
          const { genesis, member } = yield* hub(["source.push"]);
          const credential = yield* mintDelegation({
            key: member,
            repo: genesis.repoId,
            capabilities: ["source.push"],
            ttlSeconds: 300,
          });
          return yield* guard(
            request("r/git-receive-pack", { method: "POST", headers: basic(credential) }),
          );
        }),
      );
      assert.equal(outcome.denied, null);
    });

    it("refuses one scoped below what the request needs", async () => {
      const outcome = await scenario(
        Effect.gen(function* () {
          const { genesis, member } = yield* hub(["source.push", "repo.read"]);
          // The member could push; this credential says it is only for reading.
          const credential = yield* mintDelegation({
            key: member,
            repo: genesis.repoId,
            capabilities: ["repo.read"],
            ttlSeconds: 300,
          });
          return yield* guard(
            request("r/git-receive-pack", { method: "POST", headers: basic(credential) }),
          );
        }),
      );
      assert.equal(outcome.denied?.status, 403);
    });

    it("cannot carry more than its issuer holds", async () => {
      const outcome = await scenario(
        Effect.gen(function* () {
          const { genesis, member } = yield* hub(["repo.read"]);
          // Asking for a capability the issuer never had: the credential is
          // well-formed and signed, and it still authorizes nothing extra.
          const credential = yield* mintDelegation({
            key: member,
            repo: genesis.repoId,
            capabilities: ["source.push"],
            ttlSeconds: 300,
          });
          return yield* guard(
            request("r/git-receive-pack", { method: "POST", headers: basic(credential) }),
          );
        }),
      );
      assert.equal(outcome.denied?.status, 403);
    });

    it("honours capability implication rather than an exact string match", async () => {
      const outcome = await scenario(
        Effect.gen(function* () {
          const { genesis, member } = yield* hub(["repo.admin"]);
          // Scoped `repo.admin`, used for a request that costs `repo.read`:
          // admin carries read, and a literal comparison would refuse it.
          const credential = yield* mintDelegation({
            key: member,
            repo: genesis.repoId,
            capabilities: ["repo.admin"],
            ttlSeconds: 300,
          });
          return yield* guard(
            request("r/git-upload-pack", { method: "POST", headers: basic(credential) }),
          );
        }),
      );
      assert.equal(outcome.denied, null);
    });

    it("refuses a credential whose lifetime exceeds the cap, however it was made", async () => {
      const opened = await scenario(
        Effect.gen(function* () {
          const { genesis, member } = yield* hub(["repo.read"]);
          // Signed by hand with a ten-year expiry: the holder signs these
          // themselves, so a cap only `mintDelegation` applied is one anybody
          // could opt out of.
          const delegation = {
            type: "auth.delegate",
            version: 1,
            repo: genesis.repoId,
            capabilities: ["repo.read"],
            expiresAt: new Date(Date.now() + 10 * 365 * 86_400_000).toISOString(),
            nonce: crypto.randomUUID(),
          } as const;
          const bytes = encodeDelegation(delegation);
          const armored = yield* sign(member, bytes, NAMESPACE);
          const forged = `hub1.${base64url(bytes)}.${base64url(new TextEncoder().encode(armored))}`;

          return yield* openDelegation(forged, genesis.repoId, new Date());
        }),
      );
      assert.equal(opened, null);
    });

    it("does not verify at another repository", async () => {
      const outcome = await scenario(
        Effect.gen(function* () {
          const { member } = yield* hub(["source.push"]);
          const elsewhere = yield* create([formatPublicKey(member.publicKey)], 1);
          const credential = yield* mintDelegation({
            key: member,
            repo: elsewhere.repoId,
            capabilities: ["source.push"],
            ttlSeconds: 300,
          });
          return yield* guard(
            request("r/git-receive-pack", { method: "POST", headers: basic(credential) }),
          );
        }),
      );
      assert.equal(outcome.denied?.status, 401);
    });

    it("expires", async () => {
      const opened = await scenario(
        Effect.gen(function* () {
          const { genesis, member } = yield* hub(["repo.read"]);
          const credential = yield* mintDelegation({
            key: member,
            repo: genesis.repoId,
            capabilities: ["repo.read"],
            ttlSeconds: 60,
          });
          return yield* openDelegation(credential, genesis.repoId, new Date(Date.now() + 120_000));
        }),
      );
      assert.equal(opened, null);
    });

    it("refuses a lifetime longer than the cap", async () => {
      const failure = await scenario(
        Effect.gen(function* () {
          const { genesis, member } = yield* hub(["repo.read"]);
          return yield* mintDelegation({
            key: member,
            repo: genesis.repoId,
            capabilities: ["repo.read"],
            ttlSeconds: MAX_DELEGATION_SECONDS + 1,
          }).pipe(Effect.flip);
        }),
      );
      assert.match(failure.reason, /between 1 and/);
    });

    it("stops working when its issuer is revoked", async () => {
      const outcome = await scenario(
        Effect.gen(function* () {
          const { genesis, member, root } = yield* hub(["source.push"]);
          const credential = yield* mintDelegation({
            key: member,
            repo: genesis.repoId,
            capabilities: ["source.push"],
            ttlSeconds: 300,
          });
          yield* Log.issue(
            Certificate.revoke({
              repo: genesis.repoId,
              subject: yield* fingerprint(member.publicKey),
              reason: "left",
              id: Log.newId(),
            }),
            [root],
          );
          return yield* guard(
            request("r/git-receive-pack", { method: "POST", headers: basic(credential) }),
          );
        }),
      );
      assert.equal(outcome.denied?.status, 403, "revoking the issuer must revoke what they minted");
    });
  });

  describe("anonymous access", () => {
    it("is refused once somebody has been granted repo.read", async () => {
      const outcome = await scenario(
        Effect.gen(function* () {
          yield* hub(["repo.read"]);
          return yield* guard(request("r/git-upload-pack", { method: "POST" }));
        }),
      );
      assert.equal(outcome.denied?.status, 401);
    });

    it("is refused when the only members hold repo.admin", async () => {
      const outcome = await scenario(
        Effect.gen(function* () {
          // `repo.admin` carries `repo.read`, so this repository has
          // restricted reading just as surely as one that granted it by name.
          yield* hub(["repo.admin"]);
          return yield* guard(request("r/git-upload-pack", { method: "POST" }));
        }),
      );
      assert.equal(outcome.denied?.status, 401);
    });

    it("is allowed when no grant restricts reading", async () => {
      const outcome = await scenario(
        Effect.gen(function* () {
          yield* hub(["source.push"]);
          return yield* guard(request("r/git-upload-pack", { method: "POST" }));
        }),
      );
      assert.equal(outcome.denied, null, "a repository nobody restricted is a public repository");
    });

    it("is never allowed to push", async () => {
      const outcome = await scenario(
        Effect.gen(function* () {
          yield* hub(["source.push"]);
          return yield* guard(request("r/git-receive-pack", { method: "POST" }));
        }),
      );
      assert.equal(outcome.denied?.status, 401);
    });

    it("carries a nonce, so a native client can sign its retry", async () => {
      const outcome = await scenario(
        Effect.gen(function* () {
          yield* hub(["repo.read"]);
          return yield* guard(request("r/git-upload-pack", { method: "POST" }));
        }),
      );
      assert.match(outcome.denied?.headers.get("www-authenticate") ?? "", /nonce="/);
    });
  });

  describe("native envelopes", () => {
    it("authenticates a signed request", async () => {
      const outcome = await scenario(
        Effect.gen(function* () {
          const { genesis, member } = yield* hub(["source.push"]);
          const nonces = yield* Nonces;
          const header = yield* signEnvelope(member, {
            type: "auth.request",
            version: 1,
            repo: genesis.repoId,
            operation: "git-receive-pack",
            commands: [{ ref: "refs/heads/main", from: null, to: "a".repeat(40) }],
            nonce: yield* nonces.issue(300),
            expiresAt: new Date(Date.now() + 60_000).toISOString(),
          });
          return yield* guard(
            request("r/git-receive-pack", { method: "POST", headers: { authorization: header } }),
          );
        }),
      );
      assert.equal(outcome.denied, null);
    });

    it("refuses the same envelope twice", async () => {
      const outcome = await scenario(
        Effect.gen(function* () {
          const { genesis, member } = yield* hub(["source.push"]);
          const nonces = yield* Nonces;
          const header = yield* signEnvelope(member, {
            type: "auth.request",
            version: 1,
            repo: genesis.repoId,
            operation: "git-receive-pack",
            commands: [],
            nonce: yield* nonces.issue(300),
            expiresAt: new Date(Date.now() + 60_000).toISOString(),
          });
          const headers = { authorization: header };
          yield* guard(request("r/git-receive-pack", { method: "POST", headers }));
          return yield* guard(request("r/git-receive-pack", { method: "POST", headers }));
        }),
      );
      assert.equal(outcome.denied?.status, 401, "a nonce is single use");
    });

    it("refuses an envelope signed for another operation", async () => {
      const outcome = await scenario(
        Effect.gen(function* () {
          const { genesis, member } = yield* hub(["source.push"]);
          const nonces = yield* Nonces;
          // Signed for a fetch, presented at a push.
          const header = yield* signEnvelope(member, {
            type: "auth.request",
            version: 1,
            repo: genesis.repoId,
            operation: "git-upload-pack",
            commands: [],
            nonce: yield* nonces.issue(300),
            expiresAt: new Date(Date.now() + 60_000).toISOString(),
          });
          return yield* guard(
            request("r/git-receive-pack", { method: "POST", headers: { authorization: header } }),
          );
        }),
      );
      assert.equal(outcome.denied?.status, 401);
    });

    it("refuses an expired envelope", async () => {
      const outcome = await scenario(
        Effect.gen(function* () {
          const { genesis, member } = yield* hub(["source.push"]);
          const nonces = yield* Nonces;
          const header = yield* signEnvelope(member, {
            type: "auth.request",
            version: 1,
            repo: genesis.repoId,
            operation: "git-receive-pack",
            commands: [],
            nonce: yield* nonces.issue(300),
            expiresAt: new Date(Date.now() - 1000).toISOString(),
          });
          return yield* guard(
            request("r/git-receive-pack", { method: "POST", headers: { authorization: header } }),
          );
        }),
      );
      assert.equal(outcome.denied?.status, 401);
    });

    it("refuses a nonce the server never issued", async () => {
      const outcome = await scenario(
        Effect.gen(function* () {
          const { genesis, member } = yield* hub(["source.push"]);
          const header = yield* signEnvelope(member, {
            type: "auth.request",
            version: 1,
            repo: genesis.repoId,
            operation: "git-receive-pack",
            commands: [],
            nonce: "not-a-nonce-this-server-issued",
            expiresAt: new Date(Date.now() + 60_000).toISOString(),
          });
          return yield* guard(
            request("r/git-receive-pack", { method: "POST", headers: { authorization: header } }),
          );
        }),
      );
      assert.equal(outcome.denied?.status, 401);
    });
  });

  it("reports who the request is, not merely that it may proceed", async () => {
    const outcome = await scenario(
      Effect.gen(function* () {
        const { genesis, member } = yield* hub(["source.push"]);
        const credential = yield* mintDelegation({
          key: member,
          repo: genesis.repoId,
          capabilities: ["source.push"],
          ttlSeconds: 300,
        });
        return yield* authenticate({
          request: request("r/git-receive-pack", { method: "POST", headers: basic(credential) }),
          capability: "source.push",
        });
      }),
    );

    assert.equal(outcome.ok, true);
    // The policy boundary needs the principal; recovering it with a second
    // lookup is how authentication and authorization come apart.
    assert.notEqual(outcome.ok === true ? outcome.authenticated.principal : null, null);
  });
});
