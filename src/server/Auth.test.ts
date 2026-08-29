import assert from "node:assert/strict";
import { describe, it } from "@effect/vitest";

import { Effect, Layer } from "effect";

import { fingerprint, formatPublicKey, generate, NAMESPACE, sign } from "../crypto/SshSignature.ts";
import { stores } from "../git/Memory.ts";
import * as GitRepository from "../git/Repository.ts";
import { Repository } from "../git/Repository.ts";
import { opensshPrivateKey } from "../testing/Hub.ts";
import * as Certificate from "../trust/Certificate.ts";
import { create, signGenesis, writeGenesis } from "../trust/Genesis.ts";
import * as Log from "../trust/Log.ts";
import {
  authenticate,
  credentialOf,
  encodeDelegation,
  guard,
  anonymousWrites,
  MAX_DELEGATION_SECONDS,
  mintDelegation,
  Nonces,
  noncesInMemory,
  openDelegation,
  present,
  REPLICATION_CAPABILITIES,
  RequestAudience,
  requestAudience,
  requiredCapability,
  signEnvelope,
} from "./Auth.ts";

/**
 * The services the guard reads, with the audience these requests arrived at.
 *
 * `requestAudience` is merged rather than left to each test: every request
 * below is built against `http://host/`, so `host` is what a host layer
 * validating the header would have produced. A test asking what happens under
 * a *different* audience provides its own, which wins — the inner `provide`
 * satisfies the requirement before this one is reached.
 */
const scenario = <A, E>(effect: Effect.Effect<A, E, Repository | Nonces | RequestAudience>) =>
  Effect.runPromise(
    effect.pipe(
      Effect.provide(
        Layer.mergeAll(
          GitRepository.layer.pipe(
            Layer.provide(GitRepository.hooksNoop),
            Layer.provideMerge(stores),
          ),
          noncesInMemory(),
          requestAudience(AUDIENCE),
        ),
      ),
    ),
  );

const AUDIENCE = "host";
const request = (url: string, init?: RequestInit) => new Request(`http://${AUDIENCE}/${url}`, init);

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
  describe("the nonce store", () => {
    it.effect("remembers a nonce across separate provides", () =>
      Effect.promise(async () => {
        // The bug this guards: `Layer.sync` is a *description*, so every
        // `Effect.provide` built a fresh map — the nonce a challenge issued was
        // unknown by the time the signed retry arrived, and native
        // authentication could never succeed on any host.
        const layer = noncesInMemory();
        const issued = await Effect.runPromise(
          Effect.flatMap(Nonces, (store) => store.issue(300)).pipe(Effect.provide(layer)),
        );
        const consumed = await Effect.runPromise(
          Effect.flatMap(Nonces, (store) => store.consume(issued)).pipe(Effect.provide(layer)),
        );
        assert.equal(consumed, true);
      }),
    );

    it.effect("survives a flood of challenges nobody answers", () =>
      Effect.promise(async () => {
        // Every 401 issues a nonce, and 401s are what unauthenticated traffic
        // produces. Remembering every issued one meant noise alone could turn
        // the store over — evicting the challenge an honest client was about to
        // answer, host-wide, for as long as the flood lasted. A nonce carries
        // its own expiry and a tag only this store can make, so issuing writes
        // nothing and only a *spent* one is remembered.
        const layer = noncesInMemory();
        const outcome = await Effect.runPromise(
          Effect.gen(function* () {
            const store = yield* Nonces;
            const honest = yield* store.issue(300);
            // Far past any ceiling the store could keep.
            for (let index = 0; index < 8192; index++) yield* store.issue(300);
            return {
              honest: yield* store.consume(honest),
              forged: yield* store.consume("9999999999999.deadbeef.0000"),
              garbage: yield* store.consume("not-a-nonce"),
            };
          }).pipe(Effect.provide(layer)),
        );

        assert.equal(outcome.honest, true, "the challenge issued first is still answerable");
        assert.equal(outcome.forged, false, "and one this store never made is not");
        assert.equal(outcome.garbage, false);
      }),
    );

    it.effect("refuses rather than forgetting that a nonce was spent", () =>
      Effect.promise(async () => {
        // Evicting the oldest record looks harmless — one client's retry — but
        // the record it drops is the one saying a nonce has been used, and
        // dropping that re-opens the replay window inside the nonce's own
        // lifetime. Refused instead: the request fails closed, and every entry
        // falls out on its own within the lifetime it was issued for.
        const layer = noncesInMemory(4);
        const outcome = await Effect.runPromise(
          Effect.gen(function* () {
            const store = yield* Nonces;
            const first = yield* store.issue(300);
            yield* store.consume(first);
            // Past the ceiling, which is the moment the old design started
            // forgetting.
            const beyond: boolean[] = [];
            for (let index = 0; index < 8; index++) {
              beyond.push(yield* store.consume(yield* store.issue(300)));
            }
            return { replayed: yield* store.consume(first), refused: beyond.includes(false) };
          }).pipe(Effect.provide(layer)),
        );

        assert.equal(outcome.refused, true, "a full store refuses rather than making room");
        assert.equal(outcome.replayed, false, "and a nonce spent long ago is still spent");
      }),
    );

    it.effect("spends a nonce exactly once", () =>
      Effect.promise(async () => {
        const layer = noncesInMemory();
        const outcome = await Effect.runPromise(
          Effect.gen(function* () {
            const store = yield* Nonces;
            const nonce = yield* store.issue(300);
            return { first: yield* store.consume(nonce), second: yield* store.consume(nonce) };
          }).pipe(Effect.provide(layer)),
        );
        assert.deepEqual(outcome, { first: true, second: false });
      }),
    );
  });

  describe("what an operation costs", () => {
    it.effect("charges a fetch a read and a push a push", () =>
      Effect.sync(() => {
        assert.deepEqual(requiredCapability(request("r/git-upload-pack", { method: "POST" })), [
          "repo.read",
        ]);
        assert.deepEqual(requiredCapability(request("r/git-receive-pack", { method: "POST" })), [
          "source.push",
          "source.delete",
        ]);
        assert.deepEqual(
          requiredCapability(
            request("r/git-receive-pack", { method: "POST", headers: { "git-inbox": "1" } }),
          ),
          ["source.push", "source.delete", "quarantine.submit"],
        );
      }),
    );

    it.effect(
      "lets a source.delete holder past the guard, since the guard cannot see the commands",
      () =>
        Effect.sync(() => {
          // Whether a push is a deletion is a question about its commands, and the
          // policy boundary is what reads them — it explicitly does *not* require
          // `source.push` for a deletion. Charging `source.push` at the door made
          // `source.delete` unusable as a standalone capability: the very holder
          // the boundary was written to admit was 403'd before it saw them.
          assert.ok(
            requiredCapability(request("r/git-receive-pack", { method: "POST" })).includes(
              "source.delete",
            ),
          );
        }),
    );

    it.effect("charges the receive-pack advertisement from the advertisement on", () =>
      Effect.sync(() => {
        assert.deepEqual(
          requiredCapability(request("r/info/refs?service=git-receive-pack")),
          ["source.push", "source.delete"],
          "a client that cannot push should not learn the ref layout through the push endpoint",
        );
      }),
    );

    it.effect("charges a read-only verb a read, and a DELETE that ends in one a write", () =>
      Effect.sync(() => {
        assert.deepEqual(requiredCapability(request("r/diff", { method: "POST" })), ["repo.read"]);
        assert.deepEqual(requiredCapability(request("r/grep", { method: "POST" })), ["repo.read"]);

        // The last path segment is also a *resource name*. Matching on the word
        // alone charged `DELETE /remotes/diff` and `DELETE /webhooks/grep`
        // `repo.read`, and neither endpoint has a policy gate behind it — so a
        // read-only credential could delete them.
        assert.deepEqual(requiredCapability(request("r/remotes/diff", { method: "DELETE" })), [
          "source.push",
          "source.delete",
        ]);
        assert.deepEqual(requiredCapability(request("r/webhooks/grep", { method: "DELETE" })), [
          "source.push",
          "source.delete",
        ]);
        assert.deepEqual(requiredCapability(request("r/branches/fsck", { method: "DELETE" })), [
          "source.push",
          "source.delete",
        ]);
      }),
    );

    it.effect("charges a whole-store read like the maintenance operation it costs", () =>
      Effect.sync(() => {
        // `fsck` mutates nothing, which is why it was grouped with `diff` and
        // `grep` — but what bounds those is one revision's tree, and what bounds
        // `bisect` is one history. `fsck` reads every object in the store.
        // Charged `repo.read`, it was anonymously reachable on exactly the
        // repositories that most want to be readable — one whose members hold no
        // read capability, or one with no genesis at all — and drivable in a
        // loop.
        assert.deepEqual(requiredCapability(request("r/fsck", { method: "POST" })), [
          "source.push",
          "source.delete",
        ]);
        // The bounded neighbours stay readable.
        assert.deepEqual(requiredCapability(request("r/bisect", { method: "POST" })), [
          "repo.read",
        ]);
      }),
    );

    it.effect("charges the LFS batch endpoint a read, POST or not", () =>
      Effect.sync(() => {
        // A reader must be able to clone a repository that uses LFS; the upload
        // it may negotiate is a separate PUT, charged separately.
        assert.deepEqual(
          requiredCapability(request("r/info/lfs/objects/batch", { method: "POST" })),
          ["repo.read"],
        );
        assert.deepEqual(requiredCapability(request("r/info/lfs/objects/abc", { method: "PUT" })), [
          "source.push",
          "source.delete",
        ]);
      }),
    );
  });

  describe("reading the credential off a request", () => {
    it.effect("takes a Basic password, which is how git sends one", () =>
      Effect.sync(() => {
        const presented = credentialOf(request("r", { headers: basic("hub1.x.y") }));
        assert.equal(presented.kind, "delegated");
        assert.equal(presented.kind === "delegated" ? presented.credential : "", "hub1.x.y");
      }),
    );

    it.effect("takes a Basic username when the password is empty", () =>
      Effect.sync(() => {
        // `http://<credential>@host/repo` arrives this way, and missing it is
        // how you get a server where curl works and `git clone` does not.
        const presented = credentialOf(
          request("r", { headers: { authorization: `Basic ${btoa("hub1.x.y:")}` } }),
        );
        assert.equal(presented.kind === "delegated" ? presented.credential : "", "hub1.x.y");
      }),
    );

    it.effect("takes a Bearer token", () =>
      Effect.sync(() => {
        const presented = credentialOf(request("r", { headers: { authorization: "Bearer abc" } }));
        assert.equal(presented.kind === "delegated" ? presented.credential : "", "abc");
      }),
    );

    it.effect("takes the native scheme as a payload and a signature", () =>
      Effect.sync(() => {
        const presented = credentialOf(
          request("r", { headers: { authorization: "Hub-SSH-v1 cGF5.c2ln" } }),
        );
        assert.equal(presented.kind, "native");
      }),
    );

    it.effect("reports nothing when there is no header", () =>
      Effect.sync(() => {
        assert.equal(credentialOf(request("r")).kind, "none");
      }),
    );
  });

  describe("a repository with no genesis", () => {
    it.effect("serves anonymously, because it is an ordinary git repository", () =>
      Effect.promise(async () => {
        const denied = await scenario(guard(request("r/git-upload-pack", { method: "POST" })));
        assert.equal(denied.denied, null);
      }),
    );

    it.effect("refuses a push, because nothing can authorize one", () =>
      Effect.promise(async () => {
        // Every write goes through here — smart-HTTP, the JSON verbs, LFS
        // uploads, webhook and remote registration — so this is the one check
        // that covers them all. Left to the ref boundary, the writes that move
        // no ref stayed open.
        const denied = await scenario(guard(request("r/git-receive-pack", { method: "POST" })));
        assert.equal(denied.denied?.status, 403);
        assert.match(await (denied.denied?.text() ?? Promise.resolve("")), /hub init/);
      }),
    );

    it.effect("serves one when the host has said to", () =>
      Effect.promise(async () => {
        const denied = await scenario(
          guard(request("r/git-receive-pack", { method: "POST" })).pipe(
            Effect.provide(anonymousWrites(true)),
          ),
        );
        assert.equal(denied.denied, null);
      }),
    );
  });

  describe("delegated credentials", () => {
    it.effect("admits a credential scoped to source.delete at the push endpoint", () =>
      Effect.promise(async () => {
        // The end-to-end shape of the same rule: the guard charges either write,
        // so a deletion-only credential reaches the policy boundary that can
        // read its commands and hold it to `source.delete` exactly.
        const outcome = await scenario(
          Effect.gen(function* () {
            const { genesis, member } = yield* hub(["source.delete"]);
            const credential = yield* mintDelegation({
              key: member,
              repo: genesis.repoId,
              capabilities: ["source.delete"],
              ttlSeconds: 300,
            });
            return yield* guard(
              request("r/git-receive-pack", { method: "POST", headers: basic(credential) }),
            );
          }),
        );
        assert.equal(outcome.denied, null, "a deletion must be allowed to reach the boundary");
      }),
    );

    it.effect("lets a member present one for what they hold", () =>
      Effect.promise(async () => {
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
      }),
    );

    it.effect("refuses one scoped below what the request needs", () =>
      Effect.promise(async () => {
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
      }),
    );

    it.effect("cannot carry more than its issuer holds", () =>
      Effect.promise(async () => {
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
      }),
    );

    it.effect("honours capability implication rather than an exact string match", () =>
      Effect.promise(async () => {
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
      }),
    );

    it.effect("refuses a credential whose lifetime exceeds the cap, however it was made", () =>
      Effect.promise(async () => {
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

            return yield* openDelegation(forged, genesis.repoId, new Date(), null);
          }),
        );
        assert.equal(opened, null);
      }),
    );

    it.effect("does not verify at another repository", () =>
      Effect.promise(async () => {
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
      }),
    );

    it.effect("expires", () =>
      Effect.promise(async () => {
        const opened = await scenario(
          Effect.gen(function* () {
            const { genesis, member } = yield* hub(["repo.read"]);
            const credential = yield* mintDelegation({
              key: member,
              repo: genesis.repoId,
              capabilities: ["repo.read"],
              ttlSeconds: 60,
            });
            return yield* openDelegation(
              credential,
              genesis.repoId,
              new Date(Date.now() + 120_000),
              null,
            );
          }),
        );
        assert.equal(opened, null);
      }),
    );

    it.effect("refuses a lifetime longer than the cap", () =>
      Effect.promise(async () => {
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
      }),
    );

    it.effect("stops working when its issuer is revoked", () =>
      Effect.promise(async () => {
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
        assert.equal(
          outcome.denied?.status,
          403,
          "revoking the issuer must revoke what they minted",
        );
      }),
    );
  });

  describe("anonymous access", () => {
    it.effect("is refused once somebody has been granted repo.read", () =>
      Effect.promise(async () => {
        const outcome = await scenario(
          Effect.gen(function* () {
            yield* hub(["repo.read"]);
            return yield* guard(request("r/git-upload-pack", { method: "POST" }));
          }),
        );
        assert.equal(outcome.denied?.status, 401);
      }),
    );

    it.effect("is refused when the only members hold repo.admin", () =>
      Effect.promise(async () => {
        const outcome = await scenario(
          Effect.gen(function* () {
            // `repo.admin` carries `repo.read`, so this repository has
            // restricted reading just as surely as one that granted it by name.
            yield* hub(["repo.admin"]);
            return yield* guard(request("r/git-upload-pack", { method: "POST" }));
          }),
        );
        assert.equal(outcome.denied?.status, 401);
      }),
    );

    it.effect("is allowed when no grant restricts reading", () =>
      Effect.promise(async () => {
        const outcome = await scenario(
          Effect.gen(function* () {
            yield* hub(["source.push"]);
            return yield* guard(request("r/git-upload-pack", { method: "POST" }));
          }),
        );
        assert.equal(outcome.denied, null, "a repository nobody restricted is a public repository");
      }),
    );

    it.effect("is never allowed to push", () =>
      Effect.promise(async () => {
        const outcome = await scenario(
          Effect.gen(function* () {
            yield* hub(["source.push"]);
            return yield* guard(request("r/git-receive-pack", { method: "POST" }));
          }),
        );
        assert.equal(outcome.denied?.status, 401);
      }),
    );

    it.effect("carries a nonce and the RepoID, so a native client can sign its retry", () =>
      Effect.promise(async () => {
        // Both halves of the envelope's binding arrive with the refusal: the
        // single-use nonce, and the repository identity the envelope must name.
        // Without the RepoID a client refused its very first read — a private
        // repository — had nowhere left to learn it from.
        const outcome = await scenario(
          Effect.gen(function* () {
            yield* hub(["repo.read"]);
            return yield* guard(request("r/git-upload-pack", { method: "POST" }));
          }),
        );
        const header = outcome.denied?.headers.get("www-authenticate") ?? "";
        assert.match(header, /nonce="/);
        assert.match(header, /repo="[^"]+"/);
      }),
    );
  });

  describe("native envelopes", () => {
    it.effect("authenticates a signed request", () =>
      Effect.promise(async () => {
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
      }),
    );

    it.effect("does not spend a nonce for a signer this repository never granted", () =>
      Effect.promise(async () => {
        // A signature proves possession, not membership, so this envelope is as
        // well-formed as a member's. Spending the nonce on it let anyone with a
        // throwaway key write into the spent set — and 4096 of them evicted a
        // captured envelope's nonce back out of it, reopening the replay window
        // inside its own TTL. The nonce must still be there afterwards.
        const outcome = await scenario(
          Effect.gen(function* () {
            const { genesis } = yield* hub(["source.push"]);
            const stranger = yield* generate("stranger@example.com");
            const nonces = yield* Nonces;
            const nonce = yield* nonces.issue(300);
            const header = yield* signEnvelope(stranger, {
              type: "auth.request",
              version: 1,
              repo: genesis.repoId,
              operation: "git-receive-pack",
              commands: [{ ref: "refs/heads/main", from: null, to: "a".repeat(40) }],
              nonce,
              expiresAt: new Date(Date.now() + 60_000).toISOString(),
            });
            const denied = yield* guard(
              request("r/git-receive-pack", { method: "POST", headers: { authorization: header } }),
            );
            return { denied: denied.denied !== null, unspent: yield* nonces.consume(nonce) };
          }),
        );
        assert.equal(outcome.denied, true, "a stranger's envelope is refused");
        assert.equal(outcome.unspent, true, "and it cost the spent set nothing");
      }),
    );

    it.effect("refuses the same envelope twice", () =>
      Effect.promise(async () => {
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
      }),
    );

    it.effect("does not let a stranger's key burn nonces", () =>
      Effect.promise(async () => {
        // Single use is enforced by a bounded store of *spent* nonces. Spending
        // on a signature alone meant anybody with a throwaway key could fill
        // that store and evict the record of a genuine spend — re-opening the
        // replay window inside the nonce's own lifetime. Behind the membership
        // check the only keys that can fill it are keys this repository granted.
        const outcome = await scenario(
          Effect.gen(function* () {
            const { genesis, member } = yield* hub(["source.push"]);
            const stranger = yield* generate("stranger@example.com");
            const nonces = yield* Nonces;

            const envelope = (nonce: string) => ({
              type: "auth.request" as const,
              version: 1 as const,
              repo: genesis.repoId,
              operation: "git-receive-pack",
              commands: [],
              nonce,
              expiresAt: new Date(Date.now() + 60_000).toISOString(),
            });

            // The stranger presents a well-formed, correctly signed envelope on
            // a nonce this server issued. They are not a member, so it is
            // refused — and it must leave the nonce unspent.
            const shared = yield* nonces.issue(300);
            const burned = yield* guard(
              request("r/git-receive-pack", {
                method: "POST",
                headers: { authorization: yield* signEnvelope(stranger, envelope(shared)) },
              }),
            );

            // The member's own request, on that same nonce.
            const honest = yield* guard(
              request("r/git-receive-pack", {
                method: "POST",
                headers: { authorization: yield* signEnvelope(member, envelope(shared)) },
              }),
            );
            return { burned: burned.denied?.status, honest: honest.denied };
          }),
        );

        assert.equal(outcome.burned, 403, "a key this repository never granted gets nowhere");
        assert.equal(outcome.honest, null, "and it spent nothing on the way");
      }),
    );

    it.effect("refuses an envelope signed for another operation", () =>
      Effect.promise(async () => {
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
      }),
    );

    it.effect("refuses an expired envelope", () =>
      Effect.promise(async () => {
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
      }),
    );

    it.effect("refuses a nonce the server never issued", () =>
      Effect.promise(async () => {
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
      }),
    );
  });

  it.effect("reports who the request is, not merely that it may proceed", () =>
    Effect.promise(async () => {
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
            capability: ["source.push"],
          });
        }),
      );

      assert.equal(outcome.ok, true);
      // The policy boundary needs the principal; recovering it with a second
      // lookup is how authentication and authorization come apart.
      assert.notEqual(outcome.ok === true ? outcome.authenticated.principal : null, null);
    }),
  );
});

describe("what a registered remote presents", () => {
  it.effect("mints from a stored key rather than handing over a stored token", () =>
    Effect.promise(async () => {
      // Every credential a hub-enabled destination accepts is either a
      // signature over the request in hand or a delegation capped at
      // `MAX_DELEGATION_SECONDS` — a day. A token written into the remote
      // registry is therefore a credential that authenticates until tomorrow
      // and then stops, and the path that presents it is a standing
      // instruction: detached, reporting into a log, watched by nobody. The
      // mirror simply goes quiet, revocations included, while the origin goes
      // on accepting the pushes it is failing to forward. So the registry
      // stores the key and the credential is minted where it is used.
      const outcome = await scenario(
        Effect.gen(function* () {
          const where = yield* hub(["source.push", "hub.comment", "member.revoke"]);
          const presented = yield* present({
            url: "https://mirror.example.com:8443/mirror.git",
            credential: null,
            key: opensshPrivateKey(where.member, "mirror@example.com"),
          });
          const at = (host: string | null) =>
            openDelegation(presented ?? "", where.genesis.repoId, new Date(), host);
          return {
            presented,
            opened: yield* at("mirror.example.com:8443"),
            // The origin holds the same `RepoID` as its mirror, so this is the
            // replay the audience exists to stop.
            elsewhere: yield* at("origin.example.com"),
            unknown: yield* at(null),
            signer: yield* fingerprint(where.member.publicKey),
          };
        }),
      );

      assert.ok(
        outcome.presented?.startsWith("hub1.") === true,
        `a key must present as a delegation, not as itself: ${outcome.presented}`,
      );
      assert.equal(outcome.opened?.signer, outcome.signer, "signed by the key that was stored");
      // Claiming is not holding: verification intersects this with what the
      // issuer holds, so a mirror carries exactly what its key could have
      // pushed by hand. Naming a subset here would be picking which of a
      // repository's refs replicate.
      assert.deepEqual(
        [...(outcome.opened?.delegation.capabilities ?? [])],
        [...REPLICATION_CAPABILITIES],
      );
      // Which is safe only because it is spendable at one host. A mirror is not
      // a party the sender has to trust, and a claim this wide handed over
      // unbound was a near-admin token for the origin, valid for the hour.
      assert.equal(outcome.opened?.delegation.audience, "mirror.example.com:8443");
      assert.equal(outcome.elsewhere, null, "a bound credential must not open at another host");
      assert.equal(outcome.unknown, null, "nor where the host is unknown");
    }),
  );

  it.effect(
    "hands over a stored token when there is no key, and nothing at all when there is neither",
    () =>
      Effect.promise(async () => {
        // The registry still holds tokens, because not every destination is a
        // hub: a mirror to a forge that takes a personal access token is exactly
        // what a stored credential is for.
        const outcome = await scenario(
          Effect.gen(function* () {
            yield* hub(["source.push"]);
            return {
              token: yield* present({
                url: "https://mirror.example.com",
                credential: "s3cret",
                key: null,
              }),
              neither: yield* present({
                url: "https://mirror.example.com",
                credential: null,
                key: null,
              }),
            };
          }),
        );

        assert.equal(outcome.token, "s3cret");
        assert.equal(outcome.neither, undefined);
      }),
  );
});

describe("verifies a host-bound delegation against the arrived-at host, not the client's", () => {
  // The audience half of a delegation exists to stop a mirror replaying, back
  // at the origin, the near-admin token it was handed. The host it is checked
  // against is supplied by the host layer (`RequestAudience`) precisely so a
  // client cannot name it: on a self-hosted server the request URL's host is
  // the client's `Host` header, so were the check read from there, a mirror
  // could set `Host` to the token's own audience and defeat the binding.

  /** Mint the near-admin, host-bound credential a mirror is handed. */
  const boundCredential = Effect.fn("test.boundCredential")(function* () {
    const { genesis, member } = yield* hub(["source.push"]);
    const credential = yield* mintDelegation({
      key: member,
      repo: genesis.repoId,
      capabilities: ["source.push"],
      audience: "mirror.example.com:8443",
      ttlSeconds: 300,
    });
    // The request URL's host is the mirror's — the forged `Host` a replay
    // sends so the URL agrees with the token's audience. The fix ignores it.
    return {
      credential,
      request: new Request("http://mirror.example.com:8443/r/git-receive-pack", {
        method: "POST",
        headers: basic(credential),
      }),
    };
  });

  it.effect("refuses the replay when the arrived-at host is not the token's audience", () =>
    Effect.promise(async () => {
      const outcome = await scenario(
        Effect.gen(function* () {
          const { request } = yield* boundCredential();
          // The host the request truly arrived at is the origin — the replay
          // target — not the mirror the `Host` header claims.
          return yield* authenticate({ request, capability: ["source.push"] }).pipe(
            Effect.provide(requestAudience("origin.example.com")),
          );
        }),
      );

      assert.equal(outcome.ok, false, "the mirror's replay must not authenticate at the origin");
    }),
  );

  it.effect("refuses it likewise where the arrived-at host is unknown", () =>
    Effect.promise(async () => {
      const outcome = await scenario(
        Effect.gen(function* () {
          const { request } = yield* boundCredential();
          // No configured host matched the `Host` header, so the host layer
          // reports no trustworthy audience — fail closed, as for a mismatch.
          return yield* authenticate({ request, capability: ["source.push"] }).pipe(
            Effect.provide(requestAudience(null)),
          );
        }),
      );

      assert.equal(outcome.ok, false, "an unknown arrived-at host refuses a bound credential");
    }),
  );

  it.effect("admits it where the arrived-at host is the token's audience", () =>
    Effect.promise(async () => {
      const outcome = await scenario(
        Effect.gen(function* () {
          const { request } = yield* boundCredential();
          // The legitimate case: the mirror itself receives the token at the
          // host it was minted for, which the host layer confirms.
          return yield* authenticate({ request, capability: ["source.push"] }).pipe(
            Effect.provide(requestAudience("mirror.example.com:8443")),
          );
        }),
      );

      assert.equal(outcome.ok, true, "the audience host must still accept the credential");
    }),
  );
});
