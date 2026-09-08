import assert from "node:assert/strict";
import { it } from "@effect/vitest";
import { Effect, Layer, Stream } from "effect";
import { generate, formatPublicKey, fingerprint } from "../crypto/SshSignature.ts";
import * as Auth from "./Auth.ts";
import * as Certificate from "../trust/Certificate.ts";
import * as Genesis from "../trust/Genesis.ts";
import * as Lfs from "./Lfs.ts";
import * as Log from "../trust/Log.ts";
import * as Policy from "./Policy.ts";
import * as Repository from "../git/Repository.ts";
import { bytesToHex } from "../git/Format.ts";
import { principalId } from "../trust/Principal.ts";
import { project } from "../trust/Projection.ts";
import { stores } from "../git/Memory.ts";

it.effect("restricts principal readers including former grants", () =>
  Effect.gen(function* () {
    const root = yield* generate("root");
    const genesis = yield* Genesis.create([formatPublicKey(root.publicKey)], 1);
    yield* Genesis.writeGenesis(genesis, [yield* Genesis.signGenesis(genesis, root)]);
    const identity = yield* Genesis.create([formatPublicKey(root.publicKey)], 1);
    yield* Log.issue(
      yield* Certificate.grantPrincipal({
        repo: genesis.repoId,
        principal: principalId(identity.repoId),
        capabilities: ["repo.read"],
        id: Log.newId(),
      }),
      [root],
    );
    const projection = yield* project(genesis);
    const outcome = yield* Auth.guard(
      new Request("http://host/private/info/refs?service=git-upload-pack"),
    );
    assert.equal(Auth.anonymousReadAllowed(projection), false);
    assert.equal(
      Auth.anonymousReadAllowed({
        ...projection,
        principals: new Map(),
        formerPrincipals: projection.principals,
      }),
      false,
    );
    assert.equal(outcome.denied?.status, 401);
    return {
      directReaders: projection.members.size,
      principalReaders: projection.principals.size,
      rejected: projection.rejected,
      anonymousReadAllowed: Auth.anonymousReadAllowed(projection),
      guardStatus: outcome.denied?.status ?? 200,
    };
  }).pipe(
    Effect.provide(
      Layer.mergeAll(
        Repository.layer.pipe(Layer.provide(Repository.hooksNoop), Layer.provideMerge(stores)),
        Auth.noncesInMemory(),
        Auth.requestAudience("host"),
      ),
    ),
  ),
);

it.effect("rejects LFS paths with trailing segments", () =>
  Effect.gen(function* () {
    const root = yield* generate("root");
    const genesis = yield* Genesis.create([formatPublicKey(root.publicKey)], 1);
    yield* Genesis.writeGenesis(genesis, [yield* Genesis.signGenesis(genesis, root)]);
    yield* Log.issue(
      yield* Certificate.grant({
        repo: genesis.repoId,
        publicKey: formatPublicKey(root.publicKey),
        capabilities: ["repo.read"],
        id: Log.newId(),
      }),
      [root],
    );
    const bytes = new TextEncoder().encode("private LFS payload");
    const oid = bytesToHex(
      new Uint8Array(yield* Effect.promise(() => crypto.subtle.digest("SHA-256", bytes))),
    );
    const store = yield* Lfs.memory;
    yield* store.write(oid, Stream.make(bytes));
    const normal = yield* Auth.guard(new Request(`http://host/private/info/lfs/objects/${oid}`));
    const request = new Request(`http://host/private/info/lfs/objects/${oid}/git-receive-pack`, {
      headers: { "git-inbox": "1" },
    });
    const bypass = yield* Auth.guard(request);
    const response = yield* Lfs.handle(request).pipe(
      Effect.provideService(Lfs.LfsStore, store),
      Effect.provide(Auth.requester(bypass.authenticated)),
    );
    assert.equal(normal.denied?.status, 401);
    assert.equal(response?.status, 404);
    assert.ok(response);
    return {
      normalStatus: normal.denied?.status ?? 200,
      bypassGuardStatus: bypass.denied?.status ?? 200,
      capabilities: bypass.authenticated.capabilities,
      responseStatus: response?.status,
      leakedBody: yield* Effect.promise(() => response.text()),
    };
  }).pipe(
    Effect.provide(
      Layer.mergeAll(
        Repository.layer.pipe(Layer.provide(Repository.hooksNoop), Layer.provideMerge(stores)),
        Auth.noncesInMemory(),
        Auth.requestAudience("host"),
      ),
    ),
  ),
);

it.effect("rechecks permissions after a revocation", () =>
  Effect.gen(function* () {
    const root = yield* generate("root");
    const genesis = yield* Genesis.create([formatPublicKey(root.publicKey)], 1);
    yield* Genesis.writeGenesis(genesis, [yield* Genesis.signGenesis(genesis, root)]);
    const member = yield* generate("member");
    const signer = yield* fingerprint(member.publicKey);
    yield* Log.issue(
      yield* Certificate.grant({
        repo: genesis.repoId,
        publicKey: formatPublicKey(member.publicKey),
        capabilities: ["source.push"],
        id: Log.newId(),
      }),
      [root],
    );
    const credential = yield* Auth.mintDelegation({
      key: member,
      repo: genesis.repoId,
      capabilities: ["source.push"],
      ttlSeconds: 300,
    });
    const guarded = yield* Auth.guard(
      new Request("http://host/private/commit", {
        method: "POST",
        headers: { authorization: `Basic ${btoa(`x:${credential}`)}` },
      }),
    );
    if (guarded.denied !== null) throw new Error("fixture authentication failed");
    yield* Log.issue(
      Certificate.revoke({
        repo: genesis.repoId,
        subject: signer,
        reason: "compromised",
        id: Log.newId(),
      }),
      [root],
    );
    const current = yield* project(genesis);
    const requester = Auth.requester(guarded.authenticated);
    const apiRefusal = yield* Policy.gateWrite("refs/heads/main").pipe(Effect.provide(requester));
    const gitVerdict = yield* Policy.gate([{ name: "refs/heads/main", value: null }], true).pipe(
      Effect.provide(requester),
    );
    assert.notEqual(apiRefusal, null);
    assert.equal(
      yield* Policy.permitsRequester("source.push").pipe(Effect.provide(requester)),
      false,
    );
    return {
      currentMembers: current.members.size,
      revoked: current.revoked.has(signer),
      apiRefusal,
      gitRefusals: gitVerdict.refused,
    };
  }).pipe(
    Effect.provide(
      Layer.mergeAll(
        Repository.layer.pipe(Layer.provide(Repository.hooksNoop), Layer.provideMerge(stores)),
        Auth.noncesInMemory(),
        Auth.requestAudience("host"),
      ),
    ),
  ),
);
