import assert from "node:assert/strict";
import { describe, it } from "@effect/vitest";

import { Effect, Layer } from "effect";

import { fingerprint, formatPublicKey, generate, NAMESPACE, sign } from "../crypto/SshSignature.ts";
import { EMPTY_TREE_OID } from "../git/Format.ts";
import { stores } from "../git/Memory.ts";
import * as GitRepository from "../git/Repository.ts";
import { Repository } from "../git/Repository.ts";
import * as PullRequest from "../hub/PullRequest.ts";
import { approvals, project as projectPullRequest } from "../hub/Projection.ts";
import * as Certificate from "./Certificate.ts";
import { create, signGenesis, writeGenesis } from "./Genesis.ts";
import * as Log from "./Log.ts";
import {
  authorizeKey,
  identitiesInMemory,
  identifyKey,
  principalId,
  type ResolvedIdentity,
} from "./Principal.ts";
import { project } from "./Projection.ts";
import * as Verify from "./Verify.ts";

const scenario = <A, E>(effect: Effect.Effect<A, E, Repository>) =>
  Effect.runPromise(
    effect.pipe(
      Effect.provide(
        GitRepository.layer.pipe(Layer.provide(GitRepository.hooksNoop), Layer.provide(stores)),
      ),
    ),
  );

describe("principal membership", () => {
  it.effect("resolves a project grant through the principal's current identity log", () =>
    Effect.promise(async () => {
      const identity = await scenario(
        Effect.gen(function* () {
          const root = yield* generate("identity-root@example.com");
          const oldDevice = yield* generate("old-device@example.com");
          const newDevice = yield* generate("new-device@example.com");
          const genesis = yield* create([formatPublicKey(root.publicKey)], 1);
          yield* writeGenesis(genesis, [yield* signGenesis(genesis, root)]);

          const oldGrant = yield* Certificate.grant({
            repo: genesis.repoId,
            publicKey: formatPublicKey(oldDevice.publicKey),
            capabilities: [],
            id: Log.newId(),
          });
          yield* Log.issue(oldGrant, [root]);
          yield* Log.issue(
            Certificate.revoke({
              repo: genesis.repoId,
              subject: oldGrant.subject,
              reason: "rotated",
              id: Log.newId(),
            }),
            [root],
          );
          yield* Log.issue(
            yield* Certificate.grant({
              repo: genesis.repoId,
              publicKey: formatPublicKey(newDevice.publicKey),
              capabilities: [],
              id: Log.newId(),
            }),
            [root],
          );

          return {
            id: principalId(genesis.repoId),
            key: newDevice,
            old: yield* fingerprint(oldDevice.publicKey),
            current: yield* fingerprint(newDevice.publicKey),
            projection: yield* project(genesis),
          };
        }),
      );

      const resolved: ResolvedIdentity = {
        principal: identity.id,
        projection: identity.projection,
        head: identity.projection.head,
      };
      const result = await scenario(
        Effect.gen(function* () {
          const root = yield* generate("project-root@example.com");
          const marker = yield* generate("marker@example.com");
          const genesis = yield* create([formatPublicKey(root.publicKey)], 1);
          yield* writeGenesis(genesis, [yield* signGenesis(genesis, root)]);
          const beforeGrant = yield* Log.issue(
            yield* Certificate.grant({
              repo: genesis.repoId,
              publicKey: formatPublicKey(marker.publicKey),
              capabilities: [],
              id: Log.newId(),
            }),
            [root],
          );
          const principalGrant = yield* Log.issue(
            yield* Certificate.grantPrincipal({
              repo: genesis.repoId,
              principal: identity.id,
              hints: ["https://identity.example/alice"],
              capabilities: ["source.push"],
              id: Log.newId(),
            }),
            [root],
          );
          const projectProjection = yield* project(genesis);
          const bytes = new TextEncoder().encode("stored event");
          const signature = yield* sign(identity.key, bytes, NAMESPACE);
          return {
            old: yield* authorizeKey({
              projection: projectProjection,
              signer: identity.old,
              capability: "source.push",
            }),
            current: yield* authorizeKey({
              projection: projectProjection,
              signer: identity.current,
              capability: "source.push",
            }),
            boundary: yield* Verify.authorizeKey({
              projection: projectProjection,
              signer: identity.current,
              capability: "source.push",
            }),
            identityOnly: yield* identifyKey({
              principal: identity.id,
              signer: identity.current,
            }),
            beforeGrant: yield* Verify.authorize({
              projection: projectProjection,
              bytes,
              signatures: [signature],
              capability: "source.push",
              made: { at: new Date(), trustHead: beforeGrant },
            }),
            afterGrant: yield* Verify.authorize({
              projection: projectProjection,
              bytes,
              signatures: [signature],
              capability: "source.push",
              made: { at: new Date(), trustHead: principalGrant },
            }),
          };
        }).pipe(Effect.provide(identitiesInMemory([resolved]))),
      );

      assert.equal(result.old.ok, false, "one identity-log revocation reaches every project grant");
      assert.equal(result.current.ok, true);
      assert.equal(result.current.ok ? result.current.identity.principal : null, identity.id);
      assert.equal(result.boundary.ok, true, "the normal policy/auth seam resolves principals too");
      assert.equal(result.identityOnly.ok, true, "an external statement can prove identity alone");
      assert.equal(
        result.beforeGrant.ok,
        false,
        "granting a PrincipalID does not retroactively authorize its old signatures",
      );
      assert.equal(result.afterGrant.ok, true, "the grant applies once the event reaches it");
      assert.equal(
        result.afterGrant.ok ? result.afterGrant.identity?.principal : null,
        identity.id,
        "stored-event authorization retains the stable identity it resolved through",
      );
    }),
  );

  it.effect("does not count two devices of one PrincipalID as independent self-review", () =>
    Effect.promise(async () => {
      const identity = await scenario(
        Effect.gen(function* () {
          const root = yield* generate("identity-root@example.com");
          const laptop = yield* generate("laptop@example.com");
          const phone = yield* generate("phone@example.com");
          const genesis = yield* create([formatPublicKey(root.publicKey)], 1);
          yield* writeGenesis(genesis, [yield* signGenesis(genesis, root)]);
          for (const device of [laptop, phone]) {
            yield* Log.issue(
              yield* Certificate.grant({
                repo: genesis.repoId,
                publicKey: formatPublicKey(device.publicKey),
                capabilities: [],
                id: Log.newId(),
              }),
              [root],
            );
          }
          const projection = yield* project(genesis);
          return {
            laptop,
            phone,
            resolved: {
              principal: principalId(genesis.repoId),
              projection,
              head: projection.head,
            } satisfies ResolvedIdentity,
          };
        }),
      );

      const result = await scenario(
        Effect.gen(function* () {
          const root = yield* generate("project-root@example.com");
          const genesis = yield* create([formatPublicKey(root.publicKey)], 1);
          yield* writeGenesis(genesis, [yield* signGenesis(genesis, root)]);
          yield* Log.issue(
            yield* Certificate.grantPrincipal({
              repo: genesis.repoId,
              principal: identity.resolved.principal,
              capabilities: ["hub.create-pr", "hub.approve"],
              id: Log.newId(),
            }),
            [root],
          );
          const opened = yield* PullRequest.open({
            repo: genesis.repoId,
            title: "same person, two devices",
            base: "refs/heads/main",
            head: EMPTY_TREE_OID,
            key: identity.laptop,
          });
          yield* PullRequest.review({
            repo: genesis.repoId,
            pr: opened.pr,
            head: EMPTY_TREE_OID,
            decision: "approve",
            key: identity.phone,
          });
          const state = yield* projectPullRequest(genesis, yield* project(genesis), opened.pr);
          return { state, approvals: approvals(state) };
        }).pipe(Effect.provide(identitiesInMemory([identity.resolved]))),
      );

      assert.equal(result.state.openerPrincipals.has(identity.resolved.principal), true);
      assert.equal(result.state.reviews[0]?.principal, identity.resolved.principal);
      assert.equal(result.approvals.length, 0);
    }),
  );

  it.effect(
    "quarantines rather than rejects when an identity repository is not available yet",
    () =>
      Effect.promise(async () => {
        const outcome = await scenario(
          Effect.gen(function* () {
            const root = yield* generate("project-root@example.com");
            const device = yield* generate("device@example.com");
            const identityGenesis = yield* create([formatPublicKey(device.publicKey)], 1);
            const genesis = yield* create([formatPublicKey(root.publicKey)], 1);
            yield* writeGenesis(genesis, [yield* signGenesis(genesis, root)]);
            yield* Log.issue(
              yield* Certificate.grantPrincipal({
                repo: genesis.repoId,
                principal: principalId(identityGenesis.repoId),
                capabilities: ["hub.review"],
                id: Log.newId(),
              }),
              [root],
            );
            return yield* authorizeKey({
              projection: yield* project(genesis),
              signer: yield* fingerprint(device.publicKey),
              capability: "hub.review",
            });
          }),
        );

        assert.equal(outcome.ok, false);
        assert.equal(outcome.ok ? false : outcome.quarantined, true);
      }),
  );
});
