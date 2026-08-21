import assert from "node:assert/strict";
import { describe, it } from "@effect/vitest";

import { Effect, Layer } from "effect";

import { fingerprint, formatPublicKey, generate } from "../crypto/SshSignature.ts";
import { stores } from "../git/Memory.ts";
import * as GitRepository from "../git/Repository.ts";
import { Repository } from "../git/Repository.ts";
import * as Certificate from "../trust/Certificate.ts";
import { create, signGenesis, writeGenesis } from "../trust/Genesis.ts";
import * as TrustLog from "../trust/Log.ts";
import { principalId } from "../trust/Principal.ts";
import { project as projectTrust } from "../trust/Projection.ts";
import * as SocialLog from "./Log.ts";
import { follow } from "./Statement.ts";

const scenario = <A, E>(effect: Effect.Effect<A, E, Repository>) =>
  Effect.runPromise(
    effect.pipe(
      Effect.provide(
        GitRepository.layer.pipe(Layer.provide(GitRepository.hooksNoop), Layer.provide(stores)),
      ),
    ),
  );

describe("social log", () => {
  it.effect("appends signed statements and verifies them against the identity repository", () =>
    Effect.promise(async () => {
      const result = await scenario(
        Effect.gen(function* () {
          const root = yield* generate("alice@example.com");
          const genesis = yield* create([formatPublicKey(root.publicKey)], 1);
          yield* writeGenesis(genesis, [yield* signGenesis(genesis, root)]);
          yield* TrustLog.issue(
            yield* Certificate.grant({
              repo: genesis.repoId,
              publicKey: formatPublicKey(root.publicKey),
              capabilities: ["social.write"],
              id: TrustLog.newId(new Date("2026-08-20T00:00:00Z")),
            }),
            [root],
          );

          const trust = yield* projectTrust(genesis);
          const author = principalId(genesis.repoId);
          const first = yield* SocialLog.issue(
            follow({
              author,
              id: TrustLog.newId(new Date("2026-08-20T00:00:01Z")),
              socialHead: null,
              trustHead: trust.head,
              subject: author,
              petname: "alice",
              at: new Date("2026-08-20T00:00:01Z"),
            }),
            root,
          );
          const second = yield* SocialLog.issue(
            follow({
              author,
              id: TrustLog.newId(new Date("2026-08-20T00:00:02Z")),
              socialHead: first,
              trustHead: trust.head,
              subject: author,
              petname: "me",
              at: new Date("2026-08-20T00:00:02Z"),
            }),
            root,
          );
          const invalid = yield* SocialLog.issue(
            follow({
              author,
              id: TrustLog.newId(new Date("2026-08-20T00:00:03Z")),
              socialHead: null,
              trustHead: trust.head,
              subject: author,
              petname: "reset",
              at: new Date("2026-08-20T00:00:03Z"),
            }),
            root,
          );

          return {
            first,
            second,
            invalid,
            stored: yield* SocialLog.verified(genesis, trust),
            head: yield* Repository.pipe(
              Effect.flatMap((repository) => repository.resolve(SocialLog.LOG_REF)),
            ),
          };
        }),
      );

      assert.equal(result.head, result.invalid);
      assert.deepEqual(
        result.stored.statements.map(({ payload }) => payload.type),
        ["social.follow", "social.follow"],
      );
      assert.equal(result.stored.statements[0]?.commit, result.first);
      assert.deepEqual(result.stored.rejected, [
        {
          commit: result.invalid,
          reason: "only a first social statement may declare an empty social head",
        },
      ]);
    }),
  );

  it.effect("does not accept a statement claiming another principal as its author", () =>
    Effect.promise(async () => {
      const result = await scenario(
        Effect.gen(function* () {
          const root = yield* generate("alice@example.com");
          const otherRoot = yield* generate("bob@example.com");
          const genesis = yield* create([formatPublicKey(root.publicKey)], 1);
          const other = yield* create([formatPublicKey(otherRoot.publicKey)], 1);
          yield* writeGenesis(genesis, [yield* signGenesis(genesis, root)]);
          yield* TrustLog.issue(
            yield* Certificate.grant({
              repo: genesis.repoId,
              publicKey: formatPublicKey(root.publicKey),
              capabilities: ["social.write"],
              id: TrustLog.newId(),
            }),
            [root],
          );
          const trust = yield* projectTrust(genesis);
          yield* SocialLog.issue(
            follow({
              author: principalId(other.repoId),
              id: TrustLog.newId(),
              socialHead: null,
              trustHead: trust.head,
              subject: principalId(other.repoId),
              petname: "bob",
            }),
            root,
          );
          return yield* SocialLog.verified(genesis, trust);
        }),
      );

      assert.equal(result.statements.length, 0);
      assert.match(result.rejected[0]?.reason ?? "", /author.*identity repository/i);
    }),
  );

  it.effect("holds a descendant statement to the newest trust head its ancestors saw", () =>
    Effect.promise(async () => {
      const result = await scenario(
        Effect.gen(function* () {
          const root = yield* generate("root@example.com");
          const revoked = yield* generate("revoked-device@example.com");
          const current = yield* generate("current-device@example.com");
          const genesis = yield* create([formatPublicKey(root.publicKey)], 1);
          yield* writeGenesis(genesis, [yield* signGenesis(genesis, root)]);
          for (const device of [revoked, current]) {
            yield* TrustLog.issue(
              yield* Certificate.grant({
                repo: genesis.repoId,
                publicKey: formatPublicKey(device.publicKey),
                capabilities: ["social.write"],
                id: TrustLog.newId(),
              }),
              [root],
            );
          }

          const beforeRevocation = yield* projectTrust(genesis);
          const author = principalId(genesis.repoId);
          const first = yield* SocialLog.issue(
            follow({
              author,
              id: TrustLog.newId(),
              socialHead: null,
              trustHead: beforeRevocation.head,
              subject: author,
              petname: "before",
            }),
            revoked,
          );

          yield* TrustLog.issue(
            Certificate.revoke({
              repo: genesis.repoId,
              subject: yield* fingerprint(revoked.publicKey),
              reason: "rotated",
              id: TrustLog.newId(),
            }),
            [root],
          );
          const afterRevocation = yield* projectTrust(genesis);
          const second = yield* SocialLog.issue(
            follow({
              author,
              id: TrustLog.newId(),
              socialHead: first,
              trustHead: afterRevocation.head,
              subject: author,
              petname: "floor",
            }),
            current,
          );
          const planted = yield* SocialLog.issue(
            follow({
              author,
              id: TrustLog.newId(),
              socialHead: second,
              trustHead: beforeRevocation.head,
              subject: author,
              petname: "backdated",
            }),
            revoked,
          );
          return { planted, verified: yield* SocialLog.verified(genesis, afterRevocation) };
        }),
      );

      assert.deepEqual(
        result.verified.statements.flatMap((entry) =>
          entry.payload.type === "social.follow" ? [entry.payload.petname] : [],
        ),
        ["before", "floor"],
      );
      assert.equal(
        result.verified.rejected.some((entry) => entry.commit === result.planted),
        true,
      );
      assert.match(
        result.verified.rejected.find((entry) => entry.commit === result.planted)?.reason ?? "",
        /revoked/,
      );
    }),
  );
});
