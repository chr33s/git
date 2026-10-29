/**
 * What an exposure's trust verdict is a verdict *about*.
 *
 * `Audit.trust` promises to distinguish "nobody signed these bytes" from "the
 * signer's authority lapsed while the evidence stayed exactly as valid as it
 * was". Keeping that promise depends on judging a stored record against what
 * its signer held when they wrote it — which is what `Verify.Made` is for, and
 * which the audit did not supply: a key revoked on Tuesday made every exposure
 * it had ever signed read as untrusted from Wednesday.
 *
 * The forward-only case and the compromise case are both here, because they
 * have to come out differently: leaving a project does not reach backwards,
 * and a compromise does.
 */
import assert from "node:assert/strict";
import { describe, it } from "@effect/vitest";

import { Effect, Layer } from "effect";

import { fingerprint, formatPublicKey, generate, type PrivateKey } from "../crypto/SshSignature.ts";
import { type Signature } from "../git/Format.ts";
import { stores } from "../git/Memory.ts";
import * as GitRepository from "../git/Repository.ts";
import { Repository } from "../git/Repository.ts";
import * as Certificate from "../trust/Certificate.ts";
import { create, signGenesis, writeGenesis } from "../trust/Genesis.ts";
import * as Log from "../trust/Log.ts";
import { project as projectTrust } from "../trust/Projection.ts";
import * as Exposure from "./Exposure.ts";
import * as Pack from "./Pack.ts";

const SESSION = "0192f000-0000-7000-8000-000000000000";

const author: Signature = {
  name: "Runner",
  email: "runner@example.com",
  at: new Date(1_700_000_000_000),
  offset: 0,
};

const world = GitRepository.layer.pipe(
  Layer.provide(GitRepository.hooksNoop),
  Layer.provideMerge(stores),
);

const scenario = <A, E>(effect: Effect.Effect<A, E, Repository>): Promise<A> =>
  Effect.runPromise(effect.pipe(Effect.provide(world)));

/** A repository, a root, and an agent granted `hub.trace`. */
const enabled = Effect.fn("test.enabled")(function* () {
  const root = yield* generate("root@example.com");
  const agent = yield* generate("agent@example.com");

  const genesis = yield* create([formatPublicKey(root.publicKey)], 1);
  yield* writeGenesis(genesis, [yield* signGenesis(genesis, root)]);
  yield* Log.issue(
    yield* Certificate.grant({
      repo: genesis.repoId,
      publicKey: formatPublicKey(agent.publicKey),
      capabilities: ["repo.read", "hub.trace"],
      id: Log.newId(),
    }),
    [root],
  );
  return { genesis, root, agent } as const;
});

const expose = Effect.fn("test.expose")(function* (repo: string, key: PrivateKey) {
  const repository = yield* Repository;
  const commit = yield* repository.commitTree({
    tree: yield* repository.writeTree([]),
    parents: [],
    message: "root\n",
    author,
  });
  return yield* Exposure.expose({
    repo,
    session: SESSION,
    key,
    pack: { version: 1, view: yield* Pack.committed(commit), items: [] },
    segments: [
      { placement: "user", mediaType: "text/plain", body: new TextEncoder().encode("ask") },
    ],
    retain: false,
  });
});

describe("Context Exposure trust", () => {
  it.effect("keeps a record valid after its signer leaves", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const where = yield* enabled();
          const exposed = yield* expose(where.genesis.repoId, where.agent);

          const before = yield* Exposure.audit({
            commit: exposed.commit,
            repo: where.genesis.repoId,
            session: SESSION,
            trust: yield* projectTrust(where.genesis),
          });
          assert.equal(before.trust?.ok, true);

          // The agent leaves. A forward-only revocation reaches what they made
          // *after* they could see it — and this record predates it.
          yield* Log.issue(
            Certificate.revoke({
              repo: where.genesis.repoId,
              subject: yield* fingerprint(where.agent.publicKey),
              reason: "left",
              id: Log.newId(),
            }),
            [where.root],
          );

          const after = yield* Exposure.audit({
            commit: exposed.commit,
            repo: where.genesis.repoId,
            session: SESSION,
            trust: yield* projectTrust(where.genesis),
          });
          // The evidence never moved, and neither did the authority that was
          // behind it when it was written.
          assert.equal(after.trust?.ok, true, "a record validly signed then is still one now");
          assert.equal(after.signature.ok, true);
          assert.equal(after.evidence?.ok, true);
          assert.equal(after.ok, true);
        }),
      ),
    ),
  );

  it.effect("withdraws the verdict when the key was compromised all along", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const where = yield* enabled();
          const exposed = yield* expose(where.genesis.repoId, where.agent);

          // A compromise reaches backwards to when it began, because the
          // premise is that those signatures were never the subject's.
          yield* Log.issue(
            Certificate.revoke({
              repo: where.genesis.repoId,
              subject: yield* fingerprint(where.agent.publicKey),
              reason: "compromised",
              compromisedAt: new Date(0),
              id: Log.newId(),
            }),
            [where.root],
          );

          const after = yield* Exposure.audit({
            commit: exposed.commit,
            repo: where.genesis.repoId,
            session: SESSION,
            trust: yield* projectTrust(where.genesis),
          });
          assert.equal(after.trust?.ok, false);
          assert.equal(after.ok, false);
          // And the bytes are still exactly what they were: a lapsed authority
          // is not a drifted view, and the audit says so in separate fields.
          assert.equal(after.signature.ok, true);
          assert.equal(after.evidence?.ok, true);
        }),
      ),
    ),
  );

  it.effect("refuses a record signed by somebody who never held the capability", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const where = yield* enabled();
          const stranger = yield* generate("stranger@example.com");
          const exposed = yield* expose(where.genesis.repoId, stranger);

          const audited = yield* Exposure.audit({
            commit: exposed.commit,
            repo: where.genesis.repoId,
            session: SESSION,
            trust: yield* projectTrust(where.genesis),
          });
          assert.equal(audited.signature.ok, true);
          assert.equal(audited.trust?.ok, false);
          assert.equal(audited.ok, false);
          assert.match(
            audited.trust?.ok === false ? audited.trust.reason : "",
            /not a member of this repository/,
          );
        }),
      ),
    ),
  );
});
