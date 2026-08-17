import assert from "node:assert/strict";
import { describe, it } from "@effect/vitest";

import { Effect, Layer } from "effect";

import { formatPublicKey, generate, type PrivateKey } from "../crypto/SshSignature.ts";
import { stores } from "../git/Memory.ts";
import * as GitRepository from "../git/Repository.ts";
import { Repository } from "../git/Repository.ts";
import {
  create,
  encodeDocument,
  GENESIS_REF,
  isRepoId,
  load,
  quorumMet,
  readGenesis,
  repoIdOf,
  rootSigners,
  signGenesis,
  writeGenesis,
} from "./Genesis.ts";

const scenario = <A, E>(effect: Effect.Effect<A, E, Repository>) =>
  Effect.runPromise(
    effect.pipe(
      Effect.provide(
        GitRepository.layer.pipe(
          Layer.provide(GitRepository.hooksNoop),
          Layer.provideMerge(stores),
        ),
      ),
    ),
  );

const keys = (count: number): Effect.Effect<ReadonlyArray<PrivateKey>> =>
  Effect.all(Array.from({ length: count }, (_, index) => generate(`root${index}@example.com`)));

const linesOf = (roots: ReadonlyArray<PrivateKey>): ReadonlyArray<string> =>
  roots.map((root) => formatPublicKey(root.publicKey));

describe("Genesis", () => {
  it("derives a stable RepoID from the stored bytes", async () => {
    const [genesis, again] = await Effect.runPromise(
      Effect.gen(function* () {
        const roots = yield* keys(3);
        const first = yield* create(linesOf(roots), 2);
        // The same document, loaded from the bytes rather than constructed.
        const second = yield* load(first.bytes);
        return [first, second];
      }),
    );

    assert.ok(isRepoId(genesis.repoId), `not a RepoID: ${genesis.repoId}`);
    assert.equal(again.repoId, genesis.repoId);
    assert.equal(again.document.threshold, 2);
    assert.equal(again.roots.length, 3);
  });

  it("hashes the bytes it was given, not a re-encoding of them", async () => {
    // A document whose stored form differs from this version's canonical
    // spelling — a host that wrote it with different whitespace. Identity must
    // follow the bytes, or every pinned entry pointing at it breaks.
    const stored = await Effect.runPromise(
      Effect.gen(function* () {
        const roots = yield* keys(1);
        const canonical = yield* create(linesOf(roots), 1);
        const compact = new TextEncoder().encode(
          JSON.stringify(JSON.parse(new TextDecoder().decode(canonical.bytes))),
        );
        return {
          canonical,
          loaded: yield* load(compact),
          expected: yield* repoIdOf(compact),
        };
      }),
    );

    assert.equal(stored.loaded.repoId, stored.expected);
    assert.notEqual(
      stored.loaded.repoId,
      stored.canonical.repoId,
      "different bytes must be a different identity",
    );
  });

  it("round-trips the canonical encoding", async () => {
    const bytes = await Effect.runPromise(
      Effect.gen(function* () {
        const roots = yield* keys(2);
        const genesis = yield* create(linesOf(roots), 2);
        return { written: genesis.bytes, re: encodeDocument(genesis.document) };
      }),
    );
    assert.deepEqual(bytes.re, bytes.written);
  });

  describe("validation", () => {
    it("refuses a threshold no set of keys can meet", async () => {
      const failure = await Effect.runPromise(
        Effect.gen(function* () {
          const roots = yield* keys(2);
          return yield* create(linesOf(roots), 3).pipe(Effect.flip);
        }),
      );
      assert.match(failure.reason, /exceeds 2 root keys/);
    });

    it("refuses a threshold below one", async () => {
      const failure = await Effect.runPromise(
        Effect.gen(function* () {
          const roots = yield* keys(1);
          return yield* create(linesOf(roots), 0).pipe(Effect.flip);
        }),
      );
      assert.match(failure.reason, /at least 1/);
    });

    it("refuses a repository with no root keys", async () => {
      const failure = await Effect.runPromise(create([], 1).pipe(Effect.flip));
      assert.match(failure.reason, /needs a root key/);
    });

    it("refuses the same key listed twice", async () => {
      const failure = await Effect.runPromise(
        Effect.gen(function* () {
          const [root] = yield* keys(1);
          const line = formatPublicKey(root!.publicKey);
          return yield* create([line, line], 2).pipe(Effect.flip);
        }),
      );
      assert.match(failure.reason, /duplicate root key/);
    });

    it("refuses a root key that is not a key", async () => {
      const failure = await Effect.runPromise(
        create(["ssh-ed25519 nonsense"], 1).pipe(Effect.flip),
      );
      assert.match(failure.reason, /bad root key/);
    });

    it("says so when the object format is one this version cannot serve", async () => {
      const document = new TextEncoder().encode(
        JSON.stringify({
          version: 1,
          objectFormat: "sha256",
          uuid: "00000000-0000-7000-8000-000000000000",
          rootKeys: [],
          threshold: 1,
        }),
      );
      const failure = await Effect.runPromise(load(document).pipe(Effect.flip));
      assert.match(failure.reason, /not supported in this version/);
    });

    it("reports malformed JSON as an invalid genesis", async () => {
      const failure = await Effect.runPromise(
        load(new TextEncoder().encode("{ not json")).pipe(Effect.flip),
      );
      assert.equal(failure._tag, "Invalid");
    });
  });

  describe("signatures", () => {
    it("counts distinct root signers towards the quorum", async () => {
      const outcome = await Effect.runPromise(
        Effect.gen(function* () {
          const roots = yield* keys(3);
          const genesis = yield* create(linesOf(roots), 2);
          const signatures = yield* Effect.all([
            signGenesis(genesis, roots[0]!),
            signGenesis(genesis, roots[1]!),
          ]);
          const signers = yield* rootSigners(genesis, signatures);
          return { genesis, signers };
        }),
      );

      assert.equal(outcome.signers.length, 2);
      assert.ok(quorumMet(outcome.genesis, outcome.signers));
    });

    it("does not let one root sign twice to reach a quorum", async () => {
      const outcome = await Effect.runPromise(
        Effect.gen(function* () {
          const roots = yield* keys(3);
          const genesis = yield* create(linesOf(roots), 2);
          // The same holder, signing twice: two armored blobs, one signer.
          const signatures = yield* Effect.all([
            signGenesis(genesis, roots[0]!),
            signGenesis(genesis, roots[0]!),
          ]);
          return { genesis, signers: yield* rootSigners(genesis, signatures) };
        }),
      );

      assert.equal(outcome.signers.length, 1);
      assert.equal(quorumMet(outcome.genesis, outcome.signers), false);
    });

    it("ignores a signature from a key that is not a root", async () => {
      const signers = await Effect.runPromise(
        Effect.gen(function* () {
          const roots = yield* keys(2);
          const stranger = yield* generate("stranger@example.com");
          const genesis = yield* create(linesOf(roots), 2);
          return yield* rootSigners(genesis, [
            yield* signGenesis(genesis, roots[0]!),
            yield* signGenesis(genesis, stranger),
          ]);
        }),
      );
      assert.equal(signers.length, 1);
    });

    it("ignores a signature over different bytes", async () => {
      const signers = await Effect.runPromise(
        Effect.gen(function* () {
          const roots = yield* keys(2);
          const genesis = yield* create(linesOf(roots), 2);
          const other = yield* create(linesOf(roots), 1);
          // Signed over a different genesis: valid armor, wrong document.
          return yield* rootSigners(genesis, [yield* signGenesis(other, roots[0]!)]);
        }),
      );
      assert.equal(signers.length, 0);
    });
  });

  it("gives two repositories built from the same key distinct identities", async () => {
    // One person setting up two projects uses the same key and the same
    // threshold for both. Hashing only those, the two shared a `RepoID` —
    // which breaks `known_repos` on both and lets a certificate or a hub event
    // bound to one verify against the other.
    const outcome = await Effect.runPromise(
      Effect.gen(function* () {
        const roots = yield* keys(1);
        const first = yield* create(linesOf(roots), 1);
        const second = yield* create(linesOf(roots), 1);
        return { first: first.repoId, second: second.repoId };
      }),
    );

    assert.notEqual(outcome.first, outcome.second);
  });

  it("keeps an identity stable when the same document is loaded again", async () => {
    const outcome = await Effect.runPromise(
      Effect.gen(function* () {
        const roots = yield* keys(1);
        const made = yield* create(linesOf(roots), 1);
        const read = yield* load(made.bytes);
        return { made: made.repoId, read: read.repoId };
      }),
    );

    assert.equal(outcome.read, outcome.made);
  });

  describe("storage", () => {
    it("writes the genesis and reads back the same identity", async () => {
      const outcome = await scenario(
        Effect.gen(function* () {
          const roots = yield* keys(3);
          const genesis = yield* create(linesOf(roots), 2);
          const signatures = yield* Effect.all([
            signGenesis(genesis, roots[0]!),
            signGenesis(genesis, roots[1]!),
          ]);
          yield* writeGenesis(genesis, signatures);

          const stored = yield* readGenesis();
          if (stored === null) throw new Error("expected a genesis");
          return {
            expected: genesis.repoId,
            got: stored.genesis.repoId,
            signers: yield* rootSigners(stored.genesis, stored.signatures),
            quorum: quorumMet(
              stored.genesis,
              yield* rootSigners(stored.genesis, stored.signatures),
            ),
          };
        }),
      );

      assert.equal(outcome.got, outcome.expected);
      assert.equal(outcome.signers.length, 2);
      assert.ok(outcome.quorum);
    });

    it("answers null for a repository that has no genesis", async () => {
      assert.equal(await scenario(readGenesis()), null);
    });

    it("refuses a genesis whose roots never signed it", async () => {
      // A `RepoID` says what a document hashes to; it does not say the roots
      // agreed to it. Left unchecked, a replica serving a genesis nobody's
      // roots signed was believed by every path that reads one — a client that
      // had pinned the identity would catch it, one meeting the repository for
      // the first time would not.
      const failure = await scenario(
        Effect.gen(function* () {
          const roots = yield* keys(2);
          const genesis = yield* create(linesOf(roots), 2);
          // One of the two the threshold asks for.
          yield* writeGenesis(genesis, [yield* signGenesis(genesis, roots[0]!)]);
          return yield* readGenesis().pipe(Effect.flip);
        }),
      );

      assert.equal(failure._tag, "Invalid");
      assert.match(failure.reason, /threshold is 2/);
    });

    it("ignores a signature from a key that is not a root", async () => {
      // It proves something true about a key nobody asked about, and treating
      // it as an error would let anyone break a genesis by appending their own.
      const outcome = await scenario(
        Effect.gen(function* () {
          const roots = yield* keys(1);
          const stranger = yield* keys(1);
          const genesis = yield* create(linesOf(roots), 1);
          yield* writeGenesis(genesis, [
            yield* signGenesis(genesis, stranger[0]!),
            yield* signGenesis(genesis, roots[0]!),
          ]);
          return yield* readGenesis();
        }),
      );

      assert.notEqual(outcome, null);
    });

    it("refuses to replace an identity a repository already has", async () => {
      const failure = await scenario(
        Effect.gen(function* () {
          const roots = yield* keys(1);
          const first = yield* create(linesOf(roots), 1);
          yield* writeGenesis(first, []);

          const usurper = yield* keys(1);
          const second = yield* create(linesOf(usurper), 1);
          return yield* writeGenesis(second, []).pipe(Effect.flip);
        }),
      );
      assert.equal(failure._tag, "RefConflict");
    });

    it("puts the genesis where a stock clone would replicate it", async () => {
      const ref = await scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const roots = yield* keys(1);
          yield* writeGenesis(yield* create(linesOf(roots), 1), []);
          return yield* repository.resolve(GENESIS_REF);
        }),
      );
      assert.notEqual(ref, null);
    });
  });
});
