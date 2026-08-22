/**
 * The stateless read path, proved at its seams.
 *
 * The claim `Snapshot.ts` makes is that a published snapshot plus the object
 * store is enough to serve a real clone — the same `Protocol`, the same
 * client, no writer involved. So the central test here *is* that clone: a
 * repository built through `Repository`, captured, and fetched back through
 * an HTTP bridge whose only server-side state is the snapshot and the
 * objects. The rest are the refusals that make the path safe: writes,
 * credentials, restricted repositories and undecodable snapshots all fall
 * through to `null`, which is the caller's cue to ask the writer instead.
 */
import assert from "node:assert/strict";
import { createServer } from "node:http";
import { describe, it } from "@effect/vitest";

import { Effect, Layer } from "effect";

import { fetchRepository } from "../client/Fetch.ts";
import { EMPTY_TREE_OID } from "../git/Format.ts";
import { stores } from "../git/Memory.ts";
import { PackStore } from "../git/Packed.ts";
import * as GitRepository from "../git/Repository.ts";
import { Repository } from "../git/Repository.ts";
import { ObjectStore, RefStore, Storage } from "../git/Store.ts";
import * as Certificate from "../trust/Certificate.ts";
import { create, signGenesis, writeGenesis } from "../trust/Genesis.ts";
import * as Log from "../trust/Log.ts";
import { formatPublicKey, generate } from "../crypto/SshSignature.ts";
import * as Snapshot from "./Snapshot.ts";

const repositoryLayer = GitRepository.layer.pipe(
  Layer.provide(GitRepository.hooksNoop),
  Layer.provideMerge(stores),
);

const alice = {
  name: "Alice",
  email: "alice@example.com",
  at: new Date(1_700_000_000_000),
  offset: 0,
};

/** A GET of the upload-pack advertisement, as a `git` client would send it. */
const advertisement = (base: string): Request =>
  new Request(`${base}/info/refs?service=git-upload-pack`);

describe("Snapshot", () => {
  it.effect("captures the refs and answers them back as a read-only store", () =>
    Effect.promise(async () => {
      const outcome = await Effect.runPromise(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const first = yield* repository.commit({
            branch: "refs/heads/main",
            tree: EMPTY_TREE_OID,
            message: "first",
            author: alice,
          });
          yield* repository.commit({
            branch: "refs/heads/topic",
            tree: EMPTY_TREE_OID,
            message: "aside",
            author: alice,
          });

          const captured = yield* Snapshot.capture();
          const decoded = Snapshot.decode(Snapshot.encode(captured));
          assert.notEqual(decoded, null, "a capture round-trips through its own encoding");

          const outcome_ = yield* Effect.gen(function* () {
            const refs = yield* RefStore;
            const listed = yield* refs.list();
            const resolved = yield* refs.resolve("HEAD");
            const refused = yield* refs
              .apply([{ name: "refs/heads/main", value: null }])
              .pipe(Effect.flip);
            return { listed, resolved, refused: refused._tag };
          }).pipe(Effect.provide(Snapshot.refStore(captured)));

          return { first, captured, ...outcome_ };
        }).pipe(Effect.provide(repositoryLayer)),
      );

      assert.equal(outcome.captured.anonymousRead, true, "no identity means a public repository");
      assert.deepEqual(
        outcome.listed.map(([name]) => name),
        ["refs/heads/main", "refs/heads/topic"],
      );
      assert.equal(outcome.resolved, outcome.first, "HEAD follows the captured symref");
      assert.equal(outcome.refused, "StorageFailure", "a snapshot refuses to move refs");
    }),
  );

  it.effect("judges anonymous readability from the trust state at capture time", () =>
    Effect.promise(async () => {
      const readabilityOf = (capabilities: ReadonlyArray<string>) =>
        Effect.runPromise(
          Effect.gen(function* () {
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
            return (yield* Snapshot.capture()).anonymousRead;
          }).pipe(Effect.provide(repositoryLayer)),
        );

      // The same rule the guard applies: a `repo.read` grant restricts, and a
      // membership that never granted reading leaves the repository public.
      assert.equal(await readabilityOf(["repo.read", "source.push"]), false);
      assert.equal(await readabilityOf(["source.push"]), true);
    }),
  );

  it.effect("serves a real clone from the snapshot and the objects, with no writer", () =>
    Effect.promise(async () => {
      // The repository, built through the ordinary writer surface…
      const source = await Effect.runPromise(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const objects = yield* ObjectStore;
          const packs = yield* PackStore;

          const readme = yield* repository.writeBlob(new TextEncoder().encode("# served\n"));
          const tree = yield* repository.writeTree([
            { mode: "100644", name: "readme.md", oid: readme },
          ]);
          const first = yield* repository.commit({
            branch: "refs/heads/main",
            tree,
            message: "publish me",
            author: alice,
          });

          const snapshot = yield* Snapshot.capture();
          return { first, objects, packs, snapshot };
        }).pipe(Effect.provide(repositoryLayer)),
      );

      // …then served with the writer gone: only the snapshot's view of the
      // refs and the same object store remain, which is exactly what the
      // front Worker holds.
      const reader = GitRepository.layer.pipe(
        Layer.provide(GitRepository.hooksNoop),
        Layer.provideMerge(
          Layer.mergeAll(
            Layer.succeed(ObjectStore)(source.objects),
            Layer.succeed(PackStore)(source.packs),
            Snapshot.refStore(source.snapshot),
            Layer.succeed(Storage)("r"),
          ),
        ),
      );

      const server = createServer((incoming, outgoing) => {
        void (async () => {
          const chunks: Buffer[] = [];
          for await (const chunk of incoming) chunks.push(Buffer.from(chunk));
          const headers = new Headers();
          for (const [name, value] of Object.entries(incoming.headers)) {
            if (value === undefined) continue;
            headers.set(name, Array.isArray(value) ? value.join(", ") : value);
          }
          const body = Buffer.concat(chunks);
          const request = new Request(`http://127.0.0.1${incoming.url ?? "/"}`, {
            method: incoming.method,
            headers,
            body: body.length > 0 ? new Uint8Array(body) : undefined,
          });
          const response = await Effect.runPromise(
            Snapshot.serve(request).pipe(
              Effect.provide(reader),
              Effect.orElseSucceed((): Response | null => null),
            ),
          );
          if (response === null) {
            outgoing.writeHead(404);
            outgoing.end("the stateless path does not answer this");
            return;
          }
          outgoing.writeHead(response.status, Object.fromEntries(response.headers));
          outgoing.end(Buffer.from(await response.arrayBuffer()));
        })();
      });
      await new Promise<void>((resolve) => server.listen(0, resolve));
      const address = server.address();
      // A string address is a pipe, which `listen(0)` never produces.
      const port = address !== null && address instanceof Object ? address.port : null;
      if (port === null) throw new Error("no port");
      const url = `http://127.0.0.1:${String(port)}/r`;

      try {
        const cloned = await Effect.runPromise(
          Effect.gen(function* () {
            const target = { objects: yield* ObjectStore, refs: yield* RefStore };
            const fetched = yield* fetchRepository({ url, stores: target });
            const repository = yield* Repository;
            const main = yield* repository.resolve("refs/heads/main");
            const commit = main === null ? null : yield* repository.readCommit(main);
            return { fetched, main, message: commit?.message ?? "" };
          }).pipe(Effect.provide(repositoryLayer)),
        );

        assert.equal(cloned.main, source.first, "the clone lands on the published tip");
        assert.match(cloned.message, /publish me/);
        assert.equal(cloned.fetched.defaultBranch, "main", "HEAD travels with the snapshot");
        assert.equal(cloned.fetched.rejected.length, 0);
      } finally {
        await new Promise<void>((resolve) => server.close(() => resolve()));
      }
    }),
  );

  it.effect(
    "journals each state with the movement that produced it, and restores any of them",
    () =>
      Effect.promise(async () => {
        const outcome = await Effect.runPromise(
          Effect.gen(function* () {
            const repository = yield* Repository;

            // Three states: main born, topic born beside it, topic gone again.
            const first = yield* repository.commit({
              branch: "refs/heads/main",
              tree: EMPTY_TREE_OID,
              message: "first",
              author: alice,
            });
            const one = Snapshot.entryOf(1, yield* Snapshot.capture(), undefined);

            const second = yield* repository.commit({
              branch: "refs/heads/topic",
              tree: EMPTY_TREE_OID,
              message: "aside",
              author: alice,
            });
            const two = Snapshot.entryOf(2, yield* Snapshot.capture(one), one);

            yield* repository.setRef({ name: "refs/heads/main", to: second });
            const three = Snapshot.entryOf(3, yield* Snapshot.capture(two), two);

            return { first, second, one, two, three };
          }).pipe(Effect.provide(repositoryLayer)),
        );

        // Each entry explains exactly what moved since the one before it.
        assert.deepEqual(outcome.one.changes, [
          { name: "refs/heads/main", from: null, to: outcome.first },
        ]);
        assert.deepEqual(outcome.two.changes, [
          { name: "refs/heads/topic", from: null, to: outcome.second },
        ]);
        assert.deepEqual(outcome.three.changes, [
          { name: "refs/heads/main", from: outcome.first, to: outcome.second },
        ]);

        // An entry round-trips through its own encoding, and garbage does not.
        assert.deepEqual(Snapshot.decodeJournal(Snapshot.encodeJournal(outcome.two)), outcome.two);
        assert.equal(Snapshot.decodeJournal(new TextEncoder().encode("[]")), null);

        // Restoring the middle entry into an empty store rebuilds that point in
        // time — refs and HEAD — not the state the repository went on to reach.
        const restored = await Effect.runPromise(
          Effect.gen(function* () {
            yield* Snapshot.restore(outcome.two);
            const refs = yield* RefStore;
            return { listed: yield* refs.list(), head: yield* refs.head };
          }).pipe(Effect.provide(stores)),
        );
        assert.deepEqual(restored.listed, [
          ["refs/heads/main", outcome.first],
          ["refs/heads/topic", outcome.second],
        ]);
        assert.equal(restored.head, "refs/heads/main");

        // And the key layout keeps the journal listable in order.
        assert.equal(Snapshot.journalKeyOf("r", 7), "r/meta/journal/0000000007.json");
        assert.ok(Snapshot.journalKeyOf("r", 99) < Snapshot.journalKeyOf("r", 100));
      }),
  );

  it.effect("answers only anonymous upload-pack conversations", () =>
    Effect.sync(() => {
      const base = "http://host/r";
      assert.equal(Snapshot.readable(advertisement(base)), true);
      assert.equal(
        Snapshot.readable(new Request(`${base}/git-upload-pack`, { method: "POST" })),
        true,
      );

      // Everything else belongs to the writer: pushes, credentials, the
      // receive-pack advertisement a push begins with, and the JSON API.
      assert.equal(
        Snapshot.readable(new Request(`${base}/git-receive-pack`, { method: "POST" })),
        false,
      );
      assert.equal(
        Snapshot.readable(new Request(`${base}/info/refs?service=git-receive-pack`)),
        false,
      );
      assert.equal(
        Snapshot.readable(
          new Request(`${base}/info/refs?service=git-upload-pack`, {
            headers: { authorization: "Bearer something" },
          }),
        ),
        false,
      );
      assert.equal(Snapshot.readable(new Request(`${base}/refs`)), false);
      assert.equal(Snapshot.decode(new TextEncoder().encode("not a snapshot")), null);
    }),
  );
});
