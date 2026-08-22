/**
 * The browser story, simulated end to end: a served repository, the derived
 * JSON client over real HTTP, a smart-HTTP clone into OPFS-shaped stores,
 * and the same `Repository` service walking the clone offline.
 */
import assert from "node:assert/strict";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { afterAll, beforeAll, describe, it } from "@effect/vitest";

import { Effect, Layer, Stream } from "effect";

import { fakeRoot } from "../adapters/Opfs.fake.ts";
import * as Opfs from "../adapters/Opfs.ts";
import { stores as nodeStores } from "../git/Node.ts";
import * as GitRepository from "../git/Repository.ts";
import { Repository } from "../git/Repository.ts";
import { ObjectStore, type Oid, RefStore } from "../git/Store.ts";
import { serve, type Server } from "../host/Node.ts";
import { mintDelegation } from "../server/Auth.ts";
import { formatPublicKey, generate } from "../crypto/SshSignature.ts";
import * as Certificate from "../trust/Certificate.ts";
import { create, signGenesis, writeGenesis } from "../trust/Genesis.ts";
import * as Log from "../trust/Log.ts";
import { fetchRepository } from "./Fetch.ts";
import { local, remote } from "./Client.ts";

const author = {
  name: "Alice",
  email: "alice@example.com",
  at: new Date(1_700_000_000_000),
  offset: 0,
};

describe("Client", () => {
  let root: string;
  let server: Server;
  let head: Oid;

  beforeAll(async () => {
    root = await fs.mkdtemp(path.join(os.tmpdir(), "client-"));
    server = await serve({ root, allowAnonymousWrites: true });
    head = await Effect.runPromise(
      Effect.gen(function* () {
        const repository = yield* Repository;
        const blob = yield* repository.writeBlob(new TextEncoder().encode("browser bound\n"));
        const tree = yield* repository.writeTree([{ mode: "100644", name: "b.txt", oid: blob }]);
        yield* repository.commit({ branch: "main", tree, message: "first", author });
        return yield* repository.commit({ branch: "main", tree, message: "second", author });
      }).pipe(
        Effect.provide(
          GitRepository.layer.pipe(
            Layer.provide(GitRepository.hooksNoop),
            Layer.provide(nodeStores(path.join(root, "origin"))),
          ),
        ),
      ),
    );
  });

  afterAll(async () => {
    await server.close();
    await fs.rm(root, { recursive: true, force: true });
  });

  it.effect("drives the JSON API through the derived client, over real HTTP", () =>
    Effect.promise(async () => {
      const { commits, refs } = await Effect.runPromise(
        Effect.gen(function* () {
          const client = yield* remote(server.url);
          const refs = yield* client.repo.refs({ params: { repo: "origin" } });
          const log = yield* client.repo.log({ params: { repo: "origin", oid: head } });
          return { refs: refs.refs, commits: log.commits };
        }).pipe(Effect.scoped),
      );
      assert.deepEqual(refs, [{ name: "refs/heads/main", oid: head }]);
      assert.deepEqual(
        commits.map((commit) => commit.message),
        ["second", "first"],
      );
    }),
  );

  it.effect("clones into OPFS stores and reads the history offline", () =>
    Effect.promise(async () => {
      const opfs = Opfs.stores(fakeRoot());

      const messages = await Effect.runPromise(
        Effect.gen(function* () {
          // Online half: clone over smart HTTP into the browser's storage.
          const target = { objects: yield* ObjectStore, refs: yield* RefStore };
          const cloned = yield* fetchRepository({ url: `${server.url}/origin`, stores: target });
          assert.equal(cloned.defaultBranch, "main");

          // Offline half: the same Repository service, no server in sight.
          return yield* Effect.gen(function* () {
            const repository = yield* Repository;
            const main = yield* repository.resolve("refs/heads/main");
            assert.equal(main, head);
            const commits = yield* Stream.runCollect(repository.log(main!, { limit: 10 }));
            return commits.map((commit) => commit.message);
          }).pipe(Effect.provide(local(opfs)));
        }).pipe(Effect.provide(opfs)),
      );

      assert.deepEqual(messages, ["second", "first"]);
    }),
  );

  it.effect("sends its credential on every derived-client request", () =>
    Effect.promise(async () => {
      const authRoot = await fs.mkdtemp(path.join(os.tmpdir(), "client-auth-"));
      const authed = await serve({ root: authRoot, allowAnonymousWrites: true });
      try {
        // A repository with a genesis and one member who may read: that is what
        // makes it private, and there is no server secret involved anywhere.
        const token = await Effect.runPromise(
          Effect.gen(function* () {
            const repository = yield* Repository;
            const blob = yield* repository.writeBlob(new TextEncoder().encode("locked\n"));
            const tree = yield* repository.writeTree([
              { mode: "100644", name: "l.txt", oid: blob },
            ]);
            yield* repository.commit({ branch: "main", tree, message: "locked", author });

            const root = yield* generate("root@example.com");
            const genesis = yield* create([formatPublicKey(root.publicKey)], 1);
            yield* writeGenesis(genesis, [yield* signGenesis(genesis, root)]);

            const reader = yield* generate("reader@example.com");
            yield* Log.issue(
              yield* Certificate.grant({
                repo: genesis.repoId,
                publicKey: formatPublicKey(reader.publicKey),
                capabilities: ["repo.read"],
                id: Log.newId(),
              }),
              [root],
            );

            return yield* mintDelegation({
              key: reader,
              repo: genesis.repoId,
              capabilities: ["repo.read"],
              ttlSeconds: 300,
            });
          }).pipe(
            Effect.provide(
              GitRepository.layer.pipe(
                Layer.provide(GitRepository.hooksNoop),
                Layer.provide(nodeStores(path.join(authRoot, "vault"))),
              ),
            ),
          ),
        );

        const denied = await Effect.runPromise(
          Effect.gen(function* () {
            const client = yield* remote(authed.url);
            return yield* client.repo.refs({ params: { repo: "vault" } }).pipe(Effect.flip);
          }).pipe(Effect.scoped),
        );
        assert.ok(denied, "an anonymous derived client must be refused");

        const allowed = await Effect.runPromise(
          Effect.gen(function* () {
            const client = yield* remote(authed.url, { token });
            return yield* client.repo.refs({ params: { repo: "vault" } });
          }).pipe(Effect.scoped),
        );
        // The trust refs are refs too, so the branch is named rather than counted.
        assert.ok(
          allowed.refs.some((ref) => ref.name === "refs/heads/main"),
          `expected the branch among ${allowed.refs.map((ref) => ref.name).join(", ")}`,
        );
      } finally {
        await authed.close();
        await fs.rm(authRoot, { recursive: true, force: true });
      }
    }),
  );
});
