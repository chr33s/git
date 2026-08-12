/**
 * The browser story, simulated end to end: a served repository, the derived
 * JSON client over real HTTP, a smart-HTTP clone into OPFS-shaped stores,
 * and the same `Repository` service walking the clone offline.
 */
import assert from "node:assert/strict";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { after, before, describe, it } from "node:test";

import { Effect, Layer, Stream } from "effect";

import { fakeRoot } from "../adapters/Opfs.fake.ts";
import * as Opfs from "../adapters/Opfs.ts";
import { stores as nodeStores } from "../git/Node.ts";
import * as GitRepository from "../git/Repository.ts";
import { Repository } from "../git/Repository.ts";
import { ObjectStore, type Oid, RefStore } from "../git/Store.ts";
import { serve, type Server } from "../host/Node.ts";
import { hmacMint, hmacVerify } from "../server/Auth.ts";
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

  before(async () => {
    root = await fs.mkdtemp(path.join(os.tmpdir(), "client-"));
    server = await serve({ root });
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
      ) as unknown as Effect.Effect<Oid>,
    );
  });

  after(async () => {
    await server.close();
    await fs.rm(root, { recursive: true, force: true });
  });

  it("drives the JSON API through the derived client, over real HTTP", async () => {
    const { commits, refs } = await Effect.runPromise(
      Effect.gen(function* () {
        const client = yield* remote(server.url);
        const refs = yield* client.repo.refs({ params: { repo: "origin" } });
        const log = yield* client.repo.log({ params: { repo: "origin", oid: head } });
        return { refs: refs.refs, commits: log.commits };
      }).pipe(Effect.scoped) as unknown as Effect.Effect<{
        refs: ReadonlyArray<{ name: string; oid: string }>;
        commits: ReadonlyArray<{ message: string }>;
      }>,
    );
    assert.deepEqual(refs, [{ name: "refs/heads/main", oid: head }]);
    assert.deepEqual(
      commits.map((commit) => commit.message),
      ["second", "first"],
    );
  });

  it("clones into OPFS stores and reads the history offline", async () => {
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
      }).pipe(Effect.provide(opfs)) as unknown as Effect.Effect<ReadonlyArray<string>>,
    );

    assert.deepEqual(messages, ["second", "first"]);
  });

  it("sends its token on every derived-client request", async () => {
    const secret = "client-secret";
    const authRoot = await fs.mkdtemp(path.join(os.tmpdir(), "client-auth-"));
    const authed = await serve({
      root: authRoot,
      verify: (repo, credential) => Effect.runPromise(hmacVerify(secret, repo, credential)),
    });
    try {
      await Effect.runPromise(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const blob = yield* repository.writeBlob(new TextEncoder().encode("locked\n"));
          const tree = yield* repository.writeTree([{ mode: "100644", name: "l.txt", oid: blob }]);
          return yield* repository.commit({ branch: "main", tree, message: "locked", author });
        }).pipe(
          Effect.provide(
            GitRepository.layer.pipe(
              Layer.provide(GitRepository.hooksNoop),
              Layer.provide(nodeStores(path.join(authRoot, "vault"))),
            ),
          ),
        ) as unknown as Effect.Effect<Oid>,
      );
      const token = await Effect.runPromise(hmacMint(secret, "vault", "read", 300));

      const denied = await Effect.runPromise(
        Effect.gen(function* () {
          const client = yield* remote(authed.url);
          return yield* client.repo.refs({ params: { repo: "vault" } }).pipe(Effect.flip);
        }).pipe(Effect.scoped) as unknown as Effect.Effect<unknown>,
      );
      assert.ok(denied, "an anonymous derived client must be refused");

      const allowed = await Effect.runPromise(
        Effect.gen(function* () {
          const client = yield* remote(authed.url, { token });
          return yield* client.repo.refs({ params: { repo: "vault" } });
        }).pipe(Effect.scoped) as unknown as Effect.Effect<{ refs: ReadonlyArray<unknown> }>,
      );
      assert.equal(allowed.refs.length, 1);
    } finally {
      await authed.close();
      await fs.rm(authRoot, { recursive: true, force: true });
    }
  });
});
