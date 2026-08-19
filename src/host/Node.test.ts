/**
 * What the per-repository gate is and is not allowed to hold up.
 *
 * The gate stands in for the Durable Object input gate, so it has to make a
 * push indivisible. It must *not* extend to writing a response body: those
 * finish at the client's pace, and a client that stops reading would take the
 * whole repository down with it. The two tests below are the two halves of
 * that: an unread body does not block anyone, and `gc` — the one caller that
 * deletes objects a body may still be reading — waits for it anyway.
 */
import assert from "node:assert/strict";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { afterAll, beforeAll, describe, it } from "@effect/vitest";

import { Effect, Layer } from "effect";

import { push } from "../client/Push.ts";
import { EMPTY_TREE_OID } from "../git/Format.ts";
import { stores as memoryStores } from "../git/Memory.ts";
import * as GitRepository from "../git/Repository.ts";
import { Repository } from "../git/Repository.ts";
import { enableHubUnder, grantMemberUnder } from "../testing/Hub.ts";
import { serve, type Server } from "./Node.ts";

let root: string;
let server: Server;

/** What these tests send: a blob to write, or nothing at all for `gc`. */
interface PostBody {
  readonly content?: string;
}

/** The fields these tests read back; the endpoints answer with more. */
interface PostReply {
  readonly oid?: string;
  readonly removed?: ReadonlyArray<string>;
}

const post = async (url: string, body: PostBody) => {
  const response = await fetch(url, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
  // SAFETY: these endpoints answer JSON objects, and `PostReply` claims only
  // the two optional fields the assertions below read.
  return { status: response.status, body: (await response.json()) as PostReply };
};

beforeAll(async () => {
  root = await fs.mkdtemp(path.join(os.tmpdir(), "host-node-"));
  server = await serve({ root, allowAnonymousWrites: true });
});

afterAll(async () => {
  await server.close();
  await fs.rm(root, { recursive: true, force: true });
});

describe("the per-repository gate", () => {
  it("answers other requests while a response body goes unread", async () => {
    const base = `${server.url}/gated`;

    // Big enough that the kernel's socket buffer cannot swallow it, so the
    // write really is outstanding while nobody reads.
    const big = await post(`${base}/blob`, { content: "x".repeat(4_000_000) });
    assert.equal(big.status, 200);

    const stalled = await fetch(`${base}/blob/${String(big.body.oid)}`);
    assert.equal(stalled.status, 200);

    try {
      // The body is deliberately never read. Before the gate stopped spanning
      // delivery this call hung until the test timed out.
      const refs = await fetch(`${base}/refs`, { signal: AbortSignal.timeout(5_000) });
      assert.equal(refs.status, 200);
      await refs.json();
    } finally {
      await stalled.body?.cancel();
    }
  });

  it("still lets a collection run once the reader has gone", async () => {
    const base = `${server.url}/collected`;

    const orphan = await post(`${base}/blob`, { content: "unreferenced\n" });
    const read = await fetch(`${base}/blob/${String(orphan.body.oid)}`);
    await read.arrayBuffer();

    const swept = await post(`${base}/gc`, {});
    assert.equal(swept.status, 200);
    assert.deepEqual(swept.body.removed, [orphan.body.oid]);
  });
});

/**
 * The router is built once per repository and the requester arrives with each
 * call. That is only safe if the second caller is judged as themselves, so
 * this is the test that would fail if the requester were ever baked back into
 * the layer graph the router is built from.
 */
describe("starting up", () => {
  it("reports a port it cannot bind instead of dying on it", async () => {
    // `listen` reports failure by emitting `error`, and with nobody
    // subscribed node promotes that to an uncaught exception — so a second
    // `serve` on a taken port took the whole process down, while the promise
    // this awaits never settled and the caller learned nothing at all.
    const taken = await serve({ root });
    try {
      const port = Number(new URL(taken.url).port);
      const refused = await serve({ root, port }).then(
        () => null,
        (error: Error) => String(error),
      );
      assert.notEqual(refused, null, "a port already in use is not a successful start");
      assert.match(refused ?? "", /EADDRINUSE/);
    } finally {
      await taken.close();
    }
  });
});

describe("a memoised router", () => {
  it("judges every request as whoever made it, not as the first caller", async () => {
    const repo = "requesters";
    const base = `${server.url}/${repo}`;

    const reader = await enableHubUnder(root, repo, ["repo.read"]);
    const writer = await grantMemberUnder(root, repo, reader.root, reader.repoId, [
      "repo.read",
      "source.push",
    ]);

    const write = (credential: string) =>
      fetch(`${base}/blob`, {
        method: "POST",
        headers: { "content-type": "application/json", authorization: `Bearer ${credential}` },
        body: JSON.stringify({ content: "hello\n" }),
      });

    // The writer goes first, so the router is built and memoised as them.
    assert.equal((await write(writer.credential)).status, 200);

    // The reader must still be refused through that same router…
    assert.equal((await write(reader.credential)).status, 403);

    // …and the writer must not have been demoted by the reader's turn.
    assert.equal((await write(writer.credential)).status, 200);
  });
});

/**
 * Two repositories under one root are two repositories, whatever they say
 * about themselves.
 *
 * A mirror is made by copying `refs/meta/trust/*`, so it has the same genesis
 * bytes and the same RepoID as its origin — and the memos that keep every
 * request from re-folding the trust log are keyed by repository. Keyed by what
 * the repository *says it is*, the two share every entry, and what they can
 * actually read need not agree: refs are applied without a connectivity check,
 * so a replica can hold a head whose objects never arrived and fold to no
 * members at all. Served for the origin, that answer is every revocation gone
 * and a private repository reporting itself as public.
 */
describe("a mirror beside its origin", () => {
  it("does not answer for the repository next to it", async () => {
    const origin = "origin";
    const member = await enableHubUnder(root, origin, ["repo.read"]);

    // A mirror whose replication was interrupted: every ref arrived, and the
    // commit the trust log points at did not. Refs are applied without a
    // connectivity check, so this is a state a replica really reaches.
    const mirror = path.join(root, "mirror");
    await fs.cp(path.join(root, origin), mirror, { recursive: true });
    const head = (
      await fs.readFile(path.join(mirror, "refs", "meta", "trust", "log"), "utf8")
    ).trim();
    await fs.rm(path.join(mirror, "objects", head.slice(0, 2), head.slice(2)), { force: true });

    // The mirror folds first, and folds to nothing it can read.
    await fetch(`${server.url}/mirror/refs`);

    // The origin must still be judged on its own trust log: it has a member,
    // that member holds `repo.read`, and nobody granted it to anonymous.
    assert.equal((await fetch(`${server.url}/${origin}/refs`)).status, 401);
    assert.equal(
      (
        await fetch(`${server.url}/${origin}/refs`, {
          headers: { authorization: `Bearer ${member.credential}` },
        })
      ).status,
      200,
    );
  });
});

/**
 * What a repository will tell a member who is not an administrator.
 *
 * The registries are the two places a repository says where it sends things.
 * Registering is charged `repo.admin` on both; reading them was charged
 * nothing at all, so anybody the repository let in at any level could read
 * every delivery URL and every remote it pushes to.
 */
describe("the administrative registries", () => {
  it("does not administer a repository nobody opened, identity or not", async () => {
    // A repository with no genesis has no membership to charge anything
    // against, and the guard lets every read through on one — exactly as a
    // plain git repository has always done. These verbs are not reads whatever
    // their method says, so "no genesis, no charge" handed a plain repository's
    // webhook delivery URLs to anybody who could reach it. The host's own
    // decision stands in for the membership there is none of.
    const plain = `${server.url}/plain-registries`;
    const seeded = await post(`${plain}/blob`, { content: "hello\n" });
    assert.equal(seeded.status, 200, "the repository exists and has no identity");

    // This server was started with `allowAnonymousWrites`, which is the
    // operator saying anybody may administer it.
    assert.equal((await fetch(`${plain}/webhooks`)).status, 200);

    const closed = await serve({ root: await fs.mkdtemp(path.join(os.tmpdir(), "closed-")) });
    try {
      const url = `${closed.url}/plain-registries`;
      await post(`${url}/blob`, { content: "hello\n" }).catch(() => null);
      const refused = await fetch(`${url}/webhooks`);
      assert.notEqual(refused.status, 200, "and a host that opened nothing administers nothing");
    } finally {
      await closed.close();
    }
  });

  it("does not show a reader where this repository sends things", async () => {
    const repo = "private-registries";
    const admin = await enableHubUnder(root, repo, ["repo.read", "repo.admin"]);
    const reader = await grantMemberUnder(root, repo, admin.root, admin.repoId, ["repo.read"]);
    const base = `${server.url}/${repo}`;

    const registered = await fetch(`${base}/webhooks`, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        authorization: `Bearer ${admin.credential}`,
      },
      body: JSON.stringify({
        url: "https://receiver.internal/hook",
        secret: "a-secret-long-enough",
      }),
    });
    assert.equal(registered.status, 200, await registered.clone().text());

    const asReader = await fetch(`${base}/webhooks`, {
      headers: { authorization: `Bearer ${reader.credential}` },
    });
    assert.notEqual(asReader.status, 200, "a reader is not shown the delivery URLs");
    assert.equal((await asReader.text()).includes("receiver.internal"), false);

    const asAdmin = await fetch(`${base}/webhooks`, {
      headers: { authorization: `Bearer ${admin.credential}` },
    });
    assert.equal(asAdmin.status, 200, "and an administrator still is");
    assert.ok((await asAdmin.text()).includes("receiver.internal"));

    const remotes = await fetch(`${base}/remotes`, {
      headers: { authorization: `Bearer ${reader.credential}` },
    });
    assert.notEqual(remotes.status, 200, "nor the list of where it pushes");
  });
});

/**
 * A standing instruction, end to end on a real host.
 *
 * The forwarding hook is composed beside webhook delivery rather than instead
 * of it, and the repository it pushes *from* is built when the push lands
 * rather than when the hook layer is. Both are the sort of wiring that
 * typechecks either way and silently does nothing when it is wrong, so this
 * pushes to one repository and waits for the other one to have it.
 */
describe("a remote configured to be sent to", () => {
  it("gets what a push landed, without anybody asking", async () => {
    const sender = `${server.url}/sender`;
    const receiver = `${server.url}/receiver`;

    // The receiver has to exist before anything is forwarded to it.
    const seeded = await post(`${receiver}/blob`, { content: "hello\n" });
    assert.equal(seeded.status, 200);

    const registered = await fetch(`${sender}/remotes`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        name: "downstream",
        url: receiver,
        sync: { mode: "push", refs: ["refs/heads/*"] },
      }),
    });
    assert.equal(registered.status, 200, await registered.clone().text());

    // A real push, because `post-receive` is what forwarding hangs off — the
    // same trigger webhook delivery uses, and the only moment at which a ref
    // has moved because somebody else said so.
    const made = await Effect.runPromise(
      Effect.gen(function* () {
        const repository = yield* Repository;
        const commit = yield* repository.commit({
          branch: "refs/heads/main",
          tree: EMPTY_TREE_OID,
          message: "forward me\n",
          author: {
            name: "Alice",
            email: "alice@example.com",
            at: new Date(1_700_000_000_000),
            offset: 0,
          },
        });
        yield* push({
          url: sender,
          refs: [{ local: "refs/heads/main", remote: "refs/heads/main" }],
        });
        return commit;
      }).pipe(
        Effect.provide(
          GitRepository.layer.pipe(
            Layer.provide(GitRepository.hooksNoop),
            Layer.provideMerge(memoryStores),
          ),
        ),
      ),
    );

    // Forwarding is detached from the response, so this is a wait rather than
    // an assertion on what has already happened.
    const deadline = Date.now() + 10_000;
    let arrived: string | undefined;
    while (arrived === undefined && Date.now() < deadline) {
      const listed = await fetch(`${receiver}/refs`);
      // SAFETY: the endpoint answers its own `refs` shape.
      const { refs } = (await listed.json()) as {
        readonly refs: ReadonlyArray<{ readonly name: string; readonly oid: string }>;
      };
      arrived = refs.find((ref) => ref.name === "refs/heads/main")?.oid;
      if (arrived === undefined) await new Promise((resolve) => setTimeout(resolve, 50));
    }

    assert.equal(arrived, made, "the branch the push moved is on the downstream remote");
  });
});
