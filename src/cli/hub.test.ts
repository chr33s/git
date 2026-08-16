/**
 * The `hub` commands, driven as a user drives them: a real process, real key
 * files on disk, real repositories under a root.
 *
 * End to end rather than unit, because the thing worth checking is that the
 * pieces line up — that a key `hub init` accepted is one `hub grant` can sign
 * with, and that what `hub members` prints is what the projection actually
 * holds.
 */
import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { promisify } from "node:util";
import { afterEach, beforeEach, describe, it } from "@effect/vitest";

import * as http from "node:http";

import { Effect, Layer } from "effect";

import { formatPublicKey } from "../crypto/SshSignature.ts";
import { stores as nodeStores } from "../git/Node.ts";
import * as GitRepository from "../git/Repository.ts";
import * as Protocol from "../server/Protocol.ts";
import { serve } from "../host/Node.ts";
import { enableHubUnder, shortOfQuorum, writeKeyPair } from "../testing/Hub.ts";

const execFileAsync = promisify(execFile);
const entry = path.join(import.meta.dirname, "bin.ts");

const cli = async (args: ReadonlyArray<string>): Promise<string> => {
  const result = await execFileAsync(process.execPath, [entry, ...args], { encoding: "utf8" });
  return `${result.stdout}${result.stderr}`;
};

/**
 * A remote that serves the smart-HTTP protocol and asks no questions.
 *
 * `host/Node.ts` runs the auth guard, which reads the repository's own genesis
 * and refuses to serve one it cannot verify — right for a repository it owns,
 * and the wrong model for a remote somebody else controls.
 */
const hostile = async (directory: string) => {
  const layer = GitRepository.layer.pipe(
    Layer.provide(GitRepository.hooksNoop),
    Layer.provide(nodeStores(directory)),
  );

  const answer = (
    incoming: http.IncomingMessage,
    body: Uint8Array<ArrayBuffer>,
  ): Promise<Response> => {
    const headers = new Headers();
    for (let at = 0; at + 1 < incoming.rawHeaders.length; at += 2) {
      headers.append(incoming.rawHeaders[at]!, incoming.rawHeaders[at + 1]!);
    }
    const url = `http://127.0.0.1${incoming.url ?? "/"}`;
    const request =
      incoming.method === "POST"
        ? new Request(url, { method: "POST", headers, body })
        : new Request(url, { headers });

    return Effect.runPromise(
      Protocol.handle(request).pipe(
        Effect.map((response) => response ?? new Response("not found", { status: 404 })),
        Effect.orElseSucceed(() => new Response("error", { status: 500 })),
        Effect.provide(layer),
      ),
    );
  };

  const server = http.createServer((incoming, outgoing) => {
    const chunks: Buffer[] = [];
    incoming.on("data", (chunk: Buffer) => chunks.push(chunk));
    incoming.on("end", () => {
      const joined = Buffer.concat(chunks);
      const body = new Uint8Array(new ArrayBuffer(joined.byteLength));
      body.set(joined);
      void answer(incoming, body)
        .then(async (response) => {
          outgoing.writeHead(response.status, Object.fromEntries(response.headers.entries()));
          outgoing.end(Buffer.from(await response.arrayBuffer()));
        })
        .catch(() => outgoing.end());
    });
  });

  const port = await new Promise<number>((resolve) => {
    server.listen(0, "127.0.0.1", () => {
      // SAFETY: the server listens on a TCP port, never a pipe, so `address()`
      // is an `AddressInfo` once the listen callback has fired.
      resolve((server.address() as import("node:net").AddressInfo).port);
    });
  });

  return {
    url: `http://127.0.0.1:${port}`,
    close: () => new Promise<void>((resolve) => server.close(() => resolve())),
  };
};

describe("cli hub", () => {
  let root = "";

  beforeEach(async () => {
    root = await fs.mkdtemp(path.join(os.tmpdir(), "cli-hub-"));
  });

  afterEach(async () => {
    await fs.rm(root, { recursive: true, force: true });
  });

  it("gives a repository an identity, then grants and lists membership", async () => {
    await cli(["init", "--root", root, "project"]);
    const rootKey = await writeKeyPair(path.join(root, "id_root"), "root@example.com");
    await writeKeyPair(path.join(root, "id_member"), "dev@example.com");

    const initialised = await cli([
      "hub",
      "init",
      "--root",
      root,
      "--key",
      path.join(root, "id_root"),
      "project",
    ]);
    assert.match(initialised, /SHA256:[A-Za-z0-9+/]{43}/);

    const granted = await cli([
      "hub",
      "grant",
      "--root",
      root,
      "--key",
      path.join(root, "id_root"),
      "--subject",
      path.join(root, "id_member.pub"),
      "--capability",
      "source.push,hub.review",
      "project",
    ]);
    assert.match(granted, /Granted source\.push,hub\.review to SHA256:/);

    const listed = await cli(["hub", "members", "--root", root, "project"]);
    assert.match(listed, /source\.push,hub\.review/);
    assert.ok(
      !listed.includes(formatPublicKey(rootKey.publicKey)),
      "a member list should name keys by fingerprint, not by their full body",
    );
    assert.ok(!listed.includes("ignored:"), `no record should be refused: ${listed}`);
  });

  it("revokes a member, and says so in the listing", async () => {
    await cli(["init", "--root", root, "project"]);
    await writeKeyPair(path.join(root, "id_root"), "root@example.com");
    await writeKeyPair(path.join(root, "id_member"), "dev@example.com");

    await cli(["hub", "init", "--root", root, "--key", path.join(root, "id_root"), "project"]);
    const granted = await cli([
      "hub",
      "grant",
      "--root",
      root,
      "--key",
      path.join(root, "id_root"),
      "--subject",
      path.join(root, "id_member.pub"),
      "--capability",
      "source.push",
      "project",
    ]);
    const subject = granted.trim().split(" ").at(-1) ?? "";

    const revoked = await cli([
      "hub",
      "revoke",
      "--root",
      root,
      "--key",
      path.join(root, "id_root"),
      "--subject",
      subject,
      "--reason",
      "left",
      "project",
    ]);
    assert.match(revoked, /Revoked SHA256:/);

    const listed = await cli(["hub", "members", "--root", root, "project"]);
    assert.match(listed, /revoked \(left\)/);
  });

  it("refuses a subject that is not a fingerprint, and says what one looks like", async () => {
    await cli(["init", "--root", root, "project"]);
    await writeKeyPair(path.join(root, "id_root"), "root@example.com");
    await cli(["hub", "init", "--root", root, "--key", path.join(root, "id_root"), "project"]);

    const failed = await cli([
      "hub",
      "revoke",
      "--root",
      root,
      "--key",
      path.join(root, "id_root"),
      "--subject",
      "dev@example.com",
      "project",
    ]).then(
      () => null,
      (error: { stderr?: string; stdout?: string }) => `${error.stdout ?? ""}${error.stderr ?? ""}`,
    );

    assert.notEqual(failed, null, "a bad subject must fail rather than write a record");
    assert.match(failed ?? "", /not a key fingerprint/);
  });

  it("leaves the root key able to push, not merely able to grant", async () => {
    // The regression: `hub init` wrote a genesis and stopped. Root authority
    // is the power to change membership, not membership itself, so the
    // repository came out with zero members — every push refused, the root
    // holder's included, while reads stayed open because a repository that
    // has restricted nothing has restricted nobody.
    await cli(["init", "--root", root, "project"]);
    await writeKeyPair(path.join(root, "id_root"), "root@example.com");

    const initialised = await cli([
      "hub",
      "init",
      "--root",
      root,
      "--key",
      path.join(root, "id_root"),
      "project",
    ]);
    assert.match(initialised, /holds repo\.admin/);

    const listed = await cli(["hub", "members", "--root", root, "project"]);
    assert.match(listed, /repo\.admin/);
    assert.ok(!listed.includes("ignored:"), `no record should be refused: ${listed}`);
  });

  it("fails rather than reporting success for a capability the fold refuses", async () => {
    // The regression: `Log.issue` storing the bytes was read as the grant
    // taking effect. A typo in a capability name left the CLI printing
    // "Granted", a rejected record in an append-only log, and a repository
    // whose membership was untouched.
    await cli(["init", "--root", root, "project"]);
    await writeKeyPair(path.join(root, "id_root"), "root@example.com");
    await writeKeyPair(path.join(root, "id_member"), "dev@example.com");
    await cli(["hub", "init", "--root", root, "--key", path.join(root, "id_root"), "project"]);

    const failed = await cli([
      "hub",
      "grant",
      "--root",
      root,
      "--key",
      path.join(root, "id_root"),
      "--subject",
      path.join(root, "id_member.pub"),
      "--capability",
      "source.pus",
      "project",
    ]).then(
      () => null,
      (error: { stderr?: string; stdout?: string }) => `${error.stdout ?? ""}${error.stderr ?? ""}`,
    );

    assert.notEqual(failed, null, "a refused grant must not exit 0 saying it worked");
    assert.match(failed ?? "", /source\.pus/);
  });

  it("administers a repository whose threshold needs two signatures", async () => {
    // A single `--key` made a quorum repository impossible to administer:
    // every record was written, refused for want of a quorum, and left in the
    // log with nothing to show for it.
    await cli(["init", "--root", root, "quorum"]);
    await writeKeyPair(path.join(root, "id_a"), "a@example.com");
    await writeKeyPair(path.join(root, "id_b"), "b@example.com");
    await writeKeyPair(path.join(root, "id_member"), "dev@example.com");

    const initialised = await cli([
      "hub",
      "init",
      "--root",
      root,
      "--key",
      path.join(root, "id_a"),
      "--key",
      path.join(root, "id_b"),
      "--threshold",
      "2",
      "quorum",
    ]);
    assert.match(initialised, /2 root key\(s\), threshold 2/);
    // No seeded admin here, deliberately: `repo.admin` carries `member.invite`,
    // so handing it to one key of a two-of-two quorum would let that key grant
    // anything alone — the quorum reduced to a formality on root changes.
    assert.match(initialised, /no member yet/);

    const granted = await cli([
      "hub",
      "grant",
      "--root",
      root,
      "--key",
      path.join(root, "id_a"),
      "--key",
      path.join(root, "id_b"),
      "--subject",
      path.join(root, "id_member.pub"),
      "--capability",
      "source.push",
      "quorum",
    ]);
    assert.match(granted, /Granted source\.push to SHA256:/);

    // And one signature alone is still not enough: nothing in this repository
    // ever held `member.invite` on its own.
    const short = await cli([
      "hub",
      "grant",
      "--root",
      root,
      "--key",
      path.join(root, "id_a"),
      "--subject",
      path.join(root, "id_member.pub"),
      "--capability",
      "hub.review",
      "quorum",
    ]).then(
      () => null,
      (error: { stderr?: string; stdout?: string }) => `${error.stdout ?? ""}${error.stderr ?? ""}`,
    );
    assert.notEqual(short, null, "one of two signatures must not pass as a quorum");
  });

  it("refuses a threshold its own signers cannot meet", async () => {
    // A genesis is written once and never moves, so a threshold nobody can
    // reach produces a repository that can never be administered: every read
    // of it fails, every request 503s, and the ref cannot be rewritten.
    await cli(["init", "--root", root, "unreachable"]);
    await writeKeyPair(path.join(root, "id_a"), "a@example.com");

    const failed = await cli([
      "hub",
      "init",
      "--root",
      root,
      "--key",
      path.join(root, "id_a"),
      "--threshold",
      "2",
      "unreachable",
    ]).then(
      () => null,
      (error: { stderr?: string; stdout?: string }) => `${error.stdout ?? ""}${error.stderr ?? ""}`,
    );

    assert.notEqual(failed, null, "an unmeetable threshold must not be written");
    assert.match(failed ?? "", /--key/);

    // And nothing was written, so the repository is still usable.
    const after = await cli([
      "hub",
      "init",
      "--root",
      root,
      "--key",
      path.join(root, "id_a"),
      "unreachable",
    ]);
    assert.match(after, /holds repo\.admin/);
  });

  it("says what to do when a repository has no genesis yet", async () => {
    await cli(["init", "--root", root, "plain"]);
    const failed = await cli(["hub", "members", "--root", root, "plain"]).then(
      () => null,
      (error: { stderr?: string; stdout?: string }) => `${error.stdout ?? ""}${error.stderr ?? ""}`,
    );
    assert.match(failed ?? "", /hub init/);
  });

  it("mints a credential for a member of a hub-enabled repository", async () => {
    await cli(["init", "--root", root, "project"]);
    await writeKeyPair(path.join(root, "id_root"), "root@example.com");
    await cli(["hub", "init", "--root", root, "--key", path.join(root, "id_root"), "project"]);

    const minted = await cli([
      "credential",
      "--root",
      root,
      "--key",
      path.join(root, "id_root"),
      "--capability",
      "repo.read",
      "project",
    ]);
    assert.match(minted, /^hub1\./m, "a delegated credential is what stock git presents");
  });

  it("lets a member bulk-commit through a host that guards the repository", async () => {
    // `commit-pack` writes a ref, so it crosses the policy boundary — which
    // means the host has to hand it the requester. When it does not, this
    // returns 400 "authentication required" for a fully authorized member.
    const serverRoot = path.join(root, "server");
    await fs.mkdir(serverRoot, { recursive: true });
    await cli(["init", "--root", serverRoot, "bulk"]);
    const { credential } = await enableHubUnder(serverRoot, "bulk", ["repo.read", "source.push"]);

    const server = await serve({ root: serverRoot });
    try {
      const body = [
        JSON.stringify({
          type: "commit",
          branch: "main",
          message: "bulk",
          author: { name: "A", email: "a@example.com" },
        }),
        JSON.stringify({ type: "file", path: "a.txt" }),
        JSON.stringify({ type: "chunk", data: btoa("hello\n") }),
        JSON.stringify({ type: "end" }),
        JSON.stringify({ type: "done" }),
      ].join("\n");

      const response = await fetch(`${server.url}/bulk/commit-pack`, {
        method: "POST",
        headers: {
          "content-type": "application/x-ndjson",
          authorization: `Bearer ${credential}`,
        },
        body,
      });
      assert.equal(response.status, 200, await response.text());
    } finally {
      await server.close();
    }
  });

  it("will not pin an identity whose own roots never signed it", async () => {
    // Trust on first use is where an identity is *adopted*, so this is the
    // check that matters most: `presented()` loaded the remote's genesis
    // without asking whether its roots agreed to it, so `hub enable` recorded
    // a document nobody had signed as this URL's identity, and the failure
    // only surfaced later and opaquely, on the first clone.
    //
    // Served by a bare protocol handler rather than by `serve`, deliberately.
    // This host refuses to serve such a repository at all, which is the right
    // answer for a repository it owns and the wrong model for the threat: the
    // remote here is somebody else's, and it will serve whatever it likes.
    const remoteRoot = path.join(root, "hostile");
    await fs.mkdir(remoteRoot, { recursive: true });
    await shortOfQuorum(remoteRoot);

    const remote = await hostile(remoteRoot);
    try {
      // `--root`, because `hub enable` fetches the presented genesis into a
      // local directory before it decides anything, and the default is the
      // working directory.
      const failed = await cli(["hub", "enable", "--root", root, "--yes", remote.url]).then(
        () => null,
        (error: { stderr?: string; stdout?: string }) =>
          `${error.stdout ?? ""}${error.stderr ?? ""}`,
      );

      assert.notEqual(failed, null, "an unsigned identity must not be pinned");
      assert.match(failed ?? "", /root signature/);

      // And nothing was recorded for that URL.
      const status = await cli(["hub", "status", remote.url]);
      assert.match(status, /not trusted/);
    } finally {
      await remote.close();
    }
  });

  describe("the client's view of a remote", () => {
    it("reports a url nothing is pinned for", async () => {
      const out = await cli(["hub", "status", "https://git.example.com/nobody"]);
      assert.match(out, /not trusted/);
    });

    it("says when there was nothing to forget", async () => {
      const out = await cli(["hub", "forget", "https://git.example.com/nobody"]);
      assert.match(out, /was not trusted/);
    });
  });
});
