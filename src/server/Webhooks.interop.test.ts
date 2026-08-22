/**
 * Interop: a real `git push` delivers a real webhook.
 *
 * `Webhooks.test.ts` proves the delivery engine — signing, retry, the
 * schedule. This proves the thing that was actually missing: that a host
 * wires it up at all. Every host provided `hooksNoop`, so the engine was
 * correct and unreachable, and only an end-to-end push can tell the two
 * apart.
 *
 * Registration goes through the JSON API rather than a seeded layer, so the
 * whole path is under test: register, push with stock git, receive.
 *
 * Skipped when `git` is not on PATH.
 */
import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { createHmac } from "node:crypto";
import * as fs from "node:fs/promises";
import * as http from "node:http";
import * as os from "node:os";
import * as path from "node:path";
import { promisify } from "node:util";
import { afterAll, beforeAll, describe, it } from "@effect/vitest";

import { Effect, Predicate } from "effect";

import { serve, type Server } from "../host/Node.ts";
import { hasGit } from "../testing/Git.ts";

const execFileAsync = promisify(execFile);

/** Async, or the server on this event loop could never answer. */
const git = async (cwd: string, ...args: string[]): Promise<string> => {
  const result = await execFileAsync(
    "git",
    ["-c", "user.name=Test", "-c", "user.email=test@example.com", ...args],
    { cwd, encoding: "utf8", env: { ...process.env, GIT_TERMINAL_PROMPT: "0" } },
  );
  return result.stdout;
};

interface Delivery {
  readonly body: string;
  readonly signature: string | null;
  readonly event: string | null;
}

/** These headers are sent at most once, so a repeated one keeps its first value. */
const single = (value: string | string[] | undefined): string | undefined =>
  Array.isArray(value) ? value[0] : value;

/** A receiver that records what it was sent. */
const receiver = async () => {
  const deliveries: Delivery[] = [];
  const server = http.createServer((incoming, outgoing) => {
    const chunks: Buffer[] = [];
    incoming.on("data", (chunk: Buffer) => chunks.push(chunk));
    incoming.on("end", () => {
      deliveries.push({
        body: Buffer.concat(chunks).toString("utf8"),
        signature: single(incoming.headers["x-signature-256"]) ?? null,
        event: single(incoming.headers["x-event"]) ?? null,
      });
      outgoing.writeHead(204);
      outgoing.end();
    });
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));

  const address = server.address();
  assert.ok(address !== null && !Predicate.isString(address), "bound to a TCP port");

  return {
    deliveries,
    // `localhost` rather than `127.0.0.1`: the registry refuses plain http
    // anywhere else, which is the policy under test everywhere but here.
    url: `http://localhost:${address.port}/hook`,
    close: () => new Promise<void>((resolve) => server.close(() => resolve())),
  };
};

/**
 * Delivery is detached from the response on purpose, so the push returns
 * before the receiver is called. Polling to a deadline is the honest way to
 * observe that — a fixed sleep would either flake or waste the difference.
 */
const waitFor = async <A>(get: () => ReadonlyArray<A>, timeoutMs = 10_000): Promise<A> => {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const first = get()[0];
    if (first !== undefined) return first;
    if (Date.now() > deadline) throw new Error("no delivery within the deadline");
    await new Promise((resolve) => setTimeout(resolve, 25));
  }
};

describe.skipIf(!hasGit)("webhook delivery on push", () => {
  let root: string;
  let server: Server;

  beforeAll(async () => {
    root = await fs.mkdtemp(path.join(os.tmpdir(), "git-webhook-interop-"));
    server = await serve({ root, allowAnonymousWrites: true });
  });

  afterAll(async () => {
    await server.close();
    await fs.rm(root, { recursive: true, force: true });
  });

  it.effect("signs and delivers what a push moved", () =>
    Effect.promise(async () => {
      const hook = await receiver();
      const secret = "a-secret-long-enough";

      try {
        // Registered over HTTP, the way a user would.
        const registered = await fetch(`${server.url}/hooked/webhooks`, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ url: hook.url, secret }),
        });
        assert.equal(registered.status, 200);

        const work = path.join(root, "work");
        await fs.mkdir(work, { recursive: true });
        await git(work, "init", "-q", "-b", "main");
        await fs.writeFile(path.join(work, "file.txt"), "content\n");
        await git(work, "add", "file.txt");
        await git(work, "commit", "-q", "-m", "first");
        await git(work, "push", "-q", `${server.url}/hooked`, "main");

        const head = (await git(work, "rev-parse", "HEAD")).trim();

        const delivery = await waitFor(() => hook.deliveries);
        assert.equal(delivery.event, "push");

        // SAFETY: the receiver recorded the exact bytes the server posted, and
        // delivery writes them as this JSON.
        const payload = JSON.parse(delivery.body) as {
          event: string;
          refs: Array<{ ref: string; before: string | null; after: string | null }>;
        };
        assert.equal(payload.event, "push");
        assert.deepEqual(
          payload.refs.map((ref) => ref.ref),
          ["refs/heads/main"],
        );
        assert.equal(payload.refs[0]!.after, head);

        // The signature is over the exact bytes sent, so a receiver that
        // recomputes it from the body agrees.
        const expected = `sha256=${createHmac("sha256", secret).update(delivery.body).digest("hex")}`;
        assert.equal(delivery.signature, expected);
      } finally {
        await hook.close();
      }
    }),
  );

  it.effect("does not deliver to a webhook that was removed", () =>
    Effect.promise(async () => {
      const hook = await receiver();

      try {
        // SAFETY: registration replies with the stored webhook, id included;
        // an id that is not there fails the DELETE below.
        const created = (await (
          await fetch(`${server.url}/unhooked/webhooks`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({ url: hook.url, secret: "a-secret-long-enough" }),
          })
        ).json()) as { id: string };

        const removed = await fetch(`${server.url}/unhooked/webhooks/${created.id}`, {
          method: "DELETE",
        });
        assert.equal(removed.status, 200);

        const work = path.join(root, "work-removed");
        await fs.mkdir(work, { recursive: true });
        await git(work, "init", "-q", "-b", "main");
        await fs.writeFile(path.join(work, "file.txt"), "content\n");
        await git(work, "add", "file.txt");
        await git(work, "commit", "-q", "-m", "first");
        await git(work, "push", "-q", `${server.url}/unhooked`, "main");

        // Nothing to wait for; give a delivery every chance to show up wrongly.
        await new Promise((resolve) => setTimeout(resolve, 250));
        assert.deepEqual(hook.deliveries, []);
      } finally {
        await hook.close();
      }
    }),
  );

  it.effect("survives a restart, because the registry is on disk", () =>
    Effect.promise(async () => {
      const hook = await receiver();

      try {
        await fetch(`${server.url}/durable/webhooks`, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ url: hook.url, secret: "a-secret-long-enough" }),
        });

        // A second server over the same root is what an eviction or a redeploy
        // looks like from the registry's point of view.
        const restarted = await serve({ root, allowAnonymousWrites: true });
        try {
          // SAFETY: the list endpoint replies with the registry's rows; a
          // reply of any other form fails the deep equality below.
          const listed = (await (await fetch(`${restarted.url}/durable/webhooks`)).json()) as {
            webhooks: Array<{ url: string }>;
          };
          assert.deepEqual(
            listed.webhooks.map((entry) => entry.url),
            [hook.url],
          );
        } finally {
          await restarted.close();
        }
      } finally {
        await hook.close();
      }
    }),
  );
});
