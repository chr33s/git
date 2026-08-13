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

import { serve, type Server } from "./Node.ts";

let root: string;
let server: Server;

const post = async (url: string, body: unknown) => {
  const response = await fetch(url, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
  return { status: response.status, body: (await response.json()) as Record<string, unknown> };
};

beforeAll(async () => {
  root = await fs.mkdtemp(path.join(os.tmpdir(), "host-node-"));
  server = await serve({ root });
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

    const stalled = await fetch(`${base}/blob/${String(big.body["oid"])}`);
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
    const read = await fetch(`${base}/blob/${String(orphan.body["oid"])}`);
    await read.arrayBuffer();

    const swept = await post(`${base}/gc`, {});
    assert.equal(swept.status, 200);
    assert.deepEqual(swept.body["removed"], [orphan.body["oid"]]);
  });
});
