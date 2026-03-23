import * as assert from "node:assert/strict";
import { describe, it } from "node:test";

import {
  ServerWebhooks,
  type Webhook,
  type WebhookStorage,
  type WebhookPushPayload,
} from "./server.webhooks.ts";

const ZERO_OID = "0".repeat(40);
const OID_A = "a".repeat(40);

/**
 * In-memory webhook storage for testing.
 */
function createMockStorage(): WebhookStorage {
  const webhooks: Webhook[] = [];
  let nextId = 1;

  return {
    createWebhook(repository, url, secret, events) {
      const id = nextId++;
      webhooks.push({ id, url, secret, events, active: true });
      return id;
    },
    deleteWebhook(repository, id) {
      const idx = webhooks.findIndex((w) => w.id === id);
      if (idx === -1) return false;
      webhooks.splice(idx, 1);
      return true;
    },
    listWebhooks(_repository) {
      return [...webhooks];
    },
    getWebhooksByEvent(repository, event) {
      return webhooks.filter((w) => w.active && w.events.includes(event));
    },
  };
}

async function readJson(response: Response): Promise<any> {
  return response.json();
}

void describe("ServerWebhooks", () => {
  void describe("register", () => {
    void it("creates webhook with valid input", async () => {
      const storage = createMockStorage();
      const webhooks = new ServerWebhooks(storage);

      const res = await webhooks.register("test-repo", {
        url: "https://example.com/hook",
        secret: "abcdefghijklmnop",
        events: ["push"],
      });

      assert.equal(res.status, 201);
      const body = await readJson(res);
      assert.equal(body.id, 1);
      assert.equal(body.url, "https://example.com/hook");
      assert.deepEqual(body.events, ["push"]);
      assert.equal(body.active, true);
    });

    void it("rejects non-HTTPS URL", async () => {
      const storage = createMockStorage();
      const webhooks = new ServerWebhooks(storage);

      const res = await webhooks.register("test-repo", {
        url: "http://example.com/hook",
        secret: "abcdefghijklmnop",
        events: ["push"],
      });

      assert.equal(res.status, 422);
      const body = await readJson(res);
      assert.ok(body.error.includes("HTTPS"));
    });

    void it("rejects short secret", async () => {
      const storage = createMockStorage();
      const webhooks = new ServerWebhooks(storage);

      const res = await webhooks.register("test-repo", {
        url: "https://example.com/hook",
        secret: "short",
        events: ["push"],
      });

      assert.equal(res.status, 422);
      assert.ok((await readJson(res)).error.includes("16"));
    });

    void it("rejects empty events", async () => {
      const storage = createMockStorage();
      const webhooks = new ServerWebhooks(storage);

      const res = await webhooks.register("test-repo", {
        url: "https://example.com/hook",
        secret: "abcdefghijklmnop",
        events: [],
      });

      assert.equal(res.status, 422);
    });

    void it("rejects unknown event type", async () => {
      const storage = createMockStorage();
      const webhooks = new ServerWebhooks(storage);

      const res = await webhooks.register("test-repo", {
        url: "https://example.com/hook",
        secret: "abcdefghijklmnop",
        events: ["invalid_event"],
      });

      assert.equal(res.status, 422);
      assert.ok((await readJson(res)).error.includes("invalid_event"));
    });
  });

  void describe("list", () => {
    void it("lists webhooks with masked secret", async () => {
      const storage = createMockStorage();
      const webhooks = new ServerWebhooks(storage);

      await webhooks.register("test-repo", {
        url: "https://example.com/hook1",
        secret: "abcdefghijklmnop",
        events: ["push"],
      });
      await webhooks.register("test-repo", {
        url: "https://example.com/hook2",
        secret: "qrstuvwxyz012345",
        events: ["push"],
      });

      const res = await webhooks.list("test-repo");
      assert.equal(res.status, 200);
      const body = await readJson(res);
      assert.equal(body.length, 2);
      assert.equal(body[0].url, "https://example.com/hook1");
      assert.equal(body[0].secret, "***");
      assert.equal(body[1].secret, "***");
    });

    void it("returns empty array when no webhooks", async () => {
      const storage = createMockStorage();
      const webhooks = new ServerWebhooks(storage);

      const res = await webhooks.list("test-repo");
      const body = await readJson(res);
      assert.deepEqual(body, []);
    });
  });

  void describe("remove", () => {
    void it("deletes existing webhook", async () => {
      const storage = createMockStorage();
      const webhooks = new ServerWebhooks(storage);

      await webhooks.register("test-repo", {
        url: "https://example.com/hook",
        secret: "abcdefghijklmnop",
        events: ["push"],
      });

      const res = await webhooks.remove("test-repo", 1);
      assert.equal(res.status, 204);

      const listRes = await webhooks.list("test-repo");
      const body = await readJson(listRes);
      assert.equal(body.length, 0);
    });

    void it("returns 404 for non-existent webhook", async () => {
      const storage = createMockStorage();
      const webhooks = new ServerWebhooks(storage);

      const res = await webhooks.remove("test-repo", 999);
      assert.equal(res.status, 404);
    });
  });

  void describe("deliver", () => {
    void it("returns empty when no webhooks registered", async () => {
      const storage = createMockStorage();
      const webhooks = new ServerWebhooks(storage);

      const results = await webhooks.deliver("test-repo", [
        { ref: "refs/heads/main", oldOid: ZERO_OID, newOid: OID_A },
      ]);

      assert.deepEqual(results, []);
    });

    void it("delivers to registered webhook", async () => {
      const storage = createMockStorage();
      const webhooks = new ServerWebhooks(storage);

      // Intercept fetch — we'll verify the call was made with correct payload
      const deliveries: { url: string; body: string; headers: Record<string, string> }[] = [];
      const originalFetch = globalThis.fetch;
      globalThis.fetch = async (input: any, init: any) => {
        deliveries.push({
          url: typeof input === "string" ? input : input.url,
          body: init.body,
          headers: init.headers,
        });
        return new Response("ok", { status: 200 });
      };

      try {
        storage.createWebhook("test-repo", "https://example.com/hook", "test-secret-1234567", [
          "push",
        ]);

        const results = await webhooks.deliver(
          "test-repo",
          [{ ref: "refs/heads/main", oldOid: ZERO_OID, newOid: OID_A }],
          async (oid) => ({ id: oid, message: "Initial commit", author: "Test User" }),
        );

        assert.equal(results.length, 1);
        assert.equal(results[0]!.ok, true);
        assert.equal(results[0]!.url, "https://example.com/hook");

        // Verify payload
        assert.equal(deliveries.length, 1);
        const payload = JSON.parse(deliveries[0]!.body) as WebhookPushPayload;
        assert.equal(payload.event, "push");
        assert.equal(payload.repository, "test-repo");
        assert.equal(payload.ref, "refs/heads/main");
        assert.equal(payload.before, ZERO_OID);
        assert.equal(payload.after, OID_A);
        assert.equal(payload.commits.length, 1);
        assert.equal(payload.commits[0]!.message, "Initial commit");

        // Verify signature header
        assert.ok(deliveries[0]!.headers["X-Signature-256"]?.startsWith("sha256="));
        assert.ok(deliveries[0]!.headers["X-Event"] === "push");
        assert.ok(deliveries[0]!.headers["X-Delivery"]);
      } finally {
        globalThis.fetch = originalFetch;
      }
    });

    void it("retries on server error", async () => {
      const storage = createMockStorage();
      const webhooks = new ServerWebhooks(storage);

      let attempts = 0;
      const originalFetch = globalThis.fetch;
      globalThis.fetch = async () => {
        attempts++;
        if (attempts < 3) {
          return new Response("error", { status: 500 });
        }
        return new Response("ok", { status: 200 });
      };

      try {
        storage.createWebhook("test-repo", "https://example.com/hook", "test-secret-1234567", [
          "push",
        ]);

        const results = await webhooks.deliver("test-repo", [
          { ref: "refs/heads/main", oldOid: ZERO_OID, newOid: OID_A },
        ]);

        assert.equal(results.length, 1);
        assert.equal(results[0]!.ok, true);
        assert.ok(attempts >= 3);
      } finally {
        globalThis.fetch = originalFetch;
      }
    });

    void it("does not retry on client error (4xx)", async () => {
      const storage = createMockStorage();
      const webhooks = new ServerWebhooks(storage);

      let attempts = 0;
      const originalFetch = globalThis.fetch;
      globalThis.fetch = async () => {
        attempts++;
        return new Response("not found", { status: 404 });
      };

      try {
        storage.createWebhook("test-repo", "https://example.com/hook", "test-secret-1234567", [
          "push",
        ]);

        const results = await webhooks.deliver("test-repo", [
          { ref: "refs/heads/main", oldOid: ZERO_OID, newOid: OID_A },
        ]);

        assert.equal(results.length, 1);
        assert.equal(results[0]!.ok, false);
        assert.equal(results[0]!.status, 404);
        assert.equal(attempts, 1); // No retries on 4xx
      } finally {
        globalThis.fetch = originalFetch;
      }
    });

    void it("handles fetch network error", async () => {
      const storage = createMockStorage();
      const webhooks = new ServerWebhooks(storage);

      const originalFetch = globalThis.fetch;
      globalThis.fetch = async () => {
        throw new Error("Network unreachable");
      };

      try {
        storage.createWebhook("test-repo", "https://example.com/hook", "test-secret-1234567", [
          "push",
        ]);

        const results = await webhooks.deliver("test-repo", [
          { ref: "refs/heads/main", oldOid: ZERO_OID, newOid: OID_A },
        ]);

        assert.equal(results.length, 1);
        assert.equal(results[0]!.ok, false);
        assert.ok(results[0]!.error?.includes("Network unreachable"));
      } finally {
        globalThis.fetch = originalFetch;
      }
    });

    void it("signature verification", async () => {
      const storage = createMockStorage();
      const webhooks = new ServerWebhooks(storage);

      let receivedSignature = "";
      let receivedBody = "";
      const secret = "test-webhook-secret-key";

      const originalFetch = globalThis.fetch;
      globalThis.fetch = async (_input: any, init: any) => {
        receivedSignature = init.headers["X-Signature-256"];
        receivedBody = init.body;
        return new Response("ok", { status: 200 });
      };

      try {
        storage.createWebhook("test-repo", "https://example.com/hook", secret, ["push"]);

        await webhooks.deliver("test-repo", [
          { ref: "refs/heads/main", oldOid: ZERO_OID, newOid: OID_A },
        ]);

        // Verify the signature manually
        assert.ok(receivedSignature.startsWith("sha256="));
        const signatureHex = receivedSignature.slice(7);

        // Recompute HMAC-SHA256
        const key = await crypto.subtle.importKey(
          "raw",
          new TextEncoder().encode(secret),
          { name: "HMAC", hash: "SHA-256" },
          false,
          ["sign"],
        );
        const sig = await crypto.subtle.sign("HMAC", key, new TextEncoder().encode(receivedBody));
        const expectedHex = Array.from(new Uint8Array(sig))
          .map((b) => b.toString(16).padStart(2, "0"))
          .join("");

        assert.equal(signatureHex, expectedHex);
      } finally {
        globalThis.fetch = originalFetch;
      }
    });
  });
});
