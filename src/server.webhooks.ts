/**
 * Webhook delivery system for Git repository events.
 *
 * Webhooks fire HTTP POST requests to registered URLs on push events,
 * signed with HMAC-SHA256 for payload verification.
 *
 * API:
 * - POST   /api/:repo/webhooks       — register webhook
 * - GET    /api/:repo/webhooks       — list webhooks
 * - DELETE /api/:repo/webhooks/:id   — remove webhook
 *
 * Webhook payload is sent as JSON with:
 * - `X-Event` header (e.g. "push")
 * - `X-Signature-256` header (HMAC-SHA256 of body using webhook secret)
 * - `X-Delivery` header (unique delivery ID)
 */

import type { HookRefUpdate } from "./git.hooks.ts";

/** Stored webhook configuration. */
export interface Webhook {
  id: number;
  url: string;
  secret: string;
  events: string[];
  active: boolean;
}

/** Push event payload delivered to webhook URL. */
export interface WebhookPushPayload {
  event: "push";
  repository: string;
  ref: string;
  before: string;
  after: string;
  commits: WebhookCommitInfo[];
}

export interface WebhookCommitInfo {
  id: string;
  message: string;
  author: string;
}

/** Webhook delivery result. */
export interface WebhookDeliveryResult {
  webhookId: number;
  url: string;
  status: number | null;
  ok: boolean;
  error?: string;
}

export interface WebhookStorage {
  createWebhook(repository: string, url: string, secret: string, events: string[]): number;
  deleteWebhook(repository: string, id: number): boolean;
  listWebhooks(repository: string): Webhook[];
  getWebhooksByEvent(repository: string, event: string): Webhook[];
}

const MAX_RETRIES = 3;
const RETRY_DELAYS = [1000, 5000, 15000];

export class ServerWebhooks {
  #storage: WebhookStorage;

  constructor(storage: WebhookStorage) {
    this.#storage = storage;
  }

  /** POST /api/:repo/webhooks — Register a new webhook. */
  async register(repository: string, body: Record<string, unknown>): Promise<Response> {
    const url = body.url;
    const secret = body.secret;
    const events = body.events;

    if (typeof url !== "string" || !url.startsWith("https://")) {
      return Response.json({ error: "url must be an HTTPS URL" }, { status: 422 });
    }

    if (typeof secret !== "string" || secret.length < 16) {
      return Response.json({ error: "secret must be at least 16 characters" }, { status: 422 });
    }

    if (
      !Array.isArray(events) ||
      events.length === 0 ||
      !events.every((e) => typeof e === "string")
    ) {
      return Response.json(
        { error: "events must be a non-empty array of strings" },
        { status: 422 },
      );
    }

    const validEvents = new Set(["push"]);
    for (const event of events) {
      if (!validEvents.has(event)) {
        return Response.json({ error: `Unknown event: ${event}` }, { status: 422 });
      }
    }

    const id = this.#storage.createWebhook(repository, url, secret, events);
    return Response.json({ id, url, events, active: true }, { status: 201 });
  }

  /** GET /api/:repo/webhooks — List webhooks (secrets are masked). */
  async list(repository: string): Promise<Response> {
    const webhooks = this.#storage.listWebhooks(repository);

    const masked = webhooks.map((w) => ({
      id: w.id,
      url: w.url,
      events: w.events,
      active: w.active,
      secret: "***",
    }));

    return Response.json(masked);
  }

  /** DELETE /api/:repo/webhooks/:id — Remove a webhook. */
  async remove(repository: string, id: number): Promise<Response> {
    const deleted = this.#storage.deleteWebhook(repository, id);
    if (!deleted) {
      return Response.json({ error: "Webhook not found" }, { status: 404 });
    }
    return new Response(null, { status: 204 });
  }

  /**
   * Fire webhooks for a push event.
   *
   * Called from post-receive hook context. Errors are caught and logged,
   * never propagated to the push response.
   */
  async deliver(
    repository: string,
    updates: HookRefUpdate[],
    commitResolver?: (oid: string) => Promise<WebhookCommitInfo | null>,
  ): Promise<WebhookDeliveryResult[]> {
    const webhooks = this.#storage.getWebhooksByEvent(repository, "push");
    if (webhooks.length === 0) return [];

    const results: WebhookDeliveryResult[] = [];

    for (const update of updates) {
      const commits: WebhookCommitInfo[] = [];
      if (commitResolver) {
        const info = await commitResolver(update.newOid).catch(() => null);
        if (info) commits.push(info);
      }

      const payload: WebhookPushPayload = {
        event: "push",
        repository,
        ref: update.ref,
        before: update.oldOid,
        after: update.newOid,
        commits,
      };

      for (const webhook of webhooks) {
        const result = await this.#deliverToWebhook(webhook, payload);
        results.push(result);
      }
    }

    return results;
  }

  async #deliverToWebhook(
    webhook: Webhook,
    payload: WebhookPushPayload,
  ): Promise<WebhookDeliveryResult> {
    const body = JSON.stringify(payload);
    const signature = await this.#sign(body, webhook.secret);
    const deliveryId = crypto.randomUUID();

    for (let attempt = 0; attempt <= MAX_RETRIES; attempt++) {
      try {
        const response = await fetch(webhook.url, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            "X-Event": payload.event,
            "X-Signature-256": `sha256=${signature}`,
            "X-Delivery": deliveryId,
          },
          body,
        });

        if (response.ok) {
          return {
            webhookId: webhook.id,
            url: webhook.url,
            status: response.status,
            ok: true,
          };
        }

        // Non-retryable client errors
        if (response.status >= 400 && response.status < 500) {
          return {
            webhookId: webhook.id,
            url: webhook.url,
            status: response.status,
            ok: false,
            error: `HTTP ${response.status}`,
          };
        }

        // Server error — retry
        if (attempt < MAX_RETRIES) {
          await this.#delay(RETRY_DELAYS[attempt]!);
        }
      } catch (error) {
        if (attempt >= MAX_RETRIES) {
          return {
            webhookId: webhook.id,
            url: webhook.url,
            status: null,
            ok: false,
            error: error instanceof Error ? error.message : "Delivery failed",
          };
        }
        await this.#delay(RETRY_DELAYS[attempt]!);
      }
    }

    return {
      webhookId: webhook.id,
      url: webhook.url,
      status: null,
      ok: false,
      error: "Max retries exceeded",
    };
  }

  async #sign(body: string, secret: string): Promise<string> {
    const key = await crypto.subtle.importKey(
      "raw",
      new TextEncoder().encode(secret),
      { name: "HMAC", hash: "SHA-256" },
      false,
      ["sign"],
    );
    const sig = await crypto.subtle.sign("HMAC", key, new TextEncoder().encode(body));
    return Array.from(new Uint8Array(sig))
      .map((b) => b.toString(16).padStart(2, "0"))
      .join("");
  }

  #delay(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}
