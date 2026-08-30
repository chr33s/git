/**
 * Webhook delivery against a real HTTP receiver, so the signature the
 * receiver would verify is the signature actually sent.
 */
import assert from "node:assert/strict";
import { createHmac } from "node:crypto";
import * as http from "node:http";
import { afterAll, beforeAll, describe, it } from "@effect/vitest";

import { Effect, Layer, Predicate } from "effect";
import { FetchHttpClient } from "effect/unstable/http";

import type { ReceiveResult } from "../git/Repository.ts";
import type { Oid } from "../git/Store.ts";
import { deliver, eventOf, sign, subscribersOf } from "./Webhooks.ts";
import { validate } from "./Subscribers.ts";

interface Received {
  readonly body: string;
  readonly signature: string | undefined;
  readonly event: string | undefined;
}

/** These headers are sent at most once, so a repeated one keeps its first value. */
const single = (value: string | string[] | undefined): string | undefined =>
  Array.isArray(value) ? value[0] : value;

/** A receiver whose reply is scripted per test: status codes in, calls out. */
const receiver = async () => {
  const calls: Received[] = [];
  let replies: number[] = [];

  const server = http.createServer((incoming, outgoing) => {
    void (async () => {
      const chunks: Buffer[] = [];
      for await (const chunk of incoming) {
        // SAFETY: a request without an encoding set yields Buffer chunks;
        // node types the async iteration as `any`.
        chunks.push(chunk as Buffer);
      }
      calls.push({
        body: Buffer.concat(chunks).toString(),
        signature: single(incoming.headers["x-signature-256"]),
        event: single(incoming.headers["x-event"]),
      });
      outgoing.writeHead(replies.shift() ?? 200);
      outgoing.end();
    })();
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));

  const address = server.address();
  assert.ok(address !== null && !Predicate.isString(address), "bound to a TCP port");

  return {
    url: `http://127.0.0.1:${address.port}/hook`,
    calls,
    reply: (...statuses: number[]) => {
      replies = statuses;
    },
    close: () => new Promise<void>((resolve) => void server.close(() => resolve())),
  };
};

// SAFETY: forty-character stand-ins for real commit ids; delivery treats an
// oid as an opaque string and never dereferences it.
const results: ReadonlyArray<ReceiveResult> = [
  { ref: "refs/heads/main", from: "a".repeat(40) as Oid, to: "b".repeat(40) as Oid, ok: true },
  { ref: "refs/heads/nope", from: null, to: null, ok: false, reason: "ref moved" },
];

describe("Webhooks", () => {
  let hook: Awaited<ReturnType<typeof receiver>>;

  beforeAll(async () => {
    hook = await receiver();
  });
  afterAll(async () => {
    await hook.close();
  });

  const run = (options?: Parameters<typeof deliver>[1]) =>
    Effect.runPromise(
      deliver(results, { baseDelay: "1 millis", ...options }).pipe(
        Effect.provide(
          Layer.mergeAll(
            subscribersOf([{ id: "1", url: hook.url, secret: "s3cret" }]),
            FetchHttpClient.layer,
          ),
        ),
      ),
    );

  it.effect("posts only the refs that moved, signed the way receivers verify", () =>
    Effect.promise(async () => {
      hook.calls.length = 0;
      hook.reply(200);
      await run();

      assert.equal(hook.calls.length, 1);
      const call = hook.calls[0]!;
      assert.equal(call.event, "push");

      // SAFETY: the receiver recorded the exact bytes `deliver` posted, and
      // `deliver` writes them as this JSON.
      const body = JSON.parse(call.body) as { event: string; refs: unknown[] };
      assert.equal(body.event, "push");
      // The rejected ref is not an event: nothing happened to it.
      assert.deepEqual(body.refs, [
        { ref: "refs/heads/main", before: "a".repeat(40), after: "b".repeat(40) },
      ]);

      // Verifiable with any off-the-shelf HMAC, over the exact bytes sent.
      const expected = `sha256=${createHmac("sha256", "s3cret").update(call.body).digest("hex")}`;
      assert.equal(call.signature, expected);
      assert.equal(await Effect.runPromise(sign(call.body, "s3cret")), expected);
    }),
  );

  it.effect("retries a 5xx and stops once it succeeds", () =>
    Effect.promise(async () => {
      hook.calls.length = 0;
      hook.reply(500, 503, 200);
      await run();
      assert.equal(hook.calls.length, 3, "two failures then a success");
    }),
  );

  it.effect("does not retry a 4xx — the receiver said never again", () =>
    Effect.promise(async () => {
      hook.calls.length = 0;
      hook.reply(404, 200);
      await run();
      assert.equal(hook.calls.length, 1);
    }),
  );

  it.effect("gives up after the retry budget, without failing the caller", () =>
    Effect.promise(async () => {
      hook.calls.length = 0;
      hook.reply(500, 500, 500, 500, 500, 500);
      // Resolving at all is the assertion: a webhook cannot fail a push.
      await run({ retries: 2 });
      assert.equal(hook.calls.length, 3, "the first attempt plus two retries");
    }),
  );

  it.effect("delivers to every subscriber, and skips the work when there are none", () =>
    Effect.promise(async () => {
      const second = await receiver();
      try {
        hook.calls.length = 0;
        hook.reply(200);
        second.reply(200);
        await Effect.runPromise(
          deliver(results, { baseDelay: "1 millis" }).pipe(
            Effect.provide(
              Layer.mergeAll(
                subscribersOf([
                  { id: "1", url: hook.url, secret: "s3cret" },
                  { id: "2", url: second.url, secret: "other" },
                ]),
                FetchHttpClient.layer,
              ),
            ),
          ),
        );
        assert.equal(hook.calls.length, 1);
        assert.equal(second.calls.length, 1);
        // Different secrets, so the two signatures must differ.
        assert.notEqual(hook.calls[0]?.signature, second.calls[0]?.signature);
      } finally {
        await second.close();
      }

      hook.calls.length = 0;
      await Effect.runPromise(
        deliver(results).pipe(
          Effect.provide(Layer.mergeAll(subscribersOf([]), FetchHttpClient.layer)),
        ),
      );
      assert.equal(hook.calls.length, 0);
    }),
  );

  it.effect("sends nothing when no ref moved", () =>
    Effect.promise(async () => {
      hook.calls.length = 0;
      const rejected = results.filter((result) => !result.ok);
      assert.deepEqual(eventOf(rejected), []);
      await Effect.runPromise(
        deliver(rejected).pipe(
          Effect.provide(
            Layer.mergeAll(
              subscribersOf([{ id: "1", url: hook.url, secret: "s" }]),
              FetchHttpClient.layer,
            ),
          ),
        ),
      );
      assert.equal(hook.calls.length, 0);
    }),
  );

  it.effect("holds a receiver's URL to the same rules a remote's is held to", () =>
    Effect.gen(function* () {
      const refused = (url: string) =>
        validate({ url, secret: "long-enough-secret" }).pipe(
          Effect.flip,
          Effect.map((failure) => failure.reason),
        );

      // The two registries had drifted in both directions. This one exempted
      // the whole *scheme* when the hostname was `localhost`, so a URL that is
      // not an HTTP address at all validated...
      assert.match(yield* refused("file://localhost/etc/passwd"), /must be https/);
      // ...while refusing the other two spellings of the same machine that
      // `Remotes.validate` has always accepted.
      yield* validate({ url: "http://127.0.0.1:8080/hook", secret: "long-enough-secret" });
      yield* validate({ url: "http://[::1]:8080/hook", secret: "long-enough-secret" });
      yield* validate({ url: "http://localhost:8080/hook", secret: "long-enough-secret" });

      // And only that one refused userinfo, though both hand the URL straight
      // back out of a list endpoint.
      assert.match(yield* refused("https://user:pw@example.com/hook"), /credentials/);
      assert.match(yield* refused("not a url"), /not a URL/);
      assert.match(yield* refused("http://example.com/hook"), /must be https/);

      // The secret is still this registry's own rule.
      assert.match(
        yield* validate({ url: "https://example.com/hook", secret: "short" }).pipe(
          Effect.flip,
          Effect.map((failure) => failure.field),
        ),
        /secret/,
      );
    }),
  );
});
