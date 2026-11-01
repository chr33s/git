import assert from "node:assert/strict";
import * as fs from "node:fs/promises";
import { createServer } from "node:http";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, it } from "@effect/vitest";
import { Effect, Predicate } from "effect";
import { EMPTY_TREE_OID } from "../git/Format.ts";
import { Hooks } from "../git/Repository.ts";
import * as AfterPush from "../server/AfterPush.node.ts";
import { file as subscribersFile } from "../server/Subscribers.node.ts";
import { Subscribers } from "../server/Subscribers.ts";
import { make } from "./Egress.node.ts";
import { serve } from "./Node.ts";

describe("Node egress wiring", () => {
  it.live(
    "blocks remote APIs and the actual webhook hook chain before private network access",
    () =>
      Effect.promise(async () => {
        const root = await fs.mkdtemp(join(tmpdir(), "git-egress-host-"));
        let hits = 0;
        const receiver = createServer((request, response) => {
          hits++;
          request.resume();
          response.end();
        });
        await new Promise<void>((resolve) => receiver.listen(0, "127.0.0.1", resolve));
        const address = receiver.address();
        assert.ok(address !== null && !Predicate.isString(address));
        const destination = `http://127.0.0.1:${address.port}`;
        const host = await serve({ root, allowAnonymousWrites: true });
        const policy = make(new Set());
        try {
          const post = (route: string, body: string) =>
            fetch(`${host.url}/r/${route}`, {
              method: "POST",
              headers: { "content-type": "application/json" },
              body,
            });
          const fetched = await post("fetch", JSON.stringify({ url: destination }));
          assert.equal(fetched.status, 400);
          assert.match(await fetched.text(), /network policy denied/);
          const created = await post(
            "commit",
            JSON.stringify({
              branch: "main",
              message: "egress fixture",
              files: [],
            }),
          );
          assert.equal(created.status, 200, await created.text());
          const pushed = await post(
            "push",
            JSON.stringify({
              url: destination,
              refs: [{ local: "refs/heads/main" }],
            }),
          );
          assert.equal(pushed.status, 400);
          assert.match(await pushed.text(), /network policy denied/);
          assert.equal(hits, 0);

          await Effect.runPromise(
            Effect.gen(function* () {
              const subscribers = yield* Subscribers;
              yield* subscribers.add({ url: destination, secret: "fixture-egress-secret" });
            }).pipe(Effect.provide(subscribersFile(join(root, "hooked", "webhooks.json")))),
          );
          let attempts = 0;
          const transport: typeof globalThis.fetch = (input, init) => {
            attempts++;
            return policy.fetch(input, init);
          };
          await Effect.runPromise(
            Effect.gen(function* () {
              const hooks = yield* Hooks;
              yield* hooks.postReceive([
                { ref: "refs/heads/main", from: null, to: EMPTY_TREE_OID, ok: true },
              ]);
            }).pipe(
              Effect.provide(
                AfterPush.chain({
                  root,
                  repo: "hooked",
                  transport,
                  background: (effect) => Effect.asVoid(Effect.orDie(effect)),
                }),
              ),
            ),
          );
          assert.equal(attempts, 4, "all webhook attempts must use the host's transport");
          assert.equal(
            hits,
            0,
            "the webhook client must not bypass policy via its default fetch reference",
          );
        } finally {
          await host.close({ force: true });
          await policy.close();
          receiver.closeAllConnections();
          await new Promise<void>((resolve) => receiver.close(() => resolve()));
          await fs.rm(root, { recursive: true, force: true });
        }
      }),
  );
});
