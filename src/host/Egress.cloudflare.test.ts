import assert from "node:assert/strict";
import { describe, it } from "@effect/vitest";
import { ConfigProvider, Effect } from "effect";
import { configuration, make, origins } from "./Egress.cloudflare.ts";

describe("Cloudflare egress policy", () => {
  it.effect("defaults to deny and validates explicit trusted hostname origins", () =>
    Effect.gen(function* () {
      const configured = (values: Record<string, string>) =>
        configuration().pipe(
          Effect.provide(ConfigProvider.layer(ConfigProvider.fromUnknown(values))),
        );
      assert.equal((yield* configured({})).size, 0);
      assert.deepEqual(
        [...(yield* configured({ GIT_EGRESS_TRUSTED_ORIGINS: "https://EXAMPLE.com:443/" }))],
        ["https://example.com"],
      );
      for (const value of [
        "http://example.com",
        "https://user:secret@example.com",
        "https://example.com/path",
        "https://example.com?secret=value",
        "https://example.com#fragment",
        "https://*.example.com",
        "https://127.0.0.1",
        "https://2130706433",
        "https://0177.0.0.1",
        "https://[::1]",
        "https://[::ffff:127.0.0.1]",
        "https://localhost",
        "https://child.localhost",
        "invalid",
      ]) {
        const result = yield* configured({ GIT_EGRESS_TRUSTED_ORIGINS: value }).pipe(Effect.result);
        assert.equal(result._tag, "Failure", value);
      }
    }),
  );

  it.effect(
    "denies unconfigured destinations before transport, including changed configuration",
    () =>
      Effect.promise(async () => {
        let calls = 0;
        const underlying: typeof fetch = async () => {
          calls++;
          return new Response("ok");
        };
        const values = new Set<string>();
        const guarded = make(values, underlying);
        values.add("https://example.com");
        for (const url of [
          "https://example.com",
          "http://127.0.0.1",
          "https://169.254.169.254",
          "https://private.example.com",
        ]) {
          await assert.rejects(guarded(url), /outbound network policy/);
        }
        assert.equal(calls, 0);
        const enabled = make(origins(["https://example.com"]), underlying);
        for (const url of [
          "https://example.com.attacker.test",
          "https://example.com:444",
          "http://example.com",
        ]) {
          await assert.rejects(enabled(url), /outbound network policy/);
        }
        assert.equal(calls, 0);
        assert.equal(await (await enabled("https://example.com/repo")).text(), "ok");
        assert.equal(calls, 1);
      }),
  );

  it.effect("checks every redirect and retains credentials only on the same origin", () =>
    Effect.promise(async () => {
      const seen: Array<Request> = [];
      const underlying: typeof fetch = async (input, init) => {
        const request = new Request(input, init);
        seen.push(request);
        assert.equal(request.redirect, "manual");
        const pathname = new URL(request.url).pathname;
        if (pathname === "/start")
          return new Response(null, { status: 302, headers: { location: "/end" } });
        if (pathname === "/cross")
          return new Response(null, {
            status: 307,
            headers: { location: "https://other.example.com" },
          });
        if (pathname === "/credentials")
          return new Response(null, {
            status: 302,
            headers: { location: "https://user:secret@example.com/end" },
          });
        if (pathname === "/loop")
          return new Response(null, { status: 308, headers: { location: "/loop" } });
        return new Response("ok");
      };
      const guarded = make(
        origins(["https://example.com", "https://other.example.com"]),
        underlying,
      );
      const response = await guarded("https://example.com/start", {
        headers: { authorization: "Bearer test" },
      });
      assert.equal(await response.text(), "ok");
      assert.deepEqual(
        seen.map((request) => request.url),
        ["https://example.com/start", "https://example.com/end"],
      );
      assert.ok(seen.every((request) => request.headers.get("authorization") === "Bearer test"));
      seen.length = 0;
      await assert.rejects(guarded("https://example.com/cross"), /outbound network policy/);
      assert.equal(
        seen.length,
        1,
        "even an allowlisted second origin cannot receive redirected credentials",
      );
      await assert.rejects(guarded("https://example.com/credentials"), /outbound network policy/);
      seen.length = 0;
      await assert.rejects(guarded("https://example.com/loop"), /outbound network policy/);
      assert.equal(seen.length, 6, "at most five redirect hops");
    }),
  );

  it.effect("streams writes once and never follows their redirects", () =>
    Effect.promise(async () => {
      let calls = 0;
      const underlying: typeof fetch = async (input, init) => {
        calls++;
        const request = new Request(input, init);
        assert.equal(await request.text(), "payload");
        return new Response(null, { status: 307, headers: { location: "/other" } });
      };
      const guarded = make(origins(["https://example.com"]), underlying);
      await assert.rejects(
        guarded("https://example.com/repo", { method: "POST", body: "payload" }),
        /outbound network policy/,
      );
      assert.equal(calls, 1);
      const manual = await guarded("https://example.com/repo", {
        method: "POST",
        body: "payload",
        redirect: "manual",
      });
      assert.equal(manual.status, 307);
      assert.equal(calls, 2);
    }),
  );

  it.effect("hands an unfinished upload to transport without buffering it", () =>
    Effect.promise(async () => {
      const reached = Promise.withResolvers<void>();
      const finish = Promise.withResolvers<void>();
      const body = new ReadableStream<Uint8Array>({
        async start(controller) {
          controller.enqueue(new TextEncoder().encode("first"));
          await finish.promise;
          controller.enqueue(new TextEncoder().encode("last"));
          controller.close();
        },
      });
      const underlying: typeof fetch = async (input, init) => {
        const request = new Request(input, init);
        assert.ok(request.body);
        const reader = request.body.getReader();
        assert.equal(new TextDecoder().decode((await reader.read()).value), "first");
        reached.resolve();
        assert.equal(new TextDecoder().decode((await reader.read()).value), "last");
        assert.equal((await reader.read()).done, true);
        reader.releaseLock();
        return new Response("accepted");
      };
      const guarded = make(origins(["https://example.com"]), underlying);
      const init: RequestInit & { duplex: "half" } = { method: "POST", body, duplex: "half" };
      const pending = guarded("https://example.com/repo", init);
      try {
        await reached.promise;
      } finally {
        finish.resolve();
      }
      assert.equal(await (await pending).text(), "accepted");
    }),
  );
});
