import assert from "node:assert/strict";
import { describe, it } from "@effect/vitest";

import { Effect } from "effect";

import { credentialOf, guard, hmacMint, hmacVerify, requiredScope } from "./Auth.ts";

const run = Effect.runPromise;

describe("Auth", () => {
  describe("requiredScope", () => {
    const cases: Array<[string, string, "read" | "write"]> = [
      ["GET", "http://x/r/info/refs?service=git-upload-pack", "read"],
      ["GET", "http://x/r/info/refs?service=git-receive-pack", "write"],
      ["POST", "http://x/r/git-upload-pack", "read"],
      ["POST", "http://x/r/git-receive-pack", "write"],
      ["GET", "http://x/r/refs", "read"],
      ["POST", "http://x/r/commit", "write"],
    ];
    for (const [method, url, expected] of cases) {
      it(`${method} ${new URL(url).pathname} needs ${expected}`, () => {
        assert.equal(requiredScope(new Request(url, { method })), expected);
      });
    }
  });

  describe("credentialOf", () => {
    const withHeader = (value: string) =>
      new Request("http://x/r", { headers: { authorization: value } });

    it("takes the Basic password — how git sends a token", () => {
      assert.equal(credentialOf(withHeader(`Basic ${btoa("alice:tok123")}`)), "tok123");
    });
    it("falls back to the Basic username when the password is empty", () => {
      assert.equal(credentialOf(withHeader(`Basic ${btoa("tok123:")}`)), "tok123");
    });
    it("accepts Bearer", () => {
      assert.equal(credentialOf(withHeader("Bearer tok123")), "tok123");
    });
    it("is null without a header", () => {
      assert.equal(credentialOf(new Request("http://x/r")), null);
    });
  });

  describe("hmac tokens", () => {
    const secret = "test-secret";

    it("round-trips, bound to the repo", async () => {
      const token = await run(hmacMint(secret, "alpha", "read", 60));
      assert.match(token, /^git1\.read\./);
      assert.equal(await run(hmacVerify(secret, "alpha", token)), "read");
      // The same token means nothing at another repository.
      assert.equal(await run(hmacVerify(secret, "beta", token)), null);
    });

    it("rejects scope escalation by editing the token", async () => {
      const token = await run(hmacMint(secret, "alpha", "read", 60));
      const forged = token.replace(".read.", ".write.");
      assert.equal(await run(hmacVerify(secret, "alpha", forged)), null);
    });

    it("rejects expiry, tampering, and the wrong secret", async () => {
      const expired = await run(hmacMint(secret, "alpha", "write", -1));
      assert.equal(await run(hmacVerify(secret, "alpha", expired)), null);

      const token = await run(hmacMint(secret, "alpha", "write", 60));
      const flipped = token.slice(0, -1) + (token.endsWith("0") ? "1" : "0");
      assert.equal(await run(hmacVerify(secret, "alpha", flipped)), null);
      assert.equal(await run(hmacVerify("other-secret", "alpha", token)), null);
      assert.equal(await run(hmacVerify(secret, "alpha", "garbage")), null);
      assert.equal(await run(hmacVerify(secret, "alpha", null)), null);
    });
  });

  describe("guard", () => {
    const secret = "guard-secret";
    const request = (url: string, method: string, token?: string) =>
      new Request(url, {
        method,
        headers: token === undefined ? {} : { authorization: `Bearer ${token}` },
      });
    const verify = (credential: string | null) => hmacVerify(secret, "r", credential);

    it("401s an anonymous request, with the challenge git needs", async () => {
      const denied = await run(guard(request("http://x/r/refs", "GET"), verify));
      assert.equal(denied?.status, 401);
      assert.equal(denied?.headers.get("www-authenticate"), 'Basic realm="git"');
    });

    it("403s a read token on a write route", async () => {
      const read = await run(hmacMint(secret, "r", "read", 60));
      const denied = await run(guard(request("http://x/r/git-receive-pack", "POST", read), verify));
      assert.equal(denied?.status, 403);
    });

    it("passes read on read and write on everything", async () => {
      const read = await run(hmacMint(secret, "r", "read", 60));
      const write = await run(hmacMint(secret, "r", "write", 60));
      assert.equal(await run(guard(request("http://x/r/refs", "GET", read), verify)), null);
      assert.equal(
        await run(guard(request("http://x/r/git-receive-pack", "POST", write), verify)),
        null,
      );
      assert.equal(await run(guard(request("http://x/r/refs", "GET", write), verify)), null);
    });
  });
});
