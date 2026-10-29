/**
 * What the scanner catches, and — as importantly — what it leaves alone.
 *
 * A scanner that refused ordinary prompts would be turned off, and a
 * repository with it turned off is the one this exists to protect.
 */
import assert from "node:assert/strict";
import { describe, it } from "@effect/vitest";

import { Effect } from "effect";

import { scan } from "./Secrets.ts";

describe("Secrets", () => {
  it.effect("catches the accidents a prompt actually carries", () =>
    Effect.sync(() => {
      const caught = (text: string) => scan(text).map((finding) => finding.kind);

      assert.deepEqual(caught("use ghp_aBcDeFgHiJkLmNoPqRsTuVwXyZ0123456789 to fetch it"), [
        "provider token",
      ]);
      assert.ok(
        caught("clone https://user:s3cr3t-p4ssw0rd@git.example.com/repo").includes(
          "credential in a URL",
        ),
      );
      assert.ok(
        caught("DATABASE_URL=postgres://admin:hunter2hunter2@db.internal:5432/app").includes(
          "connection string",
        ),
      );
      assert.ok(
        caught('config: { "api_key": "9f8Xk2Lm4Qp7Rs1Tv6Wy" }').includes("named credential"),
      );
    }),
  );

  it.effect("finds a token the same prompt also names in prose", () =>
    Effect.sync(() => {
      // The first `AKIA` is a bare word, and a scan that stopped at the first
      // occurrence never looked at the key beside it. Nothing else covers a
      // twenty-character token.
      const caught = scan("AWS keys start with AKIA. Mine is AKIAIOSFODNN7EXAMPLE, rotate it.");
      assert.deepEqual(
        caught.map((finding) => finding.kind),
        ["provider token"],
      );
      assert.deepEqual(
        scan("xoxb- tokens look like xoxb-2334-4444-abcdefghijklmnop").map(
          (finding) => finding.kind,
        ),
        ["provider token"],
      );
    }),
  );

  it.effect("leaves a prefix alone inside an ordinary word", () =>
    Effect.sync(() => {
      const caught = (text: string) => scan(text).map((finding) => finding.kind);
      // Each of these carries `sk-` or `npm_` mid-word with a long tail behind
      // it — the shape a tool name or a task sentence takes, and the shape
      // that refused every record naming one.
      assert.deepEqual(caught("run risk-assessment-tool over the tree"), []);
      assert.deepEqual(caught("disk-usage-report for the build cache"), []);
      assert.deepEqual(caught("mask-secrets-in-output is on"), []);
      assert.deepEqual(caught("call my_npm_publish_helper first"), []);
      // And still finds the token at the front of a word, however it is fenced.
      assert.deepEqual(caught("key=sk-abcdefghijklmnop"), ["provider token"]);
      assert.deepEqual(caught('"sk-abcdefghijklmnop"'), ["provider token"]);
      assert.deepEqual(caught("(npm_abcdefghijklmnop)"), ["provider token"]);
    }),
  );

  it.effect("says what it found without repeating it", () =>
    Effect.sync(() => {
      const [finding] = scan("token=ghp_aBcDeFgHiJkLmNoPqRsTuVwXyZ0123456789");
      assert.notEqual(finding, undefined);
      assert.ok(
        !(finding?.hint ?? "").includes("QrSt"),
        `a refusal must not reprint the secret: ${finding?.hint}`,
      );
      assert.match(finding?.hint ?? "", /…/);
    }),
  );

  it.effect("leaves alone the prompts a fleet actually writes", () =>
    Effect.sync(() => {
      // Every one of these is a real instruction somebody would give, and a
      // scanner that refused them would be a scanner nobody leaves on.
      for (const prose of [
        "document how to set up agents with their own ssh key",
        "the test in src/server/Policy.test.ts fails on refs/heads/main",
        "run npm install before typecheck; postinstall applies patches/",
        "set DATABASE_URL=${DATABASE_URL} from the environment, never inline",
        "rename the thing or keep an alias? see docs/agents.md section 19",
        "fix: 90c7f2e1 broke the workerd integration project",
      ]) {
        assert.deepEqual(scan(prose), [], `plain prose must pass: ${prose}`);
      }
    }),
  );
});
