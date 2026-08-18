/**
 * What the scanner catches, and — as importantly — what it leaves alone.
 *
 * A scanner that refused ordinary prompts would be turned off, and a
 * repository with it turned off is the one this exists to protect.
 */
import assert from "node:assert/strict";
import { describe, it } from "@effect/vitest";

import { scan } from "./Secrets.ts";

describe("Secrets", () => {
  it("catches the accidents a prompt actually carries", () => {
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
    assert.ok(caught('config: { "api_key": "9f8Xk2Lm4Qp7Rs1Tv6Wy" }').includes("named credential"));
  });

  it("says what it found without repeating it", () => {
    const [finding] = scan("token=ghp_aBcDeFgHiJkLmNoPqRsTuVwXyZ0123456789");
    assert.notEqual(finding, undefined);
    assert.ok(
      !(finding?.hint ?? "").includes("QrSt"),
      `a refusal must not reprint the secret: ${finding?.hint}`,
    );
    assert.match(finding?.hint ?? "", /…/);
  });

  it("leaves alone the prompts a fleet actually writes", () => {
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
  });
});
