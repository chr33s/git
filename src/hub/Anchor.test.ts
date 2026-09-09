import assert from "node:assert/strict";
import { describe, it } from "@effect/vitest";
import { Effect } from "effect";

import { AnchorResolver } from "./Anchor.ts";
import { syntax } from "./Anchor.syntax.ts";

const encoder = new TextEncoder();
const resolve = (path: string, source: string, anchor: string) =>
  Effect.gen(function* () {
    return yield* (yield* AnchorResolver).resolve(path, encoder.encode(source), anchor);
  }).pipe(Effect.provide(syntax));

describe("anchored source", () => {
  it.effect("qualifies TypeScript symbols and ignores formatting and comments", () =>
    Effect.gen(function* () {
      const first = yield* resolve(
        "src/auth.ts",
        "export function verify(token: string): boolean {\n  return token.length > 0\n}\n",
        "verify",
      );
      const formatted = yield* resolve(
        "src/auth.ts",
        "export function verify( token: string ): boolean {\n  // retained constraint\n  return token.length > 0;\n}\n",
        "function verify",
      );
      assert.equal(first._tag, "Found");
      assert.equal(formatted._tag, "Found");
      if (first._tag !== "Found" || formatted._tag !== "Found") return;
      assert.equal(first.anchor.value, "function verify");
      // A semicolon is a token rather than formatting, so compare a source
      // whose only differences really are whitespace and comments.
      const commentOnly = yield* resolve(
        "src/auth.ts",
        "export function verify( token: string ): boolean {\n  // retained constraint\n  return token.length > 0\n}\n",
        "function verify",
      );
      assert.equal(commentOnly._tag, "Found");
      if (commentOnly._tag !== "Found") return;
      assert.equal(first.fingerprint.contentHash, commentOnly.fingerprint.contentHash);
      assert.notEqual(first.fingerprint.rawHash, commentOnly.fingerprint.rawHash);
    }),
  );

  it.effect("separates contract drift from implementation drift", () =>
    Effect.gen(function* () {
      const original = yield* resolve(
        "a.ts",
        "function verify(token: string): boolean { return token.length > 0 }",
        "function verify",
      );
      const body = yield* resolve(
        "a.ts",
        "function verify(token: string): boolean { return token.length > 1 }",
        "function verify",
      );
      const contract = yield* resolve(
        "a.ts",
        "function verify(token: Uint8Array): boolean { return token.length > 0 }",
        "function verify",
      );
      assert.equal(original._tag, "Found");
      assert.equal(body._tag, "Found");
      assert.equal(contract._tag, "Found");
      if (original._tag !== "Found" || body._tag !== "Found" || contract._tag !== "Found") return;
      assert.equal(original.fingerprint.signatureHash, body.fingerprint.signatureHash);
      assert.notEqual(original.fingerprint.contentHash, body.fingerprint.contentHash);
      assert.notEqual(original.fingerprint.signatureHash, contract.fingerprint.signatureHash);
    }),
  );

  it.effect("discovers Markdown heading spans", () =>
    Effect.gen(function* () {
      const resolver = yield* AnchorResolver;
      const found = yield* resolver.anchors(
        "readme.md",
        encoder.encode("# Top\nintro\n## Authentication\nbody\n## Other\nrest\n"),
      );
      assert.deepEqual(
        found.map((anchor) => [anchor.value, anchor.startLine, anchor.endLine]),
        [
          ["@file", 1, 6],
          ["# Top", 1, 6],
          ["## Authentication", 3, 4],
          ["## Other", 5, 6],
        ],
      );
    }).pipe(Effect.provide(syntax)),
  );
});
