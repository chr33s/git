import assert from "node:assert/strict";
import { describe, it } from "@effect/vitest";

import { Effect, Result } from "effect";

import type { RepoId } from "../trust/Genesis.ts";
import { principalId } from "../trust/Principal.ts";
import { decodeIdentifier, encodePrincipal, encodeRepository } from "./Encode.ts";

/** SAFETY: forty-three base64 characters after `SHA256:`, which is the shape. */
const repo = (seed: string): RepoId => {
  // SAFETY: forty-three base64 characters with canonical zero padding bits in
  // the final character, matching a real SHA-256 digest's unpadded encoding.
  return `SHA256:${seed.repeat(42).slice(0, 42)}A` as RepoId;
};

const value = <A, E>(result: Result.Result<A, E>): A => {
  assert.ok(Result.isSuccess(result));
  return result.success;
};

describe("shareable identifiers", () => {
  it.effect("round-trips a typed PrincipalID with bootstrap-only location hints", () =>
    Effect.sync(() => {
      const id = principalId(repo("a"));
      const encoded = value(
        encodePrincipal({
          id,
          hints: ["https://git.alice.example/id", "https://mirror.example/alice"],
        }),
      );

      assert.match(encoded, /^gid1/);
      assert.deepEqual(value(decodeIdentifier(encoded)), {
        kind: "principal",
        id,
        hints: ["https://git.alice.example/id", "https://mirror.example/alice"],
      });
    }),
  );

  it.effect("keeps repository and principal identifiers type-distinct", () =>
    Effect.sync(() => {
      const id = repo("b");
      const encoded = value(encodeRepository({ id, hints: [] }));
      const decoded = value(decodeIdentifier(encoded));

      assert.match(encoded, /^grepo1/);
      assert.equal(decoded.kind, "repository");
      assert.equal(decoded.id, id);
    }),
  );

  it.effect("detects a one-character transcription error", () =>
    Effect.sync(() => {
      const encoded = value(encodeRepository({ id: repo("c"), hints: [] }));
      const last = encoded.at(-1);
      if (last === undefined) assert.fail("encoded identifiers cannot be empty");
      const corrupted = `${encoded.slice(0, -1)}${last === "q" ? "p" : "q"}`;
      const decoded = decodeIdentifier(corrupted);

      assert.ok(Result.isFailure(decoded));
      assert.match(decoded.failure.reason, /checksum/);
    }),
  );

  it.effect("bounds the bootstrap data carried in an identifier", () =>
    Effect.sync(() => {
      const encoded = encodeRepository({
        id: repo("d"),
        hints: [
          "https://a.example",
          "https://b.example",
          "https://c.example",
          "https://d.example",
          "https://e.example",
        ],
      });
      assert.ok(Result.isFailure(encoded));
      assert.match(encoded.failure.reason, /at most four/i);
    }),
  );
});
