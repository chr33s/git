/**
 * The partition, exhaustively.
 *
 * Six variants of one defect reached the reviewers over fifty rounds, and
 * every one of them was a record that neither reader claimed or that both did.
 * They were closed one case at a time. What this asserts is the property that
 * makes a seventh impossible: `ownerOf` is total, so every combination of
 * payload and commit message lands in exactly one namespace.
 */
import assert from "node:assert/strict";
import { describe, it } from "@effect/vitest";

import * as Claim from "./Claim.ts";

/** A payload as it sits in `event.json`: whatever a writer chose to put there. */
type Written = Record<string, string | number> | ReadonlyArray<number>;
const bytes = (value: Written) => new TextEncoder().encode(JSON.stringify(value));

describe("who owns a record", () => {
  it("puts every payload and message combination in exactly one namespace", () => {
    const payloads = [
      undefined,
      bytes({ type: Claim.EXPOSURE }),
      bytes({ type: "invocation-telemetry" }),
      bytes({ type: "something-newer" }),
      bytes({ type: 7 }),
      bytes({ foo: 1 }),
      bytes([1, 2]),
      new TextEncoder().encode("not json at all"),
      new Uint8Array(),
    ];
    const messages = [null, undefined, "", Claim.EXPOSURE, "tool-operation", "noise"];

    for (const payload of payloads) {
      for (const type of messages) {
        const owner = Claim.ownerOf({ payload, type });
        assert.equal(owner === "context" || owner === "telemetry", true, `${String(type)}`);
      }
    }
  });

  it("lets the signed payload outrank the commit message", () => {
    // The message is unsigned — whoever may append chooses it — so a
    // mislabelled record is read as what its signer put their key to.
    assert.equal(
      Claim.ownerOf({ payload: bytes({ type: Claim.EXPOSURE }), type: "tool-operation" }),
      "context",
    );
    assert.equal(
      Claim.ownerOf({ payload: bytes({ type: "tool-operation" }), type: Claim.EXPOSURE }),
      "telemetry",
    );
  });

  it("falls back to the message only when the payload cannot answer", () => {
    // Which is the state a redaction leaves: the commit stays so the hash
    // chain holds, and the payload is gone.
    assert.equal(Claim.ownerOf({ type: Claim.EXPOSURE }), "context");
    assert.equal(Claim.ownerOf({ payload: bytes({ foo: 1 }), type: Claim.EXPOSURE }), "context");
    assert.equal(Claim.ownerOf({ type: null }), "telemetry");
  });

  it("reads a type written with a JSON escape", () => {
    // A byte search for the literal misses this; parsing does not. This
    // codebase's writers never emit it and another implementation may.
    const escaped = new TextEncoder().encode('{"type":"context-exposur\\u0065"}');
    assert.equal(Claim.ownerOf({ payload: escaped }), "context");
  });

  it("binds a record to a repository and a ref, and to each independently", () => {
    const payload = { repo: "SHA256:here", session: "S" };
    assert.equal(Claim.bound(payload, { repo: "SHA256:here", session: "S" }), true);
    assert.equal(Claim.bound(payload, { repo: "SHA256:elsewhere", session: "S" }), false);
    assert.equal(Claim.bound(payload, { repo: "SHA256:here", session: "T" }), false);
    // A caller that knows only one half asks about only that half: `entries`
    // is handed a session and may not know the repository.
    assert.equal(Claim.bound(payload, { session: "S" }), true);
    assert.equal(Claim.bound(payload, { repo: "SHA256:here" }), true);
    assert.equal(Claim.bound(payload, {}), true);
  });
});
