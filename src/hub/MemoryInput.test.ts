import assert from "node:assert/strict";
import { Buffer } from "node:buffer";
import { it } from "@effect/vitest";
import { Effect } from "effect";
import { compareUtf8 } from "../text.ts";
import * as Input from "./MemoryInput.ts";

it("orders decoded keys by UTF-8 bytes, including numeric-looking and astral keys", () => {
  const value = { "2": [{ 𐀀: "astral", "": "BMP" }], "10": "ten", a: null };
  assert.equal(Input.canonical(value), '{"10":"ten","2":[{"":"BMP","𐀀":"astral"}],"a":null}');
  const names = ["𐀀", "", "a", "2", "10"];
  assert.deepEqual(
    [...names].sort(compareUtf8),
    [...names].sort((left, right) => Buffer.compare(Buffer.from(left), Buffer.from(right))),
  );
});

it.effect("key insertion order cannot change nested manifest commitments", () =>
  Effect.gen(function* () {
    const one = { "2": { b: 2, a: 1 }, "10": ["first", "second"], 𐀀: true, "": false };
    const two = { "": false, 𐀀: true, "10": ["first", "second"], "2": { a: 1, b: 2 } };
    assert.equal(yield* Input.digest(one), yield* Input.digest(two));
    assert.notEqual(
      yield* Input.digest(["first", "second"]),
      yield* Input.digest(["second", "first"]),
    );
  }),
);
