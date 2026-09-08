import assert from "node:assert/strict";
import { describe, it } from "@effect/vitest";
import { hierarchy } from "./TaskHierarchy.ts";

describe("task hierarchy", () => {
  it("cuts cycle members while preserving their descendants and missing-parent edges", () => {
    const result = hierarchy(
      new Map([
        ["child", "a"],
        ["a", "b"],
        ["b", "a"],
        ["self", "self"],
        ["orphan", "missing"],
        ["root", null],
      ]),
    );
    assert.deepEqual(
      [...result.parents],
      [
        ["child", "a"],
        ["a", null],
        ["b", null],
        ["self", null],
        ["orphan", "missing"],
        ["root", null],
      ],
    );
    assert.deepEqual(
      [...result.children],
      [
        ["a", ["child"]],
        ["missing", ["orphan"]],
      ],
    );
  });

  it("walks a deep namespace without recursion or repeated ancestry scans", () => {
    const edges = new Map<string, string | null>();
    for (let index = 0; index < 40_000; index++) edges.set(String(index), String(index + 1));
    edges.set("40000", "39999");
    const result = hierarchy(edges);
    assert.equal(result.parents.get("0"), "1");
    assert.equal(result.parents.get("39999"), null);
    assert.equal(result.parents.get("40000"), null);
    assert.deepEqual(result.children.get("39999"), ["39998"]);
    assert.equal(result.children.has("40000"), false);
  });
});
