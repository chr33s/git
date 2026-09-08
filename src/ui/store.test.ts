import assert from "node:assert/strict";
import { describe, it } from "@effect/vitest";
import type { Task } from "./model.ts";
import { TaskStore } from "./store.ts";

const task = (id: string, parent?: string, children?: readonly string[]): Task => {
  const base: Task = {
    id,
    title: id,
    kind: "Task",
    status: "Todo",
    avatar: "R",
    desc: "",
    assignees: [],
    labels: [],
    comments: [],
    updated: "now",
  };
  const nested = parent === undefined ? base : { ...base, parent };
  return children === undefined ? nested : { ...nested, children };
};

describe("task store", () => {
  it("preserves a live task's hierarchy when the remote move fails", async () => {
    const store = new TaskStore();
    store.adopt([
      task("old", undefined, ["child"]),
      task("new"),
      { ...task("child", "old"), hub: true },
    ]);
    const before = store.list();
    assert.equal(await store.move("child", "new"), false);
    assert.deepEqual(store.list(), before);
  });

  it("still moves a fixture task locally", async () => {
    const store = new TaskStore();
    store.adopt([task("old", undefined, ["child"]), task("new"), task("child", "old")]);
    assert.equal(await store.move("child", "new"), true);
    assert.equal(store.get("child")?.parent, "new");
    assert.deepEqual(store.get("old")?.children, []);
    assert.deepEqual(store.get("new")?.children, ["child"]);
  });
  it("keeps deep descendants visible and terminates cycles", () => {
    const store = new TaskStore();
    store.adopt([
      task("root", undefined, ["child"]),
      task("child", "root", ["grandchild"]),
      task("grandchild", "child", ["deep"]),
      task("deep", "grandchild", ["root"]),
    ]);
    assert.deepEqual(
      store.rows().map((row) => row.task.id),
      ["root", "child", "grandchild", "deep"],
    );
    assert.equal(store.rows("tasks").length, 4);
  });
  it("allocates valid, distinct local ids from empty and mixed state", () => {
    const store = new TaskStore();
    const input = { title: "new", desc: "", author: { name: "Review", avatar: "R" } };
    store.adopt([]);
    assert.equal(store.create(input).id, "T-1");
    store.adopt([task("T-1"), task("live-abc"), task("T-3")]);
    assert.equal(store.create(input).id, "T-4");
    assert.equal(store.create(input).id, "T-5");
  });
});
