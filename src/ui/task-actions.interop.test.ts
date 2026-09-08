import assert from "node:assert/strict";
import { existsSync } from "node:fs";
import { describe, it } from "@effect/vitest";
import { chromium } from "playwright";
import { browserBundle } from "../testing/Browser.ts";

const entry = `
  import "./src/ui/screen.detail.ts";
  import { store } from "./src/ui/store.ts";
  let accepted = false;
  let calls = 0;
  export const mount = () => {
    store.adopt([{ ...store.get("T-12"), id: "live-task", kind: "Task", hub: true,
      title: "Live task", status: "Todo", children: [], parent: undefined }]);
    store.taskActionRemote = async () => { calls++; return accepted; };
    const detail = document.createElement("gp-detail");
    detail.taskId = "live-task";
    document.body.append(detail);
  };
  export const prepare = (status, succeeds) => {
    accepted = succeeds;
    store.patch("live-task", task => ({ ...task, status }));
  };
  export const snapshot = () => ({ calls, status: store.get("live-task").status });
`;

describe.skipIf(!existsSync(chromium.executablePath()))("task action feedback in Chromium", () => {
  it("shows refused task actions and clears the notice when retrying", async () => {
    const bundle = await browserBundle(entry, "TaskActionReview");
    const browser = await chromium.launch();
    try {
      const page = await browser.newPage();
      await page.route("**/*", (route) =>
        route.fulfill({
          contentType:
            route.request().resourceType() === "document" ? "text/html" : "application/json",
          body:
            route.request().resourceType() === "document"
              ? "<!doctype html><title>Task action feedback</title>"
              : JSON.stringify({
                  items: [],
                  has_more: false,
                  next_cursor: null,
                  enabled: false,
                  reason: "fixture",
                }),
        }),
      );
      await page.goto("http://localhost");
      await page.addScriptTag({ content: bundle });
      await page.evaluate("TaskActionReview.mount()");
      let calls = 0;
      for (const label of ["Claim", "Release", "Complete", "Abandon"]) {
        const status = label === "Release" ? "In progress" : "Todo";
        await page.evaluate(`TaskActionReview.prepare(${JSON.stringify(status)}, false)`);
        await page.getByRole("button", { name: label, exact: true }).click();
        calls++;
        await page.waitForFunction(`TaskActionReview.snapshot().calls === ${calls}`);
        const notice = page.getByRole("alert");
        await notice.waitFor({ state: "visible", timeout: 2000 });
        assert.match(await notice.innerText(), /refused the task update/);
        assert.equal(await page.evaluate("TaskActionReview.snapshot().status"), status);

        await page.evaluate(`TaskActionReview.prepare(${JSON.stringify(status)}, true)`);
        await page.getByRole("button", { name: label, exact: true }).click();
        calls++;
        await page.waitForFunction(`TaskActionReview.snapshot().calls === ${calls}`);
        await notice.waitFor({ state: "hidden", timeout: 2000 });
      }
    } finally {
      await browser.close();
    }
  });
});
