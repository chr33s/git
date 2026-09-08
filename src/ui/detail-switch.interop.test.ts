import assert from "node:assert/strict";
import { existsSync } from "node:fs";
import { describe, it } from "@effect/vitest";
import { chromium } from "playwright";
import { browserBundle } from "../testing/Browser.ts";

/**
 * A refusal belongs to the Change Request it was asked about.
 *
 * The reader can open another one while the hub is still deciding; the answer
 * that comes back must not put its notice on whatever is on screen by then.
 */
const entry = `
  import "./src/ui/screen.detail.ts";
  import { store } from "./src/ui/store.ts";
  let settle;
  export const mount = () => {
    const base = store.get("CR-14");
    const cr = (id, title) => ({ ...base, id, title, hub: true, parent: undefined,
      children: [], comments: [],
      review: { headline: "Waiting", detail: "one approval needed", ok: false, action: "Merge" } });
    store.adopt([cr("live-a", "First proposal"), cr("live-b", "Second proposal")]);
    store.reviewRemote = () => new Promise(resolve => { settle = resolve; });
    const detail = document.createElement("gp-detail");
    detail.taskId = "live-a";
    document.body.append(detail);
  };
  export const openOther = () => {
    document.querySelector("gp-detail").taskId = "live-b";
  };
  export const refuse = () => { if (settle === undefined) throw new Error("nothing in flight"); settle(false); };
  export const notices = () => [...document.querySelectorAll("gp-detail .gp-notice")].map(n => n.textContent.trim());
`;

describe.skipIf(!existsSync(chromium.executablePath()))("detail switching in Chromium", () => {
  it("keeps a refusal on the Change Request it was asked about", async () => {
    const bundle = await browserBundle(entry, "DetailSwitchReview");
    const browser = await chromium.launch();
    try {
      const page = await browser.newPage();
      await page.route("**/*", (route) =>
        route.fulfill({
          contentType:
            route.request().resourceType() === "document" ? "text/html" : "application/json",
          body:
            route.request().resourceType() === "document"
              ? "<!doctype html><title>Detail switching</title>"
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
      await page.evaluate("DetailSwitchReview.mount()");

      await page.getByRole("button", { name: "Approve", exact: true }).click();
      await page.evaluate("DetailSwitchReview.openOther()");
      await page.waitForFunction(
        () =>
          document.querySelector("gp-detail")?.textContent?.includes("Second proposal") === true,
      );
      await page.evaluate("DetailSwitchReview.refuse()");

      // The refusal answers a question asked about the other Change Request.
      await page.waitForTimeout(200);
      assert.deepEqual(await page.evaluate("DetailSwitchReview.notices()"), []);
      assert.equal(await page.getByRole("alert").count(), 0);

      // Asked and answered on the same one, the refusal is shown — and
      // announced, as every other refusal on this screen is.
      await page.getByRole("button", { name: "Approve", exact: true }).click();
      await page.evaluate("DetailSwitchReview.refuse()");
      const alert = page.getByRole("alert");
      await alert.waitFor({ state: "visible", timeout: 2000 });
      assert.match(await alert.innerText(), /refused the review/);
    } finally {
      await browser.close();
    }
  });
});
