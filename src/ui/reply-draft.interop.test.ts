import assert from "node:assert/strict";
import { existsSync } from "node:fs";
import { describe, it } from "@effect/vitest";
import { chromium } from "playwright";
import { browserBundle } from "../testing/Browser.ts";

const entry = `
  import "./src/ui/screen.detail.ts";
  import { store } from "./src/ui/store.ts";
  let pending;
  const requests = [];
  export const mount = () => {
    store.adopt([{ ...store.get("CR-14"), id: "live-pull", hub: true,
      children: [], parent: undefined,
      threads: [{ id: "thread", path: null, resolved: false, comments: [] }] }]);
    store.replyRemote = (id, thread, body) => {
      requests.push({ id, thread, body });
      pending = Promise.withResolvers();
      return pending.promise;
    };
    store.commentRemote = (id, body) => store.replyRemote(id, null, body);
    const detail = document.createElement("gp-detail");
    detail.taskId = "live-pull";
    document.body.append(detail);
  };
  export const finish = async sent => {
    pending.resolve(sent);
    await new Promise(requestAnimationFrame);
  };
  export const submitted = () => requests;
`;

describe.skipIf(!existsSync(chromium.executablePath()))("reply drafts in Chromium", () => {
  for (const kind of ["reply", "comment"]) {
    it(`retains refused ${kind} drafts and edits made during a send`, async () => {
      const bundle = await browserBundle(entry, "ReplyDraftReview");
      const browser = await chromium.launch();
      try {
        const page = await browser.newPage();
        await page.route("**/*", (route) =>
          route.fulfill({
            contentType:
              route.request().resourceType() === "document" ? "text/html" : "application/json",
            body:
              route.request().resourceType() === "document"
                ? "<!doctype html><title>Reply draft retention</title>"
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
        await page.evaluate("ReplyDraftReview.mount()");
        const field = page.locator(
          kind === "reply" ? ".gp-thread-reply input" : ".gp-comment-form textarea",
        );
        let count = 0;
        for (const scenario of [
          { sent: false, edited: false },
          { sent: true, edited: true },
          { sent: true, edited: false },
        ]) {
          const draft = `Reply ${++count}`;
          await field.fill(draft);
          if (kind === "reply") await field.press("Enter");
          else await page.locator(".gp-comment-form button[type=submit]").click();
          await page.waitForFunction(`ReplyDraftReview.submitted().length === ${count}`);
          if (kind === "reply")
            await page.waitForFunction("document.querySelector('.gp-thread-head button').disabled");
          if (scenario.edited) await field.fill("My next reply");
          await page.evaluate(`ReplyDraftReview.finish(${scenario.sent})`);
          await page.waitForFunction("!document.querySelector('.gp-thread-head button').disabled");
          assert.equal(
            await field.inputValue(),
            scenario.edited ? "My next reply" : scenario.sent ? "" : draft,
          );
          assert.deepEqual(await page.evaluate("ReplyDraftReview.submitted().at(-1)"), {
            id: "live-pull",
            thread: kind === "reply" ? "thread" : null,
            body: draft,
          });
        }
      } finally {
        await browser.close();
      }
    });
  }
});
