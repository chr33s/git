import assert from "node:assert/strict";
import { existsSync } from "node:fs";
import { describe, it } from "@effect/vitest";
import { chromium } from "playwright";
import { browserBundle } from "../testing/Browser.ts";

const entry = `
  import "./src/ui/screen.code.ts";
  import { store } from "./src/ui/store.ts";
  const gate = Promise.withResolvers();
  const started = Promise.withResolvers();
  let hold = false;
  const pushes = [];
  const proposals = [];
  const heads = { main: "c".repeat(40), topic: "a".repeat(40), other: "b".repeat(40) };
  const api = {
    repo: "core", cloneUrl: "http://localhost/core",
    refs: async () => Object.entries(heads).map(([name, oid]) => ({ name: "refs/heads/" + name, oid })),
    refState: async () => ({ head: "refs/heads/main", refs: await api.refs() }),
    files: async () => [], file: async () => "",
    commitDetail: async oid => ({ oid, subject: oid, author: "Test", email: "test@example.com", at: new Date(0), parents: [] }),
    sync: async branch => {
      if (hold) { started.resolve(); await gate.promise; }
      return { branch, ahead: 1, behind: 0, remote: heads[branch] };
    },
    push: async branch => { pushes.push(branch); return [{ ref: "refs/heads/" + branch, ok: true }]; },
  };
  let code;
  export const mount = (accepted = true) => {
    store.openPullRemote = async input => { proposals.push(input); return accepted ? "submitted" : null; };
    code = document.createElement("gp-code");
    code.api = api;
    document.body.append(code);
  };
  export const select = branch => code.querySelector(".gp-branch-menu").dispatchEvent(
    new CustomEvent("menu-select", { detail: { value: branch }, bubbles: true }),
  );
  export const submit = () => {
    const form = code.querySelector(".gp-propose form");
    form.elements.namedItem("title").value = "Review topic";
    form.elements.namedItem("desc").value = "Original description";
    hold = true;
    form.dispatchEvent(new SubmitEvent("submit", { bubbles: true, cancelable: true }));
    return started.promise;
  };
  export const release = () => { hold = false; gate.resolve(); };
  export const result = () => ({ pushes, proposals });
`;

describe.skipIf(!existsSync(chromium.executablePath()))("proposal submission in Chromium", () => {
  for (const accepted of [true, false]) {
    it(
      accepted
        ? "keeps the submitted branch, revision, and description during an awaited sync"
        : "retains the proposal dialog and draft when submission is refused",
      async () => {
        const bundle = await browserBundle(entry, "ProposalReview");
        const browser = await chromium.launch();
        try {
          const page = await browser.newPage();
          await page.route("**/*", (route) =>
            route.fulfill({
              contentType:
                route.request().resourceType() === "document" ? "text/html" : "application/json",
              body:
                route.request().resourceType() === "document"
                  ? "<!doctype html><title>Proposal regression</title>"
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
          await page.evaluate(`ProposalReview.mount(${accepted})`);
          await page.waitForSelector(".gp-commit-sha");
          await page.evaluate("ProposalReview.select('topic')");
          await page.waitForSelector(".gp-propose form", { state: "attached" });
          if (!accepted) await page.evaluate("document.querySelector('.gp-propose').show()");
          await page.evaluate("ProposalReview.submit()");
          assert.equal(await page.isEnabled(".gp-branch-menu [data-menu-trigger]"), true);
          await page.evaluate("ProposalReview.select('other')");
          await page.waitForFunction(
            "document.querySelector('.gp-commit-sha').textContent.includes('bbbbbbb')",
          );
          await page.locator("#gp-propose-desc").evaluate((element) => {
            if (element instanceof HTMLTextAreaElement) element.value = "Changed after submitting";
          });
          await page.evaluate("ProposalReview.release()");
          await page.waitForFunction("ProposalReview.result().proposals.length === 1");
          const result = await page.evaluate<{
            pushes: string[];
            proposals: Array<{ head: string; base: string; description: string }>;
          }>("ProposalReview.result()");
          assert.deepEqual(result.pushes, ["topic"]);
          assert.equal(result.proposals[0]?.head, "a".repeat(40));
          assert.equal(result.proposals[0]?.base, "main");
          assert.equal(result.proposals[0]?.description, "Original description");
          if (!accepted) {
            await page.waitForFunction(
              "!document.querySelector('.gp-propose button[type=submit]').disabled",
            );
            assert.equal(await page.locator("#gp-propose-title").inputValue(), "Review topic");
            assert.equal(
              await page.locator("#gp-propose-desc").inputValue(),
              "Changed after submitting",
            );
            assert.equal(await page.locator("#gp-propose-title").isVisible(), true);
            assert.match(await page.locator(".gp-propose [role=alert]").innerText(), /refused/);
          }
        } finally {
          await browser.close();
        }
      },
    );
  }
});
