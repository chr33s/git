import assert from "node:assert/strict";
import { existsSync } from "node:fs";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { describe, it } from "@effect/vitest";
import { Effect } from "effect";
import { chromium } from "playwright";
import { serve } from "../host/Node.ts";
import { browserBundle } from "../testing/Browser.ts";
import { fastImport, gitIn, hasGit, importCommit } from "../testing/Git.ts";

const entry = `
  import "./src/ui/screen.code.ts";
  import "./src/ui/screen.settings.ts";
  import "./src/ui/screen.search.ts";
  import "./src/ui/screen.activity.ts";
  import { GitApi } from "./src/ui/api.ts";
  import { LocalGitApi } from "./src/ui/local.ts";
  import { store } from "./src/ui/store.ts";
  const api = new GitApi({ repo: "fixture" });
  export const proposals = [];
  let code;
  export const mount = async (local, repo = "fixture") => {
    code?.remove();
    code = document.createElement("gp-code");
    const selected = new GitApi({ repo });
    code.api = local ? await LocalGitApi.open({ repo, cloneUrl: selected.cloneUrl }) : selected;
    document.body.append(code);
  };
  export const settings = () => {
    const settings = document.createElement("gp-settings");
    settings.api = api;
    document.body.append(settings);
  };
  export const search = async local => {
    document.querySelector("gp-search")?.remove();
    const search = document.createElement("gp-search");
    search.api = local ? await LocalGitApi.open({ repo: "fixture", cloneUrl: api.cloneUrl }) : api;
    search.query = "release search marker";
    document.body.append(search);
  };
  export const activity = async local => {
    document.querySelector("gp-activity")?.remove();
    const activity = document.createElement("gp-activity");
    activity.api = local ? await LocalGitApi.open({ repo: "fixture", cloneUrl: api.cloneUrl }) : api;
    document.body.append(activity);
  };
  export const propose = () => {
    store.openPullRemote = async input => { proposals.push(input); return "submitted"; };
    const form = code.querySelector(".gp-propose form");
    form.elements.namedItem("title").value = "Propose main";
    form.dispatchEvent(new SubmitEvent("submit", { bubbles: true, cancelable: true }));
  };
`;

describe.skipIf(!hasGit || !existsSync(chromium.executablePath()))(
  "default branch in Chromium",
  () => {
    it.live("uses Git's symbolic HEAD for code, proposals, and the settings deletion guard", () =>
      Effect.promise(async () => {
        const root = await fs.mkdtemp(path.join(os.tmpdir(), "ui-default-branch-"));
        const directory = path.join(root, "fixture");
        await fs.mkdir(directory);
        const git = gitIn(directory);
        git("init", "--bare", "-q", "-b", "release");
        fastImport(
          directory,
          [
            importCommit({ branch: "refs/heads/main", mark: 1, message: "main tip", files: [] }),
            importCommit({
              branch: "refs/heads/release",
              mark: 2,
              from: 1,
              message: "release tip",
              files: [{ path: "README.md", content: "release search marker\n" }],
            }),
          ].join(""),
        );
        const defaultBranch = git("symbolic-ref", "--short", "HEAD").trim();
        const head = git("rev-parse", "HEAD").trim();
        const server = await serve({ root, allowAnonymousWrites: true });
        try {
          const bundle = await browserBundle(entry, "DefaultBranchReview");
          const browser = await chromium.launch();
          try {
            const page = await browser.newPage();
            await page.goto(server.url);
            await page.addScriptTag({ content: bundle });
            for (const local of [false, true]) {
              await page.evaluate(`DefaultBranchReview.mount(${local})`);
              await page.waitForSelector(".gp-commit-sha");
              assert.equal((await page.textContent(".gp-commit-sha"))?.trim(), head.slice(0, 7));
              assert.equal(await page.locator(".gp-propose form").count(), 0);
            }
            for (const local of [false, true]) {
              await page.evaluate(`DefaultBranchReview.search(${local})`);
              await page.waitForFunction(
                () =>
                  document.querySelector("gp-search .gp-search-hit") !== null ||
                  [...document.querySelectorAll("gp-search .gp-empty")].some((element) =>
                    element.textContent?.includes("No file contents match"),
                  ),
              );
              assert.equal(await page.locator("gp-search .gp-search-hit-text").count(), 1);
              assert.equal(
                await page.locator("gp-search .gp-search-hit-text").textContent(),
                "release search marker",
              );
              await page.clock.setFixedTime(new Date(1_700_000_002_000));
              await page.evaluate(`DefaultBranchReview.activity(${local})`);
              await page.locator("gp-activity .gp-cal-event-title").first().waitFor();
              assert.equal(
                await page
                  .locator("gp-activity .gp-cal-event-title")
                  .filter({ hasText: /^release tip$/ })
                  .count(),
                1,
              );
            }
            await page
              .locator(".gp-branch-menu")
              .evaluate((element) =>
                element.dispatchEvent(
                  new CustomEvent("menu-select", { detail: { value: "main" }, bubbles: true }),
                ),
              );
            await page.waitForSelector(".gp-propose form", { state: "attached" });
            assert.match(
              (await page.textContent(".gp-propose")) ?? "",
              new RegExp(`against ${defaultBranch}`),
            );
            await page.evaluate("DefaultBranchReview.propose()");
            await page.waitForFunction("DefaultBranchReview.proposals.length === 1");
            assert.equal(
              await page.evaluate("DefaultBranchReview.proposals[0].base"),
              defaultBranch,
            );
            await page
              .locator(".gp-branch-menu")
              .evaluate((element) =>
                element.dispatchEvent(
                  new CustomEvent("menu-select", { detail: { value: "__rebase" }, bubbles: true }),
                ),
              );
            await page.waitForFunction(
              (short) => document.querySelector(".gp-commit-sha")?.textContent?.trim() === short,
              head.slice(0, 7),
            );
            assert.notEqual(
              git("rev-parse", "main").trim(),
              head,
              "rebasing the browser branch does not change the server branch",
            );

            await page.evaluate("DefaultBranchReview.settings()");
            await page.waitForSelector('[data-card="branches"] .gp-admin-row');
            const rows = page.locator('[data-card="branches"] .gp-admin-row');
            assert.equal(
              await rows
                .filter({ hasText: defaultBranch })
                .getByRole("button", { name: "Delete" })
                .isDisabled(),
              true,
            );
            assert.equal(
              await rows
                .filter({ hasText: "main" })
                .getByRole("button", { name: "Delete" })
                .isEnabled(),
              true,
            );

            const unborn = path.join(root, "unborn");
            await fs.mkdir(unborn);
            gitIn(unborn)("init", "--bare", "-q", "-b", "release");
            for (const local of [false, true]) {
              await page.evaluate(`DefaultBranchReview.mount(${local}, 'unborn')`);
              await page.waitForSelector(".gp-branch-menu");
              await page.waitForFunction(
                () =>
                  document.querySelector<HTMLButtonElement>('button[aria-label="New file"]')
                    ?.disabled === false,
              );
              assert.match((await page.textContent(".gp-branch-trigger")) ?? "", /release/);
              assert.equal(
                await page.getByRole("button", { name: "New file", exact: true }).isEnabled(),
                true,
              );
            }

            git("update-ref", "--no-deref", "HEAD", git("rev-parse", "main").trim());
            await page.evaluate("DefaultBranchReview.mount(false)");
            await page.waitForSelector(".gp-commit-sha");
            assert.equal(
              (await page.textContent(".gp-commit-sha"))?.trim(),
              git("rev-parse", "--short=7", "HEAD").trim(),
            );
            assert.equal(
              await page.getByRole("button", { name: "New file", exact: true }).isDisabled(),
              true,
            );
            assert.equal(await page.locator(".gp-propose form").count(), 0);
          } finally {
            await browser.close();
          }
        } finally {
          await server.close();
          await fs.rm(root, { recursive: true, force: true });
        }
      }),
    );
  },
);
