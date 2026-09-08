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

/**
 * The shell hands every screen one client and swaps it when the local OPFS
 * repository comes ready. A screen that already answered from an unreachable
 * client must read the replacement, not keep its offline fallback forever.
 */
const entry = `
  import "./src/ui/screen.activity.ts";
  import "./src/ui/screen.search.ts";
  import { GitApi } from "./src/ui/api.ts";
  const DOWN = "http://127.0.0.1:1";
  let activity;
  let search;
  export const mount = () => {
    activity?.remove();
    search?.remove();
    activity = document.createElement("gp-activity");
    activity.api = new GitApi({ repo: "fixture", base: DOWN });
    search = document.createElement("gp-search");
    search.api = new GitApi({ repo: "fixture", base: DOWN });
    search.query = "swap marker";
    document.body.append(activity, search);
  };
  export const swap = () => {
    activity.api = new GitApi({ repo: "fixture" });
    search.api = new GitApi({ repo: "fixture" });
  };
`;

describe.skipIf(!hasGit || !existsSync(chromium.executablePath()))(
  "client replacement in Chromium",
  () => {
    it.live("reloads mounted screens when the shell swaps the client", () =>
      Effect.promise(async () => {
        const root = await fs.mkdtemp(path.join(os.tmpdir(), "ui-client-swap-"));
        const directory = path.join(root, "fixture");
        await fs.mkdir(directory);
        const git = gitIn(directory);
        git("init", "--bare", "-q", "-b", "main");
        fastImport(
          directory,
          importCommit({
            branch: "refs/heads/main",
            mark: 1,
            message: "recovered tip",
            files: [{ path: "README.md", content: "swap marker\n" }],
          }),
        );
        const server = await serve({ root, allowAnonymousWrites: true });
        try {
          const bundle = await browserBundle(entry, "ClientSwapReview");
          const browser = await chromium.launch();
          try {
            const page = await browser.newPage();
            await page.goto(server.url);
            await page.addScriptTag({ content: bundle });
            await page.clock.setFixedTime(new Date(1_700_000_002_000));
            await page.evaluate("ClientSwapReview.mount()");
            await page.locator("gp-activity .gp-notice").waitFor();
            await page.waitForFunction(
              () => document.querySelector("gp-search .gp-empty, gp-search .gp-notice") !== null,
            );
            await page.evaluate("ClientSwapReview.swap()");
            await page
              .locator("gp-activity .gp-cal-event-title")
              .filter({ hasText: /^recovered tip$/ })
              .waitFor({ timeout: 5_000 });
            assert.equal(await page.locator("gp-activity .gp-notice").count(), 0);
            await page.locator("gp-search .gp-search-hit-text").waitFor({ timeout: 5_000 });
            assert.equal(
              await page.locator("gp-search .gp-search-hit-text").textContent(),
              "swap marker",
            );
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
