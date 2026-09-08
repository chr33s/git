import assert from "node:assert/strict";
import { existsSync } from "node:fs";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { describe, it } from "@effect/vitest";
import { Effect } from "effect";
import { chromium, type Page } from "playwright";
import { serve } from "../host/Node.ts";
import { browserBundle } from "../testing/Browser.ts";
import { fastImport, gitIn, hasGit, importCommit } from "../testing/Git.ts";

const entry = `
  import "./src/ui/screen.code.ts";
  import { GitApi } from "./src/ui/api.ts";
  import { LocalGitApi } from "./src/ui/local.ts";
  let code;
  let api;
  export const mount = async (local = false) => {
    code = document.createElement("gp-code");
    const remote = new GitApi({ repo: "fixture" });
    api = local ? await LocalGitApi.open({ repo: "fixture", cloneUrl: remote.cloneUrl }) : remote;
    code.api = api;
    document.body.append(code);
  };
  export const select = path => { code.wanted = path; };
  export const arrive = () => api.commitFiles({ branch: "main", message: "concurrent first commit", files: [{ path: "README.md", content: "concurrent version\\n" }] });
  export const current = async () => ({ state: await api.refState(), content: await api.file("main", "README.md") });
`;

const withRepository = async (
  test: (page: Page, directory: string) => Promise<void>,
): Promise<void> => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "ui-snapshot-"));
  const directory = path.join(root, "fixture");
  await fs.mkdir(directory);
  gitIn(directory)("init", "--bare", "-q", "-b", "main");
  const server = await serve({ root, allowAnonymousWrites: true });
  try {
    const bundle = await browserBundle(entry, "SnapshotReview");
    const browser = await chromium.launch();
    try {
      const page = await browser.newPage();
      await page.goto(server.url);
      await page.addScriptTag({ content: bundle });
      await test(page, directory);
    } finally {
      await browser.close();
    }
  } finally {
    await server.close();
    await fs.rm(root, { recursive: true, force: true });
  }
};

describe.skipIf(!hasGit || !existsSync(chromium.executablePath()))(
  "browser revision snapshots",
  () => {
    it.live("keeps the tree and subsequently opened files at the displayed commit", () =>
      Effect.promise(() =>
        withRepository(async (page, directory) => {
          fastImport(
            directory,
            [
              importCommit({
                branch: "refs/heads/main",
                mark: 1,
                message: "original snapshot",
                files: [
                  { path: "README.md", content: "original readme\n" },
                  { path: "second.txt", content: "original second\n" },
                ],
              }),
              importCommit({
                branch: "refs/heads/future",
                mark: 2,
                from: 1,
                message: "concurrent snapshot",
                files: [
                  { path: "README.md", content: "concurrent readme\n" },
                  { path: "second.txt", content: "concurrent second\n" },
                  { path: "new.txt", content: "concurrent file\n" },
                ],
              }),
            ].join(""),
          );
          const git = gitIn(directory);
          const original = git("rev-parse", "main").trim();
          const future = git("rev-parse", "future").trim();
          let moved = false;
          await page.route("**/fixture/files?*", async (route) => {
            if (!moved) {
              moved = true;
              git("update-ref", "refs/heads/main", future, original);
            }
            await route.continue();
          });
          await page.evaluate("SnapshotReview.mount()");
          await page.waitForSelector(".gp-commit-sha");
          await page.locator("diffs-container [data-line]").first().waitFor();
          assert.equal((await page.textContent(".gp-commit-sha"))?.trim(), original.slice(0, 7));
          assert.equal(git("rev-parse", "main").trim(), future);
          assert.equal(await page.getByText("original readme", { exact: true }).count(), 1);
          assert.equal(await page.locator('[data-item-path="new.txt"]').count(), 0);

          await page.evaluate("SnapshotReview.select('second.txt')");
          await page.getByText(/^(original|concurrent) second$/).waitFor();
          assert.equal(await page.getByText("original second", { exact: true }).count(), 1);
          assert.equal((await page.textContent(".gp-commit-sha"))?.trim(), original.slice(0, 7));
        }),
      ),
    );

    for (const local of [false, true]) {
      it.live(
        `refuses a first commit after another writer creates the ${local ? "OPFS" : "HTTP"} branch`,
        () =>
          Effect.promise(() =>
            withRepository(async (page, directory) => {
              fastImport(
                directory,
                importCommit({
                  branch: "refs/heads/future",
                  mark: 1,
                  message: "concurrent first commit",
                  files: [{ path: "README.md", content: "concurrent version\n" }],
                }),
              );
              await page.evaluate(`SnapshotReview.mount(${local})`);
              const create = page.getByRole("button", { name: "New file", exact: true });
              await create.click();
              const editor = page.locator(".gp-source-host [role='textbox']");
              await editor.waitFor({ state: "visible" });
              await page.fill(".gp-editor-path", "README.md");
              await editor.click();
              await page.keyboard.press("ControlOrMeta+A");
              await editor.evaluate((element) =>
                element.dispatchEvent(
                  new InputEvent("beforeinput", {
                    bubbles: true,
                    cancelable: true,
                    composed: true,
                    data: "my initial draft\n",
                    inputType: "insertText",
                  }),
                ),
              );
              const git = gitIn(directory);
              let concurrent: string;
              if (local) {
                concurrent = await page.evaluate(
                  "SnapshotReview.arrive().then(result => result.oid)",
                );
              } else {
                concurrent = git("rev-parse", "future").trim();
                git("update-ref", "refs/heads/main", concurrent);
              }
              await page.locator(".gp-editor-bar .gp-btn-primary").click();
              await page.waitForFunction(
                () =>
                  document.querySelector(".gp-notice[data-error]") !== null ||
                  document.querySelector(".gp-editor-bar") === null,
              );
              const current = await page.evaluate<{
                state: { refs: Array<{ name: string; oid: string }> };
                content: string;
              }>("SnapshotReview.current()");
              assert.equal(
                current.state.refs.find((ref) => ref.name === "refs/heads/main")?.oid,
                concurrent,
              );
              assert.equal(current.content, "concurrent version\n");
              assert.match(
                (await page.locator(".gp-notice[data-error]").textContent()) ?? "",
                /someone else committed/,
              );
              assert.equal(await editor.getByText("my initial draft", { exact: true }).count(), 1);
            }),
          ),
      );
    }
  },
);
