/** Run the source CLI and the shipped executable against repositories made by real Git. */
import assert from "node:assert/strict";
import { execFileSync, spawn } from "node:child_process";
import { existsSync } from "node:fs";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { describe, it } from "@effect/vitest";
import { Effect } from "effect";
import { hasGit } from "../testing/Git.ts";

const standalone = path.resolve("dist/sea/git+");
const source = path.resolve("src/cli/main.ts");
const modes = existsSync(standalone) ? ["source", "standalone"] : ["source"];

for (const mode of modes) {
  describe.skipIf(!hasGit)(`${mode} single-repository UI`, () => {
    it.live("refuses to start outside a repository", () =>
      Effect.promise(async () => {
        const directory = await fs.mkdtemp(path.join(os.tmpdir(), "serve-outside-"));
        try {
          assert.throws(
            () =>
              execFileSync(
                mode === "source" ? process.execPath : standalone,
                [...(mode === "source" ? [source] : []), "serve", "--ui"],
                {
                  cwd: directory,
                  env: {
                    ...process.env,
                    GIT_ROOT: undefined,
                    GIT_DIR: undefined,
                    GIT_WORK_TREE: undefined,
                  },
                  stdio: "pipe",
                },
              ),
            /not inside a Git repository/,
          );
        } finally {
          await fs.rm(directory, { recursive: true, force: true });
        }
      }),
    );
    for (const layout of ["nested", "bare", "linked"]) {
      it.live(`serves the ${layout} repository without a core directory`, () =>
        Effect.promise(async () => {
          const directory = await fs.mkdtemp(path.join(os.tmpdir(), "serve-repository-"));
          const checkout = path.join(directory, "project with spaces");
          const git = (...args: string[]) => execFileSync("git", args, { encoding: "utf8" }).trim();
          git("init", "-q", "-b", "main", checkout);
          git(
            "-C",
            checkout,
            "-c",
            "user.name=Test",
            "-c",
            "user.email=test@example.com",
            "commit",
            "-q",
            "--allow-empty",
            "-m",
            "fixture",
          );
          const head = git("-C", checkout, "rev-parse", "HEAD");
          let cwd = path.join(checkout, "nested", "deeper");
          await fs.mkdir(cwd, { recursive: true });
          if (layout === "bare") {
            cwd = path.join(directory, "bare.git");
            git("clone", "-q", "--bare", checkout, cwd);
          }
          if (layout === "linked") {
            cwd = path.join(directory, "linked");
            git("-C", checkout, "worktree", "add", "-q", "--detach", cwd);
          }
          const ui = path.join(directory, "override-ui");
          await fs.mkdir(ui);
          await fs.writeFile(
            path.join(ui, "index.html"),
            '<meta name="gp-repo" content="core" /><title>override</title>',
          );
          let command = process.execPath;
          const args = mode === "source" ? [source] : [];
          if (mode === "standalone") {
            command = path.join(directory, "git+");
            await fs.copyFile(standalone, command);
            await fs.chmod(command, 0o755);
          }
          const child = spawn(
            command,
            [
              ...args,
              "serve",
              "--ui",
              ...(mode === "source" ? ["--ui-dir", ui] : []),
              "--port",
              "0",
              "--hostname",
              "127.0.0.1",
            ],
            {
              cwd,
              env: {
                ...process.env,
                GIT_ROOT: undefined,
                GIT_DIR: undefined,
                GIT_WORK_TREE: undefined,
                GIT_HOSTS: "",
              },
              stdio: ["ignore", "pipe", "pipe"],
            },
          );
          let output = "";
          const ready = Promise.withResolvers<string>();
          child.stdout.on("data", (chunk: Buffer) => {
            output += chunk.toString();
            const url = /server on (http:\/\/[^\s,]+)/.exec(output)?.[1];
            if (url !== undefined) ready.resolve(url);
          });
          child.stderr.on("data", (chunk: Buffer) => {
            output += chunk.toString();
          });
          const finished = new Promise<void>((resolve, reject) => {
            child.once("error", reject);
            child.once("close", () => resolve());
          });
          const timer = setTimeout(
            () => ready.reject(new Error(`server did not start: ${output}`)),
            10_000,
          );
          void finished.then(() => ready.reject(new Error(`server exited: ${output}`)));
          try {
            const url = await ready.promise;
            const page = await fetch(`${url}/hub/code/nested/file.ts`);
            assert.equal(page.status, 200);
            const html = await page.text();
            assert.match(html, /name="gp-repo" content="repo"/);
            assert.doesNotMatch(html, /fonts\.googleapis|fonts\.gstatic/);
            assert.ok(git("ls-remote", `${url}/repo.git`, "HEAD").includes(head));
            const refs = await fetch(`${url}/repo/refs`);
            assert.equal(refs.status, 200);
            assert.ok((await refs.text()).includes(head));
            assert.equal((await fetch(`${url}/other/refs`)).status, 404);
            const redirect = await fetch(url, { redirect: "manual" });
            assert.equal(redirect.headers.get("location"), "/hub/code");
            if (mode === "standalone") {
              const css = await fetch(`${url}/hub/main.css`);
              assert.equal(css.status, 200);
              const text = await css.text();
              const fonts = [...text.matchAll(/url\(["']?([^\s)"']+\.woff2)["']?\)/g)];
              assert.ok(fonts.length > 0, "Fontsource fonts are in the build");
              for (const font of fonts) {
                const response = await fetch(new URL(font[1] ?? "", `${url}/hub/main.css`));
                assert.equal(response.headers.get("content-type"), "font/woff2");
                assert.equal(
                  Buffer.from(await response.arrayBuffer())
                    .subarray(0, 4)
                    .toString(),
                  "wOF2",
                );
              }
              const license = await fetch(`${url}/hub/licenses/instrument-sans.txt`);
              assert.equal(license.status, 200);
              assert.match(await license.text(), /SIL OPEN FONT LICENSE/);
              const script = await fetch(`${url}/hub/main.js`);
              assert.equal(script.headers.get("content-type"), "text/javascript");
              await script.body?.cancel();
            }
          } finally {
            clearTimeout(timer);
            child.kill("SIGTERM");
            await finished;
            await fs.rm(directory, { recursive: true, force: true });
          }
        }),
      );
    }
  });
}
