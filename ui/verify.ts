/**
 * Drives the built UI in a real browser and checks it behaves.
 *
 * Three suites, because there are three things worth proving and they fail for
 * different reasons:
 *
 *   render      every screen mounts in both palettes, with no page errors
 *   interact    the behaviours the design conversation settled on still work
 *   live        Code and Diff read the JSON API, not the fixtures
 *
 * The `live` suite serves the shapes from `src/server/Api.ts` rather than
 * booting a Worker, so it runs anywhere and fails loudly if this UI and that
 * declaration drift apart — which is the risk a hand-written client carries
 * (see the note at the top of `api.ts`).
 *
 *   node ui/build.ts && node ui/verify.ts
 *   node ui/verify.ts --shots <dir>    # also write screenshots
 *
 * Playwright is already a devDependency, and Chromium is expected on PATH or at
 * PLAYWRIGHT_BROWSERS_PATH; pass --executable to point at a specific binary.
 */
import { chromium, type Browser, type Page } from "playwright";
import { createServer, type Server } from "node:http";
import { readFile, stat } from "node:fs/promises";
import { dirname, extname, join, normalize } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
/** Output lives at the repository root alongside `dist/sea`, not under `ui/`. */
const dist = join(here, "..", "dist", "ui");

const flag = (name: string): string | undefined => {
  const at = process.argv.indexOf(name);
  return at === -1 ? undefined : process.argv[at + 1];
};

const shots = flag("--shots");
const executable = flag("--executable") ?? process.env["CHROMIUM_PATH"];

/** The extensions the built UI actually serves; anything else is a byte stream. */
const mimeOf = (extension: string): string => {
  switch (extension) {
    case ".css":
      return "text/css";
    case ".html":
      return "text/html";
    case ".js":
      return "text/javascript";
    case ".map":
      return "application/json";
    default:
      return "application/octet-stream";
  }
};

const failures: string[] = [];

const check = (name: string, ok: boolean, detail = ""): void => {
  console.info(`${ok ? "  ok  " : "FAIL  "}${name}${detail === "" ? "" : `  — ${detail}`}`);
  if (!ok) failures.push(name);
};

/** A repository the `live` suite serves: two refs, and a file that differs. */
const OID_MAIN = "2".repeat(40);
const OID_BRANCH = "1".repeat(40);
const BRANCH = "rbaek/auth-middleware";

/** Blob contents keyed by tree path — one repository, at two revisions. */
interface Tree {
  readonly [path: string]: string | undefined;
}

const AT_MAIN: Tree = {
  "README.md": "# core\n\nLive from the API.\n",
  "src/git/Store.ts": "export const store = 2;\n",
  "src/server/Api.ts": "export const api = 1;\n",
};

const AT_BRANCH: Tree = {
  ...AT_MAIN,
  "src/server/Api.ts": "export const api = 2;\nexport const added = true;\n",
};

const base64 = (text: string): string => Buffer.from(text, "utf8").toString("base64");

/**
 * The success bodies this stub returns, transcribed from `src/server/Api.ts`
 * alongside the client in `api.ts`. Naming them is what makes a drift between
 * the two show up as a type error here rather than as a silent test pass.
 */
type ApiAnswer =
  | { readonly refs: ReadonlyArray<{ readonly name: string; readonly oid: string }> }
  | {
      readonly files: ReadonlyArray<{
        readonly path: string;
        readonly mode: string;
        readonly oid: string;
      }>;
    }
  | {
      readonly path: string;
      readonly mode: string;
      readonly oid: string;
      readonly content: string;
      readonly encoding: "base64";
      readonly size: number;
    }
  | { readonly commits: ReadonlyArray<{ readonly oid: string; readonly message: string }> }
  | {
      readonly files: ReadonlyArray<{
        readonly path: string;
        readonly status: "added" | "removed" | "modified";
        readonly binary: boolean;
        readonly patch: string;
      }>;
    };

/**
 * Static files, plus — when `api` is set — the JSON endpoints the UI calls.
 *
 * With the API off, every call 404s, which is exactly the condition the
 * fixture fallback exists for; the `render` and `interact` suites rely on that.
 */
const serve = async (api: boolean, port: number): Promise<Server> => {
  const server = createServer((request, response) => {
    void (async () => {
      const url = new URL(request.url ?? "/", "http://localhost");
      const path = url.pathname;
      /** Every answer this stub gives is one of the API's success shapes. */
      const json = (body: ApiAnswer): void => {
        response.writeHead(200, { "content-type": "application/json" });
        response.end(JSON.stringify(body));
      };

      if (api) {
        const ref = url.searchParams.get("ref");
        const tree = ref === BRANCH ? AT_BRANCH : AT_MAIN;

        if (path === "/core/refs") {
          return json({
            refs: [
              { name: "refs/heads/main", oid: OID_MAIN },
              { name: `refs/heads/${BRANCH}`, oid: OID_BRANCH },
            ],
          });
        }
        if (path === "/core/files") {
          return json({
            files: Object.keys(tree).map((p) => ({ path: p, mode: "100644", oid: OID_BRANCH })),
          });
        }
        if (path === "/core/file") {
          const wanted = url.searchParams.get("path") ?? "";
          const content = tree[wanted];
          if (content === undefined) {
            response.writeHead(404, { "content-type": "application/json" });
            return response.end(JSON.stringify({ _tag: "ObjectNotFound" }));
          }
          return json({
            path: wanted,
            mode: "100644",
            oid: OID_BRANCH,
            content: base64(content),
            encoding: "base64",
            size: content.length,
          });
        }
        if (path.startsWith("/core/log/")) {
          return json({
            commits: [
              { oid: OID_MAIN, message: "merge CR-18: update pipeline config\n\nbody" },
              { oid: OID_BRANCH, message: "earlier" },
            ],
          });
        }
        if (path === "/core/diff" && request.method === "POST") {
          return json({
            files: [{ path: "src/server/Api.ts", status: "modified", binary: false, patch: "" }],
          });
        }
      }

      const file = join(dist, normalize(path === "/" ? "/index.html" : path));
      try {
        await stat(file);
      } catch {
        response.writeHead(404);
        return response.end("not found");
      }
      response.writeHead(200, { "content-type": mimeOf(extname(file)) });
      response.end(await readFile(file));
    })();
  });
  await new Promise<void>((resolve) => server.listen(port, resolve));
  return server;
};

const shot = async (page: Page, name: string): Promise<void> => {
  if (shots !== undefined) await page.screenshot({ path: join(shots, `${name}.png`) });
};

/** Every screen mounts, in both palettes, with nothing thrown. */
const render = async (browser: Browser, origin: string): Promise<void> => {
  console.info("\nrender");
  const page = await browser.newPage({ viewport: { width: 1440, height: 900 } });
  const thrown: string[] = [];
  page.on("pageerror", (error) => thrown.push(error.message));

  const screens = ["code", "activity", "tasks", "detail/CR-14", "settings"];
  for (const theme of ["light", "dark"]) {
    for (const screen of screens) {
      await page.goto(`${origin}/#/${screen}`, { waitUntil: "domcontentloaded" });
      await page.evaluate((value) => {
        localStorage.setItem("gp-theme", value);
        document.documentElement.dataset["theme"] = value;
      }, theme);
      await page.waitForTimeout(700);
      const name = `${theme}-${screen.replace("/", "-")}`;
      await shot(page, name);
      const mounted = await page.evaluate(() => ({
        shell: document.querySelector(".gp-shell") !== null,
        nav: document.querySelectorAll(".gp-nav-item").length,
        background: getComputedStyle(document.body).backgroundColor,
      }));
      check(
        `${name} mounts`,
        mounted.shell && mounted.nav === 4,
        `nav ${String(mounted.nav)}, bg ${mounted.background}`,
      );
      // The two palettes must actually differ, or `tokens.css` is not switching.
      const dark = mounted.background === "rgb(12, 12, 13)";
      check(`${name} uses the ${theme} palette`, theme === "dark" ? dark : !dark);
    }
  }
  check("no uncaught errors while rendering", thrown.length === 0, thrown.join("; "));
  await page.close();
};

/** The behaviours the design conversation settled on. */
const interact = async (browser: Browser, origin: string): Promise<void> => {
  console.info("\ninteract");
  const page = await browser.newPage({ viewport: { width: 1440, height: 900 } });
  page.on("pageerror", (error) => failures.push(`pageerror: ${error.message}`));
  const hash = (): Promise<string> => page.evaluate(() => globalThis.location.hash);
  const railWidth = (): Promise<number> =>
    page.evaluate(() => document.querySelector(".gp-sidebar")?.getBoundingClientRect().width ?? 0);

  await page.goto(`${origin}/#/tasks`, { waitUntil: "domcontentloaded" });
  await page.waitForTimeout(800);

  check("tasks list shows the whole hierarchy", (await page.locator(".gp-task-row").count()) === 9);

  // 224px plus the 1px right border — content-box, as the design sizes it.
  const wide = await railWidth();
  await page.click(".gp-logo-row");
  await page.waitForTimeout(400);
  const narrow = await railWidth();
  await page.click(".gp-logo-row");
  await page.waitForTimeout(400);
  check(
    "the logo collapses the nav and restores it",
    Math.round(wide) === 225 && Math.round(narrow) === 65 && Math.round(await railWidth()) === 225,
    `${String(Math.round(wide))} → ${String(Math.round(narrow))}`,
  );

  await page.click(".gp-task-row:nth-child(3)");
  await page.waitForTimeout(500);
  check("a task row opens its detail", (await hash()) === "#/detail/CR-14");
  check(
    "the detail names the Change Request",
    (await page.textContent(".gp-detail-title"))?.trim() === "Add auth middleware",
  );

  for (const [tab, rows] of [
    ["commits", 3],
    ["checks", 3],
  ] as const) {
    await page.click(`.gp-tab[value="${tab}"]`);
    await page.waitForTimeout(500);
    const shown = await page.evaluate((value) => {
      const panel = document.querySelector<HTMLElement>(`.gp-tabpanel[value="${value}"]`);
      return panel === null || panel.hidden ? -1 : panel.querySelectorAll(".gp-list-row").length;
    }, tab);
    check(`the ${tab} tab shows its rows`, shown === rows, `${String(shown)}`);
  }
  check(
    "base-wc keeps ui-tabs in step with the panel",
    (await page.getAttribute("ui-tabs", "value")) === "checks",
  );

  await page.click('.gp-tab[value="diff"]');
  await page.waitForTimeout(900);
  check(
    "the diff tab falls back to the design's diff when the refs are absent",
    (await page.locator(".gp-diff-static .gp-diff-line").count()) === 7,
  );

  await page.click(".gp-parent-link");
  await page.waitForTimeout(500);
  check("the parent link walks up the hierarchy", (await hash()) === "#/detail/T-12");
  check(
    "the parent lists its four subtasks",
    (await page.locator(".gp-subtask-row").count()) === 4,
  );

  await page.click(".gp-subtask-row:nth-child(2)");
  await page.waitForTimeout(500);
  check("a subtask walks back down", (await hash()) === "#/detail/CR-14");

  const before = await page.evaluate(() => getComputedStyle(document.body).backgroundColor);
  await page.click(".gp-theme-toggle");
  await page.waitForTimeout(400);
  const after = await page.evaluate(() => getComputedStyle(document.body).backgroundColor);
  check("the theme toggle flips the palette", before !== after, `${before} → ${after}`);
  check(
    "and remembers the choice",
    ["light", "dark"].includes(await page.evaluate(() => localStorage.getItem("gp-theme") ?? "")),
  );

  await page.goto(`${origin}/#/activity`, { waitUntil: "domcontentloaded" });
  await page.waitForTimeout(600);
  check("the timeline lays out every event", (await page.locator(".gp-cal-event").count()) === 7);
  await page.click(".gp-cal-event:nth-child(1)");
  await page.waitForTimeout(500);
  check("a timeline card opens its task", (await hash()).startsWith("#/detail/"), await hash());

  await page.goto(`${origin}/#/settings`, { waitUntil: "domcontentloaded" });
  await page.waitForTimeout(600);
  const state = (): Promise<string> =>
    page.evaluate(
      () => document.querySelectorAll("ui-switch")[2]?.getAttribute("data-state") ?? "",
    );
  const off = await state();
  await page.evaluate(() =>
    document.querySelectorAll("ui-switch")[2]?.querySelector("input")?.click(),
  );
  await page.waitForTimeout(300);
  check(
    "ui-switch mirrors the native checkbox it adopted",
    off === "unchecked" && (await state()) === "checked",
  );

  await page.goto(`${origin}/#/detail/CR-15`, { waitUntil: "domcontentloaded" });
  await page.waitForTimeout(600);
  check(
    "a deep link restores the right Change Request",
    (await page.textContent(".gp-detail-title"))?.trim() === "Add login UI",
  );
  check(
    "a blocked Change Request cannot be merged",
    await page.evaluate(
      () => document.querySelector<HTMLButtonElement>(".gp-merge-btn")?.disabled === true,
    ),
  );
  await page.close();
};

/** Code and Diff read the API rather than the fixtures. */
const live = async (browser: Browser, origin: string): Promise<void> => {
  console.info("\nlive");
  const page = await browser.newPage({ viewport: { width: 1440, height: 900 } });
  page.on("pageerror", (error) => failures.push(`pageerror: ${error.message}`));

  await page.goto(`${origin}/#/code`, { waitUntil: "networkidle" });
  await page.waitForTimeout(2500);
  check(
    "no fallback notice when the API answers",
    (await page.locator(".gp-notice").count()) === 0,
  );
  check(
    "the commit bar carries the subject from /log",
    (await page.textContent(".gp-commit-bar"))?.includes("merge CR-18: update pipeline config") ===
      true,
  );
  check(
    "and the short oid",
    (await page.textContent(".gp-commit-sha"))?.trim() === OID_MAIN.slice(0, 7),
  );
  // Both libraries render inside shadow roots: Playwright's selector engine
  // pierces them when matching, but `innerText` on the light-DOM host is "" —
  // so these match elements inside rather than reading the host's own text.
  check(
    "the explorer is built from /files",
    (await page.locator('[data-item-path="src/server/Api.ts"]').count()) > 0 &&
      (await page.locator('[data-item-path="src/git/Store.ts"]').count()) > 0,
  );
  check(
    "the source pane shows content from /file",
    (await page.getByText("Live from the API.").count()) > 0,
  );
  await shot(page, "live-code");

  await page.goto(`${origin}/#/detail/CR-14`, { waitUntil: "networkidle" });
  await page.waitForTimeout(800);
  await page.click('.gp-tab[value="diff"]');
  await page.waitForTimeout(2500);
  check(
    "the diff tab used /diff, not the fixture",
    (await page.locator(".gp-diff-static").count()) === 0,
  );
  check(
    "the diff names the changed file",
    (await page.textContent(".gp-diff-file-head"))?.includes("src/server/Api.ts") === true,
  );
  check(
    "and reports its status",
    (await page.textContent(".gp-diff-state"))?.trim() === "modified",
  );
  check("the removed side rendered", (await page.getByText("export const api = 1;").count()) > 0);
  check("the added side rendered", (await page.getByText("export const api = 2;").count()) > 0);
  await shot(page, "live-diff");
  await page.close();
};

const offline = await serve(false, 8131);
const online = await serve(true, 8132);
const browser = await chromium.launch(
  executable === undefined ? {} : { executablePath: executable },
);

try {
  await render(browser, "http://localhost:8131");
  await interact(browser, "http://localhost:8131");
  await live(browser, "http://localhost:8132");
} finally {
  await browser.close();
  offline.close();
  online.close();
}

if (failures.length > 0) {
  console.error(`\n${String(failures.length)} failed:\n  ${failures.join("\n  ")}`);
  process.exitCode = 1;
} else {
  console.info("\nall checks passed");
}
