/**
 * Same-origin UI development server.
 *
 * Vite owns source transforms, assets and HMR; the node host owns every Git
 * route. Mounting Vite's middleware on that host instead of proxying between
 * two ports keeps smart HTTP, JSON requests and OPFS in one browser origin.
 */
import { readFile, stat, symlink } from "node:fs/promises";
import { createServer as createViteServer } from "vite-plus";
import { mkdtemp } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

import { UI_PREFIX } from "../server/Route.ts";
import { isScreen } from "./route.ts";
import { serve as serveHost } from "../host/Node.ts";

const ui = dirname(fileURLToPath(import.meta.url));
const root = join(ui, "..", "..");

/**
 * Whether this address is a screen rather than a module.
 *
 * The screen names and Vite's module ids share one prefix, and some of them
 * collide outright: `/hub/code` is the Code screen, and `src/ui/code.ts` is a
 * module Vite would happily resolve for the same path. So the check is the
 * closed set of screens rather than a guess about what a module path looks
 * like — `route.ts` owns that set, and both halves read it from there.
 *
 * Production has no such ambiguity: the asset manifest holds built file names,
 * and the Worker serves the page for everything else.
 */
const screenRoute = (pathname: string): boolean => {
  if (pathname === UI_PREFIX || pathname === `${UI_PREFIX}/`) return true;
  if (!pathname.startsWith(`${UI_PREFIX}/`)) return false;
  const [screen] = pathname.slice(UI_PREFIX.length + 1).split("/");
  return screen !== undefined && (screen === "index.html" || isScreen(screen));
};

const isDirectory = (target: string): Promise<boolean> =>
  stat(target).then(
    (found) => found.isDirectory(),
    () => false,
  );

/** Preview this checkout as the `core` repo when no repository root was supplied. */
const previewRoot = async (): Promise<string | undefined> => {
  if (await isDirectory(join(process.cwd(), "core"))) return undefined;
  const git = join(process.cwd(), ".git");
  if (!(await isDirectory(git))) return undefined;
  const preview = await mkdtemp(join(tmpdir(), "git-ui-preview-"));
  await symlink(git, join(preview, "core"), "dir");
  return preview;
};

const configured = process.env["GIT_ROOT"];
const preview = configured === undefined ? await previewRoot() : undefined;
const repositories = configured ?? preview ?? process.cwd();

const host = await serveHost({
  root: repositories,
  port: Number(process.env["PORT"] ?? 8000),
  development: async (server) => {
    const vite = await createViteServer({
      configFile: join(root, "vite.config.ts"),
      root: ui,
      server: { middlewareMode: { server } },
    });
    return {
      handle: (request, response, next) => {
        const pathname = new URL(request.url ?? "/", "http://localhost").pathname;

        // The root belongs to the UI: match what the Worker answers there.
        if (pathname === "/" && request.method === "GET") {
          response.writeHead(302, { location: `${UI_PREFIX}/code` });
          response.end();
          return;
        }

        // Every client route renders the same page. Serving it here is what
        // makes a deep link survive a reload in development the way it does
        // in production, where the Worker serves the page from the asset
        // layer for exactly the same misses.
        const page = (): void => {
          void readFile(join(ui, "index.html"), "utf8")
            .then((html) => vite.transformIndexHtml(pathname, html))
            .then((html) => {
              response.writeHead(200, { "content-type": "text/html" });
              response.end(html);
            })
            .catch(next);
        };

        // Ahead of Vite, not behind it: `/hub/code` is the Code screen, and
        // Vite would otherwise resolve the same path to `src/ui/code.ts` and
        // answer with a module. `page()` transforms the entry through Vite
        // either way, so `/hub/index.html` still gets its HMR client.
        if (screenRoute(pathname) && (request.method === "GET" || request.method === "HEAD")) {
          page();
          return;
        }
        vite.middlewares(request, response, next);
      },
      close: () => vite.close(),
    };
  },
});

console.info(`\nui:   ${host.url}${UI_PREFIX}/code`);
console.info(
  `      repositories under ${
    preview === undefined
      ? repositories
      : `${repositories} — this checkout's .git, previewed as "core"`
  }\n`,
);
