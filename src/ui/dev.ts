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

import { serve as serveHost } from "../host/Node.ts";

const ui = dirname(fileURLToPath(import.meta.url));
const root = join(ui, "..", "..");

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
        if ((pathname === "/" || pathname === "/index.html") && request.method === "GET") {
          void readFile(join(ui, "index.html"), "utf8")
            .then((page) => vite.transformIndexHtml(pathname, page))
            .then((page) => {
              response.writeHead(200, { "content-type": "text/html" });
              response.end(page);
            })
            .catch(next);
          return;
        }
        vite.middlewares(request, response, next);
      },
      close: () => vite.close(),
    };
  },
});

console.info(`\nui:   ${host.url}`);
console.info(
  `      repositories under ${
    preview === undefined
      ? repositories
      : `${repositories} — this checkout's .git, previewed as "core"`
  }\n`,
);
