/**
 * Serving the built UI from the same origin as the API.
 *
 * The browser half calls `/{repo}/...` with no scheme or host, so the page and
 * the API have to answer on one origin — that is not a convenience, it is the
 * only arrangement a browser permits. The deployed Worker gives them one, and
 * `src/ui/build.ts --serve` fakes one with a proxy for development. This is
 * the third place that needed it, so it is the one place it lives: `serve
 * --ui` hands the same bundle to the same origin without a proxy in front.
 *
 * Reads are attempted, not routed. A path either names a file that was built
 * or it does not, and the caller falls through to the git handler when it does
 * not — which is why nothing here needs a list of the routes the API owns.
 */
import { readFile, stat } from "node:fs/promises";
import { extname, join, normalize, resolve, sep } from "node:path";

/** The extensions the built UI actually emits; anything else is a byte stream. */
export const mimeOf = (extension: string): string => {
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

/**
 * The file `pathname` names under `root`, or `null`.
 *
 * `null` for a directory, for anything missing, and for a path that climbs out
 * of `root` — the last checked after resolving rather than by looking for
 * `..`, because the encodings of `..` are not a list anybody finishes.
 *
 * Copied out of node's Buffer rather than handed over: `readFile` returns a
 * view into a pooled allocation typed `ArrayBufferLike`, and `BodyInit` takes
 * a view over a plain `ArrayBuffer`. The same distinction `src/ui/build.ts`
 * documents where it collects a request body.
 */
export const fileAt = async (
  root: string,
  pathname: string,
): Promise<Uint8Array<ArrayBuffer> | null> => {
  const within = normalize(pathname === "/" ? "/index.html" : pathname);
  const full = resolve(join(root, within));
  const base = resolve(root);
  if (full !== base && !full.startsWith(base + sep)) return null;

  const found = await stat(full).catch(() => null);
  if (found === null || !found.isFile()) return null;

  const read = await readFile(full);
  const bytes = new Uint8Array(new ArrayBuffer(read.byteLength));
  bytes.set(read);
  return bytes;
};

/**
 * The built UI's answer for this request, or `null` to let the API have it.
 *
 * Only GET and HEAD: everything else is a mutation, and a mutation belongs to
 * the API whatever its path looks like. A HEAD keeps the headers and drops the
 * body, which is what makes it a HEAD.
 */
export const assetResponse = async (root: string, request: Request): Promise<Response | null> => {
  if (request.method !== "GET" && request.method !== "HEAD") return null;

  const { pathname } = new URL(request.url);
  const bytes = await fileAt(root, pathname);
  if (bytes === null) return null;

  const headers = {
    "content-type": mimeOf(extname(pathname === "/" ? "/index.html" : pathname)),
    "content-length": String(bytes.byteLength),
  };
  return new Response(request.method === "HEAD" ? null : bytes, { headers });
};

/**
 * Whether `root` holds a built UI.
 *
 * Asked once at startup so `--ui` can refuse with the command that fixes it,
 * rather than serving a port that answers 404 for the page and nothing else.
 */
export const built = async (root: string): Promise<boolean> =>
  (await stat(join(root, "index.html")).catch(() => null))?.isFile() === true;
