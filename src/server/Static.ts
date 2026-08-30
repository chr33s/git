/**
 * Serving the built UI from the same origin as the API.
 *
 * The browser half calls `/{repo}/...` with no scheme or host, so the page and
 * the API have to answer on one origin — that is not a convenience, it is the
 * only arrangement a browser permits. The deployed Worker gives them one, and
 * `src/ui/dev.ts` mounts Vite's middleware directly on the node host for
 * development. This is the third place that needed it, so it is the one place
 * it lives: `serve --ui` hands the finished bundle to the same origin too.
 *
 * Reads are attempted, not routed. A path either names a file that was built
 * or it does not, and the caller falls through to the git handler when it does
 * not — which is why nothing here needs a list of the routes the API owns.
 */
import { readFile, realpath, stat } from "node:fs/promises";
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
 * A URL path as a filesystem path, or `null` when it is not one.
 *
 * `URL.pathname` keeps its percent-escapes, so the raw value is not the name
 * of any file: a built asset whose name contains an escaped character was
 * looked up under its escaped spelling and 404'd. Decoded here, once, and
 * before `normalize` — which is the only order that is also safe, because the
 * containment check below runs on the *resolved* result and so sees whatever
 * the decoding produced, `..` included. Decoding afterwards would be the
 * dangerous order and is what this function exists to make impossible to write
 * by accident.
 *
 * `null` for an escape sequence that is not one: a lone `%` is not a path.
 */
const decodedPath = (pathname: string): string | null => {
  try {
    return decodeURIComponent(pathname === "/" ? "/index.html" : pathname);
  } catch {
    return null;
  }
};

/**
 * The file `pathname` names under `root`, or `null`.
 *
 * `null` for a directory, for anything missing, and for a path that climbs out
 * of `root` — the last checked after resolving rather than by looking for
 * `..`, because the encodings of `..` are not a list anybody finishes.
 *
 * Containment is checked twice, and the second one is the one that matters: a
 * lexical `resolve` does not follow symlinks, so a link inside the built UI
 * pointing anywhere at all satisfied the prefix test and was then read. The
 * file is known to exist by then, so `realpath` resolves rather than guesses —
 * this is the same guard `git/Work.node.ts` applies to a checkout, which had
 * the rule right while this one had it lexically.
 *
 * Copied out of node's Buffer rather than handed over: `readFile` returns a
 * view into a pooled allocation typed `ArrayBufferLike`, and `BodyInit` takes
 * a view over a plain `ArrayBuffer`.
 */
export const fileAt = async (
  root: string,
  pathname: string,
): Promise<Uint8Array<ArrayBuffer> | null> => {
  const decoded = decodedPath(pathname);
  if (decoded === null) return null;
  const within = normalize(decoded);
  const full = resolve(join(root, within));
  const base = resolve(root);
  const contains = (candidate: string): boolean =>
    candidate === base || candidate.startsWith(base + sep);
  if (!contains(full)) return null;

  const found = await stat(full).catch(() => null);
  if (found === null || !found.isFile()) return null;

  // The root is resolved too: a served directory that is itself reached
  // through a link would otherwise fail its own containment test.
  const real = await realpath(full).catch(() => null);
  const anchor = await realpath(base).catch(() => base);
  if (real === null) return null;
  if (real !== anchor && !real.startsWith(anchor + sep)) return null;

  const read = await readFile(real);
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

  // The decoded name, so an escaped extension picks the same type the lookup
  // just used rather than falling through to `application/octet-stream`.
  const headers = {
    "content-type": mimeOf(extname(decodedPath(pathname) ?? pathname)),
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
