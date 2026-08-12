/**
 * Access control for both server surfaces.
 *
 * The guard maps a request to the scope it needs — fetching is `read`,
 * pushing and every mutating JSON call is `write` — extracts the credential,
 * and answers with `null` (authorized), 401 (no or bad credential, with
 * `WWW-Authenticate: Basic` so `git` prompts and retries) or 403 (a real
 * token without the needed scope).
 *
 * The compatibility detail the evaluation flagged: `git` sends credentials as
 * HTTP Basic, so the token is accepted as the *password* field — and as the
 * username when the password is empty, and as a Bearer token for plain HTTP
 * clients. Miss the first and `git clone` fails while `curl` works.
 *
 * Two verifiers ship. `hmacVerify` is stateless — HMAC-SHA256 over
 * `repo|scope|expiry` with a server secret, nothing stored, which is what a
 * Durable Object can enforce with one secret binding. The Artifacts
 * provider's `Tokens.verify` plugs into the same guard for revocable tokens.
 */
import { Effect } from "effect";

export type Scope = "read" | "write";

/** What this request needs; receive-pack costs `write` from the advertisement on. */
export const requiredScope = (request: Request): Scope => {
  const url = new URL(request.url);
  const last = url.pathname.split("/").at(-1);
  if (last === "git-receive-pack") return "write";
  if (last === "refs" && url.searchParams.get("service") === "git-receive-pack") return "write";
  if (last === "git-upload-pack") return "read";
  return request.method === "GET" || request.method === "HEAD" ? "read" : "write";
};

/** The presented token: Basic password (or username), or a Bearer token. */
export const credentialOf = (request: Request): string | null => {
  const header = request.headers.get("authorization");
  if (header === null) return null;
  if (header.startsWith("Bearer ")) return header.slice(7);
  if (header.startsWith("Basic ")) {
    try {
      const decoded = atob(header.slice(6));
      const colon = decoded.indexOf(":");
      if (colon === -1) return decoded;
      const password = decoded.slice(colon + 1);
      return password.length > 0 ? password : decoded.slice(0, colon);
    } catch {
      return null;
    }
  }
  return null;
};

const unauthorized = () =>
  new Response("authentication required", {
    status: 401,
    headers: { "www-authenticate": 'Basic realm="git"' },
  });

const forbidden = () => new Response("insufficient scope", { status: 403 });

/**
 * `null` means proceed. A `write` scope implies `read`, matching what a repo
 * token means everywhere else git is hosted.
 */
export const guard = (
  request: Request,
  verify: (credential: string | null) => Effect.Effect<Scope | null>,
): Effect.Effect<Response | null> =>
  Effect.gen(function* () {
    const credential = credentialOf(request);
    const scope = yield* verify(credential);
    if (scope === "write" || scope === requiredScope(request)) return null;
    return credential === null ? unauthorized() : scope === null ? unauthorized() : forbidden();
  });

const encoder = new TextEncoder();

const key = (secret: string) =>
  Effect.promise(() =>
    crypto.subtle.importKey(
      "raw",
      encoder.encode(secret),
      { name: "HMAC", hash: "SHA-256" },
      false,
      ["sign", "verify"],
    ),
  );

const payload = (repo: string, scope: Scope, expiresAtMs: number) =>
  encoder.encode(`${repo}|${scope}|${expiresAtMs}`);

const hex = (bytes: ArrayBuffer) =>
  [...new Uint8Array(bytes)].map((byte) => byte.toString(16).padStart(2, "0")).join("");

/** `git1.<scope>.<expiry-ms>.<hmac>` — everything needed to verify statelessly. */
export const hmacMint = (
  secret: string,
  repo: string,
  scope: Scope,
  ttlSeconds: number,
): Effect.Effect<string> =>
  Effect.gen(function* () {
    const expiresAtMs = Date.now() + ttlSeconds * 1000;
    const cryptoKey = yield* key(secret);
    const signature = yield* Effect.promise(() =>
      crypto.subtle.sign("HMAC", cryptoKey, payload(repo, scope, expiresAtMs)),
    );
    return `git1.${scope}.${expiresAtMs}.${hex(signature)}`;
  });

/**
 * The repo binding is in the signature, not the token: a token presented at
 * any other repository fails verification with no lookup anywhere.
 */
export const hmacVerify = (
  secret: string,
  repo: string,
  credential: string | null,
): Effect.Effect<Scope | null> =>
  Effect.gen(function* () {
    if (credential === null) return null;
    const [version, scope, expiry, signature] = credential.split(".");
    if (version !== "git1" || (scope !== "read" && scope !== "write")) return null;
    const expiresAtMs = Number(expiry);
    if (!Number.isFinite(expiresAtMs) || expiresAtMs <= Date.now()) return null;
    if (signature === undefined || !/^[0-9a-f]{64}$/.test(signature)) return null;

    const cryptoKey = yield* key(secret);
    const bytes = new Uint8Array(32);
    for (let index = 0; index < 32; index++) {
      bytes[index] = Number.parseInt(signature.slice(index * 2, index * 2 + 2), 16);
    }
    const valid = yield* Effect.promise(() =>
      // `subtle.verify` is constant-time; a hand-rolled compare would not be.
      crypto.subtle.verify("HMAC", cryptoKey, bytes, payload(repo, scope, expiresAtMs)),
    );
    return valid ? scope : null;
  });
