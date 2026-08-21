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
 * The `live` suite decodes every stub response with the shared schemas used by
 * `src/server/Api.ts` and the browser client. It therefore runs without a
 * Worker while still failing immediately if a fixture drifts from the wire.
 *
 *   node src/ui/build.ts && node src/ui/verify.ts
 *   node src/ui/verify.ts --shots <dir>    # also write screenshots
 *
 * Playwright is already a devDependency, and Chromium is expected on PATH or at
 * PLAYWRIGHT_BROWSERS_PATH; pass --executable to point at a specific binary.
 */
import { chromium, type Browser, type Page } from "playwright";
import { Effect, Layer, Schema } from "effect";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { createServer, type Server } from "node:http";
import { mkdtemp, readFile, rm, stat } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

import { generate } from "../crypto/SshSignature.ts";
import { stores as nodeStores } from "../git/Node.ts";
import * as GitRepository from "../git/Repository.ts";
import * as HubTask from "../hub/Task.ts";
import { serve as serveHost } from "../host/Node.ts";
import * as Contract from "../server/ApiContract.ts";
import { assetResponse } from "../server/Static.ts";
import { enableHubUnder } from "../testing/Hub.ts";
import * as Certificate from "../trust/Certificate.ts";
import * as Log from "../trust/Log.ts";

const here = dirname(fileURLToPath(import.meta.url));
/** Output lives at the repository root alongside `dist/sea`, not under `src/ui/`. */
const dist = join(here, "..", "..", "dist", "ui");

const flag = (name: string): string | undefined => {
  const at = process.argv.indexOf(name);
  return at === -1 ? undefined : process.argv[at + 1];
};

const shots = flag("--shots");
const executable = flag("--executable") ?? process.env["CHROMIUM_PATH"];

const failures: string[] = [];

const check = (name: string, ok: boolean, detail = ""): void => {
  console.info(`${ok ? "  ok  " : "FAIL  "}${name}${detail === "" ? "" : `  — ${detail}`}`);
  if (!ok) failures.push(name);
};

/** A repository the `live` suite serves: two refs, and a file that differs. */
const oid = Schema.decodeUnknownSync(Contract.OidString);
const OID_MAIN = oid("2".repeat(40));
const OID_BRANCH = oid("1".repeat(40));
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
 * The commits the stub serves, dated relative to now.
 *
 * Relative rather than fixed, because the assertions are on what `ago()`
 * renders and on the Activity grid's window — both of which move with today.
 */
interface Stubbed {
  readonly oid: Contract.Ref["oid"];
  readonly subject: string;
  readonly author: string;
  readonly daysAgo: number;
}

const HISTORY: readonly Stubbed[] = [
  {
    oid: OID_MAIN,
    subject: "merge CR-18: update pipeline config",
    author: "Rune Baek",
    daysAgo: 1,
  },
  {
    oid: oid("3".repeat(40)),
    subject: "widen the api surface",
    author: "Maya Kessler",
    daysAgo: 3,
  },
  { oid: oid("4".repeat(40)), subject: "seed the repository", author: "Maya Kessler", daysAgo: 6 },
];

/**
 * The slice of the server's commit payload the stub honours.
 *
 * `files` decodes against the contract's `FileWrite` — the same schema the
 * real declaration uses — so a client that drifts from the wire fails here.
 */
const CommitPayload = Schema.Struct({
  branch: Schema.optional(Schema.String),
  message: Schema.optional(Schema.String),
  expected: Schema.optional(Schema.NullOr(Contract.OidString)),
  files: Schema.optional(Schema.Array(Contract.FileWrite)),
});

/** The slices of the other write payloads the stub honours. */
const BranchCreatePayload = Schema.Struct({ name: Schema.String, base: Schema.String });
const TagCreatePayload = Schema.Struct({
  name: Schema.String,
  target: Schema.String,
  message: Schema.optional(Schema.String),
});
const ResetPayload = Schema.Struct({ ref: Schema.String, to: Schema.String });
const MergePayload = Schema.Struct({
  ours: Schema.String,
  theirs: Schema.String,
  into: Schema.optional(Schema.String),
  message: Schema.optional(Schema.String),
});
const GrepPayload = Schema.Struct({
  pattern: Schema.String,
  ref: Schema.optional(Schema.String),
});
const RemoteAddPayload = Schema.Struct({
  name: Schema.String,
  url: Schema.String,
  credential: Schema.optional(Schema.String),
});
const WebhookAddPayload = Schema.Struct({ url: Schema.String, secret: Schema.String });
const NamePayload = Schema.Struct({ name: Schema.optional(Schema.String) });

/** A fixture tree as a mutable map, dropping the index type's `undefined`. */
const entries = (tree: Tree): Map<string, string> => {
  const map = new Map<string, string>();
  for (const [key, value] of Object.entries(tree)) if (value !== undefined) map.set(key, value);
  return map;
};

/** A raw commit object, in the format `/object/:oid` returns base64-encoded. */
const rawCommit = (commit: Stubbed): string => {
  const at = Math.floor((Date.now() - commit.daysAgo * 86400000) / 1000);
  const email = `${commit.author.split(" ")[0]?.toLowerCase() ?? "who"}@example.com`;
  return [
    `tree ${"a".repeat(40)}`,
    `parent ${"b".repeat(40)}`,
    `author ${commit.author} <${email}> ${String(at)} +0000`,
    `committer ${commit.author} <${email}> ${String(at)} +0000`,
    "",
    commit.subject,
    "",
  ].join("\n");
};

/**
 * Static files, plus — when `api` is set — the JSON endpoints the UI calls.
 *
 * With the API off, every call 404s, which is exactly the condition the
 * fixture fallback exists for; the `render` and `interact` suites rely on that.
 */
const serve = async (api: boolean, port: number): Promise<Server> => {
  // Mutable per-server repository state. The live suite commits through
  // `POST /commit`, and every read endpoint answers from these maps so a
  // write is observable exactly the way a real server would show it.
  const trees = new Map<string, Map<string, string>>([
    ["main", entries(AT_MAIN)],
    [BRANCH, entries(AT_BRANCH)],
  ]);
  const tips = new Map<string, Contract.Ref["oid"]>([
    ["main", OID_MAIN],
    [BRANCH, OID_BRANCH],
  ]);
  /** Commits written during the run, served back by `/object/:oid`. */
  const written = new Map<string, Stubbed>();
  const NEXT_TIPS = ["5", "6", "7", "8", "9", "ab", "cd", "ef"] as const;
  let commitCount = 0;
  const nextOid = (): Contract.Ref["oid"] => {
    const digit = NEXT_TIPS[commitCount % NEXT_TIPS.length] ?? "9";
    commitCount += 1;
    return oid(digit.repeat(40 / digit.length));
  };
  const tags = new Map<string, Contract.Ref["oid"]>();
  const stubRemotes: { name: string; url: string; has_credential: boolean }[] = [];
  const stubHooks: { id: string; url: string; created_at: string }[] = [];
  let hookSerial = 0;
  /** Resolve a payload's branchish name — `refs/heads/x`, `x`, or an oid. */
  const tipOf = (name: string): Contract.Ref["oid"] | undefined => {
    if (/^[0-9a-f]{40}$/.test(name)) return oid(name);
    const bare = name.startsWith("refs/heads/") ? name.slice("refs/heads/".length) : name;
    return tips.get(bare);
  };
  const server = createServer((request, response) => {
    void (async () => {
      const url = new URL(request.url ?? "/", "http://localhost");
      const path = url.pathname;
      /** Decode fixtures with the same schema the server and browser use. */
      const json = <S extends Schema.ConstraintDecoder<unknown>>(
        schema: S,
        body: S["Type"],
      ): void => {
        const decoded = Schema.decodeUnknownSync(schema)(body);
        response.writeHead(200, { "content-type": "application/json" });
        response.end(JSON.stringify(decoded));
      };
      /**
       * The request body, decoded with the schema that endpoint expects.
       *
       * Decoding here rather than returning `unknown` is the point: a client
       * that drifts from the wire fails inside this stub, which is what the
       * live suite exists to catch.
       */
      const body = async <S extends Schema.ConstraintDecoder<unknown>>(
        schema: S,
      ): Promise<S["Type"]> => {
        const chunks: Buffer[] = [];
        for await (const chunk of request) chunks.push(Buffer.from(chunk));
        const input: unknown = JSON.parse(Buffer.concat(chunks).toString("utf8"));
        return Schema.decodeUnknownSync(schema)(input);
      };
      const notFound = (): void => {
        response.writeHead(404, { "content-type": "application/json" });
        response.end(JSON.stringify({ _tag: "ObjectNotFound" }));
      };

      if (api) {
        const ref = url.searchParams.get("ref");
        // The real server resolves refs strictly — an oid, `HEAD`, or a full
        // `refs/...` name — and answers 400 for a bare branch name. The stub
        // enforces the same rule: accepting anything is what let a client that
        // sent short names pass this suite and then fail against the server.
        if (ref !== null && !ref.startsWith("refs/") && !/^[0-9a-f]{40}$/.test(ref)) {
          response.writeHead(400, { "content-type": "application/json" });
          return response.end(
            JSON.stringify({ _tag: "Invalid", field: "ref", reason: `unknown ref '${ref}'` }),
          );
        }
        const branchOf = (name: string | null): string =>
          name !== null && name.startsWith("refs/heads/")
            ? name.slice("refs/heads/".length)
            : "main";
        const tree = trees.get(branchOf(ref)) ?? new Map<string, string>();
        const refsNow = [...tips.entries()].map(([name, tip]) => ({
          name: `refs/heads/${name}`,
          oid: tip,
        }));

        if (path === "/core/refs") {
          return json(Contract.RefsResponse, { refs: refsNow });
        }
        if (path === "/core/files") {
          return json(Contract.FilesResponse, {
            files: [...tree.keys()].map((p) => ({ path: p, mode: "100644", oid: OID_BRANCH })),
          });
        }
        if (path === "/core/file") {
          const wanted = url.searchParams.get("path") ?? "";
          const content = tree.get(wanted);
          if (content === undefined) {
            response.writeHead(404, { "content-type": "application/json" });
            return response.end(JSON.stringify({ _tag: "ObjectNotFound" }));
          }
          return json(Contract.FileContent, {
            path: wanted,
            mode: "100644",
            oid: OID_BRANCH,
            content: base64(content),
            encoding: "base64",
            size: content.length,
          });
        }
        // Settings reads the paged `/branches`, which is the endpoint built for
        // a branch list; Code reads `/refs` because it wants the tip oid too.
        if (path === "/core/branches") {
          return json(Contract.RefPage, { items: refsNow, next_cursor: null, has_more: false });
        }
        if (path === "/core/whoami") {
          return json(Contract.WhoamiAnswer, {
            repo: "core",
            subject: null,
            member: false,
            why: "this repository has no genesis",
            capabilities: [],
            expiresAt: null,
            trust: null,
            budget: null,
            branches: {},
          });
        }
        if (path === "/core/policy") {
          return json(Contract.PolicyAnswer, {
            rules: {
              protected: ["refs/heads/main"],
              requiredApprovals: 0,
              requiredChecks: [],
              requireResolvedThreads: false,
              requirePullRequest: false,
              maxTrustAgeSeconds: 0,
              requireProvenance: false,
              maxUsageTokens: 0,
              usageWindowSeconds: 0,
              queueCandidates: false,
              queueDepth: 8,
            },
            ref: null,
          });
        }
        if (path.startsWith("/core/object/")) {
          const requestedOid = oid(path.slice("/core/object/".length));
          const commit =
            HISTORY.find((entry) => entry.oid === requestedOid) ?? written.get(requestedOid);
          const subject = commit ?? {
            oid: requestedOid,
            subject: "add auth middleware",
            author: "Rune Baek",
            daysAgo: 0,
          };
          return json(Contract.RawObject, {
            oid: requestedOid,
            type: "commit",
            size: rawCommit(subject).length,
            content: base64(rawCommit(subject)),
            encoding: "base64",
          });
        }
        if (path.startsWith("/core/commits/")) {
          return json(Contract.CommitPage, {
            items: HISTORY.map((entry) => ({ oid: entry.oid, message: `${entry.subject}\n` })),
            next_cursor: null,
            has_more: false,
          });
        }
        if (path.startsWith("/core/log/")) {
          return json(Contract.LogResponse, {
            commits: [
              { oid: OID_MAIN, message: "merge CR-18: update pipeline config\n\nbody" },
              { oid: OID_BRANCH, message: "earlier" },
            ],
          });
        }
        // --- the administration surface the Settings screen drives -------
        if (request.method === "DELETE") {
          if (path.startsWith("/core/branches/")) {
            const name = decodeURIComponent(path.slice("/core/branches/".length));
            const existed = tips.delete(name);
            trees.delete(name);
            return json(Contract.Deleted, { deleted: existed });
          }
          if (path.startsWith("/core/tags/")) {
            const name = decodeURIComponent(path.slice("/core/tags/".length));
            return json(Contract.Deleted, { deleted: tags.delete(name) });
          }
          if (path.startsWith("/core/webhooks/")) {
            const id = decodeURIComponent(path.slice("/core/webhooks/".length));
            const at = stubHooks.findIndex((hook) => hook.id === id);
            if (at !== -1) stubHooks.splice(at, 1);
            return json(Contract.Deleted, { deleted: at !== -1 });
          }
          if (path.startsWith("/core/remotes/")) {
            const name = decodeURIComponent(path.slice("/core/remotes/".length));
            const at = stubRemotes.findIndex((remote) => remote.name === name);
            if (at !== -1) stubRemotes.splice(at, 1);
            return json(Contract.Deleted, { deleted: at !== -1 });
          }
        }
        if (path === "/core/branches/create" && request.method === "POST") {
          const payload = await body(BranchCreatePayload);
          const base = tipOf(payload.base);
          if (base === undefined) return notFound();
          tips.set(payload.name, base);
          trees.set(payload.name, new Map(trees.get(branchOf(payload.base)) ?? trees.get("main")));
          return json(Contract.Ref, { name: `refs/heads/${payload.name}`, oid: base });
        }
        if (path === "/core/tags" && request.method === "GET") {
          return json(Contract.RefPage, {
            items: [...tags.entries()].map(([name, tagOid]) => ({
              name: `refs/tags/${name}`,
              oid: tagOid,
            })),
            next_cursor: null,
            has_more: false,
          });
        }
        if (path === "/core/tags" && request.method === "POST") {
          const payload = await body(TagCreatePayload);
          const target = tipOf(payload.target);
          if (target === undefined) return notFound();
          tags.set(payload.name, target);
          return json(Contract.TagCreated, {
            ref: `refs/tags/${payload.name}`,
            oid: target,
            target,
          });
        }
        if (path === "/core/reset" && request.method === "POST") {
          const payload = await body(ResetPayload);
          const bare = branchOf(payload.ref);
          const previous = tips.get(bare) ?? null;
          const to = tipOf(payload.to);
          if (to === undefined) return notFound();
          tips.set(bare, to);
          return json(Contract.ResetResult, { ref: `refs/heads/${bare}`, oid: to, previous });
        }
        if (path === "/core/merge" && request.method === "POST") {
          const payload = await body(MergePayload);
          const ours = tipOf(payload.ours);
          const theirs = tipOf(payload.theirs);
          if (ours === undefined || theirs === undefined) return notFound();
          const commit = nextOid();
          written.set(commit, {
            oid: commit,
            subject: (payload.message ?? "merge").split("\n", 1)[0] ?? "merge",
            author: "Rune Baek",
            daysAgo: 0,
          });
          if (payload.into !== undefined) tips.set(branchOf(payload.into), commit);
          return json(Contract.MergeResult, {
            kind: "merged",
            commit,
            tree: oid("a".repeat(40)),
            base: theirs,
            conflicts: [],
          });
        }
        if (path === "/core/grep" && request.method === "POST") {
          const payload = await body(GrepPayload);
          const target = trees.get(branchOf(payload.ref ?? null)) ?? new Map<string, string>();
          const needle = payload.pattern.toLowerCase();
          const matches: { path: string; line: number; text: string }[] = [];
          for (const [entryPath, content] of target) {
            content.split("\n").forEach((line, index) => {
              if (line.toLowerCase().includes(needle)) {
                matches.push({ path: entryPath, line: index + 1, text: line });
              }
            });
          }
          return json(Contract.GrepResponse, { matches, truncated: false, skipped: [] });
        }
        if (path.startsWith("/core/history/")) {
          return json(Contract.HistoryPage, {
            items: HISTORY.slice(0, 2).map((entry) => ({
              oid: entry.oid,
              message: `${entry.subject}\n`,
              blob: null,
            })),
            next_cursor: null,
            has_more: false,
          });
        }
        if (path === "/core/reflog") {
          return json(Contract.ReflogResponse, {
            entries: [
              {
                from: OID_MAIN,
                to: tips.get("main") ?? OID_MAIN,
                at: new Date().toISOString(),
                message: "commit: moved during the suite",
              },
              {
                from: null,
                to: OID_MAIN,
                at: new Date().toISOString(),
                message: "commit (initial): seed the repository",
              },
            ],
          });
        }
        if (path === "/core/fsck" && request.method === "POST") {
          return json(Contract.FsckReport, {
            checked: 12,
            ok: true,
            problems: [],
            dangling_refs: [],
          });
        }
        if (path === "/core/gc" && request.method === "POST") {
          return json(Contract.GcReport, {
            scanned: 12,
            reachable: 12,
            removed: [],
            retained: [],
            packed: null,
            repack_skipped: null,
          });
        }
        if (path === "/core/webhooks" && request.method === "GET") {
          return json(Contract.WebhookList, { webhooks: stubHooks });
        }
        if (path === "/core/webhooks" && request.method === "POST") {
          const payload = await body(WebhookAddPayload);
          hookSerial += 1;
          const hook = {
            id: `hook-${String(hookSerial)}`,
            url: payload.url,
            created_at: new Date().toISOString(),
          };
          stubHooks.push(hook);
          return json(Contract.WebhookWire, hook);
        }
        if (path === "/core/remotes" && request.method === "GET") {
          return json(Contract.RemoteList, {
            remotes: stubRemotes.map((remote) => ({
              ...remote,
              has_key: false,
              sync: null,
              created_at: new Date().toISOString(),
            })),
          });
        }
        if (path === "/core/remotes" && request.method === "POST") {
          const payload = await body(RemoteAddPayload);
          const remote = {
            name: payload.name,
            url: payload.url,
            has_credential: payload.credential !== undefined,
          };
          stubRemotes.push(remote);
          return json(Contract.RemoteWire, {
            ...remote,
            has_key: false,
            sync: null,
            created_at: new Date().toISOString(),
          });
        }
        if (path === "/core/fetch" && request.method === "POST") {
          const payload = await body(NamePayload);
          return json(Contract.FetchResult, {
            remote: `refs/remotes/${payload.name ?? "origin"}`,
            refs: [],
            objects: 0,
          });
        }
        if (path === "/core/push" && request.method === "POST") {
          await body(Schema.Unknown);
          return json(Contract.PushResult, {
            refs: [{ ref: "refs/heads/main", ok: true, reason: null }],
          });
        }
        if (path === "/core/pull" && request.method === "POST") {
          await body(Schema.Unknown);
          const tip = tips.get("main") ?? OID_MAIN;
          return json(Contract.PullResult, {
            kind: "up-to-date",
            branch: "refs/heads/main",
            tracking: "refs/remotes/origin/main",
            from: tip,
            to: tip,
            objects: 0,
          });
        }
        if (path === "/core/commit" && request.method === "POST") {
          try {
            const chunks: Buffer[] = [];
            for await (const chunk of request) chunks.push(Buffer.from(chunk));
            const input: unknown = JSON.parse(Buffer.concat(chunks).toString("utf8"));
            const payload = Schema.decodeUnknownSync(CommitPayload)(input);
            const branch = payload.branch ?? "main";
            const tip = tips.get(branch);
            const target = trees.get(branch);
            if (tip === undefined || target === undefined) {
              response.writeHead(400, { "content-type": "application/json" });
              return response.end(
                JSON.stringify({ _tag: "Invalid", field: "branch", reason: `unknown '${branch}'` }),
              );
            }
            // The compare-and-swap the real server performs: a stale
            // `expected` is a conflict, never a silent overwrite.
            if (payload.expected !== undefined && payload.expected !== tip) {
              response.writeHead(409, { "content-type": "application/json" });
              return response.end(
                JSON.stringify({
                  _tag: "RefConflict",
                  message: `refs/heads/${branch} is at ${tip}`,
                }),
              );
            }
            for (const file of payload.files ?? []) {
              if (file.content === null) target.delete(file.path);
              else target.set(file.path, file.content);
            }
            const next = nextOid();
            tips.set(branch, next);
            written.set(next, {
              oid: next,
              subject: (payload.message ?? "").split("\n", 1)[0] ?? "",
              author: "Rune Baek",
              daysAgo: 0,
            });
            return json(Contract.CommitCreated, { oid: next, tree: oid("a".repeat(40)) });
          } catch (cause) {
            response.writeHead(400, { "content-type": "application/json" });
            return response.end(
              JSON.stringify({
                _tag: "Invalid",
                reason: cause instanceof Error ? cause.message : String(cause),
              }),
            );
          }
        }
        if (path === "/core/diff" && request.method === "POST") {
          try {
            const chunks: Buffer[] = [];
            for await (const chunk of request) chunks.push(Buffer.from(chunk));
            const input: unknown = JSON.parse(Buffer.concat(chunks).toString("utf8"));
            const payload = Schema.decodeUnknownSync(Contract.DiffRequest)(input);
            // CR-15 is the valid empty-diff case. CR-14 is deliberately slower
            // so the live suite can prove an older request cannot overwrite it.
            if (payload.to === "refs/heads/atran/login-ui") {
              return json(Contract.DiffResponse, { files: [] });
            }
            await new Promise((resolve) => setTimeout(resolve, 250));
          } catch (cause) {
            response.writeHead(400, { "content-type": "application/json" });
            return response.end(
              JSON.stringify({
                _tag: "Invalid",
                reason: cause instanceof Error ? cause.message : String(cause),
              }),
            );
          }
          return json(Contract.DiffResponse, {
            files: [{ path: "src/server/Api.ts", status: "modified", binary: false, patch: "" }],
          });
        }

        // The hub, honestly empty: the sample repository has no genesis and
        // no tasks, so the store keeps the fixtures — which is exactly the
        // fallback these suites' expectations are written against.
        if (path === "/core/hub/tasks") {
          return json(Contract.HubTaskPage, { items: [], next_cursor: null, has_more: false });
        }
        if (path === "/core/hub/pulls") {
          return json(Contract.HubPullPage, {
            enabled: false,
            reason: "the sample repository has no genesis",
            items: [],
            next_cursor: null,
            has_more: false,
          });
        }
      }

      const asset = await assetResponse(dist, new Request(`http://localhost${path}`));
      if (asset === null) {
        response.writeHead(404);
        return response.end("not found");
      }
      response.writeHead(asset.status, Object.fromEntries(asset.headers));
      response.end(Buffer.from(await asset.arrayBuffer()));
    })();
  });
  await new Promise<void>((resolve) => server.listen(port, resolve));
  return server;
};

/** Exercise the actual `dev:ui` proxy, including its one-shot request stream. */
/**
 * One origin, from whichever entry point put it there.
 *
 * Both ways of running the UI hand the built directory to the same server —
 * `dev:ui` while watching, `serve --ui` from a finished bundle — and the
 * property worth pinning is the one a browser enforces: the page, the bundle
 * and `/{repo}/...` all answer on a single port. It used to take a proxy in
 * front of esbuild's own server to arrange that, and the proxy was the part
 * that could be wrong; there is nothing between them now.
 */
const oneOrigin = async (input: {
  readonly label: string;
  /** Built from the root this makes, since the two entry points differ on how
   * they are told about it: `dev:ui` reads the environment, the CLI a flag. */
  readonly argv: (root: string, port: number) => readonly string[];
  readonly ready: string;
  readonly port: number;
  readonly browser: Browser;
}): Promise<void> => {
  console.info(`\n${input.label}`);
  const root = await mkdtemp(join(tmpdir(), "git-plus-one-origin-"));
  const cli = join(here, "..", "cli", "bin.ts");
  const init = spawn(process.execPath, [cli, "init", "--root", root, "core"], { stdio: "ignore" });
  await once(init, "exit");

  const child = spawn(process.execPath, [...input.argv(root, input.port)], {
    cwd: join(here, "..", ".."),
    env: { ...process.env, GIT_ROOT: root, PORT: String(input.port) },
    stdio: ["ignore", "pipe", "pipe"],
  });
  const page = await input.browser.newPage();
  const thrown: string[] = [];
  page.on("pageerror", (error) => thrown.push(error.message));
  let output = "";
  try {
    await new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(
        () => reject(new Error(`${input.label} did not start:\n${output}`)),
        20_000,
      );
      const append = (chunk: Buffer): void => {
        output += chunk.toString("utf8");
        if (!output.includes(input.ready)) return;
        clearTimeout(timeout);
        resolve();
      };
      child.stdout.on("data", append);
      child.stderr.on("data", append);
      child.once("error", reject);
      child.once("exit", (code) => {
        if (!output.includes(input.ready)) {
          clearTimeout(timeout);
          reject(new Error(`${input.label} exited ${String(code)}:\n${output}`));
        }
      });
    });

    const origin = `http://127.0.0.1:${String(input.port)}`;
    const index = await fetch(`${origin}/`);
    check(
      `${input.label} answers the page itself`,
      index.status === 200 && (index.headers.get("content-type") ?? "").startsWith("text/html"),
      `${String(index.status)} ${index.headers.get("content-type") ?? ""}`,
    );
    const bundle = await fetch(`${origin}/main.js`);
    check(
      "and the bundle beside it",
      bundle.status === 200 && (bundle.headers.get("content-type") ?? "") === "text/javascript",
      `${String(bundle.status)} ${bundle.headers.get("content-type") ?? ""}`,
    );
    // The same origin, which is the whole point: a path the bundle does not
    // hold falls through to the repository the server is hosting.
    const api = await fetch(`${origin}/core/hub/tasks`);
    check("and the API on that same origin", api.status === 200, String(api.status));
    // A method that is not a read is the API's whatever it is addressed to,
    // and its body has to survive the trip — this is what the proxy used to
    // get wrong, so it is still asked.
    const post = await fetch(`${origin}/core/diff`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ from: "refs/heads/nowhere", to: "refs/heads/main" }),
    });
    // The repository is empty, so the honest answer is that the ref is not
    // there — and it can only name the ref if the body arrived and was read.
    const said = await post.text();
    check(
      "and reads a POST body on it",
      said.includes("refs/heads/nowhere"),
      `${String(post.status)} ${said.slice(0, 80)}`,
    );

    await page.goto(`${origin}/#/tasks`, { waitUntil: "networkidle" });
    await page.waitForTimeout(1500);
    check(
      "and the UI it served actually mounts",
      (await page.locator(".gp-sidebar").count()) === 1 &&
        (await page.locator(".gp-task-row").count()) > 0,
    );
    check("with no uncaught errors", thrown.length === 0, thrown.join("; "));
  } finally {
    await page.close();
    if (child.exitCode === null) {
      child.kill();
      await once(child, "exit");
    }
    await rm(root, { force: true, recursive: true });
  }
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

  // Both fallback reasons, because they used to read identically. This suite's
  // server is up but serves no API routes, so the notice must name the answer;
  // a client that cannot connect at all must say something different, which is
  // what the dead-port pass below checks. Conflating them is how a client
  // sending unqualified refs looked exactly like a missing server.
  await page.goto(`${origin}/#/code`, { waitUntil: "domcontentloaded" });
  await page.waitForTimeout(1500);
  const answered = ((await page.textContent(".gp-notice")) ?? "").replace(/\s+/g, " ").trim();
  check(
    "a fallback caused by a bad answer names it",
    answered.includes("the git+ API answered"),
    answered,
  );

  // 9 is the discard port: nothing accepts there, so `fetch` fails at transport.
  await page.route("**/index.html", async (route) => {
    const response = await route.fetch();
    const body = (await response.text()).replace(
      'name="gp-api-base" content=""',
      'name="gp-api-base" content="http://127.0.0.1:9"',
    );
    await route.fulfill({ response, body });
  });
  await page.goto(`${origin}/index.html#/code`, { waitUntil: "domcontentloaded" });
  await page.waitForTimeout(2000);
  const dead = ((await page.textContent(".gp-notice")) ?? "").replace(/\s+/g, " ").trim();
  check(
    "a fallback caused by nothing listening says so instead",
    dead.includes("the git+ API is not running"),
    dead,
  );
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
  // The releases head their work rather than taking rows of their own, so the
  // hierarchy the design drew keeps the depths it was drawn at.
  check(
    "and heads it with the releases the work belongs to",
    (await page.locator(".gp-milestone-head").count()) === 2 &&
      ((await page.textContent(".gp-milestone-head")) ?? "").includes("v0.4 — Identity"),
  );

  // --- the kind filter ----------------------------------------------------
  await page.click('.gp-segment[value="crs"]');
  await page.waitForTimeout(400);
  check(
    "the CR segment narrows the list to Change Requests",
    (await page.locator(".gp-task-row").count()) === 4,
  );
  check(
    "and marks itself active",
    (await page.locator('.gp-segment[value="crs"][data-active]').count()) === 1,
  );
  await page.click('.gp-segment[value="tasks"]');
  await page.waitForTimeout(400);
  check(
    "the Tasks segment shows only pure Tasks",
    // Seven, not five: a release is a task, and a narrowed list has no
    // hierarchy left to group by, so they take rows like everything else.
    (await page.locator(".gp-task-row").count()) === 7,
  );
  await page.click('.gp-segment[value="all"]');
  await page.waitForTimeout(400);
  check("All restores the hierarchy", (await page.locator(".gp-task-row").count()) === 9);

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
  // The crumbs alone, not the whole sidebar: the move control below them
  // lists every task as an option, so `.gp-meta` contains every title there
  // is. Read as one chain, outermost first — this sits under the epic T-12,
  // which sits under the release, and one row says both.
  const crumbs = (): Promise<readonly string[]> =>
    page.evaluate(() =>
      [...document.querySelectorAll(".gp-crumbs .gp-parent-link")].map(
        (crumb) => crumb.textContent?.trim() ?? "",
      ),
    );
  const belongsTo = (): Promise<string> =>
    page.textContent(".gp-meta-value").then((text) => text?.trim() ?? "");
  const CHAIN = JSON.stringify(["v0.4 — Identity", "Implement authentication"]);
  check(
    "the detail reads the whole chain a task hangs from",
    JSON.stringify(await crumbs()) === CHAIN,
    JSON.stringify(await crumbs()),
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

  // The last crumb is the task's own parent; the first is the release.
  await page.click(".gp-crumbs .gp-parent-link:last-of-type");
  await page.waitForTimeout(500);
  check("the last crumb walks up the hierarchy", (await hash()) === "#/detail/T-12");
  check(
    "the parent lists its four subtasks",
    (await page.locator(".gp-subtask-row").count()) === 4,
  );

  await page.click(".gp-subtask-row:nth-child(2)");
  await page.waitForTimeout(500);
  check("a subtask walks back down", (await hash()) === "#/detail/CR-14");

  // Last of the checks on this hierarchy: re-filing appends rather than
  // restoring a position, so T-12's subtask order changes and the reads by
  // position above would no longer mean what they say.
  await page.selectOption(".gp-meta-select", "");
  await page.waitForTimeout(1500);
  check(
    "the move control detaches a fixture task in the tab",
    (await crumbs()).length === 0 && (await belongsTo()) === "—",
  );
  await page.selectOption(".gp-meta-select", "T-12");
  await page.waitForTimeout(1500);
  check("and files it back under the epic it came from", JSON.stringify(await crumbs()) === CHAIN);

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
  const columns = (): Promise<number> => page.locator(".gp-cal-day").count();
  check("the sample timeline spans the design's fortnight", (await columns()) === 14);
  await page.click('.gp-segment[value="day"]');
  await page.waitForTimeout(300);
  // The sample timeline is anchored to dates, so zooming re-windows it rather
  // than disabling — the day columns are what prove the window actually moved.
  check("the zoom narrows the sample timeline too", (await columns()) === 7);
  await page.click('.gp-segment[value="week"]');
  await page.waitForTimeout(300);
  check("and widens it back", (await columns()) === 14);
  await page.click(".gp-cal-event:nth-child(1)");
  await page.waitForTimeout(500);
  check("a timeline card opens its task", (await hash()).startsWith("#/detail/"), await hash());

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

  // --- merging ------------------------------------------------------------
  // Hash-only navigations share one document, so the store carries state
  // from here on: the merge and the created task below stay visible.
  await page.goto(`${origin}/#/detail/CR-14`, { waitUntil: "domcontentloaded" });
  await page.waitForTimeout(600);
  await page.click(".gp-merge-btn");
  await page.waitForTimeout(400);
  check(
    "merging a mergeable Change Request lands it",
    (await page.textContent(".gp-detail-eyebrow .gp-status"))?.trim() === "Merged",
  );
  check(
    "and the button settles into its merged state",
    await page.evaluate(() => {
      const btn = document.querySelector<HTMLButtonElement>(".gp-merge-btn");
      return btn?.dataset["state"] === "merged" && btn.disabled;
    }),
  );
  check(
    // Eight, not six: the two releases are tasks, and open ones. Counting them
    // is the premise of this design rather than an oversight — a release that
    // is not a task would need a type of its own again.
    "and the rail badge counts one fewer open item",
    (await page.textContent(".gp-nav-badge"))?.trim() === "8",
  );

  // --- creating a task ----------------------------------------------------
  await page.goto(`${origin}/#/tasks`, { waitUntil: "domcontentloaded" });
  await page.waitForTimeout(400);
  await page.click(".gp-tasks-head .gp-btn-primary");
  await page.waitForTimeout(400);
  await page.fill("#gp-new-title", "Verify the composer");
  await page.fill("#gp-new-desc", "Created by verify.ts");
  await page.click('.gp-dialog button[type="submit"]');
  // The submit signs and tries the hub first; offline that fails fast and
  // falls back to the tab-local store, but the round trip needs a moment.
  await page.waitForTimeout(1200);
  check("creating a task opens its detail", (await hash()) === "#/detail/T-21");
  check(
    "and it carries the typed title",
    (await page.textContent(".gp-detail-title"))?.trim() === "Verify the composer",
  );
  check(
    "and the rail badge counts it as open",
    (await page.textContent(".gp-nav-badge"))?.trim() === "9",
  );

  // --- the comment composer ----------------------------------------------
  await page.fill(".gp-comment-form textarea", "First comment.");
  await page.click('.gp-comment-form button[type="submit"]');
  await page.waitForTimeout(400);
  check(
    "a submitted comment joins the discussion",
    (await page.locator(".gp-comment").count()) === 1 &&
      ((await page.textContent(".gp-comment-body")) ?? "").includes("First comment."),
  );
  check(
    "authored as whoami's answer — anonymous with no API",
    (await page.textContent(".gp-comment-author"))?.trim() === "anonymous",
  );

  await page.click(".gp-back");
  await page.waitForTimeout(400);
  check("the created task joined the list", (await page.locator(".gp-task-row").count()) === 10);

  // --- ⌘K and the rail search --------------------------------------------
  await page.keyboard.press("Control+k");
  check(
    "ctrl/cmd-K focuses the rail search",
    await page.evaluate(() => document.activeElement?.matches(".gp-search input") === true),
  );
  await page.keyboard.type("auth");
  await page.waitForTimeout(900);
  check("a query opens the Search screen", (await hash()) === "#/search");
  check("and matches tasks by title", (await page.locator(".gp-search-task-row").count()) === 2);
  check(
    "code search says it needs the server when there is none",
    ((await page.textContent(".gp-notice")) ?? "").includes("Code search needs the server"),
  );

  // --- what has no endpoint says so, rather than pretending ---------------
  await page.goto(`${origin}/#/settings`, { waitUntil: "domcontentloaded" });
  await page.waitForTimeout(700);
  check(
    "the danger zone is disabled while it has no endpoint",
    await page.evaluate(() => {
      const buttons = [...document.querySelectorAll<HTMLButtonElement>(".gp-danger-btn")];
      return buttons.length === 2 && buttons.every((button) => button.disabled);
    }),
  );
  check(
    "and the admin cards name the unreachable API rather than showing nothing",
    ((await page.textContent('[data-card="remotes"]')) ?? "").includes("not reachable"),
  );

  await page.goto(`${origin}/#/code`, { waitUntil: "domcontentloaded" });
  await page.waitForTimeout(1400);
  check(
    "the editor is read-only against the sample repository",
    await page.evaluate(
      () =>
        document.querySelector<HTMLButtonElement>('.gp-file-card button[aria-label="Edit file"]')
          ?.disabled === true &&
        document.querySelector<HTMLButtonElement>('.gp-explorer button[aria-label="New file"]')
          ?.disabled === true,
    ),
  );
  check(
    "and so is the branch picker, with nothing to create against",
    await page.evaluate(
      () =>
        document.querySelector<HTMLButtonElement>(".gp-branch-trigger")?.disabled === true &&
        document.querySelector("ui-menu.gp-branch-menu") === null,
    ),
  );

  // --- hash routes carry any path, and malformed ones fail visibly --------
  // Spaces, a literal `%`, a `#`, Unicode and nesting all ride inside
  // component-encoded segments, so the copied URL and a refresh reopen the
  // same path instead of truncating at the first reserved character.
  const tricky = "docs/a b%c✓/file #1.md";
  const encoded = tricky
    .split("/")
    .map((segment) => encodeURIComponent(segment))
    .join("/");
  await page.goto(`${origin}/#/code/${encoded}`, { waitUntil: "domcontentloaded" });
  await page.waitForTimeout(700);
  const wantedPath = () =>
    page.evaluate(() => {
      const code = document.querySelector("gp-code");
      // SAFETY: `wanted` is gp-code's reactive property, set by the shell's
      // route parsing; absent means the element or the property is not there.
      return (code as { wanted?: string | null } | null)?.wanted ?? null;
    });
  check(
    "an encoded file path survives navigation and refresh",
    (await hash()) === `#/code/${encoded}` && (await wantedPath()) === tricky,
  );
  await page.reload({ waitUntil: "domcontentloaded" });
  await page.waitForTimeout(700);
  check("and the reload decodes it back to the same file", (await wantedPath()) === tricky);

  await page.goto(`${origin}/#/code/%E0%A4%A`, { waitUntil: "domcontentloaded" });
  await page.waitForTimeout(700);
  check(
    "a malformed escape leaves the shell usable with a visible navigation error",
    await page.evaluate(
      () =>
        document.querySelector(".gp-shell") !== null &&
        document.querySelector(".gp-nav-error") !== null,
    ),
  );

  await page.close();
};

/** Code and Diff read the API rather than the fixtures. */
const live = async (browser: Browser, origin: string): Promise<void> => {
  console.info("\nlive");
  const page = await browser.newPage({ viewport: { width: 1440, height: 900 } });
  page.on("pageerror", (error) => failures.push(`pageerror: ${error.message}`));
  const hash = (): Promise<string> => page.evaluate(() => globalThis.location.hash);

  await page.goto(`${origin}/#/code`, { waitUntil: "networkidle" });
  await page.waitForTimeout(2500);
  check(
    "no fallback notice when the API answers",
    (await page.locator(".gp-notice").count()) === 0,
  );
  const bar = (await page.textContent(".gp-commit-bar")) ?? "";
  check("the commit bar carries the subject", bar.includes("merge CR-18: update pipeline config"));
  check(
    "and the short oid",
    (await page.textContent(".gp-commit-sha"))?.trim() === OID_MAIN.slice(0, 7),
  );
  // Author and age exist only in the raw commit header, so these two prove the
  // `/object/:oid` round trip and the header parse, not just that a bar rendered.
  check("and the author from the raw commit header", bar.includes("Rune Baek"), bar.trim());
  check("and a relative age", /\b\d+[wdhm] ago\b|just now/.test(bar));
  check(
    "the sidebar identity comes from /whoami",
    (await page.textContent(".gp-user-name"))?.trim() === "anonymous",
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
  const rendererTheme = (): Promise<string> =>
    page.evaluate(
      () =>
        document
          .querySelector("diffs-container")
          ?.shadowRoot?.querySelector("style[data-theme-css]")?.textContent ?? "",
    );
  const themeBefore = await rendererTheme();
  await page.click(".gp-theme-toggle");
  await page.waitForTimeout(500);
  const themeAfter = await rendererTheme();
  check(
    "a mounted source renderer follows theme changes",
    themeBefore !== "" && themeAfter !== "" && themeBefore !== themeAfter,
  );
  await shot(page, "live-code");

  // --- editing, through POST /commit --------------------------------------
  await page.click('.gp-file-card button[aria-label="Edit file"]');
  await page.waitForTimeout(300);
  check(
    "the pencil opens the blob in an editor",
    (await page.inputValue(".gp-editor")).includes("Live from the API."),
  );
  await page.fill(".gp-editor", "# core\n\nEdited from the UI.\n");
  await page.fill(".gp-editor-message", "update the README from the browser");
  await page.click(".gp-editor-bar .gp-btn-primary");
  await page.waitForTimeout(1500);
  check(
    "committing returns to the view with the new content",
    (await page.getByText("Edited from the UI.").count()) > 0,
  );
  check(
    "and the commit bar carries the new tip",
    ((await page.textContent(".gp-commit-bar")) ?? "").includes(
      "update the README from the browser",
    ),
  );
  check(
    "and stays on the edited file",
    ((await page.textContent(".gp-card-head")) ?? "").includes("README.md"),
  );

  // --- a new file, from the explorer's "+" --------------------------------
  await page.click('.gp-explorer button[aria-label="New file"]');
  await page.waitForTimeout(300);
  await page.fill(".gp-editor-path", "docs/notes.md");
  await page.fill(".gp-editor", "# Notes\n");
  await page.click(".gp-editor-bar .gp-btn-primary");
  await page.waitForTimeout(1500);
  check(
    "a new file lands in the explorer",
    (await page.locator('[data-item-path="docs/notes.md"]').count()) > 0,
  );
  check(
    "and opens in the viewer",
    ((await page.textContent(".gp-card-head")) ?? "").includes("docs/notes.md"),
  );

  // --- a commit that lost the race ----------------------------------------
  await page.click('.gp-file-card button[aria-label="Edit file"]');
  await page.waitForTimeout(300);
  await page.route("**/core/commit", async (route) => {
    await route.fulfill({
      status: 409,
      contentType: "application/json",
      body: JSON.stringify({ _tag: "RefConflict", message: "refs/heads/main moved" }),
    });
  });
  await page.click(".gp-editor-bar .gp-btn-primary");
  await page.waitForTimeout(600);
  check(
    "a conflicting tip surfaces as an error, not an overwrite",
    ((await page.textContent(".gp-notice[data-error]")) ?? "").includes("someone else committed"),
  );
  await page.unroute("**/core/commit");

  // --- deleting: the same request, with `content: null` -------------------
  await page.click(".gp-editor-bar button:has-text('Delete file')");
  await page.waitForTimeout(1500);
  check(
    "deleting removes the file and falls back to the README",
    (await page.locator('[data-item-path="docs/notes.md"]').count()) === 0 &&
      ((await page.textContent(".gp-card-head")) ?? "").includes("README.md"),
  );
  await shot(page, "live-code-edited");

  // --- Clone, over the real origin ---------------------------------------
  await page.click(".gp-clone [data-dialog-trigger]");
  await page.waitForTimeout(400);
  check(
    "Clone offers the smart-HTTP URL this page is served from",
    (await page.inputValue(".gp-clone-url")) === `${origin}/core`,
    await page.inputValue(".gp-clone-url"),
  );
  await page.keyboard.press("Escape");
  await page.waitForTimeout(300);

  // --- history, from /commits and /history --------------------------------
  await page.click(".gp-commit-bar");
  await page.waitForTimeout(1200);
  check(
    "the commit bar opens the branch's history",
    (await page.locator(".gp-history-panel .gp-list-row").count()) === HISTORY.length,
  );
  await page.click(".gp-commit-bar");
  await page.waitForTimeout(300);
  check("and closes it again", (await page.locator(".gp-history-panel").count()) === 0);
  await page.click('.gp-file-card button[aria-label="File history"]');
  await page.waitForTimeout(1200);
  check(
    "the clock opens the file's own history",
    (await page.locator(".gp-filelog-row").count()) === 2,
  );
  await page.click(".gp-filelog-row:nth-child(2)");
  await page.waitForTimeout(1200);
  check(
    "choosing a commit shows that revision, read-only",
    ((await page.textContent(".gp-notice[data-history]")) ?? "").includes("read-only") &&
      (await page.evaluate(
        () =>
          document.querySelector<HTMLButtonElement>('.gp-file-card button[aria-label="Edit file"]')
            ?.disabled === true,
      )),
  );
  await page.click(".gp-notice[data-history] .gp-link-btn");
  await page.waitForTimeout(1000);
  check(
    "and Back to tip restores the editable view",
    (await page.locator(".gp-notice[data-history]").count()) === 0,
  );
  await page.click('.gp-file-card button[aria-label="File history"]');
  await page.waitForTimeout(300);

  // --- creating a branch, through POST /branches/create -------------------
  await page.click(".gp-branch-trigger");
  await page.waitForTimeout(400);
  await page.click('.gp-menu-item[value="__new-branch"]');
  await page.waitForTimeout(400);
  await page.fill("#gp-new-branch-name", "topic/from-the-ui");
  await page.click(".gp-new-branch button[type='submit']");
  await page.waitForTimeout(1800);
  check(
    "creating a branch switches the screen onto it",
    ((await page.textContent(".gp-branch-trigger")) ?? "").includes("topic/from-the-ui"),
  );
  await page.click(".gp-branch-trigger");
  await page.waitForTimeout(400);
  check(
    "and it joins the picker",
    (await page.locator(".gp-branch-menu .gp-menu-item:not([data-action])").count()) === 3,
  );
  await page.click('.gp-menu-item[value="main"]');
  await page.waitForTimeout(1500);

  // --- the branch picker, over the real ref list -------------------------
  check(
    "the branch picker is a menu when there is more than one branch",
    (await page.locator("ui-menu.gp-branch-menu").count()) === 1,
  );
  await page.click(".gp-branch-trigger");
  await page.waitForTimeout(400);
  check(
    "it lists every branch",
    (await page.locator(".gp-branch-menu .gp-menu-item:not([data-action])").count()) === 3,
  );
  await page.click(`.gp-menu-item[value="${BRANCH}"]`);
  await page.waitForTimeout(2000);
  check(
    "choosing one switches the ref",
    ((await page.textContent(".gp-branch-trigger")) ?? "").includes(BRANCH),
  );
  check(
    "and the commit bar follows it",
    ((await page.textContent(".gp-commit-bar")) ?? "").includes("add auth middleware"),
  );
  check(
    "and the explorer refetches at the new ref",
    (await page.locator('[data-item-path="src/server/Api.ts"]').count()) > 0,
  );

  // Start a slow switch to main, then immediately choose the already-visible
  // branch again. Only the second request is allowed to commit its snapshot.
  await page.route("**/core/files?*", async (route) => {
    const ref = new URL(route.request().url()).searchParams.get("ref");
    await new Promise((resolve) => setTimeout(resolve, ref === "refs/heads/main" ? 450 : 20));
    await route.continue();
  });
  await page.locator("ui-menu.gp-branch-menu").evaluate((menu, branch) => {
    menu.dispatchEvent(
      new CustomEvent("menu-select", { bubbles: true, detail: { value: "main" } }),
    );
    menu.dispatchEvent(
      new CustomEvent("menu-select", { bubbles: true, detail: { value: branch } }),
    );
  }, BRANCH);
  await page.waitForTimeout(1200);
  check(
    "an older branch request cannot overwrite the latest selection",
    ((await page.textContent(".gp-branch-trigger")) ?? "").includes(BRANCH) &&
      ((await page.textContent(".gp-commit-bar")) ?? "").includes("add auth middleware"),
  );
  await page.unroute("**/core/files?*");

  // A slow first blob must not be painted under the second path's heading.
  await page.route("**/core/file?*", async (route) => {
    const path = new URL(route.request().url()).searchParams.get("path");
    await new Promise((resolve) => setTimeout(resolve, path === "src/server/Api.ts" ? 450 : 20));
    await route.continue();
  });
  await page.locator('[data-item-path="src/server/Api.ts"]').click();
  await page.locator('[data-item-path="src/git/Store.ts"]').click();
  await page.waitForTimeout(1000);
  check(
    "an older file request cannot overwrite the latest selection",
    ((await page.textContent(".gp-card-head")) ?? "").includes("src/git/Store.ts") &&
      (await page.getByText("export const store = 2;").count()) > 0,
  );
  await page.unroute("**/core/file?*");
  await shot(page, "live-code-switched");

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

  // Start the deliberately slow CR-14 request, navigate away, then load a
  // legitimate empty diff. The old response must not replace the new state,
  // and an empty live result must never fall through to fixture content.
  await page.goto(`${origin}/#/detail/CR-14`, { waitUntil: "domcontentloaded" });
  await page.click('.gp-tab[value="diff"]');
  await page.evaluate(() => {
    globalThis.location.hash = "#/detail/CR-15";
  });
  await page.waitForFunction(
    () => document.querySelector(".gp-detail-title")?.textContent?.trim() === "Add login UI",
  );
  await page.click('.gp-tab[value="diff"]');
  await page.waitForTimeout(900);
  check(
    "an empty live diff is explicit and rejects a stale prior request",
    ((await page.textContent(".gp-empty")) ?? "").includes("No textual changes") &&
      (await page.locator(".gp-diff-static").count()) === 0,
  );

  // --- Activity, from real commit history --------------------------------
  await page.goto(`${origin}/#/activity`, { waitUntil: "networkidle" });
  await page.waitForTimeout(2500);
  check("activity does not fall back", (await page.locator(".gp-notice").count()) === 0);
  check(
    "activity lays out one card per commit",
    (await page.locator(".gp-cal-event").count()) === HISTORY.length,
    `${String(await page.locator(".gp-cal-event").count())} cards`,
  );
  check(
    "activity shows a real subject",
    ((await page.textContent(".gp-cal-grid")) ?? "").includes("widen the api surface"),
  );
  check(
    "activity computes its month rather than hardcoding one",
    ((await page.textContent(".gp-cal-month")) ?? "").includes(
      new Date().toLocaleString(undefined, { month: "long" }),
    ),
  );

  // --- the window controls, over real history ----------------------------
  await page.click('.gp-range button[aria-label="Earlier"]');
  await page.waitForTimeout(400);
  check(
    "paging back shows the explicitly empty older window",
    (await page.locator(".gp-cal-event").count()) === 0 &&
      ((await page.textContent(".gp-cal-body")) ?? "").includes("No commits in this window"),
  );
  await page.click('.gp-range button[aria-label="Later"]');
  await page.waitForTimeout(400);
  check(
    "paging forward restores today's window",
    (await page.locator(".gp-cal-event").count()) === HISTORY.length,
  );
  await page.click('.gp-segment[value="day"]');
  await page.waitForTimeout(400);
  check("day zoom narrows the grid to a week", (await page.locator(".gp-cal-day").count()) === 7);
  await page.click('.gp-segment[value="month"]');
  await page.waitForTimeout(400);
  check("month zoom widens it to 31 days", (await page.locator(".gp-cal-day").count()) === 31);
  await page.click('.gp-segment[value="week"]');
  await page.waitForTimeout(400);
  check(
    "week zoom restores the design's fortnight",
    (await page.locator(".gp-cal-day").count()) === 14,
  );
  await shot(page, "live-activity");

  // --- search, over /grep -------------------------------------------------
  await page.goto(`${origin}/#/code`, { waitUntil: "networkidle" });
  await page.waitForTimeout(1500);
  await page.keyboard.press("Control+k");
  await page.keyboard.type("export const");
  await page.waitForTimeout(1500);
  check(
    "code search finds real file contents",
    (await page.locator(".gp-search-hit").count()) >= 2,
    `${String(await page.locator(".gp-search-hit").count())} hits`,
  );
  check(
    "and names the file and line",
    ((await page.textContent(".gp-search-hit-path")) ?? "").includes(":1"),
  );
  await page.click(".gp-search-hit");
  await page.waitForTimeout(1800);
  check("a hit opens the file in Code", (await hash()).startsWith("#/code/"), await hash());
  check(
    "and the viewer shows that file",
    ((await page.textContent(".gp-card-head")) ?? "").includes(".ts"),
  );
  await shot(page, "live-search");

  // --- Settings, the administration surface ------------------------------
  await page.goto(`${origin}/#/settings`, { waitUntil: "networkidle" });
  await page.waitForTimeout(1500);
  const fields = await page.locator(".gp-field-value").allTextContents();
  check("settings names the repository the client is pointed at", fields[0]?.trim() === "core");
  check("settings resolves the default branch from /branches", fields[1]?.trim() === "main");
  check(
    "the identity card reports what /whoami answered",
    ((await page.textContent('[data-card="identity"]')) ?? "").includes("no genesis"),
  );
  check(
    "the branch card lists every head",
    (await page.locator('[data-card="branches"] .gp-admin-row').count()) >= 2,
  );
  check(
    "and refuses to delete the default branch",
    await page.evaluate(
      () =>
        document.querySelector<HTMLButtonElement>('[data-card="branches"] .gp-btn-quiet')
          ?.disabled === true,
    ),
  );

  // The policy card's toggles are the ui-switch adoption in the flesh:
  // clicking the native checkbox must flip the element's `data-state`.
  const switchState = (): Promise<string> =>
    page.evaluate(
      () =>
        document.querySelector('[data-card="policy"] ui-switch')?.getAttribute("data-state") ?? "",
    );
  const off = await switchState();
  await page.evaluate(() =>
    document.querySelector('[data-card="policy"] ui-switch')?.querySelector("input")?.click(),
  );
  await page.waitForTimeout(300);
  check(
    "ui-switch mirrors the native checkbox it adopted",
    off === "unchecked" && (await switchState()) === "checked",
  );

  // A tag, then its removal — the whole round trip through /tags.
  await page.fill('[data-card="tags"] input[name="name"]', "v0.4.0");
  await page.fill('[data-card="tags"] input[name="message"]', "identity milestone");
  await page.click('[data-card="tags"] button[type="submit"]');
  await page.waitForTimeout(1200);
  check(
    "creating a tag reports it and lists it",
    ((await page.textContent('[data-card-note="tags"]')) ?? "").includes("v0.4.0") &&
      (await page.locator('[data-card="tags"] .gp-admin-row').count()) === 1,
  );
  await page.click('[data-card="tags"] .gp-admin-row .gp-btn-quiet');
  await page.waitForTimeout(1200);
  check(
    "and deleting it empties the list again",
    (await page.locator('[data-card="tags"] .gp-admin-row').count()) === 0,
  );

  // Moving a ref, through /reset.
  await page.fill('[data-card="branches"] input[name="to"]', OID_BRANCH);
  await page.click('[data-card="branches"] button[type="submit"]');
  await page.waitForTimeout(1200);
  check(
    "moving a branch reports where it went",
    ((await page.textContent('[data-card-note="branches"]')) ?? "").includes(
      OID_BRANCH.slice(0, 7),
    ),
    (await page.textContent('[data-card-note="branches"]')) ?? "",
  );

  // A remote, then its sync verbs.
  await page.fill('[data-card="remotes"] input[name="name"]', "origin");
  await page.fill('[data-card="remotes"] input[name="url"]', "https://git.example.com/core");
  await page.click('[data-card="remotes"] button[type="submit"]');
  await page.waitForTimeout(1200);
  check(
    "registering a remote lists it",
    (await page.locator('[data-card="remotes"] .gp-admin-row').count()) === 1,
  );
  await page.click('[data-card="remotes"] .gp-admin-actions .gp-btn-quiet:nth-child(3)');
  await page.waitForTimeout(1200);
  check(
    "pulling reports what the remote had",
    ((await page.textContent('[data-card-note="remotes"]')) ?? "").includes("up-to-date"),
    (await page.textContent('[data-card-note="remotes"]')) ?? "",
  );

  // A webhook.
  await page.fill('[data-card="webhooks"] input[name="url"]', "https://ci.example.com/hook");
  await page.fill('[data-card="webhooks"] input[name="secret"]', "s3cret");
  await page.click('[data-card="webhooks"] button[type="submit"]');
  await page.waitForTimeout(1200);
  check(
    "registering a webhook lists it without its secret",
    (await page.locator('[data-card="webhooks"] .gp-admin-row').count()) === 1 &&
      !((await page.textContent('[data-card="webhooks"]')) ?? "").includes("s3cret"),
  );

  // Maintenance: fsck, gc, reflog.
  await page.click('[data-card="maintenance"] .gp-btn-quiet:nth-child(1)');
  await page.waitForTimeout(1000);
  check(
    "fsck reports the store sound",
    ((await page.textContent('[data-card-note="maintenance"]')) ?? "").includes("all sound"),
  );
  await page.click('[data-card="maintenance"] .gp-btn-quiet:nth-child(2)');
  await page.waitForTimeout(1000);
  check(
    "a dry-run collection says what would go",
    ((await page.textContent('[data-card-note="maintenance"]')) ?? "").includes("dry run"),
  );
  await page.click('[data-card="maintenance"] .gp-btn-quiet:nth-child(4)');
  await page.waitForTimeout(1000);
  check(
    "the reflog lists where the branch has been",
    (await page.locator('[data-card="maintenance"] .gp-admin-row').count()) === 2,
  );
  await shot(page, "live-settings");

  // --- merging a Change Request against the real endpoint -----------------
  await page.goto(`${origin}/#/detail/CR-19`, { waitUntil: "networkidle" });
  await page.waitForTimeout(1200);
  await page.click(".gp-merge-btn");
  await page.waitForTimeout(1500);
  check(
    "a merge the server can run lands without a notice",
    (await page.textContent(".gp-detail-eyebrow .gp-status"))?.trim() === "Merged" &&
      (await page.locator(".gp-review .gp-notice").count()) === 0,
  );
  await page.close();
};

/**
 * The whole loop, against a real repository: the page clones into OPFS over
 * smart HTTP, reads and commits locally, and pushes back — with the server
 * demoted to `origin`. A real node host holds the repository; a small
 * static-plus-proxy front serves the bundle same-origin over it, which is the
 * deployed shape (the Worker serves both) and the one a browser's CORS rules
 * allow.
 */
const localMode = async (browser: Browser): Promise<void> => {
  console.info("\nlocal");

  const root = await mkdtemp(join(tmpdir(), "gp-local-"));
  const author = {
    name: "Origin",
    email: "origin@example.com",
    at: new Date(1_700_000_000_000),
    offset: 0,
  };
  const repoLayer = GitRepository.layer.pipe(
    Layer.provide(GitRepository.hooksNoop),
    Layer.provide(nodeStores(join(root, "core"))),
  );

  // The origin's history: one commit — and one hub task, signed in-process,
  // because a task needs no genesis to be projected.
  const head = await Effect.runPromise(
    Effect.gen(function* () {
      const repository = yield* GitRepository.Repository;
      const blob = yield* repository.writeBlob(
        new TextEncoder().encode("# core\n\nServed from origin.\n"),
      );
      const tree = yield* repository.writeTree([{ mode: "100644", name: "readme.md", oid: blob }]);
      const commit = yield* repository.commit({
        branch: "main",
        tree,
        message: "first from origin",
        author,
      });
      const key = yield* generate("fleet@example.com");
      // A release is an ordinary task, and the work belongs to it — which is
      // all "Milestone" is on the detail screen.
      const milestone = yield* HubTask.open({ repo: "core", title: "v0.4 — Identity", key });
      yield* HubTask.open({
        repo: "core",
        title: "Review the local mode",
        description: "Adopted from the hub, not the fixtures.",
        parent: milestone.task,
        key,
      });
      return commit;
    }).pipe(Effect.provide(repoLayer)),
  );

  const host = await serveHost({ root, allowAnonymousWrites: true });
  const upstream = host.url;

  // Static for the bundle, everything else forwarded — `dev:ui`'s shape.
  const front = createServer((request, response) => {
    void (async () => {
      const url = new URL(request.url ?? "/", "http://localhost");
      const asset = await assetResponse(
        dist,
        new Request(url, { method: request.method ?? "GET" }),
      );
      if (asset !== null) {
        response.writeHead(asset.status, Object.fromEntries(asset.headers));
        return response.end(Buffer.from(await asset.arrayBuffer()));
      }
      const chunks: Buffer[] = [];
      for await (const chunk of request) chunks.push(Buffer.from(chunk));
      const body = Buffer.concat(chunks);
      // Every header both ways except the hop-by-hop set: the native auth
      // scheme lives in `www-authenticate` and `authorization`, and a proxy
      // that drops either silently breaks the challenge.
      const HOP = new Set(["host", "connection", "content-length", "transfer-encoding"]);
      const headers = new Headers();
      for (const [name, value] of Object.entries(request.headers)) {
        // Node models repeated headers as arrays; none of the ones this
        // proxy carries repeat, so the first value is the value.
        const single = Array.isArray(value) ? value[0] : value;
        if (HOP.has(name.toLowerCase()) || single === undefined) continue;
        headers.set(name, single);
      }
      const proxied = await fetch(`${upstream}${request.url ?? "/"}`, {
        method: request.method ?? "GET",
        headers,
        body: body.length === 0 ? undefined : new Uint8Array(body),
      });
      const out: Record<string, string> = {};
      proxied.headers.forEach((value, name) => {
        if (!HOP.has(name) && name !== "content-encoding") out[name] = value;
      });
      response.writeHead(proxied.status, out);
      response.end(Buffer.from(await proxied.arrayBuffer()));
    })().catch(() => {
      response.writeHead(502);
      response.end("proxy failure");
    });
  });
  await new Promise<void>((resolve) => front.listen(8134, resolve));
  const origin = "http://localhost:8134";

  const page = await browser.newPage({ viewport: { width: 1440, height: 900 } });

  try {
    await page.goto(`${origin}/#/code`, { waitUntil: "networkidle" });

    // The swap is asynchronous: the clone runs off the boot path, and the
    // sync controls exist only once the screen holds the local client.
    await page.waitForSelector(".gp-sync", { timeout: 30_000 });
    check("the OPFS clone takes over the Code screen", true);
    await page.waitForTimeout(500);
    check(
      "the commit bar shows the origin's commit, read locally",
      ((await page.textContent(".gp-commit-subject")) ?? "").includes("first from origin"),
    );
    check(
      "there is nothing to push after a fresh clone",
      ((await page.textContent(".gp-sync")) ?? "").includes("↑0"),
    );

    // Edit and commit — locally: the tip moves in OPFS, not on the server.
    await page.click('.gp-file-card button[aria-label="Edit file"]');
    await page.waitForTimeout(400);
    await page.fill(".gp-editor", "# core\n\nEdited in the browser, committed to OPFS.\n");
    await page.fill(".gp-editor-message", "edit readme locally");
    await page.click(".gp-editor-bar .gp-btn-primary");
    await page.waitForTimeout(1200);
    check(
      "a local commit moves the local tip",
      ((await page.textContent(".gp-commit-subject")) ?? "").includes("edit readme locally"),
    );
    check(
      "the sync badge counts the unpushed commit",
      ((await page.textContent(".gp-sync")) ?? "").includes("↑1"),
    );

    // Reload before pushing. The tracking refs are observations of origin,
    // recorded at clone, push and fetch — a reload must not rewrite them
    // from local heads, or the unpushed commit above would read as "nothing
    // to push" and Push would disable over exactly the work that needs it.
    await page.reload({ waitUntil: "networkidle" });
    await page.waitForSelector(".gp-sync", { timeout: 30_000 });
    await page.waitForTimeout(800);
    const reloaded = (await page.textContent(".gp-sync")) ?? "";
    check(
      "the unpushed commit is still ahead after a full reload",
      reloaded.includes("↑1"),
      reloaded,
    );
    check("and being ahead does not read as also being behind", !reloaded.includes("↓"), reloaded);

    const before = await fetch(`${upstream}/core/refs`).then(async (response) =>
      Schema.decodeUnknownSync(Contract.RefsResponse)(await response.json()),
    );
    check(
      "the server has not seen the commit yet",
      before.refs.find((ref) => ref.name === "refs/heads/main")?.oid === head,
    );

    // Push — the manual sync the whole design argues for.
    await page.click(".gp-sync button:first-child");
    await page.waitForTimeout(2000);
    check(
      "the push reports success",
      ((await page.textContent(".gp-notice")) ?? "").includes("Pushed"),
    );
    check(
      "and leaves nothing to push",
      ((await page.textContent(".gp-sync")) ?? "").includes("↑0"),
    );
    const after = await fetch(`${upstream}/core/refs`).then(async (response) =>
      Schema.decodeUnknownSync(Contract.RefsResponse)(await response.json()),
    );
    const pushed = after.refs.find((ref) => ref.name === "refs/heads/main")?.oid;
    check("the server's main moved to the pushed commit", pushed !== undefined && pushed !== head);

    // The hub task, adopted from `GET /hub/tasks` in place of the fixtures.
    await page.goto(`${origin}/#/tasks`, { waitUntil: "networkidle" });
    await page.waitForTimeout(1200);
    check(
      "the Tasks screen shows the hub's task, not the fixtures",
      ((await page.textContent(".gp-task-list")) ?? "").includes("Review the local mode") &&
        (await page.locator(".gp-task-row").count()) === 1,
    );
    check(
      "the release the hub recorded heads the work filed under it",
      (await page.locator(".gp-milestone-head").count()) === 1 &&
        ((await page.textContent(".gp-milestone-head")) ?? "").includes("v0.4 — Identity"),
    );
    await page.click(".gp-task-row");
    await page.waitForTimeout(900);
    // The parent edge, read as a chain of one: the title, not the id the hub
    // actually records.
    check(
      "the detail names the release the task belongs to",
      ((await page.textContent(".gp-crumbs")) ?? "").trim() === "v0.4 — Identity",
    );

    // A task created here: signed with the browser's own key, appended over
    // POST /hub/events, and read back from the server's projection.
    await page.goto(`${origin}/#/tasks`, { waitUntil: "networkidle" });
    await page.waitForTimeout(1200);
    await page.click(".gp-tasks-head .gp-btn-primary");
    await page.waitForTimeout(400);
    await page.fill("#gp-new-title", "Opened by the browser key");
    await page.fill("#gp-new-desc", "Signed in the page, appended by the server.");
    await page.click('.gp-dialog button[type="submit"]');
    await page.waitForTimeout(2500);
    const served = await fetch(`${upstream}/core/hub/tasks`).then(async (response) =>
      Schema.decodeUnknownSync(Contract.HubTaskPage)(await response.json()),
    );
    check(
      "the signed task landed in the repository's hub",
      served.items.some((task) => task.title === "Opened by the browser key"),
    );
    check(
      "and its detail is the projection, not a tab-local copy",
      ((await page.textContent(".gp-detail-title")) ?? "").includes("Opened by the browser key"),
    );
    await page.goto(`${origin}/#/tasks`, { waitUntil: "networkidle" });
    await page.waitForTimeout(1200);
    // Two rows and one header: the release, the work under it, and the task
    // this browser opened, which belongs to nothing.
    check("the list counts every hub task", (await page.locator(".gp-task-row").count()) === 2);

    // The move control: a `task.reparented` signed in the page, appended over
    // POST /hub/events, and read back from the server's own projection —
    // filing work this browser did not open, which is what the looser rule on
    // re-filing (see `hub/Task.ts`) exists to allow.
    const idOf = (title: string): string =>
      served.items.find((task) => task.title === title)?.task ?? "";
    const created = idOf("Opened by the browser key");
    const release = idOf("v0.4 — Identity");
    await page.goto(`${origin}/#/detail/${created}`, { waitUntil: "networkidle" });
    await page.waitForTimeout(1200);
    await page.selectOption(".gp-meta-select", release);
    await page.waitForTimeout(2500);
    const filed = await fetch(`${upstream}/core/hub/tasks`).then(async (response) =>
      Schema.decodeUnknownSync(Contract.HubTaskPage)(await response.json()),
    );
    check(
      "the move control files a task in the hub itself",
      filed.items.find((task) => task.task === created)?.parent === release,
    );
    check(
      "and the release reads it back as its own",
      (filed.items.find((task) => task.task === release)?.children ?? []).includes(created),
    );
    await page.goto(`${origin}/#/tasks`, { waitUntil: "networkidle" });
    await page.waitForTimeout(1200);
    check(
      "so the list draws it under that release",
      (await page.locator(".gp-milestone-head").count()) === 1 &&
        (await page.locator(".gp-task-row").count()) === 2,
    );

    // --- enrolment: a genesis lands, and the browser's key is granted ------
    // Stage the proposal's branch while writes are still open: a browser's
    // git push has no envelope, so on a genesis'd repository the push comes
    // first and the signed events after.
    const hash = async (): Promise<string> => await page.evaluate(() => window.location.hash);

    await page.goto(`${origin}/#/code`, { waitUntil: "networkidle" });
    await page.waitForSelector(".gp-sync", { timeout: 30_000 });
    await page.click(".gp-branch-trigger");
    await page.waitForTimeout(400);
    await page.click('.gp-menu-item[value="__new-branch"]');
    await page.waitForTimeout(400);
    await page.fill("#gp-new-branch-name", "topic");
    await page.click(".gp-new-branch button[type='submit']");
    await page.waitForTimeout(1500);
    await page.click('.gp-file-card button[aria-label="Edit file"]');
    await page.waitForTimeout(400);
    await page.fill(".gp-editor", "# core\n\nProposed from the browser.\n");
    await page.fill(".gp-editor-message", "propose from the browser");
    await page.click(".gp-editor-bar .gp-btn-primary");
    await page.waitForTimeout(1200);
    await page.click(".gp-sync button:first-child");
    await page.waitForTimeout(2000);

    await page.goto(`${origin}/#/settings`, { waitUntil: "networkidle" });
    await page.waitForSelector('input[aria-label="Browser public key"]', { timeout: 15_000 });
    const browserKey = await page.inputValue('input[aria-label="Browser public key"]');
    check("Settings shows the browser's public key", browserKey.startsWith("ssh-ed25519 "));

    // Node-side, as an operator would: give the repository an identity and
    // grant the key the page showed. Nobody is granted `repo.read`, so reads
    // stay public and only writes start asking who is asking.
    const fixture = await enableHubUnder(root, "core", ["source.push"]);
    await Effect.runPromise(
      Effect.gen(function* () {
        yield* Log.issue(
          yield* Certificate.grant({
            repo: fixture.repoId,
            publicKey: browserKey,
            capabilities: [
              "source.push",
              "hub.create-pr",
              "hub.approve",
              "hub.comment",
              "hub.task",
              "hub.merge",
              "policy.write",
            ],
            id: Log.newId(),
          }),
          [fixture.root],
        );
      }).pipe(Effect.provide(repoLayer)),
    );

    // --- a Change Request, opened and reviewed under the granted key -------
    await page.goto(`${origin}/#/code`, { waitUntil: "networkidle" });
    await page.reload({ waitUntil: "networkidle" });
    await page.waitForSelector(".gp-sync", { timeout: 30_000 });
    await page.click(".gp-branch-trigger");
    await page.waitForTimeout(400);
    await page.click('.gp-menu-item[value="topic"]');
    await page.waitForTimeout(1500);
    await page.waitForSelector("ui-dialog.gp-propose button[data-dialog-trigger]", {
      timeout: 10_000,
    });
    await page.click("ui-dialog.gp-propose button[data-dialog-trigger]");
    await page.waitForTimeout(400);
    await page.fill("#gp-propose-title", "Proposed from the browser");
    await page.click("ui-dialog.gp-propose form button[type='submit']");
    await page.waitForTimeout(4000);
    const detailHash = await hash();
    check("proposing opens the new Change Request", detailHash.startsWith("#/detail/"));
    const prId = detailHash.slice("#/detail/".length);

    const pulls = await fetch(`${upstream}/core/hub/pulls`).then(async (response) =>
      Schema.decodeUnknownSync(Contract.HubPullPage)(await response.json()),
    );
    check("the hub lists it, judged under the genesis", pulls.enabled === true);
    check(
      "and it names the browser's proposal",
      pulls.items.some((entry) => entry.id === prId && entry.title === "Proposed from the browser"),
    );

    await page.waitForSelector(".gp-review .gp-btn-quiet", { timeout: 10_000 });
    await page.click(".gp-review .gp-btn-quiet");
    await page.waitForTimeout(2500);
    const reviewed = await fetch(`${upstream}/core/hub/pulls/${prId}`).then(async (response) =>
      Schema.decodeUnknownSync(Contract.HubPullDetail)(await response.json()),
    );
    check(
      "the approval landed as a signed review event",
      reviewed.reviews.length === 1 && reviewed.reviews[0]?.decision === "approve",
    );

    // --- the merge, as one hub transition ----------------------------------
    // The button waits for the server's own mergeability, and the merged
    // state the screen shows afterwards is the projection re-read — the
    // endpoint advanced the base and appended pr.merged as one judged batch.
    await page.waitForSelector('.gp-merge-btn[data-state="ready"]', { timeout: 10_000 });
    await page.click(".gp-merge-btn");
    await page.waitForTimeout(3000);
    const settledPull = await fetch(`${upstream}/core/hub/pulls/${prId}`).then(async (response) =>
      Schema.decodeUnknownSync(Contract.HubPullDetail)(await response.json()),
    );
    check("the merge settles the pull request in the hub", settledPull.state === "merged");
    const refsAfterMerge = await fetch(`${upstream}/core/refs`).then(async (response) =>
      Schema.decodeUnknownSync(Contract.RefsResponse)(await response.json()),
    );
    check(
      "and the base advanced to exactly the approved head",
      settledPull.mergeCommit !== null &&
        settledPull.mergeCommit === reviewed.head &&
        refsAfterMerge.refs.find((ref) => ref.name === "refs/heads/main")?.oid ===
          settledPull.mergeCommit,
    );
    check(
      "the screen shows it merged only after re-reading the projection",
      ((await page.textContent(".gp-detail-eyebrow .gp-status")) ?? "").trim() === "Merged",
    );

    // --- branch rules, published from the Settings form --------------------
    await page.goto(`${origin}/#/settings`, { waitUntil: "networkidle" });
    await page.waitForSelector('[data-card="policy"] form', { timeout: 15_000 });
    await page.fill("#gp-policy-approvals", "1");
    await page.click('[data-card="policy"] button[type="submit"]');
    await page.waitForTimeout(2000);
    const policy = await fetch(`${upstream}/core/policy`).then(async (response) =>
      Schema.decodeUnknownSync(Contract.PolicyAnswer)(await response.json()),
    );
    check("the published policy is what a push now meets", policy.rules.requiredApprovals === 1);

    await shot(page, "local-mode");
  } finally {
    await page.close();
    front.close();
    await host.close();
    await rm(root, { recursive: true, force: true });
  }
};

// --- the deployment artifact --------------------------------------------
// `worker.ts` binds `dist/ui` as the deployed Worker's static assets, so the
// build output is a deployment contract, not just this suite's fixture: the
// entry page, the two entry bundles it references, and the cache rules the
// asset layer applies. A missing one of these is a deploy that would publish
// the API with no UI behind it.
for (const artifact of ["index.html", "main.js", "main.css", "_headers"]) {
  const present = await stat(join(dist, artifact)).then(
    () => true,
    () => false,
  );
  check(`the deployable asset directory carries ${artifact}`, present);
}
check(
  "the cache rules pin the entry page to revalidation",
  await readFile(join(dist, "_headers"), "utf8").then(
    (rules) => rules.includes("/index.html") && rules.includes("must-revalidate"),
    () => false,
  ),
);

const offline = await serve(false, 8131);
const online = await serve(true, 8132);
const browser = await chromium.launch(
  executable === undefined ? {} : { executablePath: executable },
);

try {
  if (process.env["GP_VERIFY_ONLY_LOCAL"] !== "1") {
    await render(browser, "http://localhost:8131");
    await interact(browser, "http://localhost:8131");
    await live(browser, "http://localhost:8132");
  }
  await localMode(browser);
  // Both entry points, held to the same property; see `oneOrigin`.
  await oneOrigin({
    label: "dev:ui",
    argv: () => [join(here, "build.ts"), "--serve"],
    ready: "ui:   http",
    port: 8133,
    browser,
  });
  await oneOrigin({
    label: "serve --ui",
    argv: (root, port) => [
      join(here, "..", "cli", "bin.ts"),
      "serve",
      "--root",
      root,
      "--port",
      String(port),
      "--ui",
    ],
    ready: "browser UI on",
    port: 8134,
    browser,
  });
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
