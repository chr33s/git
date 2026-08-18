/**
 * The browser client in a real browser.
 *
 * `Opfs.test.ts` proves the adapter against a faked directory handle; this
 * proves the fake told the truth. esbuild bundles `adapters/Opfs.ts`,
 * `client/Client.ts` and the domain into an IIFE, Playwright loads it in
 * Chromium on the node host's own origin (localhost is a secure context, so
 * OPFS is live and fetches are same-origin), and the scenario runs against
 * the *actual* Origin Private File System.
 *
 * Since `Pack.ts` moved onto the platform-neutral `Inflate`/`Sha1`, the
 * bundle carries no `node:` imports at all — and the scenario ends with the
 * headline: a real smart-HTTP clone executed inside the browser.
 *
 * Skipped when Chromium is not installed — but not when it is installed and
 * fails to launch; see `hasChromium`.
 */
import assert from "node:assert/strict";
import { existsSync } from "node:fs";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { describe, it } from "@effect/vitest";

import { build } from "esbuild";
import { Effect, Layer } from "effect";
import { chromium } from "playwright";

import { stores as nodeStores } from "../git/Node.ts";
import * as GitRepository from "../git/Repository.ts";
import { Repository } from "../git/Repository.ts";
import { serve } from "../host/Node.ts";

const projectRoot = path.join(import.meta.dirname, "..", "..");

const author = {
  name: "Alice",
  email: "alice@example.com",
  at: new Date(1_700_000_000_000),
  offset: 0,
};

const scenarioEntry = `
  import { Effect, Stream } from "effect";
  import * as Opfs from "./src/adapters/Opfs.ts";
  import * as Client from "./src/client/Client.ts";
  import { fetchRepository } from "./src/client/Fetch.ts";
  import { Repository } from "./src/git/Repository.ts";
  import { ObjectStore, RefStore } from "./src/git/Store.ts";

  const author = ${JSON.stringify({ ...author, at: author.at.toISOString() })};

  (globalThis /* window */).scenario = async (repoName, head) => {
    // Real OPFS: the origin's private filesystem, a fresh directory per run.
    const origin = await navigator.storage.getDirectory();
    const root = await origin.getDirectoryHandle(
      "repo-" + Math.random().toString(36).slice(2),
      { create: true },
    );
    const stores = Opfs.stores(root);

    const localMessages = await Effect.runPromise(
      Effect.gen(function* () {
        const repository = yield* Repository;
        const blob = yield* repository.writeBlob(new TextEncoder().encode("in the browser\\n"));
        const tree = yield* repository.writeTree([{ mode: "100644", name: "b.txt", oid: blob }]);
        yield* repository.commit({
          branch: "main",
          tree,
          message: "first from OPFS",
          author: { ...author, at: new Date(author.at) },
        });
        yield* repository.commit({
          branch: "main",
          tree,
          message: "second from OPFS",
          author: { ...author, at: new Date(author.at) },
        });
        return [];
      }).pipe(Effect.provide(Client.local(stores))),
    );

    // A second, independent stores instance over the same handle: what was
    // written is really in OPFS, not in some layer's memory.
    const reread = await Effect.runPromise(
      Effect.gen(function* () {
        const repository = yield* Repository;
        const main = yield* repository.resolve("refs/heads/main");
        const commits = yield* Stream.runCollect(repository.log(main, { limit: 10 }));
        return commits.map((commit) => commit.message);
      }).pipe(Effect.provide(Client.local(Opfs.stores(root)))),
    );

    // The derived JSON client, same-origin against the node host.
    const api = await Effect.runPromise(
      Effect.gen(function* () {
        const client = yield* Client.remote(location.origin);
        const refs = yield* client.repo.refs({ params: { repo: repoName } });
        const log = yield* client.repo.log({ params: { repo: repoName, oid: head } });
        return { refs: refs.refs, messages: log.commits.map((commit) => commit.message) };
      }).pipe(Effect.scoped),
    );

    // The headline: a smart-HTTP clone, entirely inside the browser — the
    // pull-based inflate decoding the pack into OPFS.
    const cloneRoot = await origin.getDirectoryHandle(
      "clone-" + Math.random().toString(36).slice(2),
      { create: true },
    );
    const cloneStores = Opfs.stores(cloneRoot);
    const clonedMessages = await Effect.runPromise(
      Effect.gen(function* () {
        const target = { objects: yield* ObjectStore, refs: yield* RefStore };
        const cloned = yield* fetchRepository({
          url: location.origin + "/" + repoName,
          stores: target,
        });
        return cloned.defaultBranch;
      }).pipe(Effect.provide(cloneStores)),
    ).then(async (defaultBranch) => {
      const log = await Effect.runPromise(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const main = yield* repository.resolve("refs/heads/" + defaultBranch);
          const commits = yield* Stream.runCollect(repository.log(main, { limit: 10 }));
          return commits.map((commit) => commit.message);
        }).pipe(Effect.provide(Client.local(Opfs.stores(cloneRoot)))),
      );
      return log;
    });

    return { localMessages, reread, api, clonedMessages };
  };
`;

/** What `scenario` above resolves with, once the browser has run it. */
interface ScenarioResult {
  readonly reread: ReadonlyArray<string>;
  readonly api: {
    readonly refs: ReadonlyArray<{ readonly name: string; readonly oid: string }>;
    readonly messages: ReadonlyArray<string>;
  };
  readonly clonedMessages: ReadonlyArray<string>;
}

/**
 * Whether Chromium is *installed* — deliberately not whether it launches.
 *
 * Resolved once, at collection time, so the skip is a fact and not a branch.
 * The presence of the executable is the question, because the two ways a
 * browser can be unavailable deserve opposite answers: a machine that never
 * downloaded it should skip, while a browser that is there and refuses to start
 * is a result worth seeing.
 *
 * A trial `launch().catch(() => false)` could not tell them apart and answered
 * "not available" to both. Playwright gives `launch()` a 30s timeout of its own,
 * so on a machine loaded enough to exceed it this suite reported *skipped* and
 * the run stayed green having quietly dropped it — the one probe in this
 * repository that could lose coverage without saying so. It also cost a browser
 * launch and teardown at import time, for an answer `existsSync` already had.
 *
 * A launch failure now reaches the test body, where it fails with Playwright's
 * own diagnostics — which say to run `npx playwright install`, the thing the
 * silent skip left the reader to work out.
 *
 * One asymmetry to know about: this checks the headed Chrome that
 * `executablePath()` names, while `launch()` defaults to the headless shell
 * beside it. `playwright install chromium` places both, so a partial install
 * reads as "installed, will not launch" and fails — which is the outcome that
 * partial install deserves.
 */
const hasChromium = existsSync(chromium.executablePath());

describe.skipIf(!hasChromium)("Client in real Chromium", () => {
  it("runs OPFS stores and the derived client inside the browser", async () => {
    const browser = await chromium.launch();

    const root = await fs.mkdtemp(path.join(os.tmpdir(), "browser-"));
    const server = await serve({ root, allowAnonymousWrites: true });
    try {
      const head = await Effect.runPromise(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const blob = yield* repository.writeBlob(new TextEncoder().encode("served\n"));
          const tree = yield* repository.writeTree([{ mode: "100644", name: "s.txt", oid: blob }]);
          return yield* repository.commit({ branch: "main", tree, message: "served", author });
        }).pipe(
          Effect.provide(
            GitRepository.layer.pipe(
              Layer.provide(GitRepository.hooksNoop),
              Layer.provide(nodeStores(path.join(root, "origin"))),
            ),
          ),
        ),
      );

      const bundle = await build({
        stdin: { contents: scenarioEntry, resolveDir: projectRoot, loader: "ts" },
        bundle: true,
        format: "iife",
        platform: "browser",
        target: "es2022",
        write: false,
      });

      const page = await browser.newPage();
      // Any response will do: the point is the origin — localhost is a secure
      // context, so OPFS exists and every fetch is same-origin.
      await page.goto(server.url);
      await page.addScriptTag({ content: bundle.outputFiles[0]!.text });

      // SAFETY: the script tag added above ran `scenarioEntry`, which installs
      // `scenario` on the page's global and resolves with exactly the members
      // `ScenarioResult` names.
      const result = await page.evaluate(
        ([repo, oid]) =>
          (
            globalThis as typeof globalThis & {
              scenario(repo: string, oid: string): Promise<ScenarioResult>;
            }
          ).scenario(repo, oid),
        ["origin", head] as const,
      );

      assert.deepEqual(result.reread, ["second from OPFS", "first from OPFS"]);
      assert.deepEqual(result.api.refs, [{ name: "refs/heads/main", oid: head }]);
      assert.deepEqual(result.api.messages, ["served"]);
      assert.deepEqual(result.clonedMessages, ["served"]);
    } finally {
      await browser.close();
      await server.close();
      await fs.rm(root, { recursive: true, force: true });
    }
  });
});
