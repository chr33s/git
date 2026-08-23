import assert from "node:assert/strict";
import { existsSync } from "node:fs";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { describe, it } from "@effect/vitest";

import { Effect } from "effect";
import { build } from "esbuild";
import { chromium } from "playwright";

import { serve } from "../host/Node.ts";

const projectRoot = path.join(import.meta.dirname, "..", "..");
const hasChromium = existsSync(chromium.executablePath());

const scenario = `
  import { Effect, Layer, ManagedRuntime } from "effect";
  import * as Opfs from "./src/adapters/Opfs.ts";
  import * as Client from "./src/client/Client.ts";
  import { Repository } from "./src/git/Repository.ts";

  const author = { name: "browser", email: "browser@example.com", at: new Date(0), offset: 0 };
  const run = (root, work) => ManagedRuntime.make(Layer.mergeAll(Client.local(Opfs.stores(root), undefined, Opfs.searchIndex(root)), Opfs.stores(root))).runPromise(work);
  const grep = (root) => run(root, Effect.gen(function*() {
    const repository = yield* Repository;
    return yield* repository.search({ ref: "refs/heads/main", pattern: "needle", fixed: true, ignoreCase: true });
  }));
  globalThis.opfsSearchPersistence = async () => {
    const origin = await navigator.storage.getDirectory();
    const root = await origin.getDirectoryHandle("search-test-" + crypto.randomUUID(), { create: true });
    await run(root, Effect.gen(function*() {
      const repository = yield* Repository;
      const tree = yield* repository.writeFiles({ changes: [
        { path: "a.txt", content: new TextEncoder().encode("needle one\\n") },
        { path: "b.txt", content: new TextEncoder().encode("needle two\\n") },
      ] });
      yield* repository.commit({ branch: "main", tree, message: "search", author });
    }));
    const warm = await grep(root);
    const index = await root.getDirectoryHandle("search");
    const version = await index.getDirectoryHandle("index-v3");
    const manifest = await version.getFileHandle("manifest.json");
    const replace = async (text) => { const writable = await manifest.createWritable(); await writable.write(text); await writable.close(); };
    await replace("corrupt");
    const corrupt = await grep(root);
    const restored = await manifest.getFile();
    const value = JSON.parse(await restored.text());
    value.version = 1;
    await replace(JSON.stringify(value));
    const old = await grep(root);
    return { warm: warm.matches, corrupt: corrupt.matches, old: old.matches };
  };
`;

describe.skipIf(!hasChromium)("SearchIndex persistence in real OPFS", () => {
  it.effect("falls back to verifier answers for corrupt and old manifests", () =>
    Effect.promise(async () => {
      const root = await fs.mkdtemp(path.join(os.tmpdir(), "git-opfs-search-"));
      const server = await serve({ root, allowAnonymousWrites: true });
      const browser = await chromium.launch();
      try {
        const bundle = await build({
          stdin: { contents: scenario, resolveDir: projectRoot, loader: "ts" },
          bundle: true,
          format: "iife",
          platform: "browser",
          target: "es2022",
          write: false,
        });
        const page = await browser.newPage();
        await page.goto(server.url);
        const output = bundle.outputFiles[0];
        if (output === undefined) assert.fail("expected browser bundle");
        await page.addScriptTag({ content: output.text });
        // SAFETY: the bundled scenario installs this exact function before evaluation.
        const result = await page.evaluate(() =>
          (
            globalThis as typeof globalThis & {
              opfsSearchPersistence(): Promise<{ warm: unknown; corrupt: unknown; old: unknown }>;
            }
          ).opfsSearchPersistence(),
        );
        assert.deepEqual(result.corrupt, result.warm);
        assert.deepEqual(result.old, result.warm);
      } finally {
        await browser.close();
        await server.close();
        await fs.rm(root, { force: true, recursive: true });
      }
    }),
  );
});
