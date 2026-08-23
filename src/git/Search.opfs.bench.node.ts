/** Real Chromium/OPFS restart benchmark for the persisted search index. */
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";

import { build } from "esbuild";
import { chromium } from "playwright";

import { serve } from "../host/Node.ts";

const projectRoot = path.join(import.meta.dirname, "..", "..");
const scenario = `
  import { Effect, Layer, ManagedRuntime } from "effect";
  import * as Opfs from "./src/adapters/Opfs.ts";
  import * as Client from "./src/client/Client.ts";
  import { Repository } from "./src/git/Repository.ts";

  const author = { name: "benchmark", email: "benchmark@example.com", at: new Date(0), offset: 0 };
  const run = async (layer, work) => ManagedRuntime.make(layer).runPromise(work);
  globalThis.searchBenchmark = async () => {
    const origin = await navigator.storage.getDirectory();
    const root = await origin.getDirectoryHandle("search-bench-" + crypto.randomUUID(), { create: true });
    const layer = () => {
      const stores = Opfs.stores(root);
      return Layer.mergeAll(Client.local(stores, undefined, Opfs.searchIndex(root)), stores);
    };
    await run(layer(), Effect.gen(function*() {
      const repository = yield* Repository;
      const changes = Array.from({ length: 100 }, (_, index) => ({
        path: "src/file-" + index + ".txt",
        content: new TextEncoder().encode("repository benchmark line " + index + "\\n"),
      }));
      const tree = yield* repository.writeFiles({ changes });
      yield* repository.commit({ branch: "main", tree, message: "benchmark", author });
    }));
    const search = () => Effect.gen(function*() {
      const repository = yield* Repository;
      return yield* repository.search({ ref: "refs/heads/main", pattern: "__search_benchmark_miss__", fixed: true, ignoreCase: true });
    });
    const start = performance.now();
    await run(layer(), search());
    const cold = performance.now() - start;
    const restart = performance.now();
    await run(layer(), search());
    return { cold_index_build_ms: cold, opfs_restart_search_ms: performance.now() - restart };
  };
`;

const root = await fs.mkdtemp(path.join(os.tmpdir(), "opfs-search-bench-"));
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
  await page.addScriptTag({ content: bundle.outputFiles[0]!.text });
  // SAFETY: the bundle above installs this exact function before evaluation.
  const result = await page.evaluate(() =>
    (
      globalThis as typeof globalThis & {
        searchBenchmark(): Promise<{ cold_index_build_ms: number; opfs_restart_search_ms: number }>;
      }
    ).searchBenchmark(),
  );
  console.log(JSON.stringify(result, null, 2));
} finally {
  await browser.close();
  await server.close();
  await fs.rm(root, { force: true, recursive: true });
}
