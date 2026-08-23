/**
 * Search-index benchmark over a real Git object database.
 *
 * Usage: `npm run bench:search -- .git repository`
 * The first argument is a bare repository (or a worktree's `.git`); the
 * second is the literal query used for warm measurements.
 */
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { performance } from "node:perf_hooks";

import { Effect, Layer, ManagedRuntime } from "effect";

import { stores } from "./Node.ts";
import { isGitlink } from "./Format.ts";
import * as GitRepository from "./Repository.ts";
import { treeAt } from "./Repository.ts";
import { file as searchIndexFile } from "./Search.node.ts";
import { BlobIndex, memory as searchMemory, SearchIndex } from "./Search.ts";

const [directory = ".git", pattern = "repository"] = process.argv.slice(2);

const dependencies = Layer.mergeAll(GitRepository.hooksNoop, stores(directory), searchMemory);
const layer = GitRepository.layerWithSearchIndex.pipe(Layer.provideMerge(dependencies));
const runtime = ManagedRuntime.make(layer);

const search = (runtime: ManagedRuntime.ManagedRuntime<GitRepository.Repository, never>) =>
  runtime.runPromise(
    Effect.gen(function* () {
      const repository = yield* GitRepository.Repository;
      return yield* repository.search({
        ref: "HEAD",
        pattern: "__git_plus_search_benchmark_miss__",
        fixed: true,
        ignoreCase: true,
        maxMatches: 2_000,
      });
    }),
  );

const elapsed = async <A>(work: () => Promise<A>) => {
  const start = performance.now();
  const value = await work();
  return { milliseconds: performance.now() - start, value };
};

try {
  // A literal unlikely to exist makes the initial pass index every searchable
  // blob reachable from HEAD instead of stopping at the result cap.
  const cold = await elapsed(() => search(runtime));

  const warm: number[] = [];
  let matches = 0;
  for (let attempt = 0; attempt < 10; attempt += 1) {
    const measured = await elapsed(() =>
      runtime.runPromise(
        Effect.gen(function* () {
          const repository = yield* GitRepository.Repository;
          return yield* repository.search({
            ref: "HEAD",
            pattern,
            fixed: true,
            ignoreCase: true,
            maxMatches: 2_000,
          });
        }),
      ),
    );
    warm.push(measured.milliseconds);
    matches = measured.value.matches.length;
  }
  warm.sort((left, right) => left - right);

  const indexData = await runtime.runPromise(
    Effect.gen(function* () {
      const repository = yield* GitRepository.Repository;
      const index = yield* SearchIndex;
      const head = yield* repository.resolve("HEAD");
      const snapshot = yield* Effect.promise(() => index.index.persisted());
      if (snapshot === null) {
        return { snapshot: null, files: 0, candidates: 0, stats: index.index.stats() };
      }
      if (head === null) return { snapshot, files: 0, candidates: 0, stats: index.index.stats() };
      const files = yield* repository.listFiles(yield* treeAt(repository, head));
      const candidates = yield* index.candidates(pattern, true);
      return {
        snapshot,
        files: files.filter((file) => !isGitlink(file.mode)).length,
        candidates: candidates?.size ?? files.length,
        stats: index.index.stats(),
      };
    }),
  );
  if (indexData.snapshot === null) throw new Error("index was not persistable");
  const snapshot = indexData.snapshot;
  // Restore timing excludes filesystem I/O: hosts differ there, while
  // manifest validation and posting reconstruction are portable.
  const validation = await elapsed(async () =>
    BlobIndex.restorePersisted(
      snapshot.manifest,
      new Map(snapshot.chunks.map((chunk) => [chunk.name, chunk.bytes])),
    ),
  );

  const cacheDirectory = await fs.mkdtemp(path.join(os.tmpdir(), "git-search-bench-"));
  try {
    const persistentDependencies = Layer.mergeAll(
      GitRepository.hooksNoop,
      stores(directory),
      searchIndexFile(cacheDirectory),
    );
    const persistentLayer = GitRepository.layerWithSearchIndex.pipe(
      Layer.provideMerge(persistentDependencies),
    );
    const first = ManagedRuntime.make(persistentLayer);
    const persistedBuild = await elapsed(() => search(first));
    await first.dispose();
    const second = ManagedRuntime.make(persistentLayer);
    const persistedRestart = await elapsed(() => search(second));
    await second.dispose();

    console.log(
      JSON.stringify(
        {
          repository: directory,
          query: pattern,
          cold_index_build_ms: cold.milliseconds,
          warm_query_median_ms: warm[Math.floor(warm.length / 2)] ?? 0,
          warm_query_samples_ms: warm,
          matches,
          indexed_blobs: indexData.stats.blobs,
          indexed_bigrams: indexData.stats.bigrams,
          reachable_blobs: indexData.files,
          candidate_blobs: indexData.candidates,
          candidate_ratio: indexData.files === 0 ? 0 : indexData.candidates / indexData.files,
          persisted_bytes:
            snapshot.manifest.length +
            snapshot.chunks.reduce((total, chunk) => total + chunk.bytes.length, 0),
          persisted_chunks: snapshot.chunks.length,
          snapshot_restore_ms: validation.milliseconds,
          node_persistent_first_search_ms: persistedBuild.milliseconds,
          node_persistent_restart_search_ms: persistedRestart.milliseconds,
        },
        null,
        2,
      ),
    );
  } finally {
    await fs.rm(cacheDirectory, { force: true, recursive: true });
  }
} finally {
  await runtime.dispose();
}
