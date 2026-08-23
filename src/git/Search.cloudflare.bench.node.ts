/** Local workerd Durable Object restart benchmark for persisted search. */
import { performance } from "node:perf_hooks";

import { createTestHarness } from "wrangler";

const harness = createTestHarness({ workers: [{ configPath: "./wrangler.search.test.json" }] });
const repo = `search-benchmark-${crypto.randomUUID()}`;
const author = {
  name: "benchmark",
  email: "benchmark@example.com",
  at: new Date(0).toISOString(),
  offset: 0,
};

const elapsed = async <A>(work: () => Promise<A>) => {
  const start = performance.now();
  await work();
  return performance.now() - start;
};

try {
  await harness.listen();
  const files = Array.from({ length: 100 }, (_, index) => ({
    path: `src/file-${index}.txt`,
    content: `repository benchmark line ${index}\n`,
  }));
  const created = await harness.fetch(`/${repo}/commit`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ author, branch: "main", message: "benchmark", files }),
  });
  if (!created.ok) throw new Error(`commit failed: ${created.status}`);

  const grep = () =>
    harness.fetch(`/${repo}/grep`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        pattern: "__search_benchmark_miss__",
        fixed: true,
        ignore_case: true,
        max_matches: 2_000,
      }),
    });
  const cold = await elapsed(grep);
  await harness.getWorker().evictDurableObject("GIT_REPO", { name: repo });
  const restart = await elapsed(grep);
  console.log(
    JSON.stringify(
      { cold_index_build_ms: cold, durable_object_restart_search_ms: restart },
      null,
      2,
    ),
  );
} finally {
  await harness.close();
}
