/**
 * Running the storage contract inside the Workers runtime.
 *
 * `createTestHarness` starts the Worker as a real local server and drives it
 * over HTTP, which is what makes it an *integration* harness — the test process
 * is outside workerd and cannot reach `state.storage.sql`. The contract suite
 * therefore has to run on the inside and report back.
 *
 * That is what this module is: a `Runner` (the same interface `node:test`
 * satisfies) that collects results instead of talking to a reporter, so
 * `Store.contract.ts` runs unmodified against the Cloudflare backend and the
 * outcome crosses the boundary as JSON.
 */
import type { Runner } from "./Store.contract.ts";

export interface ConformanceResult {
  readonly name: string;
  readonly ok: boolean;
  readonly error?: string;
}

export interface ConformanceReport {
  readonly failed: number;
  readonly passed: number;
  readonly results: ReadonlyArray<ConformanceResult>;
}

/** What `collector` hands back: the runner to register with, and the run. */
export interface Collector {
  readonly report: () => Promise<ConformanceReport>;
  readonly runner: Runner;
}

/**
 * A `Runner` that records tests rather than reporting them.
 *
 * `describe`/`it` are registration calls, so the suite is collected
 * synchronously and run afterwards by `report()`, one test at a time.
 */
export const collector = (): Collector => {
  const cases: Array<{ body: () => Promise<void> | void; name: string }> = [];
  let prefix = "";

  const runner: Runner = {
    describe: (name, body) => {
      const previous = prefix;
      prefix = prefix === "" ? name : `${prefix} > ${name}`;
      body();
      prefix = previous;
    },
    it: (name, body) => {
      cases.push({ body, name: prefix === "" ? name : `${prefix} > ${name}` });
    },
  };

  const report = async (): Promise<ConformanceReport> => {
    const results: ConformanceResult[] = [];

    for (const testCase of cases) {
      try {
        await testCase.body();
        results.push({ name: testCase.name, ok: true });
      } catch (error) {
        results.push({
          name: testCase.name,
          ok: false,
          error: error instanceof Error ? (error.stack ?? error.message) : String(error),
        });
      }
    }

    return {
      failed: results.filter((result) => !result.ok).length,
      passed: results.filter((result) => result.ok).length,
      results,
    };
  };

  return { report, runner };
};
