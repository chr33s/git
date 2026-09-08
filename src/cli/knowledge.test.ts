/**
 * `git+ knowledge check` as an operator and a CI job actually meet it: a real
 * process, a real repository under a root, and an exit code something is
 * going to be chained onto.
 *
 * P-02 of docs/context-pack.knowledge.md §16. The behaviour under test is the
 * contract in §12.3 — a missing bundle is a successful no-op, warnings do not
 * fail the default gate, `--strict` fails on changed dependencies and expired
 * deadlines, and `--json` prints exactly one parseable report on stdout with
 * incidental messages kept off it.
 */
import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { promisify } from "node:util";
import { afterEach, beforeEach, describe, it } from "@effect/vitest";

import { Effect, Layer } from "effect";

import { stores as nodeStores } from "../git/Node.ts";
import * as GitRepository from "../git/Repository.ts";
import { qualify } from "../git/Oid.ts";
import type { Oid } from "../git/Store.ts";

const execFileAsync = promisify(execFile);
const entry = path.join(import.meta.dirname, "bin.ts");

interface Ran {
  readonly code: number;
  readonly stdout: string;
  readonly stderr: string;
  /** Both streams, for assertions about a diagnostic wherever it was logged. */
  readonly output: string;
}

const run = (args: ReadonlyArray<string>): Promise<Ran> =>
  execFileAsync(process.execPath, [entry, ...args], { encoding: "utf8" }).then(
    (result) => ({
      code: 0,
      stdout: result.stdout,
      stderr: result.stderr,
      output: `${result.stdout}${result.stderr}`,
    }),
    (error: { code?: number; stdout?: string; stderr?: string }) => ({
      code: error.code ?? 1,
      stdout: error.stdout ?? "",
      stderr: error.stderr ?? "",
      output: `${error.stdout ?? ""}${error.stderr ?? ""}`,
    }),
  );

const inRepository = <A, E>(
  directory: string,
  effect: Effect.Effect<A, E, GitRepository.Repository>,
) =>
  Effect.runPromise(
    effect.pipe(
      Effect.provide(
        GitRepository.layer.pipe(
          Layer.provide(GitRepository.hooksNoop),
          Layer.provide(nodeStores(directory)),
        ),
      ),
    ),
  );

describe("cli knowledge check", () => {
  let root = "";
  let project = "";

  beforeEach(async () => {
    root = await fs.mkdtemp(path.join(os.tmpdir(), "cli-knowledge-"));
    project = path.join(root, "project");
    await run(["init", "--root", root, "project"]);
  });

  afterEach(async () => {
    await fs.rm(root, { recursive: true, force: true });
  });

  /** Commit these files on `main`, and hand back the blob oids written. */
  const commit = (files: Record<string, string>) =>
    inRepository(
      project,
      Effect.gen(function* () {
        const repository = yield* GitRepository.Repository;
        const written: Record<string, Oid> = {};
        const entries: Array<{ path: string; oid: Oid; mode: string }> = [];
        for (const [name, contents] of Object.entries(files)) {
          const oid = yield* repository.writeBlob(new TextEncoder().encode(contents));
          written[name] = oid;
          entries.push({ path: name, oid, mode: "100644" });
        }
        const tree = yield* repository.writePaths(entries);
        const head = yield* repository.readRef("refs/heads/main");
        const made = yield* repository.commitTree({
          tree,
          parents: head === null ? [] : [head],
          message: "files\n",
          author: { name: "R", email: "r@example.com", at: new Date(1_700_000_000_000), offset: 0 },
        });
        yield* repository.setRef({ name: "refs/heads/main", to: made, expected: head });
        return written;
      }),
    );

  const check = (args: ReadonlyArray<string>) =>
    run(["knowledge", "check", "--root", root, "--repo", "project", "--ref", "main", ...args]);

  const conceptFor = (input: { evidence?: string; staleAfter?: string; status?: string }) =>
    [
      "---",
      "type: Gotcha",
      "title: Worker auth needs the production fixture",
      "description: Use the production policy fixture.",
      ...(input.status === undefined ? [] : [`status: ${input.status}`]),
      ...(input.staleAfter === undefined ? [] : [`stale_after: "${input.staleAfter}"`]),
      ...(input.evidence === undefined ? [] : ["gitplus:", "  evidence:", input.evidence]),
      "---",
      "Prose.",
      "",
    ].join("\n");

  it.effect("a missing bundle is a successful no-op, not a claim that knowledge exists", () =>
    Effect.promise(async () => {
      await commit({ "src/a.ts": "export const a = 1\n" });
      const ran = await check(["--json"]);
      assert.equal(ran.code, 0);
      const report = JSON.parse(ran.stdout);
      assert.equal(report.bundleAbsent, true);
      assert.deepEqual(report.concepts, []);
      // Strict does not invent a failure where there is nothing to check.
      assert.equal((await check(["--strict"])).code, 0);
    }),
  );

  it.effect("prints one parseable report, with the view and profile it describes", () =>
    Effect.promise(async () => {
      await commit({ ".gitplus/knowledge/gotchas/worker-auth.md": conceptFor({}) });
      const ran = await check(["--json"]);
      assert.equal(ran.code, 0);

      const report = JSON.parse(ran.stdout);
      assert.equal(report.version, 1);
      assert.equal(report.bundle, ".gitplus/knowledge");
      assert.match(report.view.tree, /^sha1:[0-9a-f]{40}$/);
      assert.equal(report.complete, true);
      assert.equal(report.concepts.length, 1);
      assert.equal(report.concepts[0].id, "gotchas/worker-auth");
      assert.equal(report.concepts[0].structure.state, "valid");
      assert.equal(report.concepts[0].temporal.state, "no-deadline");
      assert.match(report.profile.okfRevision, /^[0-9a-f]{40}$/);
      // The parse result itself is this module's input, not the response.
      assert.equal("concept" in report.concepts[0], false);
    }),
  );

  it.effect("a named Concept that does not exist is an error, not an empty report", () =>
    Effect.promise(async () => {
      await commit({ ".gitplus/knowledge/gotchas/worker-auth.md": conceptFor({}) });
      const found = await check(["gotchas/worker-auth"]);
      assert.equal(found.code, 0);
      assert.equal(
        JSON.parse((await check(["--json", "gotchas/worker-auth"])).stdout).concepts.length,
        1,
      );

      // By repository-relative path too, which is what a shell completes.
      assert.equal((await check([".gitplus/knowledge/gotchas/worker-auth.md"])).code, 0);

      const missing = await check(["gotchas/nope"]);
      assert.equal(missing.code, 1);
      assert.match(missing.output, /no Concept 'gotchas\/nope'/);
    }),
  );

  it.effect("a malformed Concept fails the default gate and says which one", () =>
    Effect.promise(async () => {
      await commit({ ".gitplus/knowledge/gotchas/bad.md": "---\ntype: A\ntype: B\n---\n" });
      const ran = await check([]);
      assert.equal(ran.code, 1);
      assert.match(ran.output, /structure invalid/);
      assert.match(ran.output, /duplicate key 'type'/);
    }),
  );

  it.effect("a changed dependency warns by default and fails under --strict", () =>
    Effect.promise(async () => {
      const written = await commit({ "tests/auth.test.ts": "the current bytes\n" });
      // The Concept cites bytes the tree no longer holds at that path.
      const stale = qualify(written["tests/auth.test.ts"]!).replace(/[0-9a-f]{4}$/, "0000");
      await commit({
        "tests/auth.test.ts": "the current bytes\n",
        ".gitplus/knowledge/gotchas/worker-auth.md": conceptFor({
          evidence: [
            "    - kind: blob",
            "      path: tests/auth.test.ts",
            `      blob: ${stale}`,
          ].join("\n"),
        }),
      });

      const relaxed = await check([]);
      assert.equal(relaxed.code, 0, relaxed.output);
      assert.match(relaxed.output, /evidence {2}changed/);

      const strict = await check(["--strict"]);
      assert.equal(strict.code, 1);
      assert.match(strict.output, /tests\/auth\.test\.ts is changed/);
    }),
  );

  it.effect("an expired deadline warns by default and fails under --strict", () =>
    Effect.promise(async () => {
      await commit({
        ".gitplus/knowledge/gotchas/worker-auth.md": conceptFor({
          staleAfter: "2020-01-01T00:00:00Z",
        }),
      });
      const relaxed = await check([]);
      assert.equal(relaxed.code, 0);
      assert.match(relaxed.output, /temporal stale/);
      // Excluded from automatic recall even where the gate passes: those are
      // two different questions (§7).
      assert.match(relaxed.output, /recall {4}excluded \(temporal-stale\)/);
      assert.equal((await check(["--strict"])).code, 1);
    }),
  );

  it.effect("a date-only deadline is an error rather than a guessed midnight", () =>
    Effect.promise(async () => {
      await commit({
        ".gitplus/knowledge/gotchas/worker-auth.md": conceptFor({ staleAfter: "2026-12-31" }),
      });
      const ran = await check([]);
      assert.equal(ran.code, 1);
      assert.match(ran.output, /temporal invalid/);
    }),
  );

  it.effect("--at pins the evaluation clock, and refuses one without an offset", () =>
    Effect.promise(async () => {
      await commit({
        ".gitplus/knowledge/gotchas/worker-auth.md": conceptFor({
          staleAfter: "2026-06-01T00:00:00Z",
        }),
      });
      const before = await check(["--at", "2026-05-01T00:00:00Z", "--strict"]);
      assert.equal(before.code, 0);
      const after = await check(["--at", "2026-07-01T00:00:00Z", "--strict"]);
      assert.equal(after.code, 1);

      const ambiguous = await check(["--at", "2026-07-01"]);
      assert.equal(ambiguous.code, 1);
      assert.match(ambiguous.output, /explicit UTC offset/);
    }),
  );

  it.effect("refuses a bundle that escapes the view", () =>
    Effect.promise(async () => {
      await commit({ ".gitplus/knowledge/gotchas/worker-auth.md": conceptFor({}) });
      for (const bundle of ["/etc", "../elsewhere", ".gitplus/../../out"]) {
        const ran = await check(["--bundle", bundle]);
        assert.equal(ran.code, 1, bundle);
        assert.match(ran.output, /repository-relative path inside the view/);
      }
    }),
  );

  it.effect("an alternate bundle root names Concept ids relative to itself", () =>
    Effect.promise(async () => {
      await commit({ "docs/knowledge/gotchas/worker-auth.md": conceptFor({}) });
      const ran = await check(["--bundle", "docs/knowledge", "--json"]);
      assert.equal(ran.code, 0);
      const report = JSON.parse(ran.stdout);
      assert.equal(report.bundle, "docs/knowledge");
      assert.equal(report.concepts[0].id, "gotchas/worker-auth");
    }),
  );

  it.effect("without a genesis it checks portable structure and judges no provenance", () =>
    Effect.promise(async () => {
      await commit({
        ".gitplus/knowledge/gotchas/worker-auth.md": [
          "---",
          "type: Gotcha",
          "description: Cites a record this repository cannot judge.",
          "gitplus:",
          "  cites:",
          "    - record: sha1:1111111111111111111111111111111111111111",
          "---",
          "Prose.",
          "",
        ].join("\n"),
      });
      const ran = await check(["--json"]);
      // §18: a repository without hub identity can still check portable source
      // Concepts, and must not fabricate signed provenance for them.
      assert.equal(ran.code, 0);
      const report = JSON.parse(ran.stdout);
      assert.equal(report.concepts[0].citations[0].state, "unavailable");
      assert.equal(report.concepts[0].eligibility.recall, false);
      assert.equal(
        report.diagnostics.some(
          (entry: { code: string }) => entry.code === "knowledge.provenance.unjudged",
        ),
        true,
      );
    }),
  );
});
