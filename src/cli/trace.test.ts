/**
 * The audit workflow as an operator runs it: record, then read one Invocation.
 *
 * The point of this suite is the seam between the two halves. `git+ context
 * for --session` writes the pre-call record and prints its OID; `git+ trace
 * record` writes the post-call one naming that OID; `git+ session show
 * --audit` prints one row. Nobody at the terminal has to know there were two
 * records, which is docs/telemetry.md §19.14 — and nothing joins them by time,
 * which is §3.
 */
import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { promisify } from "node:util";
import { afterEach, beforeEach, describe, it } from "@effect/vitest";

import { Effect } from "effect";

import { enableHub, opensshPrivateKey } from "../testing/Hub.ts";

const execFileAsync = promisify(execFile);
const entry = path.join(import.meta.dirname, "bin.ts");

const cli = async (args: ReadonlyArray<string>): Promise<string> => {
  const result = await execFileAsync(process.execPath, [entry, ...args], { encoding: "utf8" });
  return `${result.stdout}${result.stderr}`;
};

/** The same command, run from inside a checkout rather than pointed at one. */
const inside = async (cwd: string, args: ReadonlyArray<string>): Promise<string> => {
  const result = await execFileAsync(process.execPath, [entry, ...args], {
    encoding: "utf8",
    cwd,
  });
  return `${result.stdout}${result.stderr}`;
};

const failing = (args: ReadonlyArray<string>): Promise<string> =>
  cli(args).then(
    () => "",
    (error: { stdout?: string; stderr?: string }) => `${error.stdout ?? ""}${error.stderr ?? ""}`,
  );

const SESSION = "0192f000-0000-7000-8000-0000000000aa";

describe("cli trace", () => {
  let root = "";
  let project = "";
  let key = "";

  beforeEach(async () => {
    root = await fs.mkdtemp(path.join(os.tmpdir(), "cli-trace-"));
    project = path.join(root, "project");
    key = path.join(root, "agent");
    await fs.mkdir(path.join(project, "src"), { recursive: true });

    await cli(["init", "--root", project, ".git"]);
    const fixture = await enableHub(path.join(project, ".git"), [
      "repo.read",
      "source.push",
      "hub.trace",
      "hub.session",
    ]);
    await fs.writeFile(key, opensshPrivateKey(fixture.member, "agent@example.com"), {
      mode: 0o600,
    });

    await fs.writeFile(path.join(project, "AGENTS.md"), "Standing instructions.\n");
    await fs.writeFile(
      path.join(project, "src", "auth.ts"),
      "export const authorize = (policy: string) => policy !== ''\n",
    );
    await cli(["add", "--work", project, "."]);
    await cli(["commit", "--work", project, "--message", "first\n"]);
  });

  afterEach(async () => {
    await fs.rm(root, { recursive: true, force: true });
  });

  /** The pre-call half, as `context for --session` writes it. */
  const exposure = async (): Promise<string> => {
    const built: { readonly exposure: string } = JSON.parse(
      await cli([
        "context",
        "for",
        "--work",
        project,
        "--task",
        "authorize policy",
        "--json",
        "--session",
        SESSION,
        "--key",
        key,
      ]),
    );
    return built.exposure;
  };

  const write = async (name: string, value: Readonly<object>) => {
    const location = path.join(root, name);
    await fs.writeFile(location, JSON.stringify(value));
    return location;
  };

  const record = (event: string, extra: ReadonlyArray<string> = []) =>
    cli([
      "trace",
      "record",
      "--root",
      root,
      "--repo",
      path.join("project", ".git"),
      "--session",
      SESSION,
      "--key",
      key,
      "--event",
      event,
      ...extra,
    ]);

  const audit = async () =>
    JSON.parse(
      await cli([
        "session",
        "show",
        "--root",
        root,
        "--repo",
        path.join("project", ".git"),
        "--audit",
        "--json",
        SESSION,
      ]),
    );

  it.effect("joins a recorded invocation to the exposure it names", () =>
    Effect.promise(async () => {
      const exposed = await exposure();
      const event = await write("event.json", {
        type: "invocation-telemetry",
        exposure: exposed,
        capture: { transport: "otel", stage: "sdk-export" },
        operation: { name: "chat" },
        model: { provider: "anthropic", requested: "model-x", response: "model-x-20260815" },
        usage: { source: "provider", inputTokens: 90_000, outputTokens: 100 },
        outcome: { status: "ok" },
        response: { finishReasons: ["length"] },
        context: {
          effectiveInputLimitTokens: 180_000,
          effectiveInputLimitSource: "harness-config",
        },
      });

      const written = (await record(event)).trim();
      assert.match(written, /^sha1:[0-9a-f]{40}$/);

      const shown = await audit();
      // The session projection is still there, unchanged, with the audit
      // hanging off it: one command, two refs, one answer.
      assert.equal(shown.session, SESSION);
      assert.equal(shown.audit.invocations.length, 1);

      const row = shown.audit.invocations[0];
      assert.equal(row.id, written);
      assert.equal(row.context.exposure, exposed);
      assert.equal(row.context.verified, true);
      assert.equal(row.context.render, "verified");
      assert.equal(row.runtime.model.requested, "model-x");
      assert.equal(row.runtime.model.response, "model-x-20260815");
      // A length finish and a successful operation, still two facts (§7.3).
      assert.deepEqual(row.runtime.finishReasons, ["length"]);
      assert.equal(row.runtime.outcome.status, "ok");
      assert.equal(row.inputPressure, 0.5);
      // No health record yet, so nothing may claim the capture was complete.
      assert.equal(shown.audit.coverage, "unknown");
    }),
  );

  it.effect("normalizes a GenAI span through the same writer", () =>
    Effect.promise(async () => {
      const exposed = await exposure();
      const span = await write("span.json", {
        traceId: "4bf92f3577b34da6a3ce929d0e0e4736",
        spanId: "00f067aa0ba902b7",
        status: { code: "ok" },
        attributes: {
          "gen_ai.operation.name": "chat",
          "gen_ai.provider.name": "anthropic",
          "gen_ai.request.model": "model-x",
          "gen_ai.usage.input_tokens": 1200,
          "gen_ai.response.finish_reasons": ["stop"],
        },
      });

      await record(span, [
        "--otel",
        "--stage",
        "sdk-export",
        "--semconv-revision",
        "1.37.0",
        "--exposure",
        exposed,
      ]);

      const shown = await audit();
      const row = shown.audit.invocations[0];
      assert.equal(row.runtime.operation, "chat");
      assert.equal(row.runtime.usage.source, "provider");
      assert.equal(row.runtime.usage.inputTokens, 1200);
      // §7.1: only the requested model was on the span, so only it is recorded.
      assert.equal(row.runtime.model.requested, "model-x");
      assert.equal(row.runtime.model.response, undefined);
      assert.equal(row.capture.traceId, "4bf92f3577b34da6a3ce929d0e0e4736");
      assert.equal(row.capture.semconv.revision, "1.37.0");
      // Attempts were not instrumented, so there are none — not one (§6.2).
      assert.equal(row.runtime.attempts, null);
      // No effective limit on the span, so no ratio at all (§9).
      assert.equal(row.inputPressure, null);
    }),
  );

  it.effect("refuses to record a retrieval span as repository context", () =>
    Effect.promise(async () => {
      const span = await write("retrieval.json", {
        attributes: { "gen_ai.operation.name": "retrieve" },
      });
      const refused = await failing([
        "trace",
        "record",
        "--root",
        root,
        "--repo",
        path.join("project", ".git"),
        "--session",
        SESSION,
        "--key",
        key,
        "--event",
        span,
        "--otel",
      ]);
      // §7.6: a retrieval span is selector diagnostics. Context Exposure is
      // what answers whether anything reached the model.
      assert.match(refused, /Context Exposure is what records repository context/);
    }),
  );

  it.effect("weakens the coverage claim when the pipeline says it sampled", () =>
    Effect.promise(async () => {
      const clean = await write("health-clean.json", {
        type: "trace-health",
        source: "otel",
        stage: "sdk-export",
        sampling: "none",
        transformed: false,
        dropped: 0,
      });
      await record(clean);
      assert.equal((await audit()).audit.coverage, "complete");

      const sampled = await write("health-sampled.json", {
        type: "trace-health",
        source: "otel",
        stage: "local-collector",
        sampling: "parentbased_traceidratio",
        transformed: true,
        dropped: 12,
        reasons: ["collector sampling enabled"],
      });
      await record(sampled);
      // §12: signals sampled or transformed before ingestion must not be
      // presented as complete, however clean the other stage was.
      assert.equal((await audit()).audit.coverage, "degraded");
    }),
  );

  it.effect("will not let a harness bind a record to somebody else's session", () =>
    Effect.promise(async () => {
      const event = await write("forged.json", {
        type: "invocation-telemetry",
        exposure: null,
        capture: null,
        // Supplied by the caller, and overwritten by the recorder: the
        // envelope is what binds a record, so it is not the caller's to write.
        repo: "SHA256:somebody-else",
        session: "0192f000-0000-7000-8000-0000000000ff",
      });
      await record(event);

      const shown = await audit();
      assert.equal(shown.audit.invocations.length, 1);
      // It landed on the session the command was told to write, not the one
      // the file claimed.
      assert.equal(shown.audit.invocations[0].runtime.record.startsWith("sha1:"), true);

      const elsewhere = JSON.parse(
        await cli([
          "session",
          "show",
          "--root",
          root,
          "--repo",
          path.join("project", ".git"),
          "--audit",
          "--json",
          "0192f000-0000-7000-8000-0000000000ff",
        ]),
      );
      assert.equal(elsewhere.audit.invocations.length, 0);
    }),
  );

  it.effect("finds the repository a person is standing in", () =>
    Effect.promise(async () => {
      // §17: repo-scoped audit commands discover the current checkout, and
      // explicit selection stays available for bare and server use. Both of
      // these run with no `--root` and no `--repo` at all.
      const event = await write("discovered.json", {
        type: "invocation-telemetry",
        exposure: null,
        capture: null,
        operation: { name: "chat" },
      });
      const written = (
        await inside(project, [
          "trace",
          "record",
          "--session",
          SESSION,
          "--key",
          key,
          "--event",
          event,
        ])
      ).trim();
      assert.match(written, /^sha1:[0-9a-f]{40}$/);

      const shown = JSON.parse(
        await inside(project, ["session", "show", "--audit", "--json", SESSION]),
      );
      assert.equal(shown.audit.invocations.length, 1);
      assert.equal(shown.audit.invocations[0].id, written);

      // And the same run, rendered: one Invocation, without the reader having
      // to know it came from two signed records (§19.14).
      const rendered = await inside(project, ["session", "show", "--audit", SESSION]);
      assert.match(rendered, new RegExp(`Invocation ${written}`));
      assert.match(rendered, /Capture\n {2}coverage unknown|coverage unknown/);
    }),
  );

  it.effect("refuses a record whose join is not a Git record oid", () =>
    Effect.promise(async () => {
      const event = await write("bad-join.json", {
        type: "invocation-telemetry",
        // An OTel trace id: correlation metadata, and not durable identity (§3).
        exposure: "4bf92f3577b34da6a3ce929d0e0e4736",
        capture: null,
      });
      const refused = await failing([
        "trace",
        "record",
        "--root",
        root,
        "--repo",
        path.join("project", ".git"),
        "--session",
        SESSION,
        "--key",
        key,
        "--event",
        event,
      ]);
      assert.match(refused, /not a qualified object id/);
    }),
  );
});
