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
      // Removing a record is charged separately from writing one, so the
      // fixture has to hold both to exercise the way back out.
      "hub.redact",
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

      // And the session it claimed does not exist here at all, which is now
      // what the reader says rather than handing back an empty document. An
      // id nothing in the repository knows is an error: `isSessionId` accepts
      // any legal ref component, so an empty projection was also what
      // `session show <a repository name>` produced.
      const elsewhere = await failing([
        "session",
        "show",
        "--root",
        root,
        "--repo",
        path.join("project", ".git"),
        "--audit",
        "--json",
        "0192f000-0000-7000-8000-0000000000ff",
      ]);
      assert.match(elsewhere, /no session refs|has no session/);
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
      // to know it came from two signed records (§19.14) — and joined to the
      // policy-visible session projection, which is what `--audit` promises
      // and what rendering only the trace half quietly dropped.
      const rendered = await inside(project, ["session", "show", "--audit", SESSION]);
      assert.match(rendered, new RegExp(`Session ${SESSION}`));
      assert.match(rendered, new RegExp(`Invocation ${written}`));
      assert.match(rendered, /Capture\n {2}coverage unknown|coverage unknown/);
    }),
  );

  it.effect("refuses span-shaping flags on the normalized-event path", () =>
    Effect.promise(async () => {
      const event = await write("plain.json", {
        type: "invocation-telemetry",
        exposure: null,
        capture: null,
      });
      // Dropped silently, `--exposure` named the record's join and the record
      // was written joined to nothing. Refused, the caller learns which half
      // of the surface they are on.
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
        "--exposure",
        `sha1:${"a".repeat(40)}`,
      ]);
      assert.match(refused, /--exposure describes how to read a span/);
    }),
  );

  it.effect("lets a tool span name the invocation it belongs beneath", () =>
    Effect.promise(async () => {
      const exposed = await exposure();
      const inference = await write("inference.json", {
        type: "invocation-telemetry",
        exposure: exposed,
        capture: null,
        operation: { name: "chat" },
      });
      const parent = (await record(inference)).trim();

      const span = await write("tool.json", {
        attributes: {
          "gen_ai.operation.name": "execute_tool",
          "gen_ai.tool.name": "read_file",
          "gen_ai.tool.call.id": "call_7",
        },
      });
      await record(span, ["--otel", "--invocation", parent]);

      const shown = await audit();
      assert.equal(shown.audit.tools.length, 1);
      // Nothing on a tool span names a Git record, so without a way to say it
      // the operation could never be attached to anything.
      assert.equal(shown.audit.tools[0].payload.invocation, parent);
    }),
  );

  it.effect("refuses a span-shaping flag that does not apply to the span given", () =>
    Effect.promise(async () => {
      const tool = await write("tool-only.json", {
        attributes: {
          "gen_ai.operation.name": "execute_tool",
          "gen_ai.tool.name": "read_file",
        },
      });
      // `--exposure` is read only for an inference span. Passed with a tool
      // span it was read by nothing and the record was written joined to
      // nothing — the same failure the `--event` guard exists to prevent.
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
        tool,
        "--otel",
        "--exposure",
        `sha1:${"a".repeat(40)}`,
      ]);
      assert.match(refused, /--exposure names the context an inference made use of/);
    }),
  );

  it.effect("removes a leaked prompt from the trace, not only from the session", () =>
    Effect.promise(async () => {
      // The retained render holds the task string verbatim, which is exactly
      // why this namespace needs a way back out and not only the session does.
      const built = JSON.parse(
        await cli([
          "context",
          "for",
          "--work",
          project,
          "--task",
          "rotate the deploy token hunter2",
          "--json",
          "--session",
          SESSION,
          "--key",
          key,
        ]),
      );

      const written = (
        await cli([
          "trace",
          "redact",
          "--root",
          root,
          "--repo",
          path.join("project", ".git"),
          "--key",
          key,
          "--session",
          SESSION,
          "--target",
          built.exposure,
          "--reason",
          "the prompt carried a token",
        ])
      ).trim();
      assert.match(written, /^sha1:[0-9a-f]{40}$/);

      const after = await audit();
      // The removal is recorded and names what it removed.
      assert.deepEqual(after.audit.redacted, [built.exposure]);

      // And the bytes actually go. This is the whole point: the tombstone
      // replicates, and `gc` is what removes the payload — including
      // `context/render.bin`, which is where the task string and the exposed
      // file contents are. Redacting only `event.json` left the leak readable.
      await cli(["gc", "--root", root, path.join("project", ".git")]);

      const single = JSON.parse(
        await cli([
          "context",
          "audit",
          "--root",
          root,
          "--repo",
          path.join("project", ".git"),
          "--json",
          built.exposure,
        ]),
      );
      // The record is still on the ref — a hash chain with a hole in it is not
      // a hash chain — its content is gone, and the removal is accounted for.
      // Both forms of the command have to say the same thing about it: for a
      // while `audit <session>` exited zero here while `audit <record>` failed
      // forever, on the same repository, for the same removal.
      assert.deepEqual(single.redacted, [built.exposure]);
      assert.deepEqual(single.audits, []);
    }),
  );

  it.effect("keeps auditing a session after an unrelated record is redacted", () =>
    Effect.promise(async () => {
      const exposed = await exposure();
      const tool = await write("tool.json", {
        type: "tool-operation",
        invocation: null,
        capture: null,
        tool: { name: "read_file" },
      });
      const written = (await record(tool)).trim();

      await cli([
        "trace",
        "redact",
        "--root",
        root,
        "--repo",
        path.join("project", ".git"),
        "--key",
        key,
        "--session",
        SESSION,
        "--target",
        written,
        "--reason",
        "the result carried a token",
      ]);
      await cli(["gc", "--root", root, path.join("project", ".git")]);

      // A routine, sanctioned redaction of a *tool operation* must not make
      // the session's exposures unauditable. Counting every unreadable commit
      // on the ref as this reader's own broke `context audit … && deploy`
      // permanently the first time anybody redacted anything.
      const audited = JSON.parse(
        await cli([
          "context",
          "audit",
          "--root",
          root,
          "--repo",
          path.join("project", ".git"),
          "--json",
          SESSION,
        ]),
      );
      assert.equal(audited.audits.length, 1);
      assert.equal(audited.audits[0].exposure, exposed);
      assert.equal(audited.audits[0].ok, true);
      assert.deepEqual(audited.unreadable, []);
    }),
  );

  it.effect("keeps auditing a session whose own exposure was redacted", () =>
    Effect.promise(async () => {
      const built = JSON.parse(
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
      await cli([
        "trace",
        "redact",
        "--root",
        root,
        "--repo",
        path.join("project", ".git"),
        "--key",
        key,
        "--session",
        SESSION,
        "--target",
        built.exposure,
        "--reason",
        "leaked",
      ]);
      await cli(["gc", "--root", root, path.join("project", ".git")]);

      // A removal is not an unreadable record. Counted together, using the
      // removal path the CLI documents made every later
      // `context audit S && deploy` fail permanently.
      const audited = JSON.parse(
        await cli([
          "context",
          "audit",
          "--root",
          root,
          "--repo",
          path.join("project", ".git"),
          "--json",
          SESSION,
        ]),
      );
      assert.deepEqual(audited.redacted, [built.exposure]);
      assert.deepEqual(audited.unreadable, []);
      assert.deepEqual(audited.audits, []);
    }),
  );

  it.effect("will not remove a tombstone", () =>
    Effect.promise(async () => {
      const built = JSON.parse(
        await cli([
          "context",
          "for",
          "--work",
          project,
          "--task",
          "authorize",
          "--json",
          "--session",
          SESSION,
          "--key",
          key,
        ]),
      );
      const redaction = (
        await cli([
          "trace",
          "redact",
          "--root",
          root,
          "--repo",
          path.join("project", ".git"),
          "--key",
          key,
          "--session",
          SESSION,
          "--target",
          built.exposure,
          "--reason",
          "leaked",
        ])
      ).trim();

      const refused = await failing([
        "trace",
        "redact",
        "--root",
        root,
        "--repo",
        path.join("project", ".git"),
        "--key",
        key,
        "--session",
        SESSION,
        "--target",
        redaction,
        "--reason",
        "again",
      ]);
      assert.match(refused, /a tombstone is the record of a removal/);
    }),
  );

  it.effect("does not present a trace-only run under a session heading", () =>
    Effect.promise(async () => {
      // `context for --session X` writes the trace ref and nothing else, so
      // this session has a full runtime account and no record of the work it
      // was doing. `Session.project` reports that as `exists: false` and the
      // `--json` path prints it; the prose header printed `Session <id>`
      // unconditionally, presenting the account of the runtime as the account
      // of the work.
      await exposure();
      const shown = await cli([
        "session",
        "show",
        "--root",
        root,
        "--repo",
        path.join("project", ".git"),
        SESSION,
        "--audit",
      ]);
      assert.match(shown, /no session record/);
    }),
  );

  it.effect("says so when a removal cannot take the bytes with it", () =>
    Effect.promise(async () => {
      // Two sessions, one task, one tree. A Pack and a ContextRender are
      // deterministic, so both exposures name the same `context/render.bin`.
      const other = "0192f000-0000-7000-8000-0000000000bb";
      const mine = await exposure();
      const theirs: { readonly exposure: string } = JSON.parse(
        await cli([
          "context",
          "for",
          "--work",
          project,
          "--task",
          "authorize policy",
          "--json",
          "--session",
          other,
          "--key",
          key,
        ]),
      );
      assert.notEqual(mine, theirs.exposure);

      const said = await cli([
        "trace",
        "redact",
        "--root",
        root,
        "--repo",
        path.join("project", ".git"),
        "--key",
        key,
        "--session",
        SESSION,
        "--target",
        mine,
        "--reason",
        "leaked",
      ]);

      // `gc` will not delete an object the other exposure still needs — it is
      // live, nobody asked for it to go, and `Maintenance.gc` re-walks only
      // the source refs, so deleting it would leave that audit reporting its
      // own render unavailable forever. So the removal is partial, and the
      // command used to print an oid and nothing else: the verbatim task
      // string and every exposed file byte stayed readable at
      // `<record>^{tree}:context/render.bin` and clonable off the ref, while
      // the operator had been told the removal happened.
      assert.match(said, /^sha1:[0-9a-f]{40}$/m);
      assert.match(said, /will not be collected/);
      // And which record holds them, because redacting that one is the only
      // thing that finishes the job.
      assert.match(said, new RegExp(theirs.exposure));
      assert.match(said, /held by 1 other live record/);

      // A third, so that redacting the second still leaves the object held.
      // Only the *live* holder counts: a record an accepted tombstone already
      // names is holding nothing — `stillNamed` skips exactly those when it
      // computes the protection — so listing it told the operator to redact
      // something already redacted, and got "other live record(s)" wrong.
      const third = "0192f000-0000-7000-8000-0000000000cc";
      const spare: { readonly exposure: string } = JSON.parse(
        await cli([
          "context",
          "for",
          "--work",
          project,
          "--task",
          "authorize policy",
          "--json",
          "--session",
          third,
          "--key",
          key,
        ]),
      );
      const second = await cli([
        "trace",
        "redact",
        "--root",
        root,
        "--repo",
        path.join("project", ".git"),
        "--key",
        key,
        "--session",
        other,
        "--target",
        theirs.exposure,
        "--reason",
        "leaked",
      ]);
      assert.match(second, /held by 1 other live record/);
      assert.match(second, new RegExp(spare.exposure));
      assert.doesNotMatch(second, new RegExp(mine));
    }),
  );

  it.effect("scans every free-text field on a health record, not just the reasons", () =>
    Effect.promise(async () => {
      // `source` and `sampling` are bare strings with no vocabulary — an
      // exporter name and a sampling description somebody wrote. Scanned only
      // for `reasons`, the same bytes were signed and appended to a ref that
      // replicates to every clone, while `reasons` refused them.
      const leaked = await write("health.json", {
        type: "trace-health",
        source: `otel-collector token=ghp_${"A".repeat(36)}`,
        sampling: "none",
        transformed: false,
        dropped: 0,
      });
      assert.match(
        await failing([
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
          leaked,
        ]),
        /looks like it carries/,
      );

      const sampled = await write("sampling.json", {
        type: "trace-health",
        source: "otel-collector",
        sampling: `head 10% key=glpat-${"A".repeat(20)}`,
        transformed: false,
        dropped: 0,
      });
      assert.match(
        await failing([
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
          sampled,
        ]),
        /looks like it carries/,
      );
    }),
  );

  it.effect("audits the exposure an invocation record names", () =>
    Effect.promise(async () => {
      const exposed = await exposure();
      const event = await write("bound.json", {
        type: "invocation-telemetry",
        exposure: exposed,
        capture: null,
        operation: { name: "chat" },
      });
      const written = (await record(event)).trim();

      // context-pack.md §13 calls this argument `<invocation-or-exposure>`,
      // and `trace record` prints this oid — so piping the one into the other
      // is the documented path. Refused, the documented command failed on the
      // oid the neighbouring command had just printed; followed, it is the
      // same audit the exposure's own id reaches.
      const audited = await cli([
        "context",
        "audit",
        "--root",
        root,
        "--repo",
        path.join("project", ".git"),
        written,
      ]);
      assert.match(audited, /blob src\/auth\.ts: verified/);
      assert.equal(
        audited,
        await cli([
          "context",
          "audit",
          "--root",
          root,
          "--repo",
          path.join("project", ".git"),
          exposed,
        ]),
      );
    }),
  );

  it.effect("says what a trace record is when it is not an exposure", () =>
    Effect.promise(async () => {
      const event = await write("runtime.json", {
        type: "invocation-telemetry",
        exposure: null,
        capture: null,
        operation: { name: "chat" },
      });
      const written = (await record(event)).trim();

      // `trace record` prints this oid, and the docs call the audit's argument
      // "a qualified Git record OID" — so one arriving there is the ordinary
      // case. Matching only exposures reported it as "on no trace ref in this
      // repository", which is false: it is on one.
      const refused = await failing([
        "context",
        "audit",
        "--root",
        root,
        "--repo",
        path.join("project", ".git"),
        written,
      ]);
      assert.match(refused, /is not a context exposure/);
      assert.match(refused, new RegExp(`session show ${SESSION} --audit`));

      // And the bare spelling, which `resolveRev` returns as-is and which
      // `isTraceId` accepts as a legal ref component — so testing the exposure
      // lookup instead of the record lookup sent it to the session branch, to
      // walk a `refs/hub/trace/<40 hex>` that does not exist. The qualified
      // form escaped only because `:` is reserved in a ref name.
      const bare = await failing([
        "context",
        "audit",
        "--root",
        root,
        "--repo",
        path.join("project", ".git"),
        written.slice("sha1:".length),
      ]);
      assert.match(bare, /is not a context exposure/);
    }),
  );

  it.effect("refuses a capture stage the vocabulary does not name, on either path", () =>
    Effect.promise(async () => {
      // The CLI flag is checked, and so is the value that comes straight
      // through `--event`: a typo lands in a signed, immutable record on an
      // append-only ref, and every later reader sees a stage that is not one.
      const flagged = await failing([
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
        await write("span-stage.json", { attributes: { "gen_ai.operation.name": "chat" } }),
        "--otel",
        "--stage",
        "sdk-exprot",
      ]);
      assert.match(flagged, /is not a capture stage/);

      const supplied = await write("typo.json", {
        type: "invocation-telemetry",
        exposure: null,
        capture: { transport: "otel", stage: "sdk-exprot" },
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
        supplied,
      ]);
      assert.match(refused, /is not a capture stage/);
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
