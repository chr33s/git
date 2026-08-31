/**
 * The three context verbs, driven as an operator drives them: a real process,
 * a real checkout, a real key on disk.
 *
 * The unit suites prove the protocol; this proves the surface actually reaches
 * it. What is worth checking at this level is that `for` discovers the checkout
 * it is standing in, that an exposure it records can be audited afterwards by
 * nothing but its own id, and that `why` keeps the two kinds of claim apart —
 * what Git can be made to agree with, and what the selector says about itself.
 */
import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { promisify } from "node:util";
import { afterEach, beforeEach, describe, it } from "@effect/vitest";

import { Effect } from "effect";

import { enableHub, grantMember, opensshPrivateKey } from "../testing/Hub.ts";

const execFileAsync = promisify(execFile);
const entry = path.join(import.meta.dirname, "bin.ts");

const cli = async (args: ReadonlyArray<string>): Promise<string> => {
  const result = await execFileAsync(process.execPath, [entry, ...args], { encoding: "utf8" });
  return `${result.stdout}${result.stderr}`;
};

const failing = (args: ReadonlyArray<string>): Promise<string> =>
  cli(args).then(
    () => "",
    (error: { stdout?: string; stderr?: string }) => `${error.stdout ?? ""}${error.stderr ?? ""}`,
  );

describe("cli context", () => {
  let root = "";
  let project = "";
  let key = "";
  /** A member of the same repository who holds no `hub.trace`. */
  let outsider = "";

  beforeEach(async () => {
    root = await fs.mkdtemp(path.join(os.tmpdir(), "cli-context-"));
    project = path.join(root, "project");
    key = path.join(root, "agent");
    await fs.mkdir(project, { recursive: true });

    // A checkout, not one of the bare repositories under `--root`: `.git`
    // inside a directory is the layout every context command discovers.
    await cli(["init", "--root", project, ".git"]);
    const fixture = await enableHub(path.join(project, ".git"), [
      "repo.read",
      "source.push",
      "hub.trace",
    ]);
    await fs.writeFile(key, opensshPrivateKey(fixture.member, "agent@example.com"), {
      mode: 0o600,
    });

    outsider = path.join(root, "outsider");
    const reader = await grantMember(path.join(project, ".git"), fixture.root, fixture.repoId, [
      "repo.read",
    ]);
    await fs.writeFile(outsider, opensshPrivateKey(reader.member, "reader@example.com"), {
      mode: 0o600,
    });

    await fs.writeFile(path.join(project, "AGENTS.md"), "Standing instructions.\n");
    await fs.mkdir(path.join(project, "src"), { recursive: true });
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

  /**
   * How a read-only verb names this repository.
   *
   * `why` and `audit` take `--root`/`--repo` rather than `--work`, because
   * neither touches the work tree and a bare repository on a server is exactly
   * where an operator audits a pushed exposure from.
   */
  const selector = () => ["--root", root, "--repo", path.join("project", ".git")];

  const packOf = async (extra: ReadonlyArray<string> = []) =>
    JSON.parse(
      await cli([
        "context",
        "for",
        "--work",
        project,
        "--task",
        "authorize policy",
        "--json",
        ...extra,
      ]),
    );

  it.effect("builds a pack against the checkout it is standing in", () =>
    Effect.promise(async () => {
      const built = await packOf();
      assert.equal(built.pack.version, 1);
      assert.match(built.pack.view.tree, /^sha1:[0-9a-f]{40}$/);
      assert.equal(
        built.pack.items.some(
          (item: { path: string; kind: string }) =>
            item.kind === "blob" && item.path === "src/auth.ts",
        ),
        true,
      );
      // Nothing was recorded, because nothing was asked to be.
      assert.equal(built.exposure, null);
    }),
  );

  it.effect("sees a change on disk that no commit holds", () =>
    Effect.promise(async () => {
      const before = await packOf();
      await fs.writeFile(
        path.join(project, "src", "auth.ts"),
        "export const authorize = (policy: string) => policy === 'allow'\n",
      );
      const after = await packOf();

      // §4.2: a dirty worktree is an overlay tree, not a relabelled HEAD.
      assert.notEqual(before.pack.view.tree, after.pack.view.tree);
      assert.equal(before.pack.view.base, after.pack.view.base);
    }),
  );

  it.effect("records an exposure that audits by its own id alone", () =>
    Effect.promise(async () => {
      const built = await packOf([
        "--session",
        "0192f000-0000-7000-8000-000000000000",
        "--key",
        key,
      ]);
      assert.match(built.exposure, /^sha1:[0-9a-f]{40}$/);
      assert.match(built.renderDigest, /^sha256:[0-9a-f]{64}$/);

      const audited = JSON.parse(
        await cli(["context", "audit", ...selector(), "--json", built.exposure]),
      ).audits;
      assert.equal(audited.length, 1);
      assert.equal(audited[0].signature.ok, true);
      // The signer is judged against the trust log, not merely against the
      // bytes: a record that arrived by replication never passed this host's
      // boundary, so the audit has to ask for itself.
      assert.equal(audited[0].trust.ok, true);
      assert.equal(audited[0].binding.ok, true);
      assert.equal(audited[0].retained.ok, true);
      assert.equal(audited[0].render.state, "verified");
      assert.equal(audited[0].ok, true);

      // And by the session it belongs to, which is the question an operator
      // asking "what did this run see?" actually has.
      const bySession = JSON.parse(
        await cli([
          "context",
          "audit",
          ...selector(),
          "--json",
          "0192f000-0000-7000-8000-000000000000",
        ]),
      ).audits;
      assert.equal(bySession.length, 1);
      assert.equal(bySession[0].exposure, built.exposure);
    }),
  );

  it.effect("distinguishes a valid signature from a signer who may not write one", () =>
    Effect.promise(async () => {
      const built = await packOf([
        "--session",
        "0192f000-0000-7000-8000-000000000002",
        "--key",
        outsider,
      ]);
      // Read through `failing`, because an audit that did not verify exits
      // non-zero: this is the one verb whose whole purpose is verification,
      // and `git+ context audit … && deploy` must not deploy.
      const audited = await failing(["context", "audit", ...selector(), built.exposure]);

      // The bytes are signed, and the record is exactly as good a description
      // of the repository evidence as any other. What it lacks is authority,
      // and those are reported apart.
      assert.match(audited, /signature ok/);
      assert.match(audited, /trust {5}no — /);
      assert.match(audited, /blob src\/auth\.ts: verified/);
      assert.match(audited, /not verified/);
      assert.match(audited, /1 of 1 exposure\(s\) did not verify/);
    }),
  );

  it.effect("audits and explains from a bare repository, with no work tree", () =>
    Effect.promise(async () => {
      const built = await packOf([
        "--session",
        "0192f000-0000-7000-8000-000000000003",
        "--key",
        key,
      ]);

      // §13: explicit repository selection stays available for bare and server
      // administration. Both verbs are read-only, so requiring a checkout made
      // them unusable in the one place a pushed exposure is audited from.
      const bare = path.join(root, "bare.git");
      await fs.cp(path.join(project, ".git"), bare, { recursive: true });
      const from = ["--root", root, "--repo", "bare.git"];

      const audited = JSON.parse(
        await cli(["context", "audit", ...from, "--json", built.exposure]),
      ).audits;
      assert.equal(audited[0].ok, true);
      assert.match(await cli(["context", "why", ...from, built.exposure]), /blob src\/auth\.ts/);
    }),
  );

  it.effect("takes an ordinary revision where it says it does", () =>
    Effect.promise(async () => {
      const built = await packOf([
        "--session",
        "0192f000-0000-7000-8000-000000000004",
        "--key",
        key,
      ]);
      // The record commit is on a ref; naming that ref has to work, because
      // §13 says CLI input may use ordinary Git revisions and the docstring
      // above `packBytes` says so too. `repository.resolve` took full ref
      // names only, so a branch fell through to the filesystem.
      const explained = await cli([
        "context",
        "why",
        ...selector(),
        "refs/hub/trace/0192f000-0000-7000-8000-000000000004",
      ]);
      assert.match(explained, /view\.tree sha1:[0-9a-f]{40}/);
      assert.match(explained, /blob src\/auth\.ts/);
      assert.equal(built.exposure.startsWith("sha1:"), true);
    }),
  );

  it.effect("audits a record named by an ordinary revision", () =>
    Effect.promise(async () => {
      const session = "0192f000-0000-7000-8000-000000000005";
      await packOf(["--session", session, "--key", key]);

      // `main`, `HEAD`, `v1.0` are all valid trace-id spellings, so deciding
      // by shape sent every revision to the session branch and reported that
      // a branch "has no exposures to audit".
      const audited = JSON.parse(
        await cli(["context", "audit", ...selector(), "--json", `refs/hub/trace/${session}`]),
      ).audits;
      assert.equal(audited.length, 1);
      assert.equal(audited[0].ok, true);
    }),
  );

  it.effect("builds a view for the revision it was given, not the checkout", () =>
    Effect.promise(async () => {
      // The first commit, then a second one that moves HEAD past it.
      const first = (await packOf()).pack.view.base;
      await fs.writeFile(
        path.join(project, "src", "auth.ts"),
        "export const authorize = () => 'second'\n",
      );
      await cli(["add", "--work", project, "."]);
      await cli(["commit", "--work", project, "--message", "second\n"]);

      const head = await packOf();
      const named = JSON.parse(
        await cli([
          "context",
          "for",
          "--work",
          project,
          "--task",
          "authorize policy",
          "--json",
          "--rev",
          first.slice("sha1:".length),
        ]),
      );

      // A view labelled with a commit is a view *of* that commit. Capturing
      // the checkout and labelling it with somebody else's revision signs a
      // false ancestry claim onto an append-only ref, and nothing downstream
      // reads `base` to catch it.
      assert.notEqual(head.pack.view.base, named.pack.view.base);
      assert.equal(named.pack.view.base, first);
      assert.notEqual(head.pack.view.tree, named.pack.view.tree);
    }),
  );

  it.effect("refuses flags that would do nothing", () =>
    Effect.promise(async () => {
      const keyOnly = await failing([
        "context",
        "for",
        "--work",
        project,
        "--task",
        "x",
        "--key",
        key,
      ]);
      assert.match(keyOnly, /name the session to record one with --session/);

      // Past the render's segment bound: refused before the selection runs,
      // rather than after every candidate blob has been read.
      const oversized = await failing([
        "context",
        "for",
        "--work",
        project,
        "--task",
        "x",
        "--max-items",
        "2000",
      ]);
      assert.match(oversized, /--max-items must be between 1 and 1023/);
    }),
  );

  it.effect("is not shadowed by a branch that shares the session's name", () =>
    Effect.promise(async () => {
      const session = "0192f000-0000-7000-8000-000000000006";
      await packOf(["--session", session, "--key", key]);
      const clean = JSON.parse(await cli(["context", "audit", ...selector(), "--json", session]));
      assert.equal(clean.audits.length, 1);

      // A branch named after the run it was worked on is the ordinary case.
      // Resolving the argument as a revision first is right; treating that as
      // the last word made the branch shadow the session outright, and the
      // documented `context audit "$session" && deploy` started failing for a
      // session whose exposures were perfectly valid.
      await cli([
        "branch",
        "--root",
        root,
        "--create",
        session,
        "--base",
        "refs/heads/main",
        path.join("project", ".git"),
      ]);
      const shadowed = JSON.parse(
        await cli(["context", "audit", ...selector(), "--json", session]),
      );
      assert.equal(shadowed.audits.length, 1);
      assert.equal(shadowed.audits[0].ok, true);
    }),
  );

  /** Every loose object in the repository, so "nothing was written" is checkable. */
  const objects = async (): Promise<number> => {
    const base = path.join(project, ".git", "objects");
    const found = await fs.readdir(base, { recursive: true, withFileTypes: true });
    return found.filter((entry) => entry.isFile()).length;
  };

  it.effect("refuses a task that looks like it carries a credential", () =>
    Effect.promise(async () => {
      // `session open --prompt` refuses the same string. The task goes into
      // `context/render.bin` verbatim and onto a ref this version cannot
      // rewind, so it is the one part of this chain somebody typed and the one
      // part that has to be scanned.
      const refused = await failing([
        "context",
        "for",
        "--work",
        project,
        "--task",
        `use the token ghp_${"A".repeat(36)}`,
        "--session",
        "0192f000-0000-7000-8000-000000000007",
        "--key",
        key,
      ]);
      assert.match(refused, /this task looks like it carries/);

      // And nothing was written for it, which is the part that has to hold on
      // a *dirty* checkout: `Pack.capture` writes a blob per tracked file and
      // the whole overlay tree, and it used to run first — so the refusal an
      // operator is most likely to hit was the one that left the most behind.
      const before = await objects();
      await fs.writeFile(path.join(project, "src", "auth.ts"), "export const authorize = 1\n");
      const dirty = await failing([
        "context",
        "for",
        "--work",
        project,
        "--task",
        `use the token ghp_${"A".repeat(36)}`,
        "--session",
        "0192f000-0000-7000-8000-000000000007",
        "--key",
        key,
      ]);
      assert.match(dirty, /this task looks like it carries/);
      assert.equal(await objects(), before);

      // And nothing was written for it: the scan runs before the objects do.
      const audited = await failing([
        "context",
        "audit",
        ...selector(),
        "--json",
        "0192f000-0000-7000-8000-000000000007",
      ]);
      assert.match(audited, /has no exposures to audit/);
    }),
  );

  it.effect("does not claim a render is retained when it was asked not to be", () =>
    Effect.promise(async () => {
      const built = await packOf([
        "--session",
        "0192f000-0000-7000-8000-00000000000a",
        "--key",
        key,
        "--retain-render=false",
      ]);

      // `withheld` means only "a signed removal names those exact bytes", and
      // reading `renderRetained` off it told a harness a record with no render
      // had one — while `context audit` on the same record reports `render
      // absent`. Both reasons a render is missing have to reach the field a
      // reader uses to decide whether the digest is recomputable.
      assert.equal(built.renderRetained, false);
      // And which of the two reasons, because `renderRetained: false`
      // otherwise conflates a signed removal with `--retain-render=false`.
      // Computed independently of the flag, this said a redaction caused what
      // the operator's own flag caused — on any repository where an earlier
      // identical render happened to be tombstoned.
      assert.equal(built.renderWithheld, false);
    }),
  );

  it.effect("builds a pack when a tracked path has become a directory", () =>
    Effect.promise(async () => {
      // `work.stat` is an `lstat`, so it answers for a directory, and
      // `modeString` maps its 0o755 to a plain executable — then `read` fails
      // `EISDIR`, which `Work.node` reports as `ObjectNotFound` and `capture`
      // had no handler for. An ordinary `rm f && mkdir f` killed the command
      // with "object not found: src/auth.ts".
      await fs.rm(path.join(project, "src", "auth.ts"));
      await fs.mkdir(path.join(project, "src", "auth.ts"));
      await fs.writeFile(path.join(project, "src", "auth.ts", "inner.ts"), "export const x = 1\n");

      const built = await packOf();
      // Read as the deletion the index sees: retrieval cannot read the path,
      // so the view must not say it could.
      assert.equal(
        built.pack.items.some((item: { path: string }) => item.path === "src/auth.ts"),
        false,
      );
    }),
  );

  it.effect("keeps the redaction answer beside the repository between runs", () =>
    Effect.promise(async () => {
      // Every verb is its own process, so the in-process memos never get a
      // second reader and `context for --session` redid the whole
      // record-history walk each time. See docs/telemetry.md §13.1.
      const held = path.join(project, ".git", "gitplus", "redaction.json");
      // Twice, because the first run asks before any trace ref exists: there
      // is nothing to walk and so nothing worth keeping. The second asks with
      // the first run's ref in place, which is the shape every run after it
      // has.
      await packOf(["--session", "0192f000-0000-7000-8000-00000000000c", "--key", key]);
      await packOf(["--session", "0192f000-0000-7000-8000-00000000000d", "--key", key]);

      const kept: { readonly entries: Record<string, ReadonlyArray<unknown>> } = JSON.parse(
        await fs.readFile(held, "utf8"),
      );

      // Kept per ref, and each key names the ref *and* its head — so the
      // exposure this very command appended invalidates its own session's
      // entry and no other. A repository-wide answer keyed on every head could
      // never hit here at all, because this caller always appends before the
      // next run asks.
      const keys = Object.keys(kept.entries);
      assert.equal(
        keys.some((entry) => entry.includes("refs/hub/trace/0192f000-0000-7000-8000-00000000000c")),
        true,
      );

      // A third session, and its ref joins the others rather than replacing
      // them: one file holding one answer would cache one ref out of however
      // many the repository has.
      await packOf(["--session", "0192f000-0000-7000-8000-00000000000e", "--key", key]);
      const again: { readonly entries: Record<string, ReadonlyArray<unknown>> } = JSON.parse(
        await fs.readFile(held, "utf8"),
      );
      assert.equal(Object.keys(again.entries).length > keys.length, true);
    }),
  );

  it.effect("prints the view dimension the audit computes", () =>
    Effect.promise(async () => {
      const built = await packOf([
        "--session",
        "0192f000-0000-7000-8000-000000000008",
        "--key",
        key,
      ]);
      // `Pack.verify` returns the view as its own check, and when it fails
      // there are no item lines at all — so every visible line read `ok` and
      // the command ended on a bare `not verified` with the reason nowhere in
      // the output. `context why` prints it; `audit` dropped it.
      const verified = await cli(["context", "audit", ...selector(), built.exposure]);
      assert.match(verified, /view\.tree ok/);
      assert.match(verified, /blob src\/auth\.ts: verified/);
    }),
  );

  it.effect("explains a pack, separating Git facts from the selector's account", () =>
    Effect.promise(async () => {
      const built = await packOf([
        "--session",
        "0192f000-0000-7000-8000-000000000001",
        "--key",
        key,
      ]);
      const explained = await cli(["context", "why", ...selector(), built.exposure, "src/auth.ts"]);

      assert.match(explained, /view\.tree sha1:[0-9a-f]{40}/);
      assert.match(explained, /blob src\/auth\.ts/);
      assert.match(explained, /git: {6}resolves under view\.tree/);
      assert.match(explained, /selector: implementation \/ search/);
    }),
  );
});
