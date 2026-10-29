/**
 * The context verbs against the `git` binary itself.
 *
 * What needs real Git here is not the reading — the unit suite drives the same
 * commands — but the *writing*: constructing a record the way somebody with
 * append access would, using plumbing this CLI deliberately does not expose.
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

const failing = (args: ReadonlyArray<string>): Promise<string> =>
  cli(args).then(
    () => "",
    (error: { stdout?: string; stderr?: string }) => `${error.stdout ?? ""}${error.stderr ?? ""}`,
  );

describe("cli context against git", () => {
  let root = "";
  let project = "";
  let key = "";

  beforeEach(async () => {
    root = await fs.mkdtemp(path.join(os.tmpdir(), "cli-context-interop-"));
    project = path.join(root, "project");
    key = path.join(root, "agent");
    await fs.mkdir(project, { recursive: true });

    await cli(["init", "--root", project, ".git"]);
    const fixture = await enableHub(path.join(project, ".git"), [
      "repo.read",
      "source.push",
      "hub.trace",
      "hub.redact",
    ]);
    await fs.writeFile(key, opensshPrivateKey(fixture.member, "agent@example.com"), {
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

  it.effect("audits a record by oid whose commit message calls it something else", () =>
    Effect.promise(async () => {
      const session = "0192f000-0000-7000-8000-000000000009";
      await packOf(["--session", session, "--key", key]);

      // The exposure's own tree, re-committed under a message that calls it a
      // tool operation. The message is unsigned — a hint that survives
      // redaction, nothing more — so whoever may append to the ref chooses it,
      // while `event.json` and its signature are untouched.
      const repo = path.join(root, "project", ".git");
      const git = async (args: ReadonlyArray<string>) =>
        (await execFileAsync("git", ["-C", repo, ...args], { encoding: "utf8" })).stdout.trim();
      const ref = `refs/hub/trace/${session}`;
      const head = await git(["rev-parse", ref]);
      const tree = await git(["rev-parse", `${head}^{tree}`]);
      const mislabelled = await git([
        "-c",
        "user.name=agent",
        "-c",
        "user.email=agent@example.com",
        "commit-tree",
        tree,
        "-p",
        head,
        "-m",
        "tool-operation 0192f000-0000-7000-8000-00000000ffff",
      ]);
      await git(["update-ref", ref, mislabelled]);

      // Selected on the message, `locate` refused this as "not a context
      // exposure" while `context audit <session>` enumerated and audited the
      // very same record — two audit surfaces giving different accounts of one
      // record, which is the shape of a gate somebody can walk around.
      const audited = await cli(["context", "audit", ...selector(), `sha1:${mislabelled}`]);
      assert.match(audited, /blob src\/auth\.ts: verified/);

      // And exactly once. `Records.entries` claimed the same commit through
      // `kinds.has(entry.type)`, failed to decode it as telemetry and called
      // it unreadable — so one `session show --audit` rendered the record as a
      // context row *and* reported it as damage, for one commit.
      const shown = await cli(["session", "show", ...selector(), session, "--audit"]);
      assert.doesNotMatch(shown, /could not be read here/);
    }),
  );

  it.effect("reads a record whose commit message names no kind at all", () =>
    Effect.promise(async () => {
      const session = "0192f000-0000-7000-8000-00000000000a";
      const built = await packOf(["--session", session, "--key", key]);

      // A redaction, re-committed under a message that names nothing. Every
      // reader selected on that message: `Records.entries` skipped it,
      // `Exposure.entries` skipped it, and `unreadable` never saw it because
      // its payload decodes fine — while `Redaction.tombstonesOn` reads
      // payloads, so `gc` honoured the removal the audit could not see.
      const repo = path.join(root, "project", ".git");
      const git = async (args: ReadonlyArray<string>) =>
        (await execFileAsync("git", ["-C", repo, ...args], { encoding: "utf8" })).stdout.trim();
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
        session,
        "--target",
        built.exposure,
        "--reason",
        "leaked",
      ]);
      const ref = `refs/hub/trace/${session}`;
      const head = await git(["rev-parse", ref]);
      const parent = await git(["rev-parse", `${head}^`]);
      const relabelled = await git([
        "-c",
        "user.name=agent",
        "-c",
        "user.email=agent@example.com",
        "commit-tree",
        await git(["rev-parse", `${head}^{tree}`]),
        "-p",
        parent,
        "-m",
        "noise",
      ]);
      await git(["update-ref", ref, relabelled]);

      // `Redaction.tombstonesOn` reads payloads, so the collection honours the
      // removal whatever the message says, and the exposure's payload goes.
      await cli(["gc", "--root", root, path.join("project", ".git")]);

      // The reader has to agree with the collector. Selecting on the message,
      // it did not: the removal was never listed, so the absence the collector
      // had just created was reported as damage — "could not be read here" for
      // bytes an operator deliberately removed, on the one command they would
      // use to confirm the removal.
      const shown = await cli(["session", "show", ...selector(), session, "--audit"]);
      assert.match(shown, /removed by a signed redaction/);
      assert.doesNotMatch(shown, /could not be read here/);
    }),
  );

  it.effect("refuses to redact a tombstone however its commit message reads", () =>
    Effect.promise(async () => {
      const session = "0192f000-0000-7000-8000-00000000000b";
      const built = await packOf(["--session", session, "--key", key]);
      const redact = (target: string) => [
        "trace",
        "redact",
        "--root",
        root,
        "--repo",
        path.join("project", ".git"),
        "--key",
        key,
        "--session",
        session,
        "--target",
        target,
        "--reason",
        "leaked",
      ];
      await cli(redact(built.exposure));

      const repo = path.join(root, "project", ".git");
      const git = async (args: ReadonlyArray<string>) =>
        (await execFileAsync("git", ["-C", repo, ...args], { encoding: "utf8" })).stdout.trim();
      const ref = `refs/hub/trace/${session}`;
      const head = await git(["rev-parse", ref]);
      const relabelled = await git([
        "-c",
        "user.name=agent",
        "-c",
        "user.email=agent@example.com",
        "commit-tree",
        await git(["rev-parse", `${head}^{tree}`]),
        "-p",
        await git(["rev-parse", `${head}^`]),
        "-m",
        `tool-operation ${"0".repeat(8)}-0000-7000-8000-00000000cccc`,
      ]);
      await git(["update-ref", ref, relabelled]);
      // The guard read the *unsigned* commit message. A tombstone whose
      // message said anything else passed it, and redacting one destroys the
      // only decodable record of the earlier removal — `tombstonesOn` stops
      // finding it, and the next `gc` re-protects the payload the operator was
      // told was gone.
      const refused = await failing(redact(`sha1:${relabelled}`));
      assert.match(refused, /a tombstone is the record of a removal/);
    }),
  );

  it.effect("keeps a redacted exposure redacted when the same work recreates its bytes", () =>
    Effect.promise(async () => {
      const session = "0192f000-0000-7000-8000-00000000000c";
      const built = await packOf(["--session", session, "--key", key]);
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
        session,
        "--target",
        built.exposure,
        "--reason",
        "leaked",
      ]);
      await cli(["gc", "--root", root, path.join("project", ".git")]);
      const removed = await cli(["context", "audit", ...selector(), session]);
      assert.match(removed, /redacted {3}sha1:/);

      // The bytes themselves, read the way anybody with a clone of the ref
      // would read them.
      const repo = path.join(root, "project", ".git");
      const render = async (): Promise<string | null> =>
        execFileAsync(
          "git",
          ["-C", repo, "cat-file", "-p", `${commit}^{tree}:context/render.bin`],
          {
            encoding: "utf8",
          },
        ).then(
          (result) => result.stdout,
          () => null,
        );
      const commit = built.exposure.slice("sha1:".length);
      assert.equal(await render(), null, "the render is gone after the collection");

      // And `why` on the removed record says what happened rather than failing
      // raw. A redaction deliberately leaves the tree entry naming a blob that
      // is gone — the commit has to stay for the hash chain — so this is the
      // ordinary post-collection state, not a corrupt repository.
      // `why` honours the tombstone too. `audit` does in both its branches
      // and `session show --audit` does — because a Pack is deterministic, so
      // reading whatever resolves flips a removed record back to intact the
      // moment anybody repeats the work, and when a live exposure shares that
      // blob it is kept forever. `why` read the record's tree with no such
      // check and printed `view.tree`, every selected path and every blob oid.
      const why = await failing(["context", "why", ...selector(), built.exposure]);
      assert.match(why, /removed by a signed redaction/);

      // And the `--json` path carries what the prose path prints. Foreign ids
      // went to stderr only, so a machine consumer had no way to learn that
      // the ref carries records naming another session — which is the rule
      // the unreadable ids were moved inside the document to follow.
      const document = JSON.parse(
        await cli(["context", "audit", ...selector(), "--json", session]),
      );
      assert.equal(Array.isArray(document.foreign), true);

      // And both spellings agree about a record this repository has already
      // decided is somebody else's. The session form routes it to `foreign`
      // and leaves the exit code alone; the single-record form handed the
      // ref-derived session and repo to `audit`, whose binding check failed —
      // so one form kept deploying while the other was broken for good over
      // the same record.
      const git = async (args: ReadonlyArray<string>) =>
        (await execFileAsync("git", ["-C", repo, ...args], { encoding: "utf8" })).stdout.trim();
      const alien = "0192f000-0000-7000-8000-000000000020";
      const theirs = await packOf(["--session", alien, "--key", key]);
      const on = `refs/hub/trace/${session}`;
      const grafted = await git([
        "-c",
        "user.name=agent",
        "-c",
        "user.email=agent@example.com",
        "commit-tree",
        await git(["rev-parse", `${theirs.exposure.slice("sha1:".length)}^{tree}`]),
        "-p",
        await git(["rev-parse", on]),
        "-m",
        "context-exposure 0192f000-0000-7000-8000-000000000021",
      ]);
      await git(["update-ref", on, grafted]);
      const named = await cli(["context", "audit", ...selector(), `sha1:${grafted}`]);
      assert.match(named, /names another session or repository/);

      // The same task against the same tree. A Pack and a ContextRender are
      // deterministic, so this writes blobs with the oids `gc` just removed —
      // and the redacted record's tree entries, kept on purpose because the
      // commit has to stay for the hash chain, resolve again.
      const rebuilt = await packOf([
        "--session",
        "0192f000-0000-7000-8000-00000000000d",
        "--key",
        key,
      ]);

      // Auditing whatever resolves, that flipped the removed record back to a
      // full `verified` audit, silently, the moment anybody repeated the work.
      // The bytes coming back through a later legitimate exposure is not
      // something this version can prevent; presenting the removed record as
      // intact is.
      const after = await cli(["context", "audit", ...selector(), session]);
      assert.match(after, /redacted {3}sha1:/);
      assert.doesNotMatch(after, /blob src\/auth\.ts: verified/);

      const shown = await cli(["session", "show", ...selector(), session, "--audit"]);
      assert.match(shown, /removed by a signed redaction/);

      // And the bytes, which is the part no presentation rule can fix: if they
      // are back, `git cat-file` reads the verbatim task string and every
      // exposed file byte out of a record an operator removed.
      assert.equal(await render(), null, "the render is still gone");

      // The pack and the view do come back — they are written whatever a
      // tombstone says, because an exposure without a pack verifies nothing
      // and `context/view` is the Git edge the protocol requires. That is a
      // trade rather than an oversight, and the exposure that makes it says so
      // instead of making it silently. Only the first one does: after it, a
      // live record names those objects and nothing has been resurrected.
      assert.equal(rebuilt.renderRetained, false);
      assert.equal(rebuilt.renderWithheld, true);

      // And with the flag as well, the flag is the reason: a removal that
      // would have withheld a render nobody asked to keep explains nothing.
      const asked = JSON.parse(
        await cli([
          "context",
          "for",
          "--work",
          project,
          "--task",
          "authorize policy",
          "--json",
          "--retain-render=false",
          "--session",
          "0192f000-0000-7000-8000-00000000000f",
          "--key",
          key,
        ]),
      );
      assert.equal(asked.renderRetained, false);
      assert.equal(asked.renderWithheld, false);
      assert.notEqual(rebuilt.resurrected.length, 0);

      // The pack, and only the pack. This checkout is clean, so
      // `Pack.capture` reproduced the base commit's own tree oid — an object
      // a branch reaches, which `gc` re-walks the source refs without the
      // exclusion to protect, so it was never removed and never could be.
      // `excluded` lists it because it knows nothing about branches, and
      // reporting it made every clean `context for` over a redacted commit
      // announce an object that never left.
      const tree = (
        await execFileAsync("git", ["-C", repo, "rev-parse", "HEAD^{tree}"], { encoding: "utf8" })
      ).stdout.trim();
      assert.equal(rebuilt.resurrected.includes(`sha1:${tree}`), false);
      assert.equal(rebuilt.resurrected.length, 1);

      // And a runtime record naming the removed exposure says a removal
      // removed it, not that this replica is behind. The projection knows
      // which it is, and reporting a deliberate signed removal as a
      // replication gap is the confusion the module's own rule is about.
      const event = path.join(root, "bound.json");
      await fs.writeFile(
        event,
        JSON.stringify({
          type: "invocation-telemetry",
          exposure: built.exposure,
          capture: null,
          operation: { name: "chat" },
        }),
      );
      await cli([
        "trace",
        "record",
        "--root",
        root,
        "--repo",
        path.join("project", ".git"),
        "--session",
        session,
        "--key",
        key,
        "--event",
        event,
      ]);
      const told = await cli(["session", "show", ...selector(), session, "--audit"]);
      assert.match(told, /which a signed redaction removed/);
    }),
  );
});
