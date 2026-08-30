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
        await cli(["context", "audit", "--work", project, "--json", built.exposure]),
      );
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
          "--work",
          project,
          "--json",
          "0192f000-0000-7000-8000-000000000000",
        ]),
      );
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
      const audited = JSON.parse(
        await cli(["context", "audit", "--work", project, "--json", built.exposure]),
      );

      // The bytes are signed, and the record is exactly as good a description
      // of the repository evidence as any other. What it lacks is authority,
      // and those are reported apart.
      assert.equal(audited[0].signature.ok, true);
      assert.equal(audited[0].trust.ok, false);
      assert.equal(audited[0].evidence.ok, true);
      assert.equal(audited[0].ok, false);
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
      const explained = await cli([
        "context",
        "why",
        "--work",
        project,
        built.exposure,
        "src/auth.ts",
      ]);

      assert.match(explained, /view\.tree sha1:[0-9a-f]{40}/);
      assert.match(explained, /blob src\/auth\.ts/);
      assert.match(explained, /git: {6}resolves under view\.tree/);
      assert.match(explained, /selector: implementation \/ search/);
    }),
  );
});
