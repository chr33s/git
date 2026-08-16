/**
 * The `hub` commands, driven as a user drives them: a real process, real key
 * files on disk, real repositories under a root.
 *
 * End to end rather than unit, because the thing worth checking is that the
 * pieces line up — that a key `hub init` accepted is one `hub grant` can sign
 * with, and that what `hub members` prints is what the projection actually
 * holds.
 */
import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { promisify } from "node:util";
import { afterEach, beforeEach, describe, it } from "@effect/vitest";

import { formatPublicKey } from "../crypto/SshSignature.ts";
import { writeKeyPair } from "../testing/Hub.ts";

const execFileAsync = promisify(execFile);
const entry = path.join(import.meta.dirname, "bin.ts");

const cli = async (args: ReadonlyArray<string>): Promise<string> => {
  const result = await execFileAsync(process.execPath, [entry, ...args], { encoding: "utf8" });
  return `${result.stdout}${result.stderr}`;
};

describe("cli hub", () => {
  let root = "";

  beforeEach(async () => {
    root = await fs.mkdtemp(path.join(os.tmpdir(), "cli-hub-"));
  });

  afterEach(async () => {
    await fs.rm(root, { recursive: true, force: true });
  });

  it("gives a repository an identity, then grants and lists membership", async () => {
    await cli(["init", "--root", root, "project"]);
    const rootKey = await writeKeyPair(path.join(root, "id_root"), "root@example.com");
    await writeKeyPair(path.join(root, "id_member"), "dev@example.com");

    const initialised = await cli([
      "hub",
      "init",
      "--root",
      root,
      "--key",
      path.join(root, "id_root"),
      "project",
    ]);
    assert.match(initialised, /SHA256:[A-Za-z0-9+/]{43}/);

    const granted = await cli([
      "hub",
      "grant",
      "--root",
      root,
      "--key",
      path.join(root, "id_root"),
      "--subject",
      path.join(root, "id_member.pub"),
      "--capability",
      "source.push,hub.review",
      "project",
    ]);
    assert.match(granted, /Granted source\.push,hub\.review to SHA256:/);

    const listed = await cli(["hub", "members", "--root", root, "project"]);
    assert.match(listed, /source\.push,hub\.review/);
    assert.ok(
      !listed.includes(formatPublicKey(rootKey.publicKey)),
      "a member list should name keys by fingerprint, not by their full body",
    );
    assert.ok(!listed.includes("ignored:"), `no record should be refused: ${listed}`);
  });

  it("revokes a member, and says so in the listing", async () => {
    await cli(["init", "--root", root, "project"]);
    await writeKeyPair(path.join(root, "id_root"), "root@example.com");
    await writeKeyPair(path.join(root, "id_member"), "dev@example.com");

    await cli(["hub", "init", "--root", root, "--key", path.join(root, "id_root"), "project"]);
    const granted = await cli([
      "hub",
      "grant",
      "--root",
      root,
      "--key",
      path.join(root, "id_root"),
      "--subject",
      path.join(root, "id_member.pub"),
      "--capability",
      "source.push",
      "project",
    ]);
    const subject = granted.trim().split(" ").at(-1) ?? "";

    const revoked = await cli([
      "hub",
      "revoke",
      "--root",
      root,
      "--key",
      path.join(root, "id_root"),
      "--subject",
      subject,
      "--reason",
      "left",
      "project",
    ]);
    assert.match(revoked, /Revoked SHA256:/);

    const listed = await cli(["hub", "members", "--root", root, "project"]);
    assert.match(listed, /revoked \(left\)/);
  });

  it("refuses a subject that is not a fingerprint, and says what one looks like", async () => {
    await cli(["init", "--root", root, "project"]);
    await writeKeyPair(path.join(root, "id_root"), "root@example.com");
    await cli(["hub", "init", "--root", root, "--key", path.join(root, "id_root"), "project"]);

    const failed = await cli([
      "hub",
      "revoke",
      "--root",
      root,
      "--key",
      path.join(root, "id_root"),
      "--subject",
      "dev@example.com",
      "project",
    ]).then(
      () => null,
      (error: { stderr?: string; stdout?: string }) => `${error.stdout ?? ""}${error.stderr ?? ""}`,
    );

    assert.notEqual(failed, null, "a bad subject must fail rather than write a record");
    assert.match(failed ?? "", /not a key fingerprint/);
  });

  it("says what to do when a repository has no genesis yet", async () => {
    await cli(["init", "--root", root, "plain"]);
    const failed = await cli(["hub", "members", "--root", root, "plain"]).then(
      () => null,
      (error: { stderr?: string; stdout?: string }) => `${error.stdout ?? ""}${error.stderr ?? ""}`,
    );
    assert.match(failed ?? "", /hub init/);
  });

  it("mints a credential for a member of a hub-enabled repository", async () => {
    await cli(["init", "--root", root, "project"]);
    await writeKeyPair(path.join(root, "id_root"), "root@example.com");
    await cli(["hub", "init", "--root", root, "--key", path.join(root, "id_root"), "project"]);

    const minted = await cli([
      "credential",
      "--root",
      root,
      "--key",
      path.join(root, "id_root"),
      "--capability",
      "repo.read",
      "project",
    ]);
    assert.match(minted, /^hub1\./m, "a delegated credential is what stock git presents");
  });

  describe("the client's view of a remote", () => {
    it("reports a url nothing is pinned for", async () => {
      const out = await cli(["hub", "status", "https://git.example.com/nobody"]);
      assert.match(out, /not trusted/);
    });

    it("says when there was nothing to forget", async () => {
      const out = await cli(["hub", "forget", "https://git.example.com/nobody"]);
      assert.match(out, /was not trusted/);
    });
  });
});
