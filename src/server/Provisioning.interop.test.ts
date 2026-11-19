import assert from "node:assert/strict";
import { execFile, execFileSync } from "node:child_process";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { promisify } from "node:util";
import { describe, it } from "@effect/vitest";
import { Effect, type Schema } from "effect";
import { fastImport, gitIn, hasGit, importCommit } from "../testing/Git.ts";
import { enableHub } from "../testing/Hub.ts";
import { GENESIS_REF } from "../trust/Genesis.ts";
import { LOG_REF } from "../trust/Log.ts";
import * as Provisioning from "./Provisioning.ts";

const { decode, validate } = Provisioning;

describe.skipIf(!hasGit)("operator provisioning preparation", () => {
  it("exports a private verified artifact whose identity-only pack stock Git can reopen", async () => {
    const root = await fs.mkdtemp(path.join(os.tmpdir(), "git-provisioning-"));
    try {
      const source = path.join(root, "source.git");
      const target = path.join(root, "target.git");
      await fs.mkdir(source);
      await fs.mkdir(target);
      const git = gitIn(source);
      git("init", "-q", "--bare", "-b", "main");
      fastImport(
        source,
        importCommit({
          branch: "refs/heads/main",
          mark: 1,
          message: "source must not be provisioned",
          files: [{ path: "private.txt", content: "private source payload" }],
        }),
      );
      const sourceHead = git("rev-parse", "refs/heads/main").trim();
      const fixture = await enableHub(source, ["repo.admin"]);
      const before = git("show-ref");
      const output = path.join(root, "provisioning.json");
      const args = [path.resolve("tools/prepare-provisioning.ts"), source, "private-repo", output];
      await promisify(execFile)(process.execPath, args, { timeout: 20_000 });
      assert.equal((await fs.stat(output)).mode & 0o777, 0o600);
      const bytes = new Uint8Array(await fs.readFile(output));
      const artifact: Schema.Schema.Type<typeof Provisioning.Artifact> = await Effect.runPromise(
        decode(bytes, "private-repo"),
      );
      assert.equal(artifact.repoId, fixture.repoId);
      // Validation above warmed genesis/signature caches. A pack with the same
      // tips but an omitted blob must still fail its independent closure check.
      const omitted = git("rev-parse", `${GENESIS_REF}:genesis.json`).trim();
      const objectIds: string[] = [];
      for (const line of git("rev-list", "--objects", GENESIS_REF, LOG_REF).trim().split("\n")) {
        const oid = line.split(" ")[0];
        if (oid !== undefined && oid !== omitted) objectIds.push(oid);
      }
      const incomplete = execFileSync("git", ["pack-objects", "--stdout"], {
        cwd: source,
        input: objectIds.join("\n") + "\n",
      });
      const missing = await Effect.runPromise(
        validate({ ...artifact, pack: new Uint8Array(incomplete) }).pipe(Effect.flip),
      );
      assert.equal(missing._tag, "ObjectNotFound");
      assert.equal(git("show-ref"), before);
      await assert.rejects(
        promisify(execFile)(process.execPath, args, { timeout: 20_000 }),
        /EEXIST/,
      );
      assert.deepEqual(new Uint8Array(await fs.readFile(output)), bytes);

      const restored = gitIn(target);
      restored("init", "-q", "--bare", "-b", "main");
      execFileSync("git", ["index-pack", "--stdin"], { cwd: target, input: artifact.pack });
      restored("update-ref", GENESIS_REF, artifact.genesis);
      restored("update-ref", LOG_REF, artifact.trust);
      restored("fsck", "--strict");
      assert.equal(restored("rev-parse", GENESIS_REF).trim(), artifact.genesis);
      assert.equal(restored("rev-parse", LOG_REF).trim(), artifact.trust);
      assert.throws(() => restored("cat-file", "-e", sourceHead));
      assert.equal(restored("for-each-ref", "--format=%(refname)", "refs/heads/"), "");
    } finally {
      await fs.rm(root, { recursive: true, force: true });
    }
  });
});
