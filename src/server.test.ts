import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { after, before, describe, it } from "node:test";

import * as helpers from "./test.helpers.ts";

const cli = await helpers.cli();
const worker = await helpers.worker({ port: 8080 });

const ZERO_OID = "0".repeat(40);

function pktLine(text: string) {
  return (text.length + 4).toString(16).padStart(4, "0") + text;
}

before(async () => {
  await cli.before();
  await worker.before();
});
after(async () => {
  await cli.after();
  await worker.after();
});

void describe("cli", () => {
  void it("git clone", async () => {
    const cd1 = await cli.setup();

    await writeFile(join(cd1, "README.md"), "# Test Repository");
    await mkdir(join(cd1, "src"), { recursive: true });
    await writeFile(join(cd1, "src/main.js"), "console.log('Hello World');");

    await cli.run("git add .", { cwd: cd1 });
    const res1 = await cli.run('git commit -m "Initial commit"', { cwd: cd1 });
    assert.ok(res1.includes("Initial commit"));

    const cloneRepo = `test-repo-${randomUUID().slice(0, 8)}`;
    const repoUrl = `${worker.url}${cloneRepo}.git`;

    await cli.run(`git remote add origin ${repoUrl}`, { cwd: cd1 });
    await cli.run("git push -u origin main", { cwd: cd1 });

    const cd2 = await cli.setup();
    await cli.run(`git -c init.defaultBranch=main clone ${repoUrl} cloned-repo`, { cwd: cd2 });

    // Verify clone contents
    const clonedDir = join(cd2, "cloned-repo");
    const res2 = await readFile(join(clonedDir, "README.md"), "utf-8");
    assert.equal(res2, "# Test Repository");
    const res3 = await readFile(join(clonedDir, "src/main.js"), "utf-8");
    assert.equal(res3, "console.log('Hello World');");
  });

  void it("git push", async () => {
    const cd1 = await cli.setup();

    await writeFile(join(cd1, "file1.txt"), "test content");
    await cli.run("git add .", { cwd: cd1 });
    await cli.run('git commit -m "commit for push test"', { cwd: cd1 });

    const pushRepo = `push-test-repo-${randomUUID().slice(0, 8)}`;
    const repoUrl = `${worker.url}${pushRepo}.git`;
    await cli.run(`git remote add origin ${repoUrl}`, { cwd: cd1 });
    await cli.run("git push -u origin main", { cwd: cd1 });
    const output = await cli.run("git ls-remote origin", { cwd: cd1 });
    assert.match(output, /refs\/heads\/main/, "Push should create refs on remote");
  });

  void it("git receive-pack rejects stale old values", async () => {
    const cd1 = await cli.setup();

    await writeFile(join(cd1, "stale.txt"), "stale test");
    await cli.run("git add .", { cwd: cd1 });
    await cli.run('git commit -m "stale base"', { cwd: cd1 });

    const repo = `stale-push-${randomUUID().slice(0, 8)}`;
    const repoUrl = `${worker.url}${repo}.git`;
    await cli.run(`git remote add origin ${repoUrl}`, { cwd: cd1 });
    await cli.run("git push -u origin main", { cwd: cd1 });

    const headOid = (await cli.run("git rev-parse HEAD", { cwd: cd1 })).trim();
    const body = pktLine(
      `${ZERO_OID} ${headOid} refs/heads/main\0report-status delete-refs ofs-delta\n`,
    );

    const response = await fetch(`${repoUrl}/git-receive-pack`, {
      body: `${body}0000`,
      headers: {
        Accept: "application/x-git-receive-pack-result",
        "Content-Type": "application/x-git-receive-pack-request",
      },
      method: "POST",
    });
    const text = await response.text();

    assert.match(text, /ng refs\/heads\/main non-fast-forward/);
  });

  void it("git receive-pack rolls back atomic batches", async () => {
    const cd1 = await cli.setup();

    await writeFile(join(cd1, "atomic.txt"), "atomic test");
    await cli.run("git add .", { cwd: cd1 });
    await cli.run('git commit -m "atomic base"', { cwd: cd1 });

    const repo = `atomic-push-${randomUUID().slice(0, 8)}`;
    const repoUrl = `${worker.url}${repo}.git`;
    await cli.run(`git remote add origin ${repoUrl}`, { cwd: cd1 });
    await cli.run("git push -u origin main", { cwd: cd1 });

    const headOid = (await cli.run("git rev-parse HEAD", { cwd: cd1 })).trim();
    const body =
      pktLine(
        `${ZERO_OID} ${headOid} refs/heads/feature\0report-status delete-refs ofs-delta atomic\n`,
      ) + pktLine(`${ZERO_OID} ${headOid} refs/heads/main\n`);

    const response = await fetch(`${repoUrl}/git-receive-pack`, {
      body: `${body}0000`,
      headers: {
        Accept: "application/x-git-receive-pack-result",
        "Content-Type": "application/x-git-receive-pack-request",
      },
      method: "POST",
    });
    const text = await response.text();

    assert.match(text, /ng refs\/heads\/feature atomic push failed/);
    assert.match(text, /ng refs\/heads\/main non-fast-forward/);

    const advertisedRefs = await fetch(`${repoUrl}/info/refs?service=git-upload-pack`).then((res) =>
      res.text(),
    );
    assert.doesNotMatch(advertisedRefs, /refs\/heads\/feature/);
  });

  void it("git fetch", async () => {
    const cd1 = await cli.setup();
    const cd2 = await cli.setup();

    await writeFile(join(cd1, "version.txt"), "v1.0");
    await cli.run("git add .", { cwd: cd1 });
    await cli.run('git commit -m "version 1.0"', { cwd: cd1 });
    await writeFile(join(cd1, "version.txt"), "v1.1");
    await cli.run('git commit -am "version 1.1"', { cwd: cd1 });

    const fetchRepo = `fetch-test-repo-${randomUUID().slice(0, 8)}`;
    const repoUrl = `${worker.url}${fetchRepo}.git`;
    await cli.run(`git remote add origin ${repoUrl}`, { cwd: cd1 });
    await cli.run("git push -u origin main", { cwd: cd1 });

    await cli.run(`git -c init.defaultBranch=main clone ${repoUrl} cloned-repo`, { cwd: cd2 });
    const clonedDir2 = join(cd2, "cloned-repo");

    await cli.run("git fetch origin", { cwd: clonedDir2 });
    const output = await cli.run("git branch -r", { cwd: clonedDir2 });
    assert.match(output, /origin\/main/, "Fetch should retrieve main branch");
  });

  void it("git pull", async () => {
    const cd1 = await cli.setup();
    const cd2 = await cli.setup();

    // Setup initial repository with multiple commits
    await writeFile(join(cd1, "data.txt"), "initial data");
    await cli.run("git add .", { cwd: cd1 });
    await cli.run('git commit -m "initial commit"', { cwd: cd1 });
    await writeFile(join(cd1, "data.txt"), "updated data");
    await cli.run('git commit -am "update data"', { cwd: cd1 });

    const pullRepo = `pull-test-repo-${randomUUID().slice(0, 8)}`;
    const repoUrl = `${worker.url}${pullRepo}.git`;
    await cli.run(`git remote add origin ${repoUrl}`, { cwd: cd1 });
    await cli.run("git push -u origin main", { cwd: cd1 });

    // Setup second repository and add it as remote
    await cli.run(`git -c init.defaultBranch=main clone ${repoUrl} cloned-repo`, { cwd: cd2 });
    const clonedDir3 = join(cd2, "cloned-repo");

    // Pull changes into the second repository
    await cli.run("git pull origin main", { cwd: clonedDir3 });

    // Verify content was pulled correctly
    const dataTxt = await readFile(join(clonedDir3, "data.txt"), "utf-8");
    assert.match(dataTxt, /updated data/, "Pull should retrieve updated content");

    // Verify commit history was pulled
    const logOutput = await cli.run("git log --oneline", { cwd: clonedDir3 });
    assert.match(logOutput, /update data/, "Pull should include latest commits");
    assert.match(logOutput, /initial commit/, "Pull should preserve history");

    // Verify branch was set up correctly
    const branchOutput = await cli.run("git branch -r", { cwd: clonedDir3 });
    assert.match(branchOutput, /origin\/main/, "Pull should set up remote tracking");
  });

  void it("git clone --depth 1", async () => {
    const cd1 = await cli.setup();

    // Create 3 commits so there's history to truncate
    await writeFile(join(cd1, "file.txt"), "v1");
    await cli.run("git add .", { cwd: cd1 });
    await cli.run('git commit -m "commit 1"', { cwd: cd1 });

    await writeFile(join(cd1, "file.txt"), "v2");
    await cli.run('git commit -am "commit 2"', { cwd: cd1 });

    await writeFile(join(cd1, "file.txt"), "v3");
    await cli.run('git commit -am "commit 3"', { cwd: cd1 });

    const repo = `shallow-test-${randomUUID().slice(0, 8)}`;
    const repoUrl = `${worker.url}${repo}.git`;
    await cli.run(`git remote add origin ${repoUrl}`, { cwd: cd1 });
    await cli.run("git push -u origin main", { cwd: cd1 });

    // Shallow clone with depth 1
    const cd2 = await cli.setup();
    await cli.run(`git -c init.defaultBranch=main clone --depth 1 ${repoUrl} shallow-repo`, {
      cwd: cd2,
    });

    const shallowDir = join(cd2, "shallow-repo");

    // File should contain latest content
    const content = await readFile(join(shallowDir, "file.txt"), "utf-8");
    assert.equal(content, "v3");

    // Should have only 1 commit in history
    const log = await cli.run("git log --oneline", { cwd: shallowDir });
    const commits = log.trim().split("\n").filter(Boolean);
    assert.equal(commits.length, 1, "Shallow clone should have only 1 commit");

    // Should be marked as shallow
    const isShallow = await cli.run("git rev-parse --is-shallow-repository", {
      cwd: shallowDir,
    });
    assert.equal(isShallow.trim(), "true", "Repository should be shallow");
  });

  void it("git fetch --deepen", async () => {
    const cd1 = await cli.setup();

    // Create 3 commits
    await writeFile(join(cd1, "file.txt"), "v1");
    await cli.run("git add .", { cwd: cd1 });
    await cli.run('git commit -m "commit 1"', { cwd: cd1 });

    await writeFile(join(cd1, "file.txt"), "v2");
    await cli.run('git commit -am "commit 2"', { cwd: cd1 });

    await writeFile(join(cd1, "file.txt"), "v3");
    await cli.run('git commit -am "commit 3"', { cwd: cd1 });

    const repo = `deepen-test-${randomUUID().slice(0, 8)}`;
    const repoUrl = `${worker.url}${repo}.git`;
    await cli.run(`git remote add origin ${repoUrl}`, { cwd: cd1 });
    await cli.run("git push -u origin main", { cwd: cd1 });

    // Shallow clone with depth 1
    const cd2 = await cli.setup();
    await cli.run(`git -c init.defaultBranch=main clone --depth 1 ${repoUrl} deepen-repo`, {
      cwd: cd2,
    });

    const deepenDir = join(cd2, "deepen-repo");

    // Deepen by 1 more commit
    await cli.run("git fetch --deepen=1", { cwd: deepenDir });

    const log = await cli.run("git log --oneline origin/main", { cwd: deepenDir });
    const commits = log.trim().split("\n").filter(Boolean);
    assert.ok(commits.length >= 2, "Deepen should add at least one more commit");
  });

  void it("git fetch --unshallow", async () => {
    const cd1 = await cli.setup();

    // Create 3 commits
    await writeFile(join(cd1, "file.txt"), "v1");
    await cli.run("git add .", { cwd: cd1 });
    await cli.run('git commit -m "commit 1"', { cwd: cd1 });

    await writeFile(join(cd1, "file.txt"), "v2");
    await cli.run('git commit -am "commit 2"', { cwd: cd1 });

    await writeFile(join(cd1, "file.txt"), "v3");
    await cli.run('git commit -am "commit 3"', { cwd: cd1 });

    const repo = `unshallow-test-${randomUUID().slice(0, 8)}`;
    const repoUrl = `${worker.url}${repo}.git`;
    await cli.run(`git remote add origin ${repoUrl}`, { cwd: cd1 });
    await cli.run("git push -u origin main", { cwd: cd1 });

    // Shallow clone with depth 1
    const cd2 = await cli.setup();
    await cli.run(`git -c init.defaultBranch=main clone --depth 1 ${repoUrl} unshallow-repo`, {
      cwd: cd2,
    });

    const unshallowDir = join(cd2, "unshallow-repo");

    // Unshallow — fetch full history
    await cli.run("git fetch --unshallow", { cwd: unshallowDir });

    // Should have all 3 commits now
    const log = await cli.run("git log --oneline origin/main", { cwd: unshallowDir });
    const commits = log.trim().split("\n").filter(Boolean);
    assert.equal(commits.length, 3, "Unshallow should fetch all commits");

    // Should no longer be shallow
    const isShallow = await cli.run("git rev-parse --is-shallow-repository", {
      cwd: unshallowDir,
    });
    assert.equal(isShallow.trim(), "false", "Repository should no longer be shallow");
  });

  void it("git clone (protocol v2)", async () => {
    const cd1 = await cli.setup();

    await writeFile(join(cd1, "hello.txt"), "protocol v2 test");
    await cli.run("git add .", { cwd: cd1 });
    await cli.run('git commit -m "v2 commit"', { cwd: cd1 });

    const repo = `v2-test-${randomUUID().slice(0, 8)}`;
    const repoUrl = `${worker.url}${repo}.git`;
    await cli.run(`git remote add origin ${repoUrl}`, { cwd: cd1 });
    await cli.run("git push -u origin main", { cwd: cd1 });

    // Clone using protocol v2
    const cd2 = await cli.setup();
    await cli.run(
      `git -c protocol.version=2 -c init.defaultBranch=main clone ${repoUrl} v2-cloned`,
      { cwd: cd2 },
    );

    const clonedDir = join(cd2, "v2-cloned");
    const content = await readFile(join(clonedDir, "hello.txt"), "utf-8");
    assert.equal(content, "protocol v2 test");

    // Verify commit came through
    const log = await cli.run("git log --oneline", { cwd: clonedDir });
    assert.match(log, /v2 commit/);
  });

  void it("git fetch (protocol v2)", async () => {
    const cd1 = await cli.setup();

    await writeFile(join(cd1, "data.txt"), "initial");
    await cli.run("git add .", { cwd: cd1 });
    await cli.run('git commit -m "first"', { cwd: cd1 });

    const repo = `v2-fetch-${randomUUID().slice(0, 8)}`;
    const repoUrl = `${worker.url}${repo}.git`;
    await cli.run(`git remote add origin ${repoUrl}`, { cwd: cd1 });
    await cli.run("git push -u origin main", { cwd: cd1 });

    // Clone with v2
    const cd2 = await cli.setup();
    await cli.run(
      `git -c protocol.version=2 -c init.defaultBranch=main clone ${repoUrl} v2-fetch-repo`,
      { cwd: cd2 },
    );
    const clonedDir = join(cd2, "v2-fetch-repo");

    // Push another commit from cd1
    await writeFile(join(cd1, "data.txt"), "updated");
    await cli.run('git commit -am "second"', { cwd: cd1 });
    await cli.run("git push origin main", { cwd: cd1 });

    // Fetch with v2
    await cli.run("git -c protocol.version=2 fetch origin", { cwd: clonedDir });
    await cli.run("git merge origin/main", { cwd: clonedDir });
    const content = await readFile(join(clonedDir, "data.txt"), "utf-8");
    assert.equal(content, "updated");
  });

  void it("git clone --depth 1 (protocol v2)", async () => {
    const cd1 = await cli.setup();

    await writeFile(join(cd1, "file.txt"), "v1");
    await cli.run("git add .", { cwd: cd1 });
    await cli.run('git commit -m "c1"', { cwd: cd1 });

    await writeFile(join(cd1, "file.txt"), "v2");
    await cli.run('git commit -am "c2"', { cwd: cd1 });

    await writeFile(join(cd1, "file.txt"), "v3");
    await cli.run('git commit -am "c3"', { cwd: cd1 });

    const repo = `v2-shallow-${randomUUID().slice(0, 8)}`;
    const repoUrl = `${worker.url}${repo}.git`;
    await cli.run(`git remote add origin ${repoUrl}`, { cwd: cd1 });
    await cli.run("git push -u origin main", { cwd: cd1 });

    const cd2 = await cli.setup();
    await cli.run(
      `git -c protocol.version=2 -c init.defaultBranch=main clone --depth 1 ${repoUrl} v2-shallow`,
      { cwd: cd2 },
    );

    const shallowDir = join(cd2, "v2-shallow");
    const content = await readFile(join(shallowDir, "file.txt"), "utf-8");
    assert.equal(content, "v3");

    const log = await cli.run("git log --oneline", { cwd: shallowDir });
    const commits = log.trim().split("\n").filter(Boolean);
    assert.equal(commits.length, 1, "Shallow clone should have only 1 commit");
  });

  void it("v2 info/refs returns capabilities", async () => {
    const cd1 = await cli.setup();

    await writeFile(join(cd1, "test.txt"), "cap test");
    await cli.run("git add .", { cwd: cd1 });
    await cli.run('git commit -m "init"', { cwd: cd1 });

    const repo = `v2-cap-${randomUUID().slice(0, 8)}`;
    const repoUrl = `${worker.url}${repo}.git`;
    await cli.run(`git remote add origin ${repoUrl}`, { cwd: cd1 });
    await cli.run("git push -u origin main", { cwd: cd1 });

    // Hit info/refs with v2 header directly
    const resp = await fetch(`${repoUrl}/info/refs?service=git-upload-pack`, {
      headers: { "Git-Protocol": "version=2" },
    });
    const text = await resp.text();

    assert.ok(text.includes("version 2"), "Should contain version 2");
    assert.ok(text.includes("ls-refs"), "Should advertise ls-refs");
    assert.ok(text.includes("fetch"), "Should advertise fetch");
    assert.ok(text.includes("object-format=sha1"), "Should advertise object-format");
  });

  void it("v1 fallback when no Git-Protocol header", async () => {
    const cd1 = await cli.setup();

    await writeFile(join(cd1, "test.txt"), "v1 fallback test");
    await cli.run("git add .", { cwd: cd1 });
    await cli.run('git commit -m "init"', { cwd: cd1 });

    const repo = `v1-fallback-${randomUUID().slice(0, 8)}`;
    const repoUrl = `${worker.url}${repo}.git`;
    await cli.run(`git remote add origin ${repoUrl}`, { cwd: cd1 });
    await cli.run("git push -u origin main", { cwd: cd1 });

    // Clone without v2 (force v1)
    const cd2 = await cli.setup();
    await cli.run(
      `git -c protocol.version=1 -c init.defaultBranch=main clone ${repoUrl} v1-clone`,
      { cwd: cd2 },
    );

    const content = await readFile(join(cd2, "v1-clone", "test.txt"), "utf-8");
    assert.equal(content, "v1 fallback test");
  });
});
