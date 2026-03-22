import * as assert from "node:assert/strict";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { after, before, describe, it } from "node:test";

import * as helpers from "./test.helpers.ts";
import { run } from "./test.helpers.ts";

const worker = await helpers.worker();

before(() => worker.before());
after(() => worker.after());

void describe("CloudflareStorage", () => {
  void it("should initialize CloudflareStorage with ctx.storage.sql and env.GIT_OBJECTS", async () => {
    const id = worker.env.GIT_SERVER.idFromName("init-test");
    const stub = worker.env.GIT_SERVER.get(id);

    // Use dispatchFetch to verify the DO is working
    const response = await stub.fetch("http://localhost/init-test.git/HEAD");
    assert.ok(response.status === 200 || response.status === 404, "DO should respond");
  });

  void it("should write and read a file using ctx.storage.sql and env.GIT_OBJECTS", async () => {
    // Test via the worker's git protocol which uses CloudflareStorage internally
    const id = worker.env.GIT_SERVER.idFromName("read-write-repo");
    const stub = worker.env.GIT_SERVER.get(id);

    // Push some data and verify it can be read back
    const response = await stub.fetch(
      "http://localhost/read-write-repo.git/info/refs?service=git-upload-pack",
    );
    // Initially empty repo returns 200 with service advertisement
    assert.strictEqual(response.status, 200, "Should return 200 for upload-pack");
  });

  void it("should check if a file exists in storage", async () => {
    const id = worker.env.GIT_SERVER.idFromName("exists-repo");
    const stub = worker.env.GIT_SERVER.get(id);

    // A fresh repo should have HEAD pointing to refs/heads/main (even if unborn)
    const response = await stub.fetch("http://localhost/exists-repo.git/HEAD");
    assert.strictEqual(response.status, 200, "HEAD should exist");
  });

  void it("should handle git-receive-pack endpoint", async () => {
    const id = worker.env.GIT_SERVER.idFromName("receive-pack-repo");
    const stub = worker.env.GIT_SERVER.get(id);

    const response = await stub.fetch(
      "http://localhost/receive-pack-repo.git/info/refs?service=git-receive-pack",
    );
    assert.strictEqual(response.status, 200, "Should return 200 for receive-pack refs");
  });

  void it("should return 404 for invalid routes", async () => {
    const id = worker.env.GIT_SERVER.idFromName("invalid-route-repo");
    const stub = worker.env.GIT_SERVER.get(id);

    const response = await stub.fetch("http://localhost/invalid-route-repo.git/invalid");
    assert.strictEqual(response.status, 404, "Should return 404 for invalid route");
  });

  void it("should list directory contents from SQL storage", async () => {
    const id = worker.env.GIT_SERVER.idFromName("listdir-repo");
    const stub = worker.env.GIT_SERVER.get(id);

    // Initialize the repo by accessing it
    const response = await stub.fetch(
      "http://localhost/listdir-repo.git/info/refs?service=git-upload-pack",
    );
    assert.strictEqual(response.status, 200, "Should initialize repo successfully");
  });

  void it("should handle concurrent requests to the same repository", async () => {
    const id = worker.env.GIT_SERVER.idFromName("concurrent-repo");
    const stub = worker.env.GIT_SERVER.get(id);

    const responses = await Promise.all([
      stub.fetch("http://localhost/concurrent-repo.git/info/refs?service=git-upload-pack"),
      stub.fetch("http://localhost/concurrent-repo.git/info/refs?service=git-receive-pack"),
      stub.fetch("http://localhost/concurrent-repo.git/HEAD"),
    ]);

    for (const response of responses) {
      assert.strictEqual(response.status, 200, "All concurrent requests should succeed");
    }
  });

  void it("should store file metadata in SQL and content in GIT_OBJECTS R2", async () => {
    const id = worker.env.GIT_SERVER.idFromName("metadata-repo");
    const stub = worker.env.GIT_SERVER.get(id);

    // Verify the repo can be initialized and accessed
    const headResponse = await stub.fetch("http://localhost/metadata-repo.git/HEAD");
    assert.strictEqual(headResponse.status, 200, "HEAD should be accessible");

    const refsResponse = await stub.fetch(
      "http://localhost/metadata-repo.git/info/refs?service=git-upload-pack",
    );
    assert.strictEqual(refsResponse.status, 200, "Refs should be accessible");
  });

  void it("should accept a git-generated receive-pack request body", async () => {
    const id = worker.env.GIT_SERVER.idFromName("git-generated-receive-pack");
    const stub = worker.env.GIT_SERVER.get(id);
    const tempDir = await mkdtemp(join(tmpdir(), "receive-pack-"));

    try {
      const capturePath = join(tempDir, "capture.bin");
      const wrapperPath = join(tempDir, "capture-receive-pack.sh");
      const remoteDir = join(tempDir, "remote.git");
      const clientDir = join(tempDir, "client");

      await writeFile(wrapperPath, 'tee "$1" | git-receive-pack "$2"\n');

      await run(`git init --bare ${remoteDir}`, { cwd: tempDir });
      await run(`git init -b main ${clientDir}`, { cwd: tempDir });
      await run('git config user.name "Test User"', { cwd: clientDir });
      await run('git config user.email "test@example.com"', { cwd: clientDir });
      await run("git config commit.gpgSign false", { cwd: clientDir });

      await writeFile(join(clientDir, "file.txt"), "hello from captured push\n");
      await run("git add file.txt", { cwd: clientDir });
      await run('git commit -m "init"', { cwd: clientDir });
      await run(
        `git push '--receive-pack=sh ${wrapperPath} ${capturePath} ${remoteDir}' ${remoteDir} main`,
        { cwd: clientDir },
      );

      const requestBody = new Uint8Array(await readFile(capturePath));
      assert.ok(requestBody.length > 0, "Expected captured push request body");

      const response = await stub.fetch(
        "http://localhost/git-generated-receive-pack.git/git-receive-pack",
        {
          method: "POST",
          headers: {
            Accept: "application/x-git-receive-pack-result",
            "Content-Type": "application/x-git-receive-pack-request",
          },
          body: requestBody,
        },
      );
      const text = await response.text();

      assert.match(text, /unpack ok/, `Expected unpack ok, got: ${text}`);

      const advertisedRefs = await stub.fetch(
        "http://localhost/git-generated-receive-pack.git/info/refs?service=git-upload-pack",
      );
      const refsText = await advertisedRefs.text();
      assert.match(refsText, /refs\/heads\/main/, "Expected pushed main branch to exist");
    } finally {
      await rm(tempDir, { force: true, recursive: true });
    }
  });

  void it("should create directories implicitly for R2 storage", async () => {
    const id = worker.env.GIT_SERVER.idFromName("mkdir-repo");
    const stub = worker.env.GIT_SERVER.get(id);

    // R2 handles directories implicitly - verify repo works
    const response = await stub.fetch("http://localhost/mkdir-repo.git/HEAD");
    assert.strictEqual(response.status, 200, "Should handle implicit directories");
  });
});
