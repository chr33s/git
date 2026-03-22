import * as assert from "node:assert/strict";
import { describe, it } from "node:test";

import { GitRepository } from "./git.repository.ts";
import { MemoryStorage } from "./git.storage.ts";
import { ServerApi, type ServerApiRequest } from "./server.api.ts";

function createRequest(url: string, method: string = "GET", body?: Record<string, unknown>) {
  let bodyStream: ServerApiRequest["body"] = null;

  if (body) {
    const encoder = new TextEncoder();
    const data = encoder.encode(JSON.stringify(body));
    bodyStream = new ReadableStream({
      start(controller) {
        controller.enqueue(data);
        controller.close();
      },
    });
  }

  return { url, method, body: bodyStream };
}

async function setupRepo() {
  const storage = new MemoryStorage();
  const config = { repoName: "test" };
  const repo = new GitRepository(storage, config);
  await repo.init();
  const api = new ServerApi(repo);
  return { repo, api };
}

async function setupRepoWithCommit() {
  const { repo, api } = await setupRepo();

  // Add a file and commit
  const content = new TextEncoder().encode("Hello, World!");
  await repo.add("test.txt", content);
  const commitOid = await repo.commit("Initial commit", {
    name: "Test",
    email: "test@example.com",
  });

  return { repo, api, commitOid };
}

void describe("fetch routing", () => {
  void it("should return 404 for unknown routes", async () => {
    const { api } = await setupRepo();
    const request = createRequest("http://localhost/api/test/unknown", "GET");
    const response = await api.fetch(request);

    assert.equal(response.status, 404);
    const json = (await response.json()) as { error: string };
    assert.equal(json.error, "Not Found");
  });

  void it("should return 404 for wrong method", async () => {
    const { api } = await setupRepo();
    const request = createRequest("http://localhost/api/test/status", "POST");
    const response = await api.fetch(request);

    assert.equal(response.status, 404);
  });
});

void describe("status endpoint", () => {
  void it("should return status for empty repository", async () => {
    const { api } = await setupRepo();
    const request = createRequest("http://localhost/api/test/status", "GET");
    const response = await api.fetch(request);

    assert.equal(response.status, 200);
    const json = (await response.json()) as {
      staged: string[];
      modified: string[];
      untracked: string[];
    };
    assert.ok(Array.isArray(json.staged));
    assert.ok(Array.isArray(json.modified));
    assert.ok(Array.isArray(json.untracked));
  });

  void it("should work with .git suffix", async () => {
    const { api } = await setupRepo();
    const request = createRequest("http://localhost/api/test.git/status", "GET");
    const response = await api.fetch(request);

    assert.equal(response.status, 200);
  });
});

void describe("log endpoint", () => {
  void it("should return empty log for new repository", async () => {
    const { api } = await setupRepo();
    const request = createRequest("http://localhost/api/test/log", "POST", {});
    const response = await api.fetch(request);

    assert.equal(response.status, 200);
    const json = (await response.json()) as { commits: unknown[] };
    assert.ok(Array.isArray(json.commits));
    assert.equal(json.commits.length, 0);
  });

  void it("should return commits", async () => {
    const { api, commitOid } = await setupRepoWithCommit();
    const request = createRequest("http://localhost/api/test/log", "POST", {});
    const response = await api.fetch(request);

    assert.equal(response.status, 200);
    const json = (await response.json()) as { commits: { oid: string; message: string }[] };
    assert.equal(json.commits.length, 1);
    assert.equal(json.commits[0]!.oid, commitOid);
    assert.ok(json.commits[0]!.message.includes("Initial commit"));
  });

  void it("should respect maxCount", async () => {
    const { repo, api } = await setupRepoWithCommit();

    // Add more commits
    await repo.add("file2.txt", new TextEncoder().encode("content2"));
    await repo.commit("Second commit", { name: "Test", email: "test@example.com" });
    await repo.add("file3.txt", new TextEncoder().encode("content3"));
    await repo.commit("Third commit", { name: "Test", email: "test@example.com" });

    const request = createRequest("http://localhost/api/test/log", "POST", { maxCount: 2 });
    const response = await api.fetch(request);

    const json = (await response.json()) as { commits: unknown[] };
    assert.equal(json.commits.length, 2);
  });
});

void describe("show endpoint", () => {
  void it("should show commit by ref", async () => {
    const { api, commitOid } = await setupRepoWithCommit();
    const request = createRequest("http://localhost/api/test/show", "POST", { ref: commitOid });
    const response = await api.fetch(request);

    assert.equal(response.status, 200);
    const json = (await response.json()) as {
      oid: string;
      type: string;
      tree: string;
      author: string;
    };
    assert.equal(json.oid, commitOid);
    assert.equal(json.type, "commit");
    assert.ok(json.tree);
    assert.ok(json.author);
  });

  void it("should default to HEAD", async () => {
    const { api } = await setupRepoWithCommit();
    const request = createRequest("http://localhost/api/test/show", "POST", {
      ref: "refs/heads/main",
    });
    const response = await api.fetch(request);

    assert.equal(response.status, 200);
    const json = (await response.json()) as { type: string };
    assert.equal(json.type, "commit");
  });
});

void describe("branch endpoint", () => {
  void it("should list branches", async () => {
    const { api } = await setupRepoWithCommit();
    const request = createRequest("http://localhost/api/test/branch", "POST", {});
    const response = await api.fetch(request);

    assert.equal(response.status, 200);
    const json = (await response.json()) as { branches: string[] };
    assert.ok(Array.isArray(json.branches));
    assert.ok(json.branches.includes("main"));
  });

  void it("should create a new branch", async () => {
    const { api } = await setupRepoWithCommit();
    const request = createRequest("http://localhost/api/test/branch", "POST", {
      name: "feature",
    });
    const response = await api.fetch(request);

    assert.equal(response.status, 200);
    const json = (await response.json()) as { created: string };
    assert.equal(json.created, "feature");
  });

  void it("should reject overwriting an existing branch", async () => {
    const { api } = await setupRepoWithCommit();

    await api.fetch(createRequest("http://localhost/api/test/branch", "POST", { name: "feature" }));

    const response = await api.fetch(
      createRequest("http://localhost/api/test/branch", "POST", { name: "feature" }),
    );

    assert.equal(response.status, 409);
    const json = (await response.json()) as { error: string };
    assert.equal(json.error, "Branch 'feature' already exists");
  });

  void it("should delete a branch", async () => {
    const { api } = await setupRepoWithCommit();

    // Create branch first
    await api.fetch(createRequest("http://localhost/api/test/branch", "POST", { name: "feature" }));

    // Delete it
    const request = createRequest("http://localhost/api/test/branch", "POST", {
      name: "feature",
      delete: true,
    });
    const response = await api.fetch(request);

    assert.equal(response.status, 200);
    const json = (await response.json()) as { deleted: string };
    assert.equal(json.deleted, "feature");
  });

  void it("should fail to create branch without HEAD commit", async () => {
    const { api } = await setupRepo();
    const request = createRequest("http://localhost/api/test/branch", "POST", {
      name: "feature",
    });
    const response = await api.fetch(request);

    assert.equal(response.status, 400);
    const json = (await response.json()) as { error: string };
    assert.equal(json.error, "No HEAD commit");
  });
});

void describe("checkout endpoint", () => {
  void it("should checkout a branch", async () => {
    const { api } = await setupRepoWithCommit();

    // Create a branch
    await api.fetch(createRequest("http://localhost/api/test/branch", "POST", { name: "feature" }));

    const request = createRequest("http://localhost/api/test/checkout", "POST", {
      ref: "refs/heads/feature",
    });
    const response = await api.fetch(request);

    assert.equal(response.status, 200);
    const json = (await response.json()) as { success: boolean; ref: string };
    assert.equal(json.success, true);
    assert.equal(json.ref, "refs/heads/feature");
  });

  void it("should require ref parameter", async () => {
    const { api } = await setupRepoWithCommit();
    const request = createRequest("http://localhost/api/test/checkout", "POST", {});
    const response = await api.fetch(request);

    assert.equal(response.status, 400);
    const json = (await response.json()) as { error: string };
    assert.equal(json.error, "ref required");
  });
});

void describe("commit endpoint", () => {
  void it("should create a commit", async () => {
    const { repo, api } = await setupRepo();

    await repo.add("new.txt", new TextEncoder().encode("new content"));

    const request = createRequest("http://localhost/api/test/commit", "POST", {
      message: "New commit",
      author: { name: "Author", email: "author@example.com" },
    });
    const response = await api.fetch(request);

    assert.equal(response.status, 200);
    const json = (await response.json()) as { success: boolean; oid: string };
    assert.equal(json.success, true);
    assert.ok(json.oid);
    assert.equal(json.oid.length, 40);
  });

  void it("should require message", async () => {
    const { api } = await setupRepo();
    const request = createRequest("http://localhost/api/test/commit", "POST", {});
    const response = await api.fetch(request);

    assert.equal(response.status, 400);
    const json = (await response.json()) as { error: string };
    assert.equal(json.error, "message required");
  });
});

void describe("add endpoint", () => {
  void it("should add a file with content", async () => {
    const { api } = await setupRepo();
    const request = createRequest("http://localhost/api/test/add", "POST", {
      path: "new.txt",
      content: "file content",
    });
    const response = await api.fetch(request);

    assert.equal(response.status, 200);
    const json = (await response.json()) as { success: boolean; path: string };
    assert.equal(json.success, true);
    assert.equal(json.path, "new.txt");
  });

  void it("should require path", async () => {
    const { api } = await setupRepo();
    const request = createRequest("http://localhost/api/test/add", "POST", { content: "test" });
    const response = await api.fetch(request);

    assert.equal(response.status, 400);
    const json = (await response.json()) as { error: string };
    assert.equal(json.error, "path required");
  });
});

void describe("rm endpoint", () => {
  void it("should remove a file from index", async () => {
    const { api } = await setupRepoWithCommit();

    const request = createRequest("http://localhost/api/test/rm", "POST", {
      paths: "test.txt",
      cached: true,
    });
    const response = await api.fetch(request);

    assert.equal(response.status, 200);
    const json = (await response.json()) as { success: boolean };
    assert.equal(json.success, true);
  });

  void it("should require paths", async () => {
    const { api } = await setupRepoWithCommit();
    const request = createRequest("http://localhost/api/test/rm", "POST", {});
    const response = await api.fetch(request);

    assert.equal(response.status, 400);
    const json = (await response.json()) as { error: string };
    assert.equal(json.error, "paths required");
  });

  void it("should error for non-existent path", async () => {
    const { api } = await setupRepoWithCommit();
    const request = createRequest("http://localhost/api/test/rm", "POST", {
      paths: "nonexistent.txt",
    });
    const response = await api.fetch(request);

    assert.equal(response.status, 400);
    const json = (await response.json()) as { error: string };
    assert.ok(json.error.includes("did not match"));
  });
});

void describe("refs endpoint", () => {
  void it("should return all refs", async () => {
    const { api } = await setupRepoWithCommit();
    const request = createRequest("http://localhost/api/test/refs", "GET");
    const response = await api.fetch(request);

    assert.equal(response.status, 200);
    const json = (await response.json()) as { refs: unknown[] };
    assert.ok(Array.isArray(json.refs));
  });
});

void describe("reflog endpoint", () => {
  void it("should return reflog entries for a ref", async () => {
    const { api } = await setupRepoWithCommit();
    const request = createRequest("http://localhost/api/test/reflog/refs%2Fheads%2Fmain", "GET");
    const response = await api.fetch(request);

    assert.equal(response.status, 200);
    const json = (await response.json()) as {
      entries: Array<{ oldOid: string; newOid: string; timestamp: string; message: string }>;
      ref: string;
    };
    assert.equal(json.ref, "refs/heads/main");
    assert.ok(json.entries.length >= 1);
    assert.equal(json.entries[0]?.newOid.length, 40);
  });
});

void describe("tag endpoint", () => {
  void it("should reject overwriting an existing tag", async () => {
    const { api } = await setupRepoWithCommit();

    let response = await api.fetch(
      createRequest("http://localhost/api/test/tag", "POST", { name: "v1.0.0" }),
    );
    assert.equal(response.status, 200);

    response = await api.fetch(
      createRequest("http://localhost/api/test/tag", "POST", { name: "v1.0.0" }),
    );

    assert.equal(response.status, 409);
    const json = (await response.json()) as { error: string };
    assert.equal(json.error, "Tag 'v1.0.0' already exists");
  });
});

void describe("tag endpoint", () => {
  void it("should create a tag", async () => {
    const { api, commitOid } = await setupRepoWithCommit();
    const request = createRequest("http://localhost/api/test/tag", "POST", { name: "v1.0" });
    const response = await api.fetch(request);

    assert.equal(response.status, 200);
    const json = (await response.json()) as { created: string; oid: string };
    assert.equal(json.created, "v1.0");
    assert.equal(json.oid, commitOid);
  });

  void it("should create a tag pointing to specific ref", async () => {
    const { api, commitOid } = await setupRepoWithCommit();
    const request = createRequest("http://localhost/api/test/tag", "POST", {
      name: "v1.0",
      ref: commitOid,
    });
    const response = await api.fetch(request);

    assert.equal(response.status, 200);
    const json = (await response.json()) as { created: string; oid: string };
    assert.equal(json.created, "v1.0");
    assert.equal(json.oid, commitOid);
  });

  void it("should delete a tag", async () => {
    const { api } = await setupRepoWithCommit();

    // Create tag first
    await api.fetch(createRequest("http://localhost/api/test/tag", "POST", { name: "v1.0" }));

    // Delete it
    const request = createRequest("http://localhost/api/test/tag", "POST", {
      name: "v1.0",
      delete: true,
    });
    const response = await api.fetch(request);

    assert.equal(response.status, 200);
    const json = (await response.json()) as { deleted: string };
    assert.equal(json.deleted, "v1.0");
  });

  void it("should require name", async () => {
    const { api } = await setupRepoWithCommit();
    const request = createRequest("http://localhost/api/test/tag", "POST", {});
    const response = await api.fetch(request);

    assert.equal(response.status, 400);
    const json = (await response.json()) as { error: string };
    assert.equal(json.error, "name required");
  });

  void it("should fail without HEAD commit", async () => {
    const { api } = await setupRepo();
    const request = createRequest("http://localhost/api/test/tag", "POST", { name: "v1.0" });
    const response = await api.fetch(request);

    assert.equal(response.status, 400);
    const json = (await response.json()) as { error: string };
    assert.equal(json.error, "No HEAD commit");
  });
});

void describe("merge endpoint", () => {
  void it("should require ref parameter", async () => {
    const { api } = await setupRepoWithCommit();
    const request = createRequest("http://localhost/api/test/merge", "POST", {});
    const response = await api.fetch(request);

    assert.equal(response.status, 400);
    const json = (await response.json()) as { error: string };
    assert.equal(json.error, "ref required");
  });

  void it("should fail without HEAD commit", async () => {
    const { api } = await setupRepo();
    const request = createRequest("http://localhost/api/test/merge", "POST", { ref: "feature" });
    const response = await api.fetch(request);

    assert.equal(response.status, 400);
    const json = (await response.json()) as { error: string };
    assert.equal(json.error, "No HEAD commit");
  });
});

void describe("reset endpoint", () => {
  void it("should reset to HEAD", async () => {
    const { api } = await setupRepoWithCommit();
    const request = createRequest("http://localhost/api/test/reset", "POST", {
      ref: "refs/heads/main",
    });
    const response = await api.fetch(request);

    assert.equal(response.status, 200);
    const json = (await response.json()) as { success: boolean; ref: string };
    assert.equal(json.success, true);
    assert.equal(json.ref, "refs/heads/main");
  });

  void it("should reset to specific ref", async () => {
    const { api, commitOid } = await setupRepoWithCommit();
    const request = createRequest("http://localhost/api/test/reset", "POST", { ref: commitOid });
    const response = await api.fetch(request);

    assert.equal(response.status, 200);
    const json = (await response.json()) as { success: boolean; ref: string };
    assert.equal(json.success, true);
    assert.equal(json.ref, commitOid);
  });

  void it("should support hard reset", async () => {
    const { api, commitOid } = await setupRepoWithCommit();
    const request = createRequest("http://localhost/api/test/reset", "POST", {
      ref: commitOid,
      hard: true,
    });
    const response = await api.fetch(request);

    assert.equal(response.status, 200);
    const json = (await response.json()) as { success: boolean };
    assert.equal(json.success, true);
  });
});

void describe("read endpoint", () => {
  void it("should read a file", async () => {
    const { repo, api } = await setupRepo();

    await repo.writeFile("readme.txt", new TextEncoder().encode("Hello World"));

    const request = createRequest("http://localhost/api/test/read", "POST", {
      path: "readme.txt",
    });
    const response = await api.fetch(request);

    assert.equal(response.status, 200);
    const json = (await response.json()) as { path: string; content: string };
    assert.equal(json.path, "readme.txt");
    assert.equal(json.content, "Hello World");
  });

  void it("should require path", async () => {
    const { api } = await setupRepo();
    const request = createRequest("http://localhost/api/test/read", "POST", {});
    const response = await api.fetch(request);

    assert.equal(response.status, 400);
    const json = (await response.json()) as { error: string };
    assert.equal(json.error, "path required");
  });
});

void describe("write endpoint", () => {
  void it("should write a file", async () => {
    const { api } = await setupRepo();
    const request = createRequest("http://localhost/api/test/write", "POST", {
      path: "output.txt",
      content: "Written content",
    });
    const response = await api.fetch(request);

    assert.equal(response.status, 200);
    const json = (await response.json()) as { success: boolean; path: string };
    assert.equal(json.success, true);
    assert.equal(json.path, "output.txt");
  });

  void it("should require path and content", async () => {
    const { api } = await setupRepo();

    const request1 = createRequest("http://localhost/api/test/write", "POST", {
      path: "test.txt",
    });
    const response1 = await api.fetch(request1);
    assert.equal(response1.status, 400);

    const request2 = createRequest("http://localhost/api/test/write", "POST", {
      content: "test",
    });
    const response2 = await api.fetch(request2);
    assert.equal(response2.status, 400);
  });
});

void describe("tree endpoint", () => {
  void it("should return tree for HEAD", async () => {
    const { api } = await setupRepoWithCommit();
    const request = createRequest("http://localhost/api/test/tree", "POST", {
      ref: "refs/heads/main",
    });
    const response = await api.fetch(request);

    assert.equal(response.status, 200);
    const json = (await response.json()) as { oid: string; entries: unknown[] };
    assert.ok(json.oid);
    assert.ok(Array.isArray(json.entries));
    assert.ok(json.entries.length > 0);
  });

  void it("should return tree for specific ref", async () => {
    const { api, commitOid } = await setupRepoWithCommit();
    const request = createRequest("http://localhost/api/test/tree", "POST", { ref: commitOid });
    const response = await api.fetch(request);

    assert.equal(response.status, 200);
    const json = (await response.json()) as { entries: { name: string }[] };
    assert.ok(json.entries.some((e) => e.name === "test.txt"));
  });

  void it("should error for non-commit ref", async () => {
    const { repo, api } = await setupRepoWithCommit();

    // Get blob oid
    const blobOid = await repo.writeObject("blob", new TextEncoder().encode("test"));

    const request = createRequest("http://localhost/api/test/tree", "POST", { ref: blobOid });
    const response = await api.fetch(request);

    assert.equal(response.status, 400);
    const json = (await response.json()) as { error: string };
    assert.equal(json.error, "Not a commit");
  });
});

void describe("diff endpoint", () => {
  void it("should return diff from HEAD", async () => {
    const { api, commitOid } = await setupRepoWithCommit();
    const request = createRequest("http://localhost/api/test/diff", "POST", {
      to: "refs/heads/main",
    });
    const response = await api.fetch(request);

    assert.equal(response.status, 200);
    const json = (await response.json()) as { to: string; changes: unknown[] };
    assert.equal(json.to, commitOid);
    assert.ok(Array.isArray(json.changes));
  });

  void it("should diff specific commits", async () => {
    const { api, commitOid } = await setupRepoWithCommit();
    const request = createRequest("http://localhost/api/test/diff", "POST", {
      from: commitOid,
      to: commitOid,
    });
    const response = await api.fetch(request);

    assert.equal(response.status, 200);
    const json = (await response.json()) as { from: string; to: string };
    assert.equal(json.from, commitOid);
    assert.equal(json.to, commitOid);
  });

  void it("should filter by path", async () => {
    const { repo, api } = await setupRepo();

    await repo.add("src/file1.txt", new TextEncoder().encode("content1"));
    await repo.add("src/file2.txt", new TextEncoder().encode("content2"));
    await repo.add("other.txt", new TextEncoder().encode("other"));
    await repo.commit("Multiple files", { name: "Test", email: "test@example.com" });

    const request = createRequest("http://localhost/api/test/diff", "POST", { path: "src" });
    const response = await api.fetch(request);

    assert.equal(response.status, 200);
    const json = (await response.json()) as { changes: { path: string }[] };
    assert.ok(json.changes.every((c) => c.path.startsWith("src")));
  });
});

void describe("object endpoint", () => {
  void it("should read object by oid", async () => {
    const { repo, api } = await setupRepo();

    const blobContent = new TextEncoder().encode("blob content");
    const oid = await repo.writeObject("blob", blobContent);

    const request = createRequest("http://localhost/api/test/object", "POST", { oid });
    const response = await api.fetch(request);

    assert.equal(response.status, 200);
    const json = (await response.json()) as { oid: string; type: string; data: string };
    assert.equal(json.oid, oid);
    assert.equal(json.type, "blob");
    assert.equal(json.data, "blob content");
  });

  void it("should require oid", async () => {
    const { api } = await setupRepo();
    const request = createRequest("http://localhost/api/test/object", "POST", {});
    const response = await api.fetch(request);

    assert.equal(response.status, 400);
    const json = (await response.json()) as { error: string };
    assert.equal(json.error, "oid required");
  });
});

void describe("error handling", () => {
  void it("should handle empty body", async () => {
    const { api } = await setupRepo();
    const request = { url: "http://localhost/api/test/log", method: "POST", body: null };
    const response = await api.fetch(request);

    assert.equal(response.status, 200);
  });

  void it("should handle abort signal", async () => {
    const { api } = await setupRepo();
    const controller = new AbortController();
    controller.abort();

    const request = createRequest("http://localhost/api/test/status", "GET");

    await assert.rejects(async () => {
      await api.fetch(request, controller.signal);
    });
  });
});

void describe("mv endpoint", () => {
  void it("should move a file", async () => {
    const { repo, api } = await setupRepo();

    await repo.writeFile("source.txt", new TextEncoder().encode("content"));
    await repo.add("source.txt", new TextEncoder().encode("content"));

    const request = createRequest("http://localhost/api/test/mv", "POST", {
      source: "source.txt",
      destination: "dest.txt",
    });
    const response = await api.fetch(request);

    assert.equal(response.status, 200);
    const json = (await response.json()) as {
      success: boolean;
      source: string;
      destination: string;
    };
    assert.equal(json.success, true);
    assert.equal(json.source, "source.txt");
    assert.equal(json.destination, "dest.txt");
  });

  void it("should require source and destination", async () => {
    const { api } = await setupRepo();
    const request = createRequest("http://localhost/api/test/mv", "POST", { source: "file.txt" });
    const response = await api.fetch(request);

    assert.equal(response.status, 400);
    const json = (await response.json()) as { error: string };
    assert.equal(json.error, "source and destination required");
  });
});

void describe("restore endpoint", () => {
  void it("should restore a file from index", async () => {
    const { repo, api } = await setupRepoWithCommit();

    // Modify the file
    await repo.writeFile("test.txt", new TextEncoder().encode("modified"));

    const request = createRequest("http://localhost/api/test/restore", "POST", {
      path: "test.txt",
    });
    const response = await api.fetch(request);

    assert.equal(response.status, 200);
    const json = (await response.json()) as { success: boolean; path: string };
    assert.equal(json.success, true);
    assert.equal(json.path, "test.txt");
  });

  void it("should require path", async () => {
    const { api } = await setupRepo();
    const request = createRequest("http://localhost/api/test/restore", "POST", {});
    const response = await api.fetch(request);

    assert.equal(response.status, 400);
    const json = (await response.json()) as { error: string };
    assert.equal(json.error, "path required");
  });

  void it("should restore staged file", async () => {
    const { api } = await setupRepoWithCommit();

    const request = createRequest("http://localhost/api/test/restore", "POST", {
      path: "test.txt",
      staged: true,
    });
    const response = await api.fetch(request);

    assert.equal(response.status, 200);
    const json = (await response.json()) as { success: boolean };
    assert.equal(json.success, true);
  });
});

void describe("switch endpoint", () => {
  void it("should switch to existing branch", async () => {
    const { api } = await setupRepoWithCommit();

    // Create a branch first
    await api.fetch(createRequest("http://localhost/api/test/branch", "POST", { name: "feature" }));

    const request = createRequest("http://localhost/api/test/switch", "POST", {
      target: "feature",
    });
    const response = await api.fetch(request);

    assert.equal(response.status, 200);
    const json = (await response.json()) as { success: boolean; branch: string };
    assert.equal(json.success, true);
    assert.equal(json.branch, "feature");
  });

  void it("should create and switch to new branch", async () => {
    const { api } = await setupRepoWithCommit();

    const request = createRequest("http://localhost/api/test/switch", "POST", {
      create: "new-branch",
    });
    const response = await api.fetch(request);

    assert.equal(response.status, 200);
    const json = (await response.json()) as { success: boolean; branch: string; created: boolean };
    assert.equal(json.success, true);
    assert.equal(json.branch, "new-branch");
    assert.equal(json.created, true);
  });

  void it("should require target or create", async () => {
    const { api } = await setupRepoWithCommit();
    const request = createRequest("http://localhost/api/test/switch", "POST", {});
    const response = await api.fetch(request);

    assert.equal(response.status, 400);
    const json = (await response.json()) as { error: string };
    assert.equal(json.error, "target or create required");
  });

  void it("should fail for non-existent branch", async () => {
    const { api } = await setupRepoWithCommit();
    const request = createRequest("http://localhost/api/test/switch", "POST", {
      target: "nonexistent",
    });
    const response = await api.fetch(request);

    assert.equal(response.status, 404);
    const json = (await response.json()) as { error: string };
    assert.ok(json.error.includes("not found"));
  });
});

void describe("rebase endpoint", () => {
  void it("should require onto", async () => {
    const { api } = await setupRepoWithCommit();
    const request = createRequest("http://localhost/api/test/rebase", "POST", {});
    const response = await api.fetch(request);

    assert.equal(response.status, 400);
    const json = (await response.json()) as { error: string };
    assert.equal(json.error, "onto required");
  });

  void it("should fail without HEAD commit", async () => {
    const { api } = await setupRepo();
    const request = createRequest("http://localhost/api/test/rebase", "POST", { onto: "main" });
    const response = await api.fetch(request);

    assert.equal(response.status, 400);
    const json = (await response.json()) as { error: string };
    assert.equal(json.error, "No HEAD commit");
  });

  void it("should rebase commits", async () => {
    const { api, commitOid } = await setupRepoWithCommit();
    const request = createRequest("http://localhost/api/test/rebase", "POST", { onto: commitOid });
    const response = await api.fetch(request);

    assert.equal(response.status, 200);
    const json = (await response.json()) as { success: boolean; replayed: number };
    assert.equal(json.success, true);
  });
});

void describe("fetch endpoint", () => {
  void it("should attempt fetch from remote", async () => {
    const { api } = await setupRepoWithCommit();
    const request = createRequest("http://localhost/api/test/fetch", "POST", { remote: "origin" });
    const response = await api.fetch(request);

    // Will fail because no remote is configured, but should handle gracefully
    assert.equal(response.status, 400);
    const json = (await response.json()) as { error: string };
    assert.ok(json.error);
  });
});

void describe("pull endpoint", () => {
  void it("should attempt pull from remote", async () => {
    const { api } = await setupRepoWithCommit();
    const request = createRequest("http://localhost/api/test/pull", "POST", {
      remote: "origin",
      branch: "main",
    });
    const response = await api.fetch(request);

    // Will fail because no remote is configured
    assert.equal(response.status, 400);
  });
});

void describe("push endpoint", () => {
  void it("should fail without remote configured", async () => {
    const { api } = await setupRepoWithCommit();
    const request = createRequest("http://localhost/api/test/push", "POST", {
      remote: "origin",
      branch: "main",
    });
    const response = await api.fetch(request);

    assert.equal(response.status, 400);
    const json = (await response.json()) as { error: string };
    assert.ok(json.error);
  });
});

void describe("remote endpoint", () => {
  void it("should list remotes (empty)", async () => {
    const { api } = await setupRepo();
    const request = createRequest("http://localhost/api/test/remote", "POST", {});
    const response = await api.fetch(request);

    assert.equal(response.status, 200);
    const json = (await response.json()) as { remotes: unknown[] };
    assert.ok(Array.isArray(json.remotes));
  });

  void it("should add a remote", async () => {
    const { api } = await setupRepo();
    const request = createRequest("http://localhost/api/test/remote", "POST", {
      action: "add",
      name: "origin",
      url: "https://github.com/user/repo.git",
    });
    const response = await api.fetch(request);

    assert.equal(response.status, 200);
    const json = (await response.json()) as { success: boolean; added: string; url: string };
    assert.equal(json.success, true);
    assert.equal(json.added, "origin");
    assert.equal(json.url, "https://github.com/user/repo.git");
  });

  void it("should get remote url", async () => {
    const { api } = await setupRepo();

    // Add remote first
    await api.fetch(
      createRequest("http://localhost/api/test/remote", "POST", {
        action: "add",
        name: "origin",
        url: "https://github.com/user/repo.git",
      }),
    );

    const request = createRequest("http://localhost/api/test/remote", "POST", {
      action: "get-url",
      name: "origin",
    });
    const response = await api.fetch(request);

    assert.equal(response.status, 200);
    const json = (await response.json()) as { name: string; url: string };
    assert.equal(json.name, "origin");
    assert.equal(json.url, "https://github.com/user/repo.git");
  });

  void it("should set remote url", async () => {
    const { api } = await setupRepo();

    // Add remote first
    await api.fetch(
      createRequest("http://localhost/api/test/remote", "POST", {
        action: "add",
        name: "origin",
        url: "https://github.com/user/repo.git",
      }),
    );

    const request = createRequest("http://localhost/api/test/remote", "POST", {
      action: "set-url",
      name: "origin",
      url: "https://github.com/user/new-repo.git",
    });
    const response = await api.fetch(request);

    assert.equal(response.status, 200);
    const json = (await response.json()) as { success: boolean; url: string };
    assert.equal(json.success, true);
    assert.equal(json.url, "https://github.com/user/new-repo.git");
  });

  void it("should remove a remote", async () => {
    const { api } = await setupRepo();

    // Add remote first
    await api.fetch(
      createRequest("http://localhost/api/test/remote", "POST", {
        action: "add",
        name: "origin",
        url: "https://github.com/user/repo.git",
      }),
    );

    const request = createRequest("http://localhost/api/test/remote", "POST", {
      action: "remove",
      name: "origin",
    });
    const response = await api.fetch(request);

    assert.equal(response.status, 200);
    const json = (await response.json()) as { success: boolean; removed: string };
    assert.equal(json.success, true);
    assert.equal(json.removed, "origin");
  });

  void it("should require name and url for add", async () => {
    const { api } = await setupRepo();
    const request = createRequest("http://localhost/api/test/remote", "POST", {
      action: "add",
      name: "origin",
    });
    const response = await api.fetch(request);

    assert.equal(response.status, 400);
    const json = (await response.json()) as { error: string };
    assert.equal(json.error, "name and url required");
  });
});

void describe("repository management", () => {
  void it("should create a repository with the requested default branch", async () => {
    const { repo, api } = await setupRepo();

    const response = await api.fetch(
      createRequest("http://localhost/api/test", "POST", {
        default_branch: "develop",
        id: "created-repo",
      }),
    );

    assert.equal(response.status, 201);
    const json = (await response.json()) as { id: string; default_branch: string };
    assert.equal(json.id, "created-repo");
    assert.equal(json.default_branch, "develop");

    await repo.initStorage("created-repo");
    assert.equal(await repo.getCurrentHead(), "refs/heads/develop");
  });
});
