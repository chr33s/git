/**
 * The `.git` suffix is the case worth pinning: git appends it to a URL that
 * has none, so a server that keys storage on the raw first segment hands the
 * same user two different repositories depending on how they typed the clone.
 */
import assert from "node:assert/strict";
import { describe, it } from "@effect/vitest";

import { normalize, routeOf } from "./Route.ts";

describe("routeOf", () => {
  it("reads the repository and the route behind it", () => {
    assert.deepEqual(routeOf("/repo/info/refs"), {
      repo: "repo",
      route: "info",
      rest: "info/refs",
    });
    assert.deepEqual(routeOf("/repo"), { repo: "repo", route: "", rest: "" });
  });

  it("strips a trailing .git, so both spellings are one repository", () => {
    assert.equal(routeOf("/repo.git/info/refs")?.repo, "repo");
    assert.equal(routeOf("/repo/info/refs")?.repo, "repo");
    assert.equal(routeOf("/repo.git")?.repo, "repo");
  });

  it("strips only the suffix", () => {
    // The name is `my.git.repo`; only a trailing `.git` is transport sugar.
    assert.equal(routeOf("/my.git.repo")?.repo, "my.git.repo");
    assert.equal(routeOf("/my.git.repo.git")?.repo, "my.git.repo");
  });

  it("keeps the tail intact", () => {
    assert.equal(routeOf("/repo.git/git-upload-pack")?.rest, "git-upload-pack");
    assert.equal(routeOf("/repo.git/info/lfs/objects/batch")?.rest, "info/lfs/objects/batch");
  });

  it("rejects what cannot be a storage key", () => {
    assert.equal(routeOf("/"), null);
    assert.equal(routeOf(""), null);
    assert.equal(routeOf("/../etc/passwd"), null);
    assert.equal(routeOf("/.hidden"), null);
    // A name that is nothing but the suffix leaves an empty key behind.
    assert.equal(routeOf("/.git"), null);
  });
});

describe("normalize", () => {
  it("rewrites the path to the stripped spelling", () => {
    const route = routeOf("/repo.git/info/refs")!;
    const request = normalize(
      new Request("http://host/repo.git/info/refs?service=git-upload-pack"),
      route,
    );
    const url = new URL(request.url);
    assert.equal(url.pathname, "/repo/info/refs");
    // The query is what selects the service; losing it would break the clone.
    assert.equal(url.searchParams.get("service"), "git-upload-pack");
  });

  it("returns the same request when there is nothing to rewrite", () => {
    const request = new Request("http://host/repo/info/refs");
    assert.equal(normalize(request, routeOf("/repo/info/refs")!), request);
  });

  it("preserves method and headers", () => {
    const route = routeOf("/repo.git/git-receive-pack")!;
    const request = normalize(
      new Request("http://host/repo.git/git-receive-pack", {
        method: "POST",
        headers: { "content-type": "application/x-git-receive-pack-request" },
      }),
      route,
    );
    assert.equal(request.method, "POST");
    assert.equal(request.headers.get("content-type"), "application/x-git-receive-pack-request");
  });
});
