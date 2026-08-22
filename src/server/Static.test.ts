/**
 * Serving a directory to the public is one bug away from serving the disk, so
 * the escape cases are the ones pinned here: what a path that climbs out of
 * the root answers, and what a method that is not a read answers.
 */
import assert from "node:assert/strict";
import { mkdtemp, mkdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, it } from "@effect/vitest";

import { Effect } from "effect";

import { assetResponse, built, fileAt, mimeOf } from "./Static.ts";

describe("Static", () => {
  let root = "";
  let outside = "";

  beforeEach(async () => {
    outside = await mkdtemp(join(tmpdir(), "static-"));
    root = join(outside, "ui");
    await mkdir(root);
    await writeFile(join(root, "index.html"), "<!doctype html><title>git+</title>");
    await writeFile(join(root, "main.js"), "globalThis.ok = true;");
    await writeFile(join(outside, "secret.txt"), "not served");
    await mkdir(join(root, "chunks"));
  });

  afterEach(async () => {
    await rm(outside, { recursive: true, force: true });
  });

  it.effect("answers the root with the page, and a built directory as built", () =>
    Effect.promise(async () => {
      assert.equal(await built(root), true);
      assert.equal(await built(join(outside, "nothing")), false);
      assert.notEqual(await fileAt(root, "/"), null);
      assert.notEqual(await fileAt(root, "/index.html"), null);
    }),
  );

  it.effect("refuses to leave the root, however the path spells it", () =>
    Effect.promise(async () => {
      // The file is really there and really readable; only the root says no.
      assert.equal(await fileAt(root, "/../secret.txt"), null);
      assert.equal(await fileAt(root, "/chunks/../../secret.txt"), null);
      assert.equal(await fileAt(root, "/./../secret.txt"), null);
      // A sibling directory whose name merely starts with the root's is outside
      // it, which is what the separator in the prefix check is for.
      assert.equal(await fileAt(`${root}x`, "/index.html"), null);
    }),
  );

  it.effect("answers nothing for a directory or a miss", () =>
    Effect.promise(async () => {
      assert.equal(await fileAt(root, "/chunks"), null);
      assert.equal(await fileAt(root, "/nope.js"), null);
    }),
  );

  it.effect("types what it serves by extension", () =>
    Effect.promise(async () => {
      const typeOf = async (path: string): Promise<string | undefined> =>
        (await assetResponse(root, new Request(`http://host${path}`)))?.headers.get(
          "content-type",
        ) ?? undefined;
      assert.equal(await typeOf("/"), "text/html");
      assert.equal(await typeOf("/index.html"), "text/html");
      assert.equal(await typeOf("/main.js"), "text/javascript");
      assert.equal(mimeOf(".css"), "text/css");
      assert.equal(mimeOf(".map"), "application/json");
      assert.equal(mimeOf(".bin"), "application/octet-stream");
    }),
  );

  it.effect("keeps a HEAD's headers and drops its body", () =>
    Effect.promise(async () => {
      const answer = await assetResponse(
        root,
        new Request("http://host/main.js", { method: "HEAD" }),
      );
      assert.equal(answer?.headers.get("content-type"), "text/javascript");
      assert.equal(answer?.headers.get("content-length"), "21");
      assert.equal(await answer?.text(), "");
    }),
  );

  it.effect("leaves every method that is not a read to the API", () =>
    Effect.promise(async () => {
      // Even at a path the bundle does hold: a mutation is the API's whatever
      // it is addressed to, and answering it here would shadow a real route.
      for (const method of ["POST", "PUT", "PATCH", "DELETE"]) {
        const request = new Request("http://host/index.html", { method });
        assert.equal(await assetResponse(root, request), null, method);
      }
    }),
  );
});
