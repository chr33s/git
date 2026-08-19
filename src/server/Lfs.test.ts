/**
 * Git LFS over the wire, against the node host.
 *
 * The `git-lfs` binary is not a dependency of this repository, so the test
 * speaks the batch protocol itself — which is all `git-lfs` does: ask the
 * batch endpoint where to put or get an object, then PUT or GET it there.
 * Driving it through `host/Node.ts` covers the routing too, and the routing
 * is the interesting part: LFS shares the `info/` prefix with the smart-HTTP
 * advertisement.
 */
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { afterAll, beforeAll, describe, it } from "@effect/vitest";

import { serve, type Server } from "../host/Node.ts";
import { formatPointer, MEDIA_TYPE, parsePointer } from "./Lfs.ts";

const sha256 = (content: string): string => createHash("sha256").update(content).digest("hex");

interface BatchObject {
  readonly oid: string;
  readonly size: number;
  readonly actions?: {
    readonly upload?: { readonly href: string };
    readonly download?: { readonly href: string };
  };
  readonly error?: { readonly code: number; readonly message: string };
}

describe("Git LFS", () => {
  let root: string;
  let server: Server;

  const batch = async (
    repo: string,
    operation: "upload" | "download",
    objects: ReadonlyArray<{ oid: string; size: number }>,
  ) => {
    const response = await fetch(`${server.url}/${repo}/info/lfs/objects/batch`, {
      method: "POST",
      headers: { "content-type": MEDIA_TYPE, accept: MEDIA_TYPE },
      body: JSON.stringify({ operation, transfers: ["basic"], objects }),
    });
    assert.equal(response.headers.get("content-type"), MEDIA_TYPE);
    // SAFETY: the server under test speaks the batch protocol; a reply that
    // does not, fails the assertions each test makes on these fields.
    return { status: response.status, body: (await response.json()) as { objects: BatchObject[] } };
  };

  beforeAll(async () => {
    root = await fs.mkdtemp(path.join(os.tmpdir(), "git-lfs-"));
    server = await serve({ root });
  });

  afterAll(async () => {
    await server.close();
    await fs.rm(root, { recursive: true, force: true });
  });

  it("round-trips an object through batch, upload and download", async () => {
    const content = "a large file, notionally\n";
    const oid = sha256(content);

    // Upload: not held yet, so the server hands back somewhere to put it.
    const asked = await batch("r", "upload", [{ oid, size: content.length }]);
    const action = asked.body.objects[0]!;
    assert.equal(action.oid, oid);
    const target = action.actions?.upload?.href;
    assert.ok(target !== undefined, "the batch reply offers an upload href");

    const put = await fetch(target, { method: "PUT", body: content });
    assert.equal(put.status, 200);

    // Asking again offers no action: the object is already here, which is
    // what makes re-pushing an unchanged file free.
    const again = await batch("r", "upload", [{ oid, size: content.length }]);
    assert.equal(again.body.objects[0]!.actions, undefined);
    assert.equal(again.body.objects[0]!.size, content.length);

    const download = await batch("r", "download", [{ oid, size: content.length }]);
    const href = download.body.objects[0]!.actions?.download?.href;
    assert.ok(href !== undefined, "the batch reply offers a download href");

    const got = await fetch(href);
    assert.equal(got.status, 200);
    assert.equal(await got.text(), content);
    assert.equal(got.headers.get("content-length"), String(content.length));
  });

  it("refuses content that does not hash to the name it was given", async () => {
    const oid = sha256("what was promised");

    const asked = await batch("r", "upload", [{ oid, size: 9 }]);
    const href = asked.body.objects[0]!.actions!.upload!.href;

    const put = await fetch(href, { method: "PUT", body: "something else entirely" });
    assert.equal(put.status, 422);

    // And nothing was stored: a rejected upload leaves no trace to serve.
    const download = await batch("r", "download", [{ oid, size: 9 }]);
    assert.equal(download.body.objects[0]!.error?.code, 404);
  });

  it("reports a missing object rather than inventing an action", async () => {
    const oid = sha256("never uploaded");
    const download = await batch("r", "download", [{ oid, size: 4 }]);
    assert.equal(download.body.objects[0]!.actions, undefined);
    assert.equal(download.body.objects[0]!.error?.code, 404);
  });

  it("rejects malformed requests with the LFS media type", async () => {
    const bad = await batch("r", "download", [{ oid: "not-an-oid", size: 1 }]);
    assert.equal(bad.body.objects[0]!.error?.code, 422);

    const unsupported = await fetch(`${server.url}/r/info/lfs/objects/batch`, {
      method: "POST",
      headers: { "content-type": MEDIA_TYPE },
      body: JSON.stringify({ operation: "teleport", objects: [] }),
    });
    assert.equal(unsupported.status, 422);

    const wrongMethod = await fetch(`${server.url}/r/info/lfs/objects/batch`);
    assert.equal(wrongMethod.status, 405);
  });

  it("keeps repositories apart", async () => {
    const content = "belongs to one repo\n";
    const oid = sha256(content);

    const asked = await batch("first", "upload", [{ oid, size: content.length }]);
    await fetch(asked.body.objects[0]!.actions!.upload!.href, { method: "PUT", body: content });

    const elsewhere = await batch("second", "download", [{ oid, size: content.length }]);
    assert.equal(elsewhere.body.objects[0]!.error?.code, 404);
  });

  it("does not shadow the smart-HTTP advertisement it shares a prefix with", async () => {
    const advertisement = await fetch(`${server.url}/r/info/refs?service=git-upload-pack`);
    assert.equal(advertisement.status, 200);
    assert.equal(
      advertisement.headers.get("content-type"),
      "application/x-git-upload-pack-advertisement",
    );
  });

  it("reads and writes pointer files", () => {
    const pointer = formatPointer({ oid: sha256("x"), size: 1234 });
    assert.deepEqual(parsePointer(pointer), { oid: sha256("x"), size: 1234 });

    assert.equal(parsePointer("not a pointer"), null);
    assert.equal(
      parsePointer("version https://git-lfs.github.com/spec/v1\noid sha256:short\nsize 1\n"),
      null,
    );
  });
});
