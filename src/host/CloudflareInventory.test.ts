import assert from "node:assert/strict";
import * as fs from "node:fs/promises";
import * as path from "node:path";
import { tmpdir } from "node:os";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { createHash } from "node:crypto";
import { it } from "@effect/vitest";
import {
  inventoryCloudflareObjects,
  verifyCloudflareObjectInventory,
} from "../../tools/inventory-cloudflare-objects.ts";

const account = "a".repeat(32);
const namespace = "b".repeat(32);
const token = "test-only-private-token";
const first = { id: "1".repeat(64), hasStoredData: true };
const second = { id: "2".repeat(64), hasStoredData: false };
const page = (cursor: string, objects = [first]) =>
  Response.json({
    success: true,
    result: objects,
    result_info: { cursor },
  });

it("streams paginated discovery privately and preserves an existing inventory", async () => {
  const root = await fs.mkdtemp(path.join(tmpdir(), "cf-inventory-"));
  const destination = path.join(root, "inventory");
  let calls = 0;
  try {
    await inventoryCloudflareObjects(
      account,
      namespace,
      destination,
      token,
      async (input, init) => {
        assert.ok(input instanceof URL);
        assert.equal(input.origin, "https://api.cloudflare.com");
        assert.equal(
          input.pathname,
          `/client/v4/accounts/${account}/workers/durable_objects/namespaces/${namespace}/objects`,
        );
        assert.equal(input.searchParams.get("limit"), "1000");
        assert.equal(new Headers(init?.headers).get("authorization"), `Bearer ${token}`);
        assert.equal(init?.redirect, "error");
        assert.ok(init?.signal);
        calls += 1;
        assert.equal(input.searchParams.get("cursor"), calls === 1 ? null : "next/+?&");
        return calls === 1 ? page("next/+?&") : page("", [second]);
      },
    );
    assert.equal(calls, 2);
    const contents = await fs.readFile(path.join(destination, "objects.ndjson"), "utf8");
    assert.equal(contents, JSON.stringify(first) + "\n" + JSON.stringify(second) + "\n");
    const manifestText = await fs.readFile(path.join(destination, "inventory.json"), "utf8");
    const manifest = JSON.parse(manifestText);
    assert.equal(manifest.schemaVersion, 2);
    assert.equal(
      manifest.inventorySha256,
      createHash("sha256")
        .update(JSON.stringify(["chr33s-git/namespace-inventory/v1", account, namespace]) + "\n")
        .update(contents)
        .digest("hex"),
    );
    assert.equal(
      (await verifyCloudflareObjectInventory(destination, account, namespace)).inventorySha256,
      manifest.inventorySha256,
    );
    assert.deepEqual((await fs.readdir(destination)).sort(), ["inventory.json", "objects.ndjson"]);
    const checked = await promisify(execFile)(
      process.execPath,
      ["tools/inventory-cloudflare-objects.ts", "--verify", account, namespace, destination],
      { env: { ENABLE_CONFORMANCE: "1", ALLOW_ANONYMOUS_WRITES: "1", CLOUDFLARE_AI_TOKEN: "" } },
    );
    assert.match(checked.stdout, new RegExp(manifest.inventorySha256));
    assert.equal(manifest.pages, 2);
    assert.equal(manifest.objects, 2);
    assert.equal(manifest.storedObjects, 1);
    assert.ok(!manifestText.includes(token));
    assert.equal((await fs.stat(destination)).mode & 0o077, 0);
    for (const name of ["inventory.json", "objects.ndjson"])
      assert.equal((await fs.stat(path.join(destination, name))).mode & 0o077, 0);
    await assert.rejects(
      inventoryCloudflareObjects(account, namespace, destination, token, async () => {
        throw new Error("must not fetch when destination exists");
      }),
      /EEXIST/,
    );
    assert.equal(await fs.readFile(path.join(destination, "objects.ndjson"), "utf8"), contents);
  } finally {
    await fs.rm(root, { recursive: true, force: true });
  }
});

for (const fault of [
  "http",
  "schema",
  "cursor",
  "duplicate",
  "oversize",
  "transport",
  "body",
] as const) {
  it(`removes partial discovery and redacts ${fault} failures`, async () => {
    const root = await fs.mkdtemp(path.join(tmpdir(), "cf-inventory-failure-"));
    const destination = path.join(root, "inventory");
    let calls = 0;
    let cancelled = false;
    try {
      await assert.rejects(
        inventoryCloudflareObjects(account, namespace, destination, token, async () => {
          calls += 1;
          if (calls === 1) return page("next");
          if (fault === "http") return new Response(token, { status: 403 });
          if (fault === "schema") return Response.json({ success: false, errors: [token] });
          if (fault === "cursor") return page("next", [second]);
          if (fault === "duplicate") return page("different-cursor");
          if (fault === "transport") throw new Error(token);
          return new Response(
            new ReadableStream({
              pull(controller) {
                if (fault === "body") controller.error(new Error(token));
                else controller.enqueue(new Uint8Array(2 * 1024 * 1024 + 1));
              },
              cancel() {
                cancelled = true;
              },
            }),
          );
        }),
        (error: Error) => {
          assert.ok(!error.message.includes(token));
          assert.match(error.message, /Cloudflare|pagination/);
          return true;
        },
      );
      assert.equal(calls, 2);
      if (fault === "oversize") assert.equal(cancelled, true);
      await assert.rejects(fs.stat(destination), /ENOENT/);
    } finally {
      await fs.rm(root, { recursive: true, force: true });
    }
  });
}

for (const fault of [
  "duplicate",
  "digest",
  "truncated",
  "count",
  "namespace",
  "oversize",
  "symlink",
  "utf8",
  "legacy",
] as const) {
  it(`rejects ${fault} saved inventory before its digest can qualify recovery`, async () => {
    const root = await fs.mkdtemp(path.join(tmpdir(), "cf-inventory-integrity-"));
    const directory = path.join(root, "inventory");
    try {
      await inventoryCloudflareObjects(account, namespace, directory, token, async () =>
        page("", [first, second]),
      );
      const data = path.join(directory, "objects.ndjson");
      const metadata = path.join(directory, "inventory.json");
      const contents = await fs.readFile(data, "utf8");
      const manifest = JSON.parse(await fs.readFile(metadata, "utf8"));
      if (fault === "duplicate") {
        const repeated = JSON.stringify(first) + "\n" + JSON.stringify(first) + "\n";
        await fs.writeFile(data, repeated);
        manifest.storedObjects = 2;
        manifest.inventorySha256 = createHash("sha256")
          .update(JSON.stringify(["chr33s-git/namespace-inventory/v1", account, namespace]) + "\n")
          .update(repeated)
          .digest("hex");
      } else if (fault === "digest") manifest.inventorySha256 = "0".repeat(64);
      else if (fault === "truncated") await fs.writeFile(data, contents.slice(0, -1));
      else if (fault === "count") manifest.objects += 1;
      else if (fault === "namespace") manifest.namespace = "c".repeat(32);
      else if (fault === "oversize") await fs.writeFile(data, "x".repeat(300));
      else if (fault === "utf8") await fs.writeFile(data, new Uint8Array([255, 10]));
      else if (fault === "legacy") manifest.schemaVersion = 1;
      else {
        const saved = path.join(root, "saved");
        await fs.rename(data, saved);
        await fs.symlink(saved, data);
      }
      await fs.writeFile(metadata, JSON.stringify(manifest));
      await assert.rejects(
        verifyCloudflareObjectInventory(directory, account, namespace),
        /verification failed/,
      );
      assert.equal(
        await fs.readFile(metadata, "utf8"),
        JSON.stringify(manifest),
        "verification must preserve the input evidence",
      );
    } finally {
      await fs.rm(root, { recursive: true, force: true });
    }
  });
}

it("rejects duplicate IDs within a single API page", async () => {
  const root = await fs.mkdtemp(path.join(tmpdir(), "cf-inventory-duplicate-"));
  const directory = path.join(root, "inventory");
  try {
    await assert.rejects(
      inventoryCloudflareObjects(account, namespace, directory, token, async () =>
        page("", [first, first]),
      ),
      /repeated an object ID/,
    );
    await assert.rejects(fs.stat(directory), { code: "ENOENT" });
  } finally {
    await fs.rm(root, { recursive: true, force: true });
  }
});

it("binds even an empty inventory to its account and namespace", async () => {
  const root = await fs.mkdtemp(path.join(tmpdir(), "cf-inventory-empty-"));
  try {
    const one = path.join(root, "one");
    const two = path.join(root, "two");
    const otherNamespace = "c".repeat(32);
    await inventoryCloudflareObjects(account, namespace, one, token, async () => page("", []));
    await inventoryCloudflareObjects(account, otherNamespace, two, token, async () => page("", []));
    const first = await verifyCloudflareObjectInventory(one, account, namespace);
    const second = await verifyCloudflareObjectInventory(two, account, otherNamespace);
    assert.equal(first.objects, 0);
    assert.equal(first.storedObjects, 0);
    assert.notEqual(first.inventorySha256, second.inventorySha256);
  } finally {
    await fs.rm(root, { recursive: true, force: true });
  }
});
