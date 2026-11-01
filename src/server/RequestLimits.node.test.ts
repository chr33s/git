import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { describe, it } from "@effect/vitest";
import { Effect } from "effect";

describe("Node request budget configuration", () => {
  it.live("enforces configured HTTP limits and remains responsive after rejection", () =>
    Effect.promise(async () => {
      // Use a separate process so Config reads real environment values without
      // racing another test's environment or caching an earlier host's budgets.
      const script = `
        import assert from "node:assert/strict";
        import * as fs from "node:fs/promises";
        import { tmpdir } from "node:os";
        import { join } from "node:path";
        import { gzipSync, deflateSync } from "node:zlib";
        import { createHash } from "node:crypto";
        import * as http from "node:http";
        import { serve } from ${JSON.stringify(new URL("../host/Node.ts", import.meta.url).href)};
        const root = await fs.mkdtemp(join(tmpdir(), "git-budgets-"));
        const server = await serve({ root, allowAnonymousWrites: true });
        try {
          const request = (route, body, headers = {}) => fetch(server.url + "/r/" + route, {
            method: "POST", body, headers,
          });
          assert.equal((await request("git-upload-pack", "0000".repeat(3))).status, 413);
          assert.equal((await request("git-upload-pack", gzipSync("0000".repeat(100)),
            { "content-encoding": "gzip" })).status, 413);
          const batch = objects => JSON.stringify({ operation: "download", objects });
          const object = { oid: "a".repeat(64), size: 1 };
          assert.equal((await request("info/lfs/objects/batch", batch([object, object]))).status, 413);
          assert.equal((await request("info/lfs/objects/batch", " ".repeat(600))).status, 413);
          assert.equal((await request("info/lfs/objects/batch", batch([object]))).status, 200);
          const json = { "content-type": "application/json" };
          assert.equal((await request("blob", JSON.stringify({ content: "x".repeat(1024) }), json)).status, 413);
          // Chunked transfer has no Content-Length; count the consumed bytes too.
          const chunks = new ReadableStream({ start(controller) {
            controller.enqueue(new TextEncoder().encode(JSON.stringify({ content: "x".repeat(1024) })));
            controller.close();
          } });
          assert.equal((await fetch(server.url + "/r/blob", {
            method: "POST", headers: json, body: chunks, duplex: "half",
          })).status, 413);
          const refusedOid = createHash("sha1").update("blob 1024\\0" + "x".repeat(1024)).digest("hex");
          assert.equal((await fetch(server.url + "/r/blob/" + refusedOid)).status, 404);
          assert.equal((await request("blob", JSON.stringify({ content: "small" }), json)).status, 200);
          const upload = content => {
            const oid = createHash("sha256").update(content).digest("hex");
            return { oid, url: server.url + "/r/info/lfs/objects/" + oid };
          };
          const large = "x".repeat(64);
          const lfs = upload(large);
          const offered = await request("info/lfs/objects/batch", JSON.stringify({
            operation: "upload", objects: [{ oid: lfs.oid, size: large.length }],
          }));
          assert.equal((await offered.json()).objects[0].error.code, 413);
          assert.equal((await fetch(lfs.url, { method: "PUT", body: large })).status, 413);
          const uploadChunks = new ReadableStream({ start(controller) {
            controller.enqueue(new TextEncoder().encode(large.slice(0, 16)));
            controller.enqueue(new TextEncoder().encode(large.slice(16)));
            controller.close();
          } });
          assert.equal((await fetch(lfs.url, { method: "PUT", body: uploadChunks, duplex: "half" })).status, 413);
          assert.equal((await fetch(lfs.url)).status, 404);
          assert.equal((await fs.readdir(root, { recursive: true })).some(name => name.endsWith(".tmp")), false);
          const small = upload("fits");
          assert.equal((await fetch(small.url, { method: "PUT", body: "fits" })).status, 200);
          assert.equal(await (await fetch(small.url)).text(), "fits");

          const payload = Buffer.from("123456789");
          const blobOid = createHash("sha1").update("blob 9\\0").update(payload).digest("hex");
          const entry = Buffer.concat([Buffer.from([0x39]), deflateSync(payload)]);
          const push = async count => {
            const header = Buffer.from("5041434b0000000200000000", "hex");
            header.writeUInt32BE(count, 8);
            const pack = Buffer.concat([header, ...Array.from({ length: count }, () => entry)]);
            const command = "0".repeat(40) + " " + blobOid + " refs/heads/main\\0report-status\\n";
            const prefix = Buffer.from((Buffer.byteLength(command) + 4).toString(16).padStart(4, "0") + command + "0000");
            return request("git-receive-pack", Buffer.concat([prefix, pack, createHash("sha1").update(pack).digest()]));
          };
          assert.match(await (await push(2)).text(), /declares 2 objects, limit 1/);
          assert.match(await (await push(1)).text(), /expanded bytes exceed 8/);
          assert.equal((await fetch(server.url + "/r/blob/" + blobOid)).status, 404);
          // A rejected header must release the repository gate even if the
          // sender never finishes its chunked body.
          const rejected = await new Promise((resolve, reject) => {
            const req = http.request(server.url + "/r/git-receive-pack", { method: "POST" }, res => {
              let report = "";
              res.setEncoding("utf8");
              res.on("data", chunk => { report += chunk; });
              res.on("end", () => { clearTimeout(timer); req.destroy(); resolve(report); });
              res.on("error", reject);
            });
            const timer = setTimeout(() => { req.destroy(); reject(new Error("rejection awaited remaining body")); }, 2000);
            req.on("error", error => { clearTimeout(timer); reject(error); });
            const command = "0".repeat(40) + " " + blobOid + " refs/heads/main\\0report-status\\n";
            req.write((Buffer.byteLength(command) + 4).toString(16).padStart(4, "0") + command + "0000");
            req.write(Buffer.from("5041434b0000000200000002", "hex"));
          });
          assert.match(rejected, /declares 2 objects, limit 1/);
          assert.equal((await fetch(server.url + "/r/info/refs?service=git-upload-pack")).status, 200);
          console.log("configured budgets enforced");
        } finally {
          await server.close();
          await fs.rm(root, { recursive: true, force: true });
        }
      `;
      const result = await promisify(execFile)(
        process.execPath,
        ["--input-type=module", "-e", script],
        {
          env: {
            ...process.env,
            GIT_NEGOTIATION_BYTES: "64",
            GIT_JSON_BODY_BYTES: "512",
            GIT_NEGOTIATION_LINES: "2",
            GIT_LFS_BATCH_BYTES: "512",
            GIT_LFS_BATCH_OBJECTS: "1",
            GIT_LFS_UPLOAD_BYTES: "32",
            GIT_RECEIVE_OBJECTS: "1",
            GIT_RECEIVE_EXPANDED_BYTES: "8",
          },
          timeout: 30_000,
        },
      );
      assert.match(result.stdout, /configured budgets enforced/);
    }),
  );
});
