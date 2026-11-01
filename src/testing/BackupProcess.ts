/** Test-only child coordinator bridge; native state survives the child process. */
import assert from "node:assert/strict";
import { spawn, type ChildProcess } from "node:child_process";
import { createServer } from "node:http";
import type { Destination } from "../../tools/copy-r2-backup.ts";
import { bridgeBackupBucket } from "./BackupBucketBridge.ts";
import { Schema } from "effect";
import { Request } from "../host/CloudflareBackupAdmin.ts";
import type { Client } from "../../tools/cloudflare-object-archive.ts";
import type { SourceRelease, AbandonSources } from "../../tools/cloudflare-source-release.ts";
import type { Restore } from "../../tools/cloudflare-namespace-restore.ts";
import type { Release } from "../../tools/cloudflare-namespace-release.ts";

interface Config {
  "source-release": Omit<SourceRelease, "client" | "archiveBucket">;
  "source-abandon": Omit<AbandonSources, "client">;
  restore: Omit<Restore, "client" | "archiveBucket" | "targetBucket">;
  "target-release": Omit<Release, "client" | "targetBucket">;
}
const childProgram = `
  import { Effect, Schema } from "effect";
  import { encodeBackupFrame, decodeBackupFrame } from ${JSON.stringify(new URL("../../tools/cloudflare-backup-codec.ts", import.meta.url).href)};
  import { releaseCapturedSources, abandonCapturedSources } from ${JSON.stringify(new URL("../../tools/cloudflare-source-release.ts", import.meta.url).href)};
  import { restoreNamespaceArchive } from ${JSON.stringify(new URL("../../tools/cloudflare-namespace-restore.ts", import.meta.url).href)};
  import { releaseNamespaceArchive } from ${JSON.stringify(new URL("../../tools/cloudflare-namespace-release.ts", import.meta.url).href)};
  const { endpoint, operation, ...options } = JSON.parse(process.argv[1]);
  const call = async (route, input) => {
    const response = await fetch(endpoint + route, { method: "POST", body: JSON.stringify(input) });
    if (!response.ok) throw new Error("fixture bridge failed");
    return response.json();
  };
  const decode = text => decodeBackupFrame(text, Schema.Unknown);
  const sizes = new Map();
  const bucket = route => {
    const request = async (method, input, body) => {
      const response = await fetch(endpoint + route, { method, body,
        ...(body === undefined ? {} : { duplex: "half" }),
        headers: { "x-backup-input": Buffer.from(encodeBackupFrame(input)).toString("base64"),
          ...(method === "PUT" ? { "content-length": String(sizes.get(input.key)) } : {}),
        },
      });
      if (!response.ok) throw new Error("fixture bucket failed: " + await response.text());
      return response;
    };
    return {
      list: async (options = {}) => decode(await (await request("POST", { options })).text()),
      get: async (key, options) => {
        const response = await request("GET", { key, options });
        const metadata = decode(Buffer.from(response.headers.get("x-backup-object"), "base64").toString("utf8"));
        if (metadata === null) { await response.body?.cancel(); return null; }
        sizes.set(key, metadata.size);
        return { ...metadata, body: response.body };
      },
      put: async (key, body, options) => decode(await (await request("PUT", { key, options }, body)).text()),
    };
  };
  const run = { "source-release": releaseCapturedSources, "source-abandon": abandonCapturedSources,
    restore: restoreNamespaceArchive, "target-release": releaseNamespaceArchive }[operation];
  if (run === undefined) throw new Error("unknown coordinator");
  await Effect.runPromise(run({ ...options,
    client: { backup: request => call("/backup", request) },
    archiveBucket: bucket("/archive"), targetBucket: bucket("/target"),
  }));
`;

export const openBackupProcess = async (options: {
  readonly client: Client;
  readonly archiveBucket: Destination;
  readonly targetBucket?: Destination;
  readonly interrupt: (request: Request, point: "before" | "after") => boolean;
  readonly onBucket?: () => void;
  readonly knownLength?: Parameters<typeof bridgeBackupBucket>[3];
  readonly interruptBucket?: (key: string, point: "during-put" | "after-put") => boolean;
}) => {
  let active: ChildProcess | undefined;
  const server = createServer((request, response) => {
    void (async () => {
      if (request.url === "/archive" || request.url === "/target") {
        options.onBucket?.();
        const bucket = request.url === "/archive" ? options.archiveBucket : options.targetBucket;
        assert.ok(bucket);
        await bridgeBackupBucket(
          request,
          response,
          bucket,
          options.knownLength ??
            (() => {
              throw new Error("stream bridge required");
            }),
          (key, point) => {
            if (!options.interruptBucket?.(key, point)) return false;
            assert.ok(active);
            assert.equal(active.kill("SIGKILL"), true);
            response.destroy();
            return true;
          },
        );
        return;
      }
      let body = "";
      for await (const chunk of request) {
        body += chunk;
        if (body.length > 16 * 1024) throw new Error("fixture request too large");
      }
      assert.equal(request.url, "/backup");
      const command = Schema.decodeUnknownSync(Request)(JSON.parse(body));
      const stop = (point: "before" | "after") => {
        if (!options.interrupt(command, point)) return false;
        assert.ok(active);
        assert.equal(active.kill("SIGKILL"), true);
        response.destroy();
        return true;
      };
      if (stop("before")) return;
      const result = await options.client.backup(command);
      if (stop("after")) return;
      response.end(JSON.stringify(result));
    })().catch((error) => {
      console.error("backup fixture bridge:", error);
      response.statusCode = 500;
      response.end("fixture bridge failed");
    });
  });
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolve);
  });
  const address = Schema.decodeUnknownSync(
    Schema.Struct({ port: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 65535 })) }),
  )(server.address());
  return {
    run: <Operation extends keyof Config>(operation: Operation, config: Config[Operation]) =>
      new Promise<{ code: number | null; signal: string | null }>((resolve, reject) => {
        assert.ok(active === undefined || active.exitCode !== null || active.signalCode !== null);
        const child = spawn(
          process.execPath,
          [
            "--input-type=module",
            "-e",
            childProgram,
            JSON.stringify({ ...config, operation, endpoint: `http://127.0.0.1:${address.port}` }),
          ],
          { stdio: ["ignore", "ignore", "pipe"] },
        );
        active = child;
        let diagnostics = "";
        child.stderr.on("data", (chunk) => {
          diagnostics = (diagnostics + chunk).slice(-8192);
        });
        const deadline = setTimeout(() => child.kill("SIGTERM"), 20_000);
        child.once("error", (error) => {
          clearTimeout(deadline);
          reject(error);
        });
        child.once("exit", (code, signal) => {
          clearTimeout(deadline);
          if (code !== null && code !== 0)
            reject(new Error(`coordinator exited ${code}: ${diagnostics}`));
          else resolve({ code, signal });
        });
      }),
    close: async () => {
      if (active?.exitCode === null && active.signalCode === null) active.kill("SIGKILL");
      server.closeAllConnections();
      if (server.listening) await new Promise<void>((resolve) => server.close(() => resolve()));
    },
  };
};
