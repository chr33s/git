import * as http from "node:http";
import * as fs from "node:fs/promises";
import { randomUUID } from "node:crypto";
import { join } from "node:path";
import { Config, Option, Predicate, Schema } from "effect";

import type { Snapshot } from "./RequestMetrics.ts";

export const port = Config.option(
  Config.schema(
    Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: 65535 })),
    "GIT_OPERATIONS_PORT",
  ),
).pipe(Config.map(Option.getOrUndefined));

/** A small real write/read/sync probe; never creates or repairs the data root. */
const storageProbe = async (root: string): Promise<void> => {
  const target = join(root, `.git-plus-health-${randomUUID()}.tmp`);
  const bytes = Buffer.from("git-plus readiness\n");
  let owned = false;
  try {
    const file = await fs.open(target, "wx+", 0o600);
    owned = true;
    try {
      await file.writeFile(bytes);
      await file.sync();
      const read = Buffer.alloc(bytes.length);
      const result = await file.read(read, 0, read.length, 0);
      if (result.bytesRead !== bytes.length || !read.equals(bytes))
        throw new Error("probe read failed");
    } finally {
      await file.close();
    }
  } finally {
    if (owned) await fs.rm(target);
  }
  const directory = await fs.open(root, "r");
  try {
    await directory.sync();
  } finally {
    await directory.close();
  }
};

export const listen = async (
  port: number,
  root: string,
  metrics: () => Snapshot,
  capacity: number,
) => {
  if (!Number.isInteger(port) || port < 0 || port > 65535)
    throw new RangeError("invalid operations port");
  let storage = false;
  let checked = Number.NEGATIVE_INFINITY;
  let pending: Promise<void> | undefined;
  let stopped = false;
  let authority = "";
  const refresh = () => {
    if (pending !== undefined || stopped) return;
    pending = storageProbe(root)
      .then(
        () => {
          storage = true;
        },
        () => {
          if (storage || !Number.isFinite(checked)) {
            console.error(JSON.stringify({ component: "NodeHost", event: "storage_probe_failed" }));
          }
          storage = false;
        },
      )
      .finally(() => {
        checked = performance.now();
        pending = undefined;
      });
  };
  const server = http.createServer(
    { headersTimeout: 2000, requestTimeout: 2000 },
    (request, response) => {
      response.setHeader("cache-control", "no-store");
      response.setHeader("content-type", "application/json");
      if (
        request.headers.host !== authority ||
        (request.headers.origin !== undefined && request.headers.origin !== `http://${authority}`)
      ) {
        response.statusCode = 403;
        response.end();
        return;
      }
      if (request.method !== "GET" && request.method !== "HEAD") {
        response.writeHead(405, { allow: "GET, HEAD" });
        response.end();
        return;
      }
      if (request.url === "/live") {
        response.end(JSON.stringify({ live: !stopped }));
      } else if (request.url === "/metrics") {
        response.end(JSON.stringify(metrics()));
      } else if (request.url === "/ready") {
        const healthyStorage = storage && performance.now() - checked < 10_000;
        const available = metrics().active < capacity;
        const ready = !stopped && healthyStorage && available;
        response.statusCode = ready ? 200 : 503;
        response.end(JSON.stringify({ ready, storage: healthyStorage, capacity: available }));
      } else {
        response.statusCode = 404;
        response.end();
      }
    },
  );
  server.maxConnections = 16;
  server.setTimeout(2000, (socket) => socket.destroy());
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(port, "127.0.0.1", () => {
      server.removeListener("error", reject);
      resolve();
    });
  });
  const address = server.address();
  if (address === null || Predicate.isString(address))
    throw new Error("operations listener has no TCP address");
  authority = `127.0.0.1:${address.port}`;
  refresh();
  const timer = setInterval(refresh, 5000);
  timer.unref();
  return {
    url: `http://127.0.0.1:${address.port}`,
    close: async () => {
      stopped = true;
      clearInterval(timer);
      await new Promise<void>((resolve, reject) => {
        server.close((error) => (error ? reject(error) : resolve()));
        server.closeAllConnections();
      });
      await pending;
    },
  };
};
