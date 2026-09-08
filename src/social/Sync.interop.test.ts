import assert from "node:assert/strict";
import * as fs from "node:fs/promises";
import * as http from "node:http";
import * as os from "node:os";
import * as path from "node:path";
import { pathToFileURL } from "node:url";
import { describe, it } from "@effect/vitest";
import { Effect, Fiber, Predicate } from "effect";

import { runProcess } from "../testing/Process.ts";
import { hasGit } from "../testing/Git.ts";
import { enableHubUnder } from "../testing/Hub.ts";
import { principalId } from "../trust/Principal.ts";
import { syncIdentity } from "./Sync.node.ts";

const identityFixture = async (root: string) => {
  const initialized = await runProcess({
    command: "git",
    args: ["init", "--bare", "--quiet", path.join(root, "identity")],
  });
  assert.equal(initialized.code, 0);
  return enableHubUnder(root, "identity", []);
};

describe.skipIf(!hasGit)("identity synchronization cancellation", () => {
  it.skipIf(process.platform === "win32")(
    "stops the stock-Git identity preflight on interruption",
    async () => {
      const root = await fs.mkdtemp(path.join(os.tmpdir(), "identity-cancel-"));
      const fixture = await identityFixture(root);
      const received = Promise.withResolvers<void>();
      const closed = Promise.withResolvers<void>();
      const server = http.createServer((_request, response) => {
        response.on("close", () => closed.resolve());
        received.resolve();
      });
      await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
      const address = server.address();
      assert.ok(address !== null && !Predicate.isString(address));
      const fiber = Effect.runFork(
        syncIdentity({
          root,
          principal: principalId(fixture.repoId),
          url: `http://127.0.0.1:${address.port}/identity`,
        }),
      );
      try {
        await received.promise;
        await Effect.runPromise(Fiber.interrupt(fiber));
        await Effect.runPromise(
          Effect.promise(() => closed.promise).pipe(Effect.timeout("2 seconds")),
        );
      } finally {
        server.closeAllConnections();
        await Effect.runPromise(Fiber.interrupt(fiber));
        await new Promise<void>((resolve) => server.close(() => resolve()));
        await fs.rm(root, { recursive: true, force: true });
      }
    },
  );

  it("interrupts the isolated fetch runtime after stock Git verifies the identity", async () => {
    const root = await fs.mkdtemp(path.join(os.tmpdir(), "identity-fetch-cancel-"));
    const fixture = await identityFixture(root);
    const original = globalThis.fetch;
    const started = Promise.withResolvers<void>();
    const response = Promise.withResolvers<Response>();
    let signal: AbortSignal | null | undefined;
    globalThis.fetch = async (_url, init) => {
      signal = init?.signal;
      signal?.addEventListener("abort", () => response.reject(signal?.reason), { once: true });
      started.resolve();
      return response.promise;
    };
    const fiber = Effect.runFork(
      syncIdentity({
        root,
        principal: principalId(fixture.repoId),
        url: pathToFileURL(path.join(root, "identity")).href,
      }),
    );
    try {
      await started.promise;
      assert.ok(signal);
      await Effect.runPromise(Fiber.interrupt(fiber));
      assert.equal(signal.aborted, true, "the isolated download must stop with its caller");
    } finally {
      response.reject(new Error("test cleanup"));
      await Effect.runPromise(Fiber.interrupt(fiber));
      globalThis.fetch = original;
      await fs.rm(root, { recursive: true, force: true });
    }
  });
});
