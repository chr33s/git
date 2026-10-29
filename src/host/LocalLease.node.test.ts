import assert from "node:assert/strict";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { it } from "@effect/vitest";
import { Effect } from "effect";
import * as Lease from "./LocalLease.node.ts";

it("excludes a live owner and recovers retained state after SIGKILL without deleting a stale PID file", async () => {
  const directory = await fs.mkdtemp(path.join(os.tmpdir(), "git-local-lease-"));
  const lock = path.join(directory, "worker.lock");
  const journal = path.join(directory, "checkpoint.json");
  const module = new URL("./LocalLease.node.ts", import.meta.url).href;
  const script = `import { Effect } from 'effect';
    import * as fs from 'node:fs';
    import * as Lease from ${JSON.stringify(module)};
    await Effect.runPromise(Lease.withLock(${JSON.stringify(lock)}, Effect.gen(function* () {
      fs.writeFileSync(${JSON.stringify(journal)}, JSON.stringify({cursor: 3}));
      process.stdout.write('locked');
      yield* Effect.never;
    })));`;
  const child = spawn(process.execPath, ["--input-type=module", "-e", script], {
    stdio: ["ignore", "pipe", "pipe"],
  });
  try {
    const ready = await Promise.race([
      once(child.stdout, "data").then(([data]) => String(data)),
      once(child, "exit").then(() => "exited before acquiring lease"),
    ]);
    assert.equal(ready, "locked");
    const contended = await Effect.runPromise(
      Lease.withLock(lock, Effect.void).pipe(Effect.result),
    );
    assert.equal(contended._tag, "Failure");
    const stopped = once(child, "exit");
    child.kill("SIGKILL");
    await stopped;
    assert.equal(JSON.parse(await fs.readFile(lock, "utf8")).pid, child.pid);
    await Effect.runPromise(
      Lease.withLock(
        lock,
        Effect.promise(async () => {
          assert.deepEqual(JSON.parse(await fs.readFile(journal, "utf8")), { cursor: 3 });
        }),
      ),
    );
    await assert.rejects(fs.access(lock));
    // A failed protected operation also releases both the OS lease and metadata.
    await Effect.runPromise(Lease.withLock(lock, Effect.fail("fixture")).pipe(Effect.result));
    await Effect.runPromise(Lease.withLock(lock, Effect.void));
  } finally {
    child.kill("SIGKILL");
    await fs.rm(directory, { recursive: true, force: true });
  }
});
