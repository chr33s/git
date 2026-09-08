import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { describe, it } from "@effect/vitest";
import { Effect } from "effect";
import { hasGit } from "../testing/Git.ts";

describe.skipIf(!hasGit)("push memory", () => {
  it.live("keeps live upload buffers below half of a 64 MiB pack", () =>
    Effect.promise(async () => {
      const script = `
        import assert from "node:assert/strict";
        import * as fs from "node:fs/promises";
        import * as os from "node:os";
        import * as path from "node:path";
        import * as http from "node:http";
        import { randomFillSync } from "node:crypto";
        import { execFileSync, spawn } from "node:child_process";
        import { Effect, Layer, Stream } from "effect";
        import * as Repository from ${JSON.stringify(new URL("../git/Repository.ts", import.meta.url).href)};
        import { stores } from ${JSON.stringify(new URL("../git/Node.ts", import.meta.url).href)};
        import { gitIn, gitEnv } from ${JSON.stringify(new URL("../testing/Git.ts", import.meta.url).href)};
        import { pkt, FLUSH } from ${JSON.stringify(new URL("../git/Pkt.ts", import.meta.url).href)};
        import { push } from ${JSON.stringify(new URL("./Push.ts", import.meta.url).href)};
        const root = await fs.mkdtemp(path.join(os.tmpdir(), "push-memory-"));
        const source = path.join(root, "source");
        const target = path.join(root, "target.git");
        const children = [];
        const server = http.createServer((request, response) => {
          if (request.method === "GET") {
            const refs = execFileSync("git", ["receive-pack", "--stateless-rpc", "--advertise-refs", target], {env: gitEnv});
            response.end(Buffer.concat([pkt("# service=git-receive-pack\\n"), FLUSH, refs]));
            return;
          }
          const receiver = spawn("git", ["receive-pack", "--stateless-rpc", target], {env: gitEnv});
          children.push({ receiver, stopped: new Promise(resolve => receiver.once("close", resolve)) });
          receiver.stdin.on("error", () => {});
          receiver.stderr.resume();
          request.pipe(receiver.stdin);
          receiver.stdout.pipe(response);
        });
        try {
          await fs.mkdir(source);
          const git = gitIn(source);
          git("init", "-q", "-b", "main");
          for (let index = 0; index < 64; index++) {
            await fs.writeFile(path.join(source, "blob-" + index), randomFillSync(new Uint8Array(1024 * 1024)));
          }
          git("add", "."); git("commit", "-qm", "ordinary binary assets");
          git("init", "--bare", "-q", "-b", "main", target);
          const repository = await Effect.runPromise(Repository.Repository.pipe(Effect.provide(
            Repository.layer.pipe(Layer.provide(Repository.hooksNoop), Layer.provide(stores(path.join(source, ".git"))))
          )));
          await new Promise(resolve => server.listen(0, "127.0.0.1", resolve));
          global.gc();
          const baseline = process.memoryUsage().arrayBuffers;
          const sample = () => { global.gc(); return process.memoryUsage().arrayBuffers - baseline; };
          let peak = 0;
          let bytes = 0;
          const measured = Repository.Repository.of({...repository, packOids: oids => repository.packOids(oids).pipe(Stream.tap(chunk => Effect.sync(() => {
            bytes += chunk.length;
            peak = Math.max(peak, sample());
          })))});
          const original = globalThis.fetch;
          globalThis.fetch = (input, init) => {
            if (init?.method === "POST") peak = Math.max(peak, sample());
            return original(input, init);
          };
          const result = await Effect.runPromise(push({url: "http://127.0.0.1:" + server.address().port,
            refs: [{local: "refs/heads/main", remote: "refs/heads/main"}]
          }).pipe(Effect.provideService(Repository.Repository, measured)));
          assert.equal(result[0].ok, true);
          assert.ok(bytes > 64 * 1024 * 1024);
          assert.ok(peak < bytes / 2, "live upload buffers " + peak + " for pack " + bytes);
          assert.equal(git("--git-dir", target, "ls-tree", "-r", "--name-only", "main").trim().split("\\n").length, 64);
          console.log(JSON.stringify({bytes, peak}));
        } finally {
          server.closeAllConnections();
          await new Promise(resolve => server.close(resolve));
          for (const child of children) { child.receiver.kill(); await child.stopped; }
          await fs.rm(root, {recursive: true, force: true});
        }
      `;
      const result = await promisify(execFile)(
        process.execPath,
        ["--expose-gc", "--input-type=module", "-e", script],
        { timeout: 60_000 },
      );
      console.info(result.stdout.trim());
    }),
  );
});
