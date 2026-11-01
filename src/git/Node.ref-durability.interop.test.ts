import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { promisify } from "node:util";
import { describe, it } from "@effect/vitest";
import { gitIn, hasGit } from "../testing/Git.ts";

const writer = `
import fs from "node:fs/promises";
import path from "node:path";
import { syncBuiltinESMExports } from "node:module";
import { Effect } from "effect";
import { stores } from ${JSON.stringify(new URL("./Node.ts", import.meta.url).href)};
import { RefStore } from ${JSON.stringify(new URL("./Store.ts", import.meta.url).href)};
const [root, fault] = process.argv.slice(1);
const open = fs.open;
fs.open = async (...args) => {
  const handle = await Reflect.apply(open, fs, args);
  const target = String(args[0]);
  if ((fault === "packed-file" && target.startsWith(path.join(root, "packed-refs") + ".") && target.endsWith(".tmp")) ||
      (fault === "packed-directory" && target === root) ||
      (fault === "loose-directory" && target === path.join(root, "refs", "heads")) ||
      (fault === "reflog" && target === path.join(root, "logs", "refs", "heads", "main"))) {
    handle.sync = async () => { throw Object.assign(new Error("private fixture secret"), {code: "EIO"}); };
  }
  return handle;
};
syncBuiltinESMExports();
console.log(await Effect.runPromise(Effect.gen(function* () {
  const refs = yield* RefStore;
  // Prime the packed-ref cache before a rename whose directory sync can fail.
  const before = yield* refs.read("refs/heads/main");
  const value = fault === "reflog" ? before : null;
  const result = yield* refs.apply([{name: "refs/heads/main", value, expected: before}], {atomic: true}).pipe(
    Effect.match({onSuccess: rows => rows[0].applied ? "applied" : "rejected", onFailure: error => error._tag + ":" + error.operation}),
  );
  return result + "\\n" + (yield* refs.read("refs/heads/main"));
}).pipe(Effect.provide(stores(root)))));
`;

describe.skipIf(!hasGit)("Node ref persistence faults", () => {
  for (const fault of ["packed-file", "packed-directory", "loose-directory", "reflog"]) {
    it(`reports ${fault} failure without silently losing acknowledged history`, async () => {
      const root = await fs.realpath(await fs.mkdtemp(path.join(os.tmpdir(), "git-ref-persist-")));
      try {
        const git = gitIn(root);
        git("init", "-q", "-b", "main");
        await fs.writeFile(path.join(root, "file"), "acknowledged\n");
        git("add", "file");
        git("commit", "-qm", "before fault");
        const head = git("rev-parse", "HEAD").trim();
        const repository = path.join(root, ".git");
        if (fault.startsWith("packed")) git("pack-refs", "--all", "--prune");
        const result = await promisify(execFile)(
          process.execPath,
          ["--input-type=module", "-e", writer, repository, fault],
          { timeout: 20_000 },
        );
        const output = result.stdout + result.stderr;
        assert.doesNotMatch(output, /private fixture secret/);
        if (fault === "reflog") {
          assert.match(output, /reflog persistence failed; the ref update remains applied/);
          assert.match(output, new RegExp(`applied\\n${head}`));
        } else {
          // A directory fault also prevents confirming the restored ref's
          // ancestor chain. Visible old bytes do not prove durable rollback.
          assert.match(
            output,
            new RegExp(
              `${fault.endsWith("directory") ? "StorageFailure:rollback" : "rejected"}\\n${head}`,
            ),
          );
        }
        // Reopen through Git, rather than trusting the in-process packed cache.
        assert.equal(git("rev-parse", "HEAD").trim(), head);
        assert.equal(git("show", "HEAD:file"), "acknowledged\n");
        git("fsck", "--strict");
        assert.equal(
          (await fs.readdir(repository, { recursive: true })).some(
            (entry) => entry.endsWith(".tmp") || entry.endsWith(".lock"),
          ),
          false,
        );
      } finally {
        await fs.rm(root, { recursive: true, force: true });
      }
    });
  }
});
