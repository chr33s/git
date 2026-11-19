import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { promisify } from "node:util";
import { describe, it } from "@effect/vitest";
import { Effect } from "effect";
import { stores } from "./Node.ts";
import { RefStore } from "./Store.ts";
import { gitIn, hasGit } from "../testing/Git.ts";

const writer = `
import fs from "node:fs/promises";
import path from "node:path";
import { syncBuiltinESMExports } from "node:module";
import { Effect } from "effect";
import { stores } from ${JSON.stringify(new URL("./Node.ts", import.meta.url).href)};
import { RefStore } from ${JSON.stringify(new URL("./Store.ts", import.meta.url).href)};
const [root, before, after, fault] = process.argv.slice(1);
const journal = path.join(root, ".git-plus-ref-transaction.json");
const open = fs.open;
fs.open = async (...args) => {
  const handle = await Reflect.apply(open, fs, args);
  const target = String(args[0]);
  if (target.startsWith(journal + ".") && target.endsWith(".tmp")) {
    if (fault === "journal-full") {
      const write = handle.writeFile.bind(handle);
      handle.writeFile = async bytes => {
        await write(bytes.slice(0, 8));
        throw Object.assign(new Error("fixture disk full"), {code: "ENOSPC"});
      };
    }
    if (fault === "journal-sync") handle.sync = async () => {
      throw Object.assign(new Error("fixture sync failure"), {code: "EIO"});
    };
  }
  return handle;
};
const rename = fs.rename;
fs.rename = async (from, to) => {
  await rename(from, to);
  if (fault === "before-first-ref" && to === journal) process.kill(process.pid, "SIGKILL");
  if (fault === "first-ref" && to === path.join(root, "refs", "heads", "main")) process.kill(process.pid, "SIGKILL");
};
const remove = fs.rm;
fs.rm = async (target, options) => {
  if (fault === "before-completion" && target === journal) process.kill(process.pid, "SIGKILL");
  return remove(target, options);
};
syncBuiltinESMExports();
const result = await Effect.runPromise(RefStore.use(refs => refs.apply([
  {name: "refs/heads/main", expected: before, value: after},
  {name: "refs/heads/other", expected: before, value: after},
], {atomic: true})).pipe(Effect.provide(stores(root)), Effect.match({
  onSuccess: rows => String(rows.every(row => row.applied)),
  onFailure: error => "failed:" + error.operation,
})));
console.log(result);
`;

describe.skipIf(!hasGit)("interrupted Node ref transactions", () => {
  for (const fault of [
    "journal-full",
    "journal-sync",
    "before-first-ref",
    "first-ref",
    "before-completion",
  ] as const) {
    it(`handles ${fault} without losing acknowledged history`, async () => {
      const root = await fs.mkdtemp(path.join(os.tmpdir(), "git-ref-transaction-"));
      try {
        const git = gitIn(root);
        git("init", "-q", "-b", "main");
        await fs.writeFile(path.join(root, "file"), "before\n");
        git("add", "file");
        git("commit", "-qm", "before");
        const before = git("rev-parse", "HEAD").trim();
        git("branch", "other");
        await fs.writeFile(path.join(root, "file"), "after\n");
        git("commit", "-qam", "after");
        const after = git("rev-parse", "HEAD").trim();
        git("update-ref", "refs/heads/main", before);
        const repository = path.join(root, ".git");
        const journal = path.join(repository, ".git-plus-ref-transaction.json");
        const execute = (point: string) =>
          promisify(execFile)(
            process.execPath,
            ["--input-type=module", "-e", writer, repository, before, after, point],
            { timeout: 20_000 },
          );
        if (fault === "journal-full" || fault === "journal-sync") {
          assert.equal((await execute(fault)).stdout.trim().split("\n").at(-1), "failed:write");
          assert.equal(git("rev-parse", "refs/heads/main").trim(), before);
          assert.equal(git("rev-parse", "refs/heads/other").trim(), before);
          await assert.rejects(fs.stat(journal), { code: "ENOENT" });
          assert.equal(
            (await fs.readdir(repository, { recursive: true })).some(
              (entry) => entry.endsWith(".tmp") || entry.endsWith(".lock"),
            ),
            false,
          );
          assert.equal((await execute("none")).stdout.trim(), "true");
          git("fsck", "--strict");
          return;
        }
        await assert.rejects(
          execute(fault),
          (error: Error) => "signal" in error && error.signal === "SIGKILL",
        );
        const evidence = await fs.readFile(journal, "utf8");
        assert.deepEqual(JSON.parse(evidence), {
          version: 1,
          worktree: repository,
          updates: [
            { name: "refs/heads/main", from: before, to: after },
            { name: "refs/heads/other", from: before, to: after },
          ],
        });
        assert.equal((await fs.stat(journal)).mode & 0o777, 0o600);
        assert.equal(
          git("rev-parse", "refs/heads/main").trim(),
          fault === "before-first-ref" ? before : after,
        );
        assert.equal(
          git("rev-parse", "refs/heads/other").trim(),
          fault === "before-completion" ? after : before,
        );
        const blocked = await Effect.runPromise(
          RefStore.use((refs) =>
            Effect.forEach(
              [
                refs.read("refs/heads/main").pipe(Effect.asVoid),
                refs.list().pipe(Effect.asVoid),
                refs.resolve("HEAD").pipe(Effect.asVoid),
                refs.head.pipe(Effect.asVoid),
                refs.setHead("refs/heads/other"),
                refs.apply([{ name: "refs/heads/new", value: null }]).pipe(Effect.asVoid),
              ],
              (operation) => Effect.result(operation),
            ),
          ).pipe(Effect.provide(stores(repository))),
        );
        assert.ok(blocked.every((result) => result._tag === "Failure"));
        assert.equal(await fs.readFile(journal, "utf8"), evidence);

        // The killed child has been reaped. In an offline recovery, preserve a
        // copy first, then remove only its known reservations and restore refs.
        await fs.copyFile(journal, path.join(root, "recovery-evidence.json"));
        for (const lock of [
          journal + ".lock",
          "packed-refs.lock",
          "refs/heads/main.lock",
          "refs/heads/other.lock",
        ]) {
          await fs.rm(path.isAbsolute(lock) ? lock : path.join(repository, lock), { force: true });
        }
        git("update-ref", "refs/heads/main", before);
        git("update-ref", "refs/heads/other", before);
        // Persist the offline repair before clearing its recovery marker.
        for (const relative of ["refs/heads/main", "refs/heads/other", "refs/heads", "refs"]) {
          const handle = await fs.open(path.join(repository, relative), "r");
          try {
            await handle.sync();
          } finally {
            await handle.close();
          }
        }
        git("fsck", "--strict");
        await fs.unlink(journal);
        const directory = await fs.open(repository, "r");
        try {
          await directory.sync();
        } finally {
          await directory.close();
        }
        const refs = await Effect.runPromise(
          RefStore.use((store) => store.list()).pipe(Effect.provide(stores(repository))),
        );
        assert.deepEqual(
          [...refs].sort((a, b) => (a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0)),
          [
            ["refs/heads/main", before],
            ["refs/heads/other", before],
          ],
        );
        assert.equal((await execute("none")).stdout.trim(), "true");
        await assert.rejects(fs.stat(journal), { code: "ENOENT" });
        git("fsck", "--strict");
      } finally {
        await fs.rm(root, { recursive: true, force: true });
      }
    });
  }
});
