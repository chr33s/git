import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { promisify } from "node:util";
import { describe, it } from "@effect/vitest";
import { gitIn, hasGit } from "../testing/Git.ts";

// Preserve real files and store logic; inject I/O faults only in this child.
const writer = `
import fs from "node:fs/promises";
import path from "node:path";
import { syncBuiltinESMExports } from "node:module";
import { Effect } from "effect";
import { filePacks } from ${JSON.stringify(new URL("./Node.ts", import.meta.url).href)};
const [root, name, fault] = process.argv.slice(1);
const directory = path.join(root, "objects", "pack");
const packPath = path.join(directory, name + ".pack");
const indexPath = path.join(directory, name + ".idx");
const pack = await fs.readFile(packPath);
const index = await fs.readFile(indexPath);
const events = [];
let published = "none";
const fail = () => { throw Object.assign(new Error("fixture I/O failure"), {code: "EIO"}); };
const open = fs.open;
fs.open = async function (...args) {
  const handle = await Reflect.apply(open, fs, args);
  const target = String(args[0]);
  const part = target.startsWith(packPath + ".") ? "pack" : target.startsWith(indexPath + ".") ? "index" : null;
  if (part !== null && target.endsWith(".tmp")) {
    const sync = handle.sync.bind(handle);
    handle.sync = async () => {
      events.push(part + "-sync");
      if (fault === part + "-sync") fail();
      return sync();
    };
    if (fault === part + "-write") {
      const write = handle.writeFile.bind(handle);
      handle.writeFile = async bytes => { await write(bytes.subarray(0, 8)); fail(); };
    }
  } else if (target === directory) {
    const sync = handle.sync.bind(handle);
    handle.sync = async () => {
      events.push(published + "-directory");
      if (fault === published + "-directory") fail();
      return sync();
    };
  }
  return handle;
};
const rename = fs.rename;
fs.rename = async (from, to) => {
  const part = to === packPath ? "pack" : "index";
  if (fault === "crash-before-index" && part === "index") process.kill(process.pid, "SIGKILL");
  await rename(from, to);
  published = part;
  events.push(part + "-rename");
};
syncBuiltinESMExports();
console.log(await Effect.runPromise(filePacks(root).write({name, pack, index}).pipe(
  Effect.match({onSuccess: () => "written", onFailure: error => error._tag + ":" + error.operation}),
)));
console.log(events.join(","));
`;

describe.skipIf(!hasGit)("Node pack publication durability", () => {
  for (const fault of [
    "pack-write",
    "pack-sync",
    "pack-directory",
    "index-write",
    "index-sync",
    "index-directory",
    "crash-before-index",
  ]) {
    it(`preserves acknowledged history after ${fault}`, async () => {
      const root = await fs.realpath(await fs.mkdtemp(path.join(os.tmpdir(), "git-pack-publish-")));
      try {
        const git = gitIn(root);
        git("init", "-q", "-b", "main");
        await fs.writeFile(path.join(root, "file"), "acknowledged history\n");
        git("add", "file");
        git("commit", "-qm", "before fault");
        const head = git("rev-parse", "HEAD").trim();
        git("gc", "--prune=now");
        const repository = path.join(root, ".git");
        const directory = path.join(repository, "objects", "pack");
        const packName = (await fs.readdir(directory)).find((entry) => entry.endsWith(".pack"));
        assert.ok(packName);
        const name = packName.slice(0, -5);
        const execute = (failure: string) =>
          promisify(execFile)(
            process.execPath,
            ["--input-type=module", "-e", writer, repository, name, failure],
            { timeout: 20_000 },
          );
        if (fault === "crash-before-index") {
          await assert.rejects(
            execute(fault),
            (error: Error) => "signal" in error && error.signal === "SIGKILL",
          );
        } else {
          const failed = await execute(fault);
          assert.match(failed.stdout, /^StorageFailure:packs.write\n/m);
          assert.equal(
            (await fs.readdir(directory)).some((entry) => entry.endsWith(".tmp")),
            false,
          );
        }
        // New git processes reopen the files after the writer has exited.
        git("verify-pack", path.join(directory, `${name}.idx`));
        git("fsck", "--strict");
        assert.equal(git("rev-parse", "HEAD").trim(), head);
        assert.equal(git("show", "HEAD:file"), "acknowledged history\n");
        const retried = await execute("none");
        assert.equal(
          retried.stdout.trim(),
          "written\npack-sync,pack-rename,pack-directory,index-sync,index-rename,index-directory",
        );
        git("fsck", "--strict");
      } finally {
        await fs.rm(root, { recursive: true, force: true });
      }
    });
  }
});
