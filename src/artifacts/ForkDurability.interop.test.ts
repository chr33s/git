import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { promisify } from "node:util";
import { describe, it } from "@effect/vitest";
import { Effect } from "effect";
import { borrowersOf } from "../git/Node.ts";
import { fastImport, gitIn, hasGit, importCommit } from "../testing/Git.ts";
import { RepoStores, repoStoresNode } from "./Namespace.ts";

const child = `
import fs from "node:fs/promises";
import path from "node:path";
import { syncBuiltinESMExports } from "node:module";
import { Effect } from "effect";
import { RepoStores, repoStoresNode } from ${JSON.stringify(new URL("./Namespace.ts", import.meta.url).href)};
const [root, fault, target] = process.argv.slice(1);
const open = fs.open;
fs.open = async function (...args) {
  if (fault === "kill-before-borrowers" && String(args[0]).startsWith(target + ".") && String(args[0]).endsWith(".tmp")) process.kill(process.pid, "SIGKILL");
  const handle = await Reflect.apply(open, fs, args);
  const name = String(args[0]);
  if (fault === "borrowers-replace" && name.startsWith(target + ".") && name.endsWith(".tmp")) {
    const write = handle.writeFile.bind(handle);
    handle.writeFile = async contents => { await write(contents.slice(0, 3)); throw Object.assign(new Error("fixture partial write"), {code: "EIO"}); };
  }
  const stat = await handle.stat();
  const expected = await fs.stat(path.dirname(target));
  if ((fault === "alternates-sync" && name.startsWith(target + ".") && name.endsWith(".tmp")) ||
      (fault === "borrowers-sync" && name.startsWith(target + ".") && name.endsWith(".tmp")) ||
      (fault === "borrowers-unlink-directory" && stat.isDirectory() && stat.dev === expected.dev && stat.ino === expected.ino)) {
    handle.sync = async () => { throw Object.assign(new Error("fixture sync failure"), {code: "EIO"}); };
  }
  return handle;
};
syncBuiltinESMExports();
console.log(await Effect.runPromise(RepoStores.use(stores =>
  fault === "borrowers-replace" || fault === "borrowers-unlink-directory"
    ? stores.forget("child") : stores.fork("child", "second")
).pipe(Effect.provide(repoStoresNode(root)), Effect.match({onSuccess: () => "acknowledged", onFailure: error => error._tag}))));
`;

describe.skipIf(!hasGit)("fork metadata durability", () => {
  for (const fault of [
    "alternates-sync",
    "borrowers-sync",
    "borrowers-replace",
    "borrowers-unlink-directory",
    "kill-before-borrowers",
  ]) {
    it(`preserves borrowed objects and permits retry after ${fault}`, async () => {
      const root = await fs.realpath(await fs.mkdtemp(path.join(os.tmpdir(), "fork-durable-")));
      const use = <A, E>(operation: (stores: RepoStores["Service"]) => Effect.Effect<A, E>) =>
        Effect.runPromise(RepoStores.use(operation).pipe(Effect.provide(repoStoresNode(root))));
      try {
        for (const name of ["first", "second", "child", "sibling"]) {
          await fs.mkdir(path.join(root, name));
          gitIn(path.join(root, name))("init", "--bare", "-q", "-b", "main");
        }
        for (const name of ["first", "second"])
          fastImport(
            path.join(root, name),
            importCommit({
              branch: "refs/heads/main",
              mark: 1,
              message: name,
              files: [{ path: "file", content: name }],
            }),
          );
        const oid = gitIn(path.join(root, "first"))("rev-parse", "main:file").trim();
        const secondOid = gitIn(path.join(root, "second"))("rev-parse", "main:file").trim();
        await use((stores) => stores.fork("child", "first"));
        if (fault !== "borrowers-unlink-directory")
          await use((stores) => stores.fork("sibling", "first"));
        const target = path.join(
          root,
          fault === "alternates-sync"
            ? "child"
            : fault === "borrowers-sync" || fault === "kill-before-borrowers"
              ? "second"
              : "first",
          "objects",
          "info",
          fault === "alternates-sync" ? "alternates" : "borrowers",
        );
        const before = await fs.readFile(
          path.join(root, "first", "objects", "info", "borrowers"),
          "utf8",
        );
        for (let attempt = 0; attempt < 2; attempt++) {
          const execution = promisify(execFile)(
            process.execPath,
            ["--input-type=module", "-e", child, root, fault, target],
            { timeout: 30_000 },
          );
          if (fault === "kill-before-borrowers") {
            await assert.rejects(execution, { signal: "SIGKILL" });
            await assert.rejects(fs.stat(target), { code: "ENOENT" });
            // The child has exited. Remove only its known reservation before
            // retrying; never infer a stale lock merely from its age.
            const lock = path.join(root, ".forks.json.lock");
            assert.ok((await fs.stat(lock)).isFile());
            await fs.unlink(lock);
          } else assert.equal((await execution).stdout.trim(), "ArtifactsError");
          if (fault === "borrowers-replace")
            assert.equal(await fs.readFile(target, "utf8"), before);
          const parent =
            fault === "borrowers-sync" || fault === "kill-before-borrowers" ? "second" : "first";
          assert.ok(borrowersOf(path.join(root, parent)).includes("child"));
          assert.equal(
            gitIn(path.join(root, "child"))(
              "cat-file",
              "blob",
              parent === "second" ? secondOid : oid,
            ),
            parent,
          );
          assert.equal(
            JSON.parse(await fs.readFile(path.join(root, ".forks.json"), "utf8")).child,
            "first",
          );
          assert.ok(
            !(await fs.readdir(root, { recursive: true })).some(
              (name) => name.endsWith(".tmp") || name.endsWith(".lock"),
            ),
          );
        }
        if (fault === "borrowers-replace" || fault === "borrowers-unlink-directory") {
          await use((stores) => stores.forget("child"));
          await assert.rejects(fs.stat(path.join(root, "child", "objects", "info", "alternates")), {
            code: "ENOENT",
          });
        } else {
          await use((stores) => stores.fork("child", "second"));
          assert.equal(gitIn(path.join(root, "child"))("cat-file", "blob", secondOid), "second");
        }
        if (fault !== "borrowers-unlink-directory")
          assert.equal(gitIn(path.join(root, "sibling"))("cat-file", "blob", oid), "first");
        gitIn(path.join(root, "first"))("fsck", "--strict");
        gitIn(path.join(root, "second"))("fsck", "--strict");
      } finally {
        await fs.rm(root, { recursive: true, force: true });
      }
    });
  }
});
