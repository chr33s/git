/**
 * The CLI, black-box: every test spawns `node src/cli/main.ts …` and looks
 * only at exit codes and output — the same interface a user gets.
 */
import assert from "node:assert/strict";
import { type ChildProcess, execFile, spawn } from "node:child_process";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { describe, it } from "@effect/vitest";
import { promisify } from "node:util";

import { Effect, Layer } from "effect";

import { stores } from "../git/Node.ts";
import * as GitRepository from "../git/Repository.ts";
import { Repository } from "../git/Repository.ts";
import { serve } from "../host/Node.ts";
import { enableHubUnder, opensshPrivateKey } from "../testing/Hub.ts";

const execFileAsync = promisify(execFile);
const entry = path.join(import.meta.dirname, "main.ts");

const cli = async (args: string[], env?: Record<string, string>) => {
  const result = await execFileAsync("node", [entry, ...args], {
    encoding: "utf8",
    env: { ...process.env, ...env },
  });
  return result.stdout;
};

/** The same, for the one command whose input is a stream rather than a flag. */
const withStdin = (args: ReadonlyArray<string>, input: string): Promise<string> =>
  new Promise((resolve, reject) => {
    const child = spawn("node", [entry, ...args], { stdio: ["pipe", "pipe", "pipe"] });
    let out = "";
    let err = "";
    child.stdout.setEncoding("utf8");
    child.stderr.setEncoding("utf8");
    child.stdout.on("data", (chunk: string) => {
      out += chunk;
    });
    child.stderr.on("data", (chunk: string) => {
      err += chunk;
    });
    child.on("error", reject);
    child.on("close", (code) => {
      if (code === 0) resolve(out.trim());
      else reject(new Error(`exit ${String(code)}: ${err}${out}`));
    });
    child.stdin.end(input);
  });

const author = {
  name: "Alice",
  email: "alice@example.com",
  at: new Date(1_700_000_000_000),
  offset: 0,
};

const seed = (directory: string, message: string) =>
  Effect.runPromise(
    Effect.gen(function* () {
      const repository = yield* Repository;
      const blob = yield* repository.writeBlob(new TextEncoder().encode(`${message}\n`));
      const tree = yield* repository.writeTree([{ mode: "100644", name: "f.txt", oid: blob }]);
      return yield* repository.commit({ branch: "main", tree, message, author });
    }).pipe(
      Effect.provide(
        GitRepository.layer.pipe(
          Layer.provide(GitRepository.hooksNoop),
          Layer.provide(stores(directory)),
        ),
      ),
    ),
  );

describe("cli", () => {
  it("init, refs and log against a seeded repository", async () => {
    const root = await fs.mkdtemp(path.join(os.tmpdir(), "cli-basic-"));
    try {
      const initOut = await cli(["init", "--root", root, "basic"]);
      assert.match(initOut, /Initialized empty repository/);

      const first = await seed(path.join(root, "basic"), "first");
      const second = await seed(path.join(root, "basic"), "second");

      const refsOut = await cli(["refs", "--root", root, "basic"]);
      assert.match(refsOut, new RegExp(`${second}\trefs/heads/main`));

      const logOut = await cli(["log", "--root", root, "basic"]);
      assert.deepEqual(
        logOut
          .trim()
          .split("\n")
          .map((line) => line.split(" ").slice(1).join(" ")),
        ["second", "first"],
      );
      assert.ok(logOut.includes(first));

      await assert.rejects(cli(["log", "--root", root, "basic", "--ref", "bogus"]));
    } finally {
      await fs.rm(root, { recursive: true, force: true });
    }
  });

  it("answers git's credential helper protocol on stdin", async () => {
    // git does not take a password on a command line: it runs a helper and
    // speaks a line protocol at it. Without this, "mint a credential" was a
    // thing a person did by hand and pasted, which is the manual step the
    // delegated path exists to remove.
    const root = await fs.mkdtemp(path.join(os.tmpdir(), "cli-helper-"));
    try {
      const member = await enableHubUnder(root, "helped", ["repo.read", "source.push"]);
      const keyFile = path.join(root, "member.key");
      await fs.writeFile(keyFile, opensshPrivateKey(member.member, "member@example.com"), {
        mode: 0o600,
      });

      // What git writes once `credential.useHttpPath` is on. Off — which is
      // its default — there is no `path` line, and a credential scoped to one
      // repository cannot be minted from protocol and host alone. The refusal
      // has to say which knob turns it on, or the helper fails obscurely on
      // every stock clone.
      const blind = await withStdin(
        ["credential-helper", "--root", root, "--key", keyFile, "get"],
        "protocol=http\nhost=git.example.com\n\n",
      ).then(
        () => null,
        (error: Error) => error.message,
      );
      assert.notEqual(blind, null, "no repository named is a refusal, not a guess");
      assert.match(blind ?? "", /useHttpPath/);

      // Spelled the way git spells a clone URL, trailing `.git` and all. The
      // server strips that suffix before it looks for a directory, so a helper
      // that does not reports every `host/repo.git` push as a repository with
      // no identity while the same push to `host/repo` works.
      const answered = await withStdin(
        ["credential-helper", "--root", root, "--key", keyFile, "get"],
        "protocol=http\nhost=git.example.com\npath=helped.git\n\n",
      );

      assert.match(answered, /^username=/m);
      const password = /^password=(.+)$/m.exec(answered)?.[1] ?? "";
      assert.notEqual(password, "", `no credential in: ${answered}`);

      // And it is the real thing: the server takes it.
      const server = await serve({ root });
      try {
        const refused = await fetch(`${server.url}/helped/info/refs?service=git-upload-pack`);
        assert.equal(refused.status, 401, "the repository is not public");
        const allowed = await fetch(`${server.url}/helped/info/refs?service=git-upload-pack`, {
          headers: { authorization: `Basic ${btoa(`x:${password}`)}` },
        });
        assert.equal(allowed.status, 200, "and the minted credential opens it");
      } finally {
        await server.close();
      }

      // `store` and `erase` are asked for after a push and must succeed
      // silently: exiting non-zero there reports a failure for a push that
      // worked.
      assert.equal(
        await withStdin(
          ["credential-helper", "--root", root, "--key", keyFile, "store"],
          "protocol=http\npath=helped\n\n",
        ),
        "",
      );
    } finally {
      await fs.rm(root, { recursive: true, force: true });
    }
  });

  it("says what to do when asked for a credential on a repository with no genesis", async () => {
    const root = await fs.mkdtemp(path.join(os.tmpdir(), "cli-credential-"));
    try {
      await seed(path.join(root, "plain"), "unclaimed");
      const failed = await cli([
        "credential",
        "--root",
        root,
        "--key",
        "/nonexistent/key",
        "plain",
      ]).then(
        () => null,
        (error: { stderr?: string; stdout?: string }) => error,
      );
      assert.ok(failed !== null, "a missing key must fail rather than mint something");
      assert.match(`${failed.stderr ?? ""}${failed.stdout ?? ""}`, /cannot read/);
    } finally {
      await fs.rm(root, { recursive: true, force: true });
    }
  });

  it("clones over smart HTTP, with and without a credential", async () => {
    const root = await fs.mkdtemp(path.join(os.tmpdir(), "cli-clone-"));
    const serverRoot = path.join(root, "server");
    const server = await serve({ root: serverRoot, allowAnonymousWrites: true });
    try {
      await seed(path.join(serverRoot, "origin"), "published");
      // Granting `repo.read` to somebody is what makes the repository private.
      const { credential: token } = await enableHubUnder(serverRoot, "origin", ["repo.read"]);

      const denied = await cli(["clone", "--root", root, `${server.url}/origin`, "denied"]).then(
        () => null,
        (error: { stderr?: string; stdout?: string }) => error,
      );
      assert.ok(denied !== null, "clone without a token must fail");
      assert.match(`${denied.stderr ?? ""}${denied.stdout ?? ""}`, /401/);

      const out = await cli([
        "clone",
        "--root",
        root,
        "--token",
        token,
        `${server.url}/origin`,
        "copy",
      ]);
      assert.match(out, /Cloned 1 ref\(s\)/);

      // The clone is a real git repository: the git binary reads it.
      const logOut = await execFileAsync(
        "git",
        [`--git-dir=${path.join(root, "copy")}`, "log", "--format=%s"],
        { encoding: "utf8" },
      );
      assert.equal(logOut.stdout.trim(), "published");
    } finally {
      await server.close();
      await fs.rm(root, { recursive: true, force: true });
    }
  });

  it("serves repositories via the serve command", async () => {
    const root = await fs.mkdtemp(path.join(os.tmpdir(), "cli-serve-"));
    await seed(path.join(root, "hosted"), "served");

    let child: ChildProcess | null = null;
    try {
      // No auth flags: whether a repository is guarded is the repository's
      // own answer now, so `serve` has nothing to be told about it.
      child = spawn("node", [entry, "serve", "--root", root, "--port", "0"], {
        stdio: ["ignore", "pipe", "pipe"],
      });
      const url = await new Promise<string>((resolve, reject) => {
        let buffered = "";
        child!.stdout!.on("data", (chunk: Buffer) => {
          buffered += chunk.toString();
          const match = buffered.match(/server on (http:\/\/[^,]+),/);
          if (match) resolve(match[1]!);
        });
        child!.on("exit", (code) => reject(new Error(`serve exited early: ${code}`)));
        setTimeout(() => reject(new Error(`serve never announced: ${buffered}`)), 15_000);
      });

      const response = await fetch(`${url}/hosted/refs`);
      assert.equal(response.status, 200);
      // SAFETY: the assertion below inspects exactly the payload the refs
      // endpoint documents; a response of any other shape fails the test.
      const body = (await response.json()) as { refs: Array<{ name: string }> };
      assert.deepEqual(
        body.refs.map((ref) => ref.name),
        ["refs/heads/main"],
      );
    } finally {
      child?.kill();
      await fs.rm(root, { recursive: true, force: true });
    }
  });

  it("serves a repository with a genesis only to its members", async () => {
    const root = await fs.mkdtemp(path.join(os.tmpdir(), "cli-serve-guarded-"));

    let child: ChildProcess | null = null;
    try {
      await seed(path.join(root, "guarded"), "members only");
      // The same server, and one repository under it that has claimed itself.
      // Nothing about `serve` changed — the genesis is what guards it.
      const { credential } = await enableHubUnder(root, "guarded", ["repo.read"]);

      child = spawn("node", [entry, "serve", "--root", root, "--port", "0"], {
        stdio: ["ignore", "pipe", "pipe"],
      });
      const url = await new Promise<string>((resolve, reject) => {
        let buffered = "";
        child!.stdout!.on("data", (chunk: Buffer) => {
          buffered += chunk.toString();
          const match = buffered.match(/server on (http:\/\/[^,]+),/);
          if (match) resolve(match[1]!);
        });
        child!.on("exit", (code) => reject(new Error(`serve exited early: ${code}`)));
        setTimeout(() => reject(new Error(`serve never announced: ${buffered}`)), 15_000);
      });

      const anonymous = await fetch(`${url}/guarded/refs`);
      assert.equal(anonymous.status, 401);

      const member = await fetch(`${url}/guarded/refs`, {
        headers: { authorization: `Bearer ${credential}` },
      });
      assert.equal(member.status, 200);
    } finally {
      child?.kill();
      await fs.rm(root, { recursive: true, force: true });
    }
  });

  it("inspects a repository: branch, tag, show, files, grep, diff", async () => {
    const root = await fs.mkdtemp(path.join(os.tmpdir(), "cli-inspect-"));
    try {
      const directory = path.join(root, "repo");
      const first = await seed(directory, "first");
      const second = await seed(directory, "second");

      // A branch, listed with the checked-out one marked.
      await cli(["branch", "--root", root, "-c", "feature", "repo"]);
      const branches = await cli(["branch", "--root", root, "repo"]);
      assert.match(branches, /^\* [0-9a-f]{40}\tmain$/m);
      assert.match(branches, /^ {2}[0-9a-f]{40}\tfeature$/m);

      // Annotated tags and lightweight ones, listed together.
      await cli(["tag", "--root", root, "--name", "v1", "-m", "release", "repo"]);
      await cli(["tag", "--root", root, "--name", "latest", "repo"]);
      const tags = await cli(["tag", "--root", root, "repo"]);
      assert.match(tags, /^[0-9a-f]{40}\tv1$/m);
      assert.match(tags, /^[0-9a-f]{40}\tlatest$/m);

      const shown = await cli(["show", "--root", root, "repo", second]);
      assert.match(shown, /commit/);
      assert.match(shown, /^second$/m);

      const listed = await cli(["files", "--root", root, "repo"]);
      assert.match(listed, /100644 [0-9a-f]{40}\tf\.txt/);

      const found = await cli(["grep", "--root", root, "repo", "second"]);
      assert.equal(found.trim(), "f.txt:1:second");

      const patch = await cli(["diff", "--root", root, "repo", first, second]);
      assert.match(patch, /^--- a\/f\.txt$/m);
      assert.match(patch, /^-first$/m);
      assert.match(patch, /^\+second$/m);
    } finally {
      await fs.rm(root, { recursive: true, force: true });
    }
  });

  it("merges, and exits non-zero when the merge conflicts", async () => {
    const root = await fs.mkdtemp(path.join(os.tmpdir(), "cli-merge-"));
    try {
      const directory = path.join(root, "repo");
      await seed(directory, "base");
      await cli(["branch", "--root", root, "-c", "side", "repo"]);

      // Both branches change the one file, on the same line.
      await Effect.runPromise(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const theirs = yield* repository.writeFiles({
            base: (yield* repository.readCommit((yield* repository.resolve("refs/heads/side"))!))
              .tree,
            changes: [{ path: "f.txt", content: new TextEncoder().encode("theirs\n") }],
          });
          yield* repository.commit({
            branch: "side",
            tree: theirs,
            message: "theirs",
            author,
          });
          const ours = yield* repository.writeFiles({
            base: (yield* repository.readCommit((yield* repository.resolve("refs/heads/main"))!))
              .tree,
            changes: [{ path: "f.txt", content: new TextEncoder().encode("ours\n") }],
          });
          yield* repository.commit({ branch: "main", tree: ours, message: "ours", author });
        }).pipe(
          Effect.provide(
            GitRepository.layer.pipe(
              Layer.provide(GitRepository.hooksNoop),
              Layer.provide(stores(directory)),
            ),
          ),
        ),
      );

      // A conflict is a failed merge to a shell, and the exit code says so.
      const failed = await cli([
        "merge",
        "--root",
        root,
        "repo",
        "refs/heads/main",
        "refs/heads/side",
      ]).then(
        () => null,
        (error: { code?: number; stdout?: string; stderr?: string }) => error,
      );
      assert.notEqual(failed, null);
      assert.equal(failed!.code, 1);
      assert.match(
        `${failed!.stdout ?? ""}${failed!.stderr ?? ""}`,
        /CONFLICT \(content\): f\.txt/,
      );

      // Choosing a side resolves it and moves the ref.
      const resolved = await cli([
        "merge",
        "--root",
        root,
        "-s",
        "theirs",
        "--into",
        "refs/heads/main",
        "repo",
        "refs/heads/main",
        "refs/heads/side",
      ]);
      assert.match(resolved, /^merged [0-9a-f]{40}$/m);
    } finally {
      await fs.rm(root, { recursive: true, force: true });
    }
  });

  it("pushes to a server and exports an archive", async () => {
    const root = await fs.mkdtemp(path.join(os.tmpdir(), "cli-push-"));
    const serverRoot = path.join(root, "server");
    const server = await serve({ root: serverRoot, allowAnonymousWrites: true });
    try {
      await seed(path.join(root, "local"), "pushed");

      const pushed = await cli(["push", "--root", root, "local", `${server.url}/remote`, "main"]);
      assert.match(pushed, /^ok refs\/heads\/main/m);

      // The server really has it: its own API says so.
      // SAFETY: the assertion below inspects exactly the payload the refs
      // endpoint documents; a response of any other shape fails the test.
      const refs = (await (await fetch(`${server.url}/remote/refs`)).json()) as {
        refs: Array<{ name: string }>;
      };
      assert.deepEqual(
        refs.refs.map((ref) => ref.name),
        ["refs/heads/main"],
      );

      // And the archive of what we pushed is a real tar.
      const tarball = path.join(root, "out.tar");
      await cli(["archive", "--root", root, "-o", tarball, "local"]);
      const listed = await execFileAsync("tar", ["-tf", tarball], { encoding: "utf8" });
      assert.match(listed.stdout, /f\.txt/);
    } finally {
      await server.close();
      await fs.rm(root, { recursive: true, force: true });
    }
  });

  it("checks integrity and collects garbage", async () => {
    const root = await fs.mkdtemp(path.join(os.tmpdir(), "cli-maint-"));
    try {
      const directory = path.join(root, "repo");
      await seed(directory, "kept");

      const clean = await cli(["fsck", "--root", root, "repo"]);
      assert.match(clean, /checked \d+ object\(s\)/);

      // A blob nothing references is what gc is for.
      await Effect.runPromise(
        Effect.gen(function* () {
          yield* (yield* Repository).writeBlob(new TextEncoder().encode("orphan\n"));
        }).pipe(
          Effect.provide(
            GitRepository.layer.pipe(
              Layer.provide(GitRepository.hooksNoop),
              Layer.provide(stores(directory)),
            ),
          ),
        ),
      );

      const dry = await cli(["gc", "--root", root, "-n", "repo"]);
      assert.match(dry, /would remove 1 of \d+ object\(s\)/);
      const swept = await cli(["gc", "--root", root, "repo"]);
      assert.match(swept, /removed 1 of \d+ object\(s\)/);
      const again = await cli(["gc", "--root", root, "repo"]);
      assert.match(again, /removed 0 of \d+ object\(s\)/);
    } finally {
      await fs.rm(root, { recursive: true, force: true });
    }
  });
});

/**
 * `bin` is not `main.ts` — it is a stub that turns node's compile cache on
 * first, which only works if nothing has been compiled yet. A static import
 * would look identical and cache nothing, so the test is that a cache appears.
 */
describe("the bin entry", () => {
  const bin = path.join(import.meta.dirname, "bin.ts");

  it("runs the CLI", async () => {
    // Pointed at a temporary cache: the entry's whole job is to write one,
    // and a test has no business filling the developer's real `~/.cache`.
    const temporary = await fs.mkdtemp(path.join(os.tmpdir(), "git-cc-"));
    try {
      // `NODE_COMPILE_CACHE` stripped as well as `XDG_CACHE_HOME` set: with
      // it, the stub steps aside and node writes wherever the developer
      // pointed it, which is not this temporary directory.
      const { NODE_COMPILE_CACHE: _, ...clean } = process.env;
      const { stdout } = await execFileAsync("node", [bin, "--version"], {
        encoding: "utf8",
        env: { ...clean, XDG_CACHE_HOME: temporary },
      });
      assert.match(stdout, /^chr33s-git v/);
    } finally {
      await fs.rm(temporary, { recursive: true, force: true });
    }
  });

  it("fills a compile cache that main.ts alone does not", async () => {
    const written = async (entry: string): Promise<number> => {
      const temporary = await fs.mkdtemp(path.join(os.tmpdir(), "git-cc-"));
      try {
        // No NODE_COMPILE_CACHE: node would enable the cache itself, and the
        // stub would prove nothing. `XDG_CACHE_HOME` is where the stub puts
        // it, deliberately not node's world-writable default.
        const { NODE_COMPILE_CACHE: _, ...clean } = process.env;
        await execFileAsync("node", [entry, "--version"], {
          encoding: "utf8",
          env: { ...clean, XDG_CACHE_HOME: temporary },
        });
        const cache = path.join(temporary, "chr33s-git");
        const entries = await fs
          .readdir(cache, { recursive: true, withFileTypes: true })
          .catch(() => []);
        return entries.filter((found) => found.isFile()).length;
      } finally {
        await fs.rm(temporary, { recursive: true, force: true });
      }
    };

    assert.equal(await written(entry), 0, "main.ts is not supposed to cache anything");
    // That anything was written, not how much: how V8's cache is packed into
    // files is node's business and has changed between releases.
    assert.ok((await written(bin)) > 0, "the bin entry cached nothing");
  });
});
