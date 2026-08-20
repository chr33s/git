import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { promisify } from "node:util";
import { describe, it } from "@effect/vitest";

import { Effect, Layer, Result } from "effect";

import { stores } from "../git/Node.ts";
import * as GitRepository from "../git/Repository.ts";
import { Repository } from "../git/Repository.ts";
import { serve, type Server } from "../host/Node.ts";
import { encodePrincipal, encodeRepository } from "../social/Encode.ts";
import { hasGit, gitEnv } from "../testing/Git.ts";
import { enableHubUnder } from "../testing/Hub.ts";
import type { RepoId } from "../trust/Genesis.ts";
import { principalId } from "../trust/Principal.ts";
import { identifierFromUrl, resolveLocation } from "./remote-id.ts";

/** SAFETY: canonical 32-byte digest spelling for identifier tests. */
const repo = (seed: string): RepoId => `SHA256:${seed.repeat(42).slice(0, 42)}A` as RepoId;

const value = <A, E>(result: Result.Result<A, E>): A => {
  assert.ok(Result.isSuccess(result));
  return result.success;
};

const execFileAsync = promisify(execFile);
const author = {
  name: "Alice",
  email: "alice@example.com",
  at: new Date("2026-08-20T00:00:00Z"),
  offset: 0,
};

const shellQuote = (value: string): string => `'${value.replaceAll("'", `'"'"'`)}'`;

describe("git+id remote helper", () => {
  it.effect("prefers a pinned location and otherwise uses an embedded bootstrap hint", () =>
    Effect.sync(() => {
      const id = repo("a");
      const encoded = value(
        encodeRepository({ id, hints: ["https://bootstrap.example/acme/project"] }),
      );

      assert.equal(identifierFromUrl(`git+id://${encoded}`), encoded);
      assert.equal(value(resolveLocation(encoded, [])), "https://bootstrap.example/acme/project");
      assert.equal(
        value(resolveLocation(encoded, [{ url: "https://pinned.example/project", repoId: id }])),
        "https://pinned.example/project",
      );
    }),
  );

  it.effect("refuses to use a PrincipalID where a repository identity is required", () =>
    Effect.sync(() => {
      const encoded = value(encodePrincipal({ id: principalId(repo("b")) }));
      const resolved = resolveLocation(encoded, []);
      assert.ok(Result.isFailure(resolved));
      assert.match(resolved.failure.reason, /PrincipalID/);
    }),
  );

  describe.skipIf(!hasGit)("stock Git interoperability", () => {
    it.live("preflights the genesis pin before delegating a clone", () =>
      Effect.promise(async () => {
        const root = await fs.mkdtemp(path.join(os.tmpdir(), "git-id-interop-"));
        let server: Server | null = null;
        try {
          const fixture = await enableHubUnder(root, "project", ["source.push"]);
          const revision = await Effect.runPromise(
            Effect.gen(function* () {
              const repository = yield* Repository;
              const blob = yield* repository.writeBlob(new TextEncoder().encode("pinned\n"));
              const tree = yield* repository.writeTree([
                { mode: "100644", name: "identity.txt", oid: blob },
              ]);
              return yield* repository.commit({ branch: "main", tree, message: "seed", author });
            }).pipe(
              Effect.provide(
                GitRepository.layer.pipe(
                  Layer.provide(GitRepository.hooksNoop),
                  Layer.provide(stores(path.join(root, "project"))),
                ),
              ),
            ),
          );

          server = await serve({ root });
          const hint = `${server.url}/project`;
          const encoded = value(encodeRepository({ id: fixture.repoId, hints: [hint] }));

          const executableDirectory = path.join(root, "bin");
          const config = path.join(root, "config");
          await fs.mkdir(executableDirectory);
          const helper = path.join(executableDirectory, "git-remote-git+id");
          await fs.writeFile(
            helper,
            [
              "#!/bin/sh",
              `exec ${shellQuote(process.execPath)} ${shellQuote(path.join(import.meta.dirname, "remote-id.node.ts"))} "$@"`,
              "",
            ].join("\n"),
            { mode: 0o755 },
          );
          const environment = {
            ...gitEnv,
            PATH: `${executableDirectory}${path.delimiter}${process.env["PATH"] ?? ""}`,
            XDG_CONFIG_HOME: config,
            GIT_TERMINAL_PROMPT: "0",
          };

          const destination = path.join(root, "clone");
          await execFileAsync("git", ["clone", "--quiet", `git+id://${encoded}`, destination], {
            cwd: root,
            env: environment,
            encoding: "utf8",
          });
          const cloned = await execFileAsync("git", ["-C", destination, "rev-parse", "HEAD"], {
            cwd: root,
            env: environment,
            encoding: "utf8",
          });
          assert.equal(cloned.stdout.trim(), revision);
          assert.equal(
            await fs.readFile(path.join(destination, "identity.txt"), "utf8"),
            "pinned\n",
          );

          const knownRepos = path.join(config, "chr33s-git", "known_repos");
          const pinned = await fs.readFile(knownRepos, "utf8");
          assert.match(pinned, new RegExp(fixture.repoId.replace(/[+/]/g, "\\$&")));

          const wrong = repo("z");
          const mismatched = value(encodeRepository({ id: wrong, hints: [hint] }));
          const refused = await execFileAsync(
            "git",
            ["clone", "--quiet", `git+id://${mismatched}`, path.join(root, "mismatch")],
            { cwd: root, env: environment, encoding: "utf8" },
          ).then(
            () => "",
            (cause: unknown) => String(cause),
          );
          assert.match(refused, /presented .* expected/);
          assert.equal((await fs.readFile(knownRepos, "utf8")).includes(wrong), false);
        } finally {
          if (server !== null) await server.close();
          await fs.rm(root, { recursive: true, force: true });
        }
      }),
    );
  });
});
