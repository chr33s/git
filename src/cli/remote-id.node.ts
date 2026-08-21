#!/usr/bin/env node
/** Verify a typed repository identity, pin it, then delegate to stock Git. */
import { execFile, spawn } from "node:child_process";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";

import { Effect, Predicate, Result } from "effect";

import { Invalid } from "../git/Error.ts";
import { decodeIdentifier } from "../social/Encode.ts";
import { load as loadGenesis, type RepoId } from "../trust/Genesis.ts";
import { canonicalUrl, KnownRepos } from "../trust/KnownRepos.ts";
import { layer as knownRepos } from "../trust/KnownRepos.node.ts";
import { identifierFromUrl, resolveLocation } from "./remote-id.ts";

const cleanGitEnvironment = (): NodeJS.ProcessEnv => {
  const environment = { ...process.env };
  // A remote helper inherits the caller's repository variables. They must not
  // redirect the isolated preflight fetch into the clone that invoked us.
  for (const name of [
    "GIT_DIR",
    "GIT_WORK_TREE",
    "GIT_PREFIX",
    "GIT_OBJECT_DIRECTORY",
    "GIT_ALTERNATE_OBJECT_DIRECTORIES",
    "GIT_INDEX_FILE",
  ]) {
    delete environment[name];
  }
  environment["GIT_TERMINAL_PROMPT"] = "0";
  return environment;
};

const git = (directory: string, arguments_: ReadonlyArray<string>): Promise<Uint8Array> =>
  new Promise((resolve, reject) => {
    execFile(
      "git",
      [...arguments_],
      {
        cwd: directory,
        encoding: null,
        env: cleanGitEnvironment(),
        maxBuffer: 4 * 1024 * 1024,
      },
      (error, stdout, stderr) => {
        if (error !== null) {
          const detail = stderr.toString("utf8").trim();
          reject(new Error(detail === "" ? error.message : detail));
          return;
        }
        resolve(new Uint8Array(stdout));
      },
    );
  });

/** Fetch only the advertised genesis record and compute its identity. */
export const identityAt = async (url: string): Promise<RepoId> => {
  const directory = await fs.mkdtemp(path.join(os.tmpdir(), "git-id-"));
  try {
    await git(directory, ["init", "--bare", "--quiet", "."]);
    await git(directory, [
      "-c",
      "protocol.version=2",
      "fetch",
      "--quiet",
      "--no-tags",
      "--depth=1",
      url,
      "refs/meta/trust/genesis",
    ]);
    const bytes = await git(directory, ["show", "FETCH_HEAD:genesis.json"]);
    return (await Effect.runPromise(loadGenesis(bytes))).repoId;
  } finally {
    await fs.rm(directory, { recursive: true, force: true });
  }
};

const locateAndPin = (source: string) =>
  Effect.gen(function* () {
    const encoded = identifierFromUrl(source);
    if (encoded === null) {
      return yield* new Invalid({ field: "url", reason: `'${source}' is not a git+id URL` });
    }
    const decoded = decodeIdentifier(encoded);
    if (Result.isFailure(decoded)) return yield* decoded.failure;
    if (decoded.success.kind !== "repository") {
      return yield* new Invalid({
        field: "identifier",
        reason: "a PrincipalID cannot be used as a repository clone URL",
      });
    }

    const store = yield* KnownRepos;
    const known = yield* store.list;
    const resolved = resolveLocation(encoded, known);
    if (Result.isFailure(resolved)) return yield* resolved.failure;

    const presented = yield* Effect.tryPromise({
      try: () => identityAt(resolved.success),
      catch: (cause) =>
        new Invalid({
          field: "identifier",
          reason: `could not verify ${resolved.success}: ${cause instanceof Error ? cause.message : String(cause)}`,
        }),
    });
    if (presented !== decoded.success.id) {
      return yield* new Invalid({
        field: "identifier",
        reason: `bootstrap location presented ${presented}, expected ${decoded.success.id}`,
      });
    }

    const url = yield* canonicalUrl(resolved.success);
    const existing = known.find((entry) => entry.repoId === presented && entry.url === url);
    yield* store.remember({
      url,
      repoId: presented,
      provenance: existing?.provenance ?? { kind: "tofu" },
    });
    // Keep credentials present on this invocation out of `known_repos`, but
    // do not strip them from the delegate that still needs to perform clone.
    return resolved.success;
  }).pipe(Effect.provide(knownRepos));

const delegate = (name: string, url: string): Promise<number> =>
  new Promise((resolve, reject) => {
    const child = spawn("git", ["remote-http", name, url], { stdio: "inherit" });
    child.once("error", reject);
    child.once("exit", (code, signal) => resolve(signal === null ? (code ?? 1) : 128));
  });

export const run = async (): Promise<void> => {
  const name = process.argv[2];
  const source = process.argv[3];
  if (name === undefined || source === undefined) {
    process.stderr.write("usage: git-remote-git+id <name> <git+id://grepo1…>\n");
    process.exitCode = 1;
    return;
  }

  try {
    const location = await Effect.runPromise(locateAndPin(source));
    process.exitCode = await delegate(name, location);
  } catch (error) {
    const reason =
      Predicate.hasProperty(error, "reason") && Predicate.isString(error.reason)
        ? error.reason
        : error instanceof Error
          ? error.message
          : String(error);
    process.stderr.write(`git+id: ${reason}\n`);
    process.exitCode = 1;
  }
};

if (import.meta.main) void run();
