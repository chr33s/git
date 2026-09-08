#!/usr/bin/env node
/** Verify a typed repository identity, pin it, then delegate to stock Git. */
import { spawn } from "node:child_process";
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

const git = (
  directory: string,
  arguments_: ReadonlyArray<string>,
  signal?: AbortSignal,
): Promise<Uint8Array> =>
  new Promise((resolve, reject) => {
    signal?.throwIfAborted();
    const grouped = process.platform !== "win32";
    const child = spawn("git", [...arguments_], {
      cwd: directory,
      env: cleanGitEnvironment(),
      stdio: ["ignore", "pipe", "pipe"],
      // Git starts transport helpers. A separate Unix process group lets
      // interruption stop their sockets as well as the parent process.
      detached: grouped,
    });
    const stdout: Buffer[] = [];
    const stderr: Buffer[] = [];
    let size = 0;
    let failure: Error | undefined;
    const abort = () => {
      if (child.pid === undefined) return;
      try {
        if (grouped) process.kill(-child.pid, "SIGKILL");
        else child.kill("SIGKILL");
      } catch (cause) {
        if (!Predicate.hasProperty(cause, "code") || cause.code !== "ESRCH") reject(cause);
      }
    };
    const capture = (chunks: Buffer[], chunk: Buffer) => {
      size += chunk.length;
      if (size > 4 * 1024 * 1024) {
        failure ??= new Error("identity preflight output exceeded 4 MiB");
        abort();
      } else chunks.push(chunk);
    };
    child.stdout.on("data", (chunk: Buffer) => capture(stdout, chunk));
    child.stderr.on("data", (chunk: Buffer) => capture(stderr, chunk));
    child.once("error", (error) => {
      failure = error;
    });
    // Settle after close, so identityAt's finally cannot remove the scratch
    // repository while a terminating Git or its transport still uses it.
    child.once("close", (code, endedBy) => {
      signal?.removeEventListener("abort", abort);
      if (failure !== undefined) reject(failure);
      else if (signal?.aborted === true) reject(signal.reason);
      else if (code !== 0) {
        const detail = Buffer.concat(stderr).toString("utf8").trim();
        reject(new Error(detail || `git preflight exited with ${endedBy ?? code}`));
      } else resolve(new Uint8Array(Buffer.concat(stdout)));
    });
    signal?.addEventListener("abort", abort, { once: true });
    if (signal?.aborted === true) abort();
  });

/** Fetch only the advertised genesis record and compute its identity. */
export const identityAt = async (url: string, signal?: AbortSignal): Promise<RepoId> => {
  const directory = await fs.mkdtemp(path.join(os.tmpdir(), "git-id-"));
  try {
    await git(directory, ["init", "--bare", "--quiet", "."], signal);
    await git(
      directory,
      [
        "-c",
        "protocol.version=2",
        "fetch",
        "--quiet",
        "--no-tags",
        "--depth=1",
        url,
        "refs/meta/trust/genesis",
      ],
      signal,
    );
    const bytes = await git(directory, ["show", "FETCH_HEAD:genesis.json"], signal);
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
      try: (signal) => identityAt(resolved.success, signal),
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

const delegate = (name: string, url: string, signal: AbortSignal): Promise<number> =>
  new Promise((resolve, reject) => {
    signal.throwIfAborted();
    const grouped = process.platform !== "win32";
    const child = spawn("git", ["remote-http", name, url], { stdio: "inherit", detached: grouped });
    let failure: Error | undefined;
    let force: ReturnType<typeof setTimeout> | undefined;
    const stop = (termination: NodeJS.Signals) => {
      if (child.pid === undefined) return;
      try {
        if (grouped) process.kill(-child.pid, termination);
        else child.kill(termination);
      } catch (cause) {
        if (!Predicate.hasProperty(cause, "code") || cause.code !== "ESRCH") reject(cause);
      }
    };
    const abort = () => {
      stop("SIGTERM");
      force = setTimeout(() => stop("SIGKILL"), 1_000);
    };
    child.once("error", (error) => {
      failure = error;
    });
    child.once("close", (code, endedBy) => {
      clearTimeout(force);
      signal.removeEventListener("abort", abort);
      if (failure !== undefined) reject(failure);
      else if (signal.aborted) reject(signal.reason);
      else resolve(endedBy === null ? (code ?? 1) : 128);
    });
    signal.addEventListener("abort", abort, { once: true });
    if (signal.aborted) abort();
  });

export const run = async (): Promise<void> => {
  const name = process.argv[2];
  const source = process.argv[3];
  if (name === undefined || source === undefined) {
    process.stderr.write("usage: git-remote-git+id <name> <git+id://grepo1…>\n");
    process.exitCode = 1;
    return;
  }

  const controller = new AbortController();
  let terminated: number | undefined;
  const handlers = (["SIGINT", "SIGTERM", "SIGHUP"] as const).map((signal) => {
    const handler = () => {
      terminated ??= 128 + os.constants.signals[signal];
      controller.abort();
    };
    process.on(signal, handler);
    return [signal, handler] as const;
  });
  try {
    const location = await Effect.runPromise(locateAndPin(source), { signal: controller.signal });
    process.exitCode = await delegate(name, location, controller.signal);
  } catch (error) {
    if (terminated !== undefined) {
      process.exitCode = terminated;
      return;
    }
    const reason =
      Predicate.hasProperty(error, "reason") && Predicate.isString(error.reason)
        ? error.reason
        : error instanceof Error
          ? error.message
          : String(error);
    process.stderr.write(`git+id: ${reason}\n`);
    process.exitCode = 1;
  } finally {
    for (const [signal, handler] of handlers) process.removeListener(signal, handler);
  }
};

if (import.meta.main) void run();
