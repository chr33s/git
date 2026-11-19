/**
 * The CLI, run inside the test process.
 *
 * Spawning `bin.ts` costs a node start-up and a ~550-module load per command,
 * about 250 ms before the command does anything; suites that drive a flow
 * through dozens of commands per test spent most of their time there. This
 * runs the same `program` `main.ts` hands to `NodeRuntime.runMain`, with a
 * `Console` that captures what the process would have written, and the exit
 * status the runner would have chosen.
 *
 * The process-wide state a CLI run touches — working directory, environment,
 * `process.exitCode` — is set for the call and put back after, and calls are
 * serialized so two never see each other's. A test about two processes
 * racing still needs real processes: use `spawnCli`.
 *
 * Git's own verbs hand off to the `git` binary with inherited stdio, so they
 * spawn here too, through the real entry point.
 */
import { execFile } from "node:child_process";
import * as path from "node:path";
import { promisify } from "node:util";
import { format } from "node:util";

import { Cause, Console, Effect, Exit, Predicate, Runtime } from "effect";

import { isCoreCompatibilityInvocation, parseInvocation } from "../cli/GitCompat.node.ts";
import { program } from "../cli/main.ts";

const entry = path.join(import.meta.dirname, "..", "cli", "bin.ts");
const execFileAsync = promisify(execFile);

export interface CliOptions {
  readonly cwd?: string | undefined;
  /** The whole environment, as `execFile` takes it; unset keys are removed for the call. */
  readonly env?: NodeJS.ProcessEnv | undefined;
}

export interface CliResult {
  readonly stdout: string;
  readonly stderr: string;
  readonly code: number;
}

/** What `execFile` rejects with, so callers reading `error.stdout` keep working. */
export class CliExit extends Error {
  readonly stdout: string;
  readonly stderr: string;
  readonly code: number;
  constructor(args: ReadonlyArray<string>, result: CliResult) {
    super(`git+ ${args.join(" ")} exited ${String(result.code)}\n${result.stderr}`);
    this.stdout = result.stdout;
    this.stderr = result.stderr;
    this.code = result.code;
  }
}

/** A real process, for tests about processes and for Git's own verbs. */
export const spawnCli = (
  args: ReadonlyArray<string>,
  options: CliOptions = {},
): Promise<CliResult> =>
  execFileAsync(process.execPath, [entry, ...args], { encoding: "utf8", ...options }).then(
    ({ stdout, stderr }) => ({ stdout, stderr, code: 0 }),
    (error: { stdout?: string; stderr?: string; code?: unknown }) => ({
      stdout: error.stdout ?? "",
      stderr: error.stderr ?? "",
      code: Predicate.isNumber(error.code) ? error.code : 1,
    }),
  );

const capture = () => {
  let stdout = "";
  let stderr = "";
  const out = (...values: ReadonlyArray<unknown>) => {
    stdout += `${format(...values)}\n`;
  };
  const err = (...values: ReadonlyArray<unknown>) => {
    stderr += `${format(...values)}\n`;
  };
  const console: Console.Console = Object.assign(Object.create(globalThis.console), {
    debug: out,
    info: out,
    log: out,
    table: out,
    dir: out,
    error: err,
    trace: err,
    warn: err,
  });
  return { console, read: () => ({ stdout, stderr }) };
};

/** Replace `process.env` for one call; the returned function restores it. */
const swapEnvironment = (env: NodeJS.ProcessEnv | undefined): (() => void) => {
  if (env === undefined) return () => {};
  const saved = { ...process.env };
  for (const key of Object.keys(process.env)) {
    if (!(key in env)) delete process.env[key];
  }
  Object.assign(process.env, env);
  return () => {
    for (const key of Object.keys(process.env)) {
      if (!(key in saved)) delete process.env[key];
    }
    Object.assign(process.env, saved);
  };
};

const inProcess = async (args: ReadonlyArray<string>, options: CliOptions): Promise<CliResult> => {
  const parsed = parseInvocation({
    argv: args,
    cwd: options.cwd ?? process.cwd(),
    environment: options.env ?? process.env,
  });
  if (parsed._tag === "InvalidInvocation") {
    return { stdout: "", stderr: `git+: ${parsed.message}\n`, code: parsed.exitCode ?? 129 };
  }
  if (isCoreCompatibilityInvocation(parsed.invocation)) return spawnCli(args, options);

  const directory = process.cwd();
  const exitCode = process.exitCode;
  const argv = process.argv;
  const restoreEnvironment = swapEnvironment(options.env);
  const { console, read } = capture();
  try {
    // What the spawned entry would see: `session install` writes this path
    // into the hooks it installs.
    process.argv = [process.execPath, entry, ...args];
    process.chdir(parsed.invocation.cwd);
    process.exitCode = undefined;
    // The same reporting `NodeRuntime.runMain` wraps around the program.
    const exit = await Effect.runPromiseExit(
      program(parsed.invocation).pipe(
        Effect.tapCause((cause) =>
          Cause.hasInterruptsOnly(cause) || !Runtime.getErrorReported(Cause.squash(cause))
            ? Effect.void
            : Effect.logError(cause),
        ),
        Effect.provideService(Console.Console, console),
      ),
    );
    let code = Number(process.exitCode ?? 0);
    if (Exit.isFailure(exit)) {
      Runtime.defaultTeardown(exit, (status) => {
        code = status;
      });
    }
    return { ...read(), code };
  } finally {
    process.argv = argv;
    process.exitCode = exitCode;
    restoreEnvironment();
    process.chdir(directory);
  }
};

let queue: Promise<unknown> = Promise.resolve();

/** Run one CLI invocation in this process; never rejects on a non-zero exit. */
export const runCli = (
  args: ReadonlyArray<string>,
  options: CliOptions = {},
): Promise<CliResult> => {
  const result = queue.then(() => inProcess(args, options));
  queue = result.catch(() => undefined);
  return result;
};

/** `execFile`'s contract: resolve on exit 0, reject with `CliExit` otherwise. */
export const execCli = async (
  args: ReadonlyArray<string>,
  options: CliOptions = {},
): Promise<{ stdout: string; stderr: string }> => {
  const result = await runCli(args, options);
  if (result.code !== 0) throw new CliExit(args, result);
  return { stdout: result.stdout, stderr: result.stderr };
};
