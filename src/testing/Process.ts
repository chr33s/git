/** Buffer-preserving process harness for CLI compatibility tests. */
import { spawn } from "node:child_process";

export interface ProcessResult {
  readonly stdout: Buffer;
  readonly stderr: Buffer;
  readonly code: number | null;
  readonly signal: NodeJS.Signals | null;
}

export interface ProcessInput {
  readonly command: string;
  readonly args: ReadonlyArray<string>;
  readonly cwd?: string | undefined;
  readonly env?: NodeJS.ProcessEnv | undefined;
  readonly stdin?: Uint8Array | undefined;
}

/**
 * Run a process without decoding its output.
 *
 * Compatibility comparisons must see every byte, including NULs and trailing
 * newlines, so this adapter intentionally has no text convenience API.
 */
export const runProcess = (input: ProcessInput): Promise<ProcessResult> =>
  new Promise((resolve, reject) => {
    const child = spawn(input.command, input.args, {
      cwd: input.cwd,
      env: input.env,
      stdio: ["pipe", "pipe", "pipe"],
    });
    const stdout: Buffer[] = [];
    const stderr: Buffer[] = [];
    child.stdout.on("data", (chunk: Buffer) => {
      stdout.push(chunk);
    });
    child.stderr.on("data", (chunk: Buffer) => {
      stderr.push(chunk);
    });
    child.on("error", reject);
    child.on("close", (code, signal) => {
      resolve({
        stdout: Buffer.concat(stdout),
        stderr: Buffer.concat(stderr),
        code,
        signal,
      });
    });
    child.stdin.end(input.stdin);
  });

const sameBytes = (left: Uint8Array, right: Uint8Array) => {
  if (left.length !== right.length) return false;
  for (let index = 0; index < left.length; index++) {
    if (left[index] !== right[index]) return false;
  }
  return true;
};

/** Exact process equivalence for source and SEA invocations. */
export const sameProcessResult = (left: ProcessResult, right: ProcessResult) =>
  left.code === right.code &&
  left.signal === right.signal &&
  sameBytes(left.stdout, right.stdout) &&
  sameBytes(left.stderr, right.stderr);
