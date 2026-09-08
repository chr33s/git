/** Stock-Git transport for the deliberately unauthenticated inbox door. */
import { type ChildProcessByStdio, spawn } from "node:child_process";
import type { Readable } from "node:stream";

import { Effect, Predicate } from "effect";

import { Invalid } from "../git/Error.ts";

export const pushInbox = Effect.fn("cli.inbox.push")(function* (input: {
  readonly url: string;
  readonly head: string;
  readonly id: string;
}) {
  const destination = `refs/quarantine/inbox/${input.id}`;
  const failed = (cause: unknown) =>
    new Invalid({
      field: "inbox",
      reason: cause instanceof Error ? cause.message : String(cause),
    });
  yield* Effect.callback<void, Invalid>((resume) => {
    const closed = Promise.withResolvers<void>();
    const grouped = process.platform !== "win32";
    let child: ChildProcessByStdio<null, null, Readable>;
    try {
      child = spawn(
        "git",
        [
          "-c",
          "http.extraHeader=Git-Inbox: 1",
          "push",
          "--no-follow-tags",
          input.url,
          `${input.head}^{commit}:${destination}`,
        ],
        {
          env: { ...process.env, GIT_TERMINAL_PROMPT: "0" },
          detached: grouped,
          stdio: ["ignore", "ignore", "pipe"],
        },
      );
    } catch (cause) {
      resume(Effect.fail(failed(cause)));
      return;
    }
    let stderr = "";
    let error: Error | undefined;
    child.stderr.setEncoding("utf8");
    child.stderr.on("data", (chunk: string) => {
      stderr = (stderr + chunk).slice(-64 * 1024);
    });
    child.once("error", (cause) => {
      error = cause;
    });
    child.once("close", (code, signal) => {
      closed.resolve();
      if (error !== undefined) resume(Effect.fail(failed(error)));
      else if (code === 0) resume(Effect.void);
      else resume(Effect.fail(failed(stderr.trim() || `git exited with ${signal ?? code}`)));
    });
    const stop = (signal: NodeJS.Signals) => {
      if (child.pid !== undefined) {
        try {
          if (grouped) process.kill(-child.pid, signal);
          else child.kill(signal);
        } catch (cause) {
          if (!Predicate.hasProperty(cause, "code") || cause.code !== "ESRCH") throw cause;
        }
      }
    };
    return Effect.promise(async () => {
      // Git starts transport helpers. Allow the group to clean up its locks,
      // then stop an unresponsive helper before completing interruption.
      stop("SIGTERM");
      const force = setTimeout(() => stop("SIGKILL"), 1_000);
      try {
        await closed.promise;
      } finally {
        clearTimeout(force);
      }
    });
  });
  return destination;
});
