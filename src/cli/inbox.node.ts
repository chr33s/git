/** Stock-Git transport for the deliberately unauthenticated inbox door. */
import { execFile } from "node:child_process";

import { Effect } from "effect";

import { Invalid } from "../git/Error.ts";

export const pushInbox = Effect.fn("cli.inbox.push")(function* (input: {
  readonly url: string;
  readonly head: string;
  readonly id: string;
}) {
  const destination = `refs/quarantine/inbox/${input.id}`;
  yield* Effect.tryPromise({
    try: () =>
      new Promise<void>((resolve, reject) => {
        execFile(
          "git",
          [
            "-c",
            "http.extraHeader=Git-Inbox: 1",
            "push",
            input.url,
            `${input.head}:${destination}`,
          ],
          { env: { ...process.env, GIT_TERMINAL_PROMPT: "0" } },
          (error) => (error === null ? resolve() : reject(error)),
        );
      }),
    catch: (cause) =>
      new Invalid({
        field: "inbox",
        reason: cause instanceof Error ? cause.message : String(cause),
      }),
  });
  return destination;
});
