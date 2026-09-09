/**
 * The administrative endpoints, as one action union and one Command.
 *
 * Every Settings action has the same shape — do a thing, say in one line what
 * happened, then reload the lists that thing changed — so they share a Command
 * rather than each getting one. What differs is which endpoint and what the
 * line says, and that is exactly what the union below carries.
 *
 * Every outcome is a `Succeeded` Message, including the refusals. A policy
 * refusal is the server answering, not failing, and an admin screen that
 * swallowed it would teach its reader that the rule does not exist. `Failed`
 * is for the request never landing at all.
 */
import { Command } from "foldkit";
import { Effect } from "effect";

import { AppMessage } from "./app.message.ts";
import { reasonOf, refused } from "./thrown.ts";
import { AdminAction, cardOf, list, short } from "./settings.ts";

const HEADS = "refs/heads/";

/**
 * Read every administrative registry.
 *
 * The branch list is the canary: every readable repository answers it. The
 * rest settle one by one, because a registry can refuse a reader whose
 * branches and policy are still theirs to see — one refused card must not
 * blank the other five.
 */
export const LoadSettings = Command.define("LoadSettings", {
  messages: [AppMessage.SucceededLoadSettings, AppMessage.FailedLoadSettings],
  interrupt: true,
  execute: Effect.gen(function* () {
    const api = yield* Effect.tryPromise(async () => await import("./api.ts"));
    const client = api.clientFromDocument();
    const state = yield* Effect.tryPromise(async () => await client.refState());
    const settled = yield* Effect.promise(
      async () =>
        await Promise.all([
          client.tags().catch(() => []),
          client.remotes().catch(() => []),
          client.webhooks().catch(() => []),
          client.policy().catch(() => null),
        ]),
    );
    const [tags, remotes, webhooks, policy] = settled;
    return AppMessage.SucceededLoadSettings({
      data: {
        branches: state.refs.filter((ref) => ref.name.startsWith(HEADS)),
        defaultBranch: state.head.startsWith(HEADS) ? state.head.slice(HEADS.length) : null,
        tags,
        remotes,
        webhooks,
        policy,
      },
    });
  }).pipe(
    // A refusal is not an outage: a private repository that turned this key
    // away is reachable and saying so, and the cards should carry that answer
    // rather than "not reachable".
    Effect.catch((cause) =>
      Effect.succeed(
        AppMessage.FailedLoadSettings({ failure: refused(cause) ? "Denied" : "Offline" }),
      ),
    ),
  ),
});

/** Read the browser's own signing key, for the identity card. */
export const LoadBrowserKey = Command.define("LoadBrowserKey", {
  messages: [AppMessage.SucceededLoadBrowserKey],
  execute: Effect.gen(function* () {
    const identity = yield* Effect.tryPromise(async () => await import("./identity.ts"));
    const described = yield* Effect.tryPromise(async () => await identity.describeIdentity());
    return AppMessage.SucceededLoadBrowserKey({
      key: {
        fingerprint: described.fingerprint,
        publicKey: described.publicKey,
        note: described.note,
      },
    });
  }).pipe(
    // No key is a state the card draws as "—". It is not worth a notice: a
    // browser without one simply cannot sign, which the card already says.
    Effect.orElseSucceed(() => AppMessage.SucceededLoadBrowserKey({ key: null })),
  ),
});

export const RunAdmin = Command.define("RunAdmin", {
  args: { action: AdminAction },
  messages: [AppMessage.SucceededAdmin, AppMessage.FailedAdmin],
  execute: ({ action }) =>
    Effect.gen(function* () {
      const api = yield* Effect.tryPromise(async () => await import("./api.ts"));
      const client = api.clientFromDocument();
      const note = yield* Effect.tryPromise(async () => await run(client, action));
      return AppMessage.SucceededAdmin({
        card: cardOf(action),
        note: note.text,
        reflog: note.reflog,
      });
    }).pipe(
      // The card says why in the same line it would have said what happened.
      Effect.catch((cause) =>
        Effect.succeed(AppMessage.FailedAdmin({ card: cardOf(action), note: reasonOf(cause) })),
      ),
    ),
});

/** The reflog an action produced, when it produced one. */
interface Outcome {
  readonly text: string;
  readonly reflog:
    | readonly {
        readonly from: string | null;
        readonly to: string | null;
        readonly at: string;
        readonly message: string;
      }[]
    | null;
}

/**
 * One action against the API, and the line it reports.
 *
 * The wording is the old screen's, verbatim: "was already gone" for a delete
 * of something absent, the ref shorthand, the counts. Those sentences are what
 * an operator reads to know whether to try again, and rewording them during a
 * framework migration would change the product under cover of the port.
 */
// SAFETY: the client is `GitApi`, whose methods this switch names one by one;
// typing the parameter as that class would import the API surface into a
// module that only needs to call it.
const run = async (client: GitApiLike, action: AdminAction): Promise<Outcome> => {
  const plain = (text: string): Outcome => ({ text, reflog: null });
  switch (action._tag) {
    case "DeleteBranch": {
      const gone = await client.branchDelete(action.name);
      return plain(`${action.name} ${gone ? "deleted" : "was already gone"}`);
    }
    case "ResetBranch": {
      const moved = await client.reset(action.ref, action.to);
      const from =
        moved.previous === null ? "into existence" : `from ${moved.previous.slice(0, 7)}`;
      return plain(`${short(moved.ref)} moved ${from} to ${moved.oid.slice(0, 7)}`);
    }
    case "DeleteTag": {
      const gone = await client.tagDelete(action.name);
      return plain(`${action.name} ${gone ? "deleted" : "was already gone"}`);
    }
    case "CreateTag": {
      const created = await client.tagCreate(
        action.message === ""
          ? { name: action.name, target: action.target }
          : { name: action.name, target: action.target, message: action.message },
      );
      const annotated = action.message === "" ? "" : " (annotated)";
      return plain(`${short(created.ref)} → ${created.target.slice(0, 7)}${annotated}`);
    }
    case "FetchRemote": {
      const result = await client.fetchRemote(action.name);
      return plain(
        `fetched ${String(result.refs.length)} refs, ${String(result.objects)} objects into ${result.remote}`,
      );
    }
    case "PushRemote": {
      const result = await client.pushRemote(action.name, action.branch);
      const failed = result.refs.filter((line) => !line.ok);
      return plain(
        failed.length === 0
          ? `pushed ${action.branch} to ${action.name}`
          : failed.map((line) => `${line.ref}: ${line.reason ?? "refused"}`).join("; "),
      );
    }
    case "PullRemote": {
      const result = await client.pullRemote(action.name, action.branch);
      return plain(
        result.kind === "non-fast-forward"
          ? `${action.branch} diverged from ${action.name} — merge or rebase, a pull cannot guess which`
          : `${action.branch}: ${result.kind}, now ${result.to.slice(0, 7)}`,
      );
    }
    case "DeleteRemote": {
      const gone = await client.remoteDelete(action.name);
      return plain(`${action.name} ${gone ? "removed" : "was already gone"}`);
    }
    case "AddRemote": {
      const added = await client.remoteAdd(
        action.name,
        action.url,
        action.credential === "" ? undefined : action.credential,
      );
      return plain(`${added.name} registered${added.has_credential ? " with a credential" : ""}`);
    }
    case "DeleteWebhook": {
      const gone = await client.webhookDelete(action.id);
      return plain(`webhook ${gone ? "removed" : "was already gone"}`);
    }
    case "AddWebhook": {
      const added = await client.webhookAdd(action.url, action.secret);
      return plain(`registered ${added.url}`);
    }
    case "Fsck": {
      const report = await client.fsck();
      return plain(
        report.ok
          ? `fsck: ${String(report.checked)} objects checked, all sound`
          : `fsck: ${String(report.problems.length)} problems, ${String(report.dangling_refs.length)} dangling refs`,
      );
    }
    case "PreviewGc": {
      const report = await client.gc({ dry_run: true });
      return plain(
        `gc (dry run): ${String(report.scanned)} scanned, ${String(report.reachable)} reachable, ${String(report.removed.length)} would go`,
      );
    }
    case "Gc": {
      const report = await client.gc();
      return plain(
        `gc: ${String(report.removed.length)} removed of ${String(report.scanned)} scanned`,
      );
    }
    case "ShowReflog": {
      const entries = await client.reflog(action.branch);
      return {
        text: `reflog of ${action.branch}: ${String(entries.length)} entries`,
        reflog: entries.map((entry) => ({
          from: entry.from,
          to: entry.to,
          at: entry.at,
          message: entry.message,
        })),
      };
    }
    case "WritePolicy": {
      const current = await client.policy();
      const written = await client.policyWrite({
        ...current.rules,
        protected: list(action.protectedRefs),
        requiredApprovals: Math.max(0, Number.parseInt(action.approvals, 10) || 0),
        requiredChecks: list(action.checks),
        requirePullRequest: action.requirePullRequest,
        requireResolvedThreads: action.requireResolvedThreads,
      });
      return plain(`policy published at ${written.commit.slice(0, 7)}`);
    }
  }
};

/** The methods `run` above calls, and only those. */
type GitApiLike = import("./api.ts").GitApi;
