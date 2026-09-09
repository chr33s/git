/**
 * The repository, read and written.
 *
 * Every read here answers with one coherent view rather than several fields:
 * a failure at any step drops the whole screen to the design's sample, because
 * a half-live tree beside a sample README is the more confusing outcome.
 *
 * The writes pin `expected` to the tip the editor opened at. That is what
 * makes a commit landing mid-edit a visible conflict the writer can act on
 * rather than a silent overwrite, and it is why the tip is a Model fact.
 *
 * The client comes from `repository.ts`: the OPFS clone when it is open, the
 * HTTP client otherwise. Neither is ever held in the Model.
 */
import { Command } from "foldkit";
import { Effect, Schema } from "effect";

import { CodeView, HistoryRow } from "./app.model.ts";
import { AppMessage } from "./app.message.ts";
import * as Code from "./code.ts";
import * as Repository from "./repository.ts";
import { ago, initials } from "./time.ts";
import { reasonOf } from "./thrown.ts";

/**
 * The whole screen at one ref, or the reason it could not be read.
 *
 * The reason is `describe`'s, not this module's: "the git+ API is not running"
 * and "the git+ API answered …" are the product's own words for an outage and
 * a refusal, and a caller inventing its own would tell the reader something
 * subtly different about the same failure. Caught here rather than in the
 * Effect, because `tryPromise` widens the thrown value to an unknown cause and
 * the class it came from is what tells the two apart.
 */
type Snapshot =
  | { readonly ok: true; readonly view: CodeView }
  | { readonly ok: false; readonly reason: string };

const snapshotOf = async (ref: string | null, keep: string | undefined): Promise<Snapshot> => {
  const api = await import("./api.ts");
  try {
    return { ok: true, view: await readAt(ref, keep) };
  } catch (error) {
    if (!(error instanceof api.ApiError) && !(error instanceof TypeError)) throw error;
    return { ok: false, reason: api.describe(error) };
  }
};

const readAt = async (ref: string | null, keep: string | undefined): Promise<CodeView> => {
  const { clientFromDocument } = await import("./api.ts");
  const { isOid } = await import("../git/Oid.ts");
  const http = clientFromDocument();
  const client = Repository.reading(http);
  const state = await client.refState();
  const at = ref ?? Code.shortRef(state.head);
  const tip = isOid(at)
    ? at
    : state.refs.find(
        (candidate) => candidate.name === (at.startsWith("refs/") ? at : `refs/heads/${at}`),
      )?.oid;
  // An unborn default branch has no tree yet; it is an editable empty
  // repository, not an API outage to replace with sample files.
  const files = tip === undefined ? [] : await client.files(tip);
  const paths = files.map((file) => file.path);
  const readme = Code.readmeOf(paths);
  const selected = keep !== undefined && paths.includes(keep) ? keep : readme;
  const [commit, content] = await Promise.all([
    tip === undefined ? null : client.commitDetail(tip),
    selected === null || tip === undefined ? null : client.file(tip, selected),
  ]);
  return {
    ref: at,
    defaultBranch: state.head.startsWith("refs/heads/") ? Code.shortRef(state.head) : null,
    branches: Code.branchNames(state.refs),
    paths,
    selected,
    content,
    head:
      commit === null
        ? null
        : {
            sha: commit.oid.slice(0, 7),
            message: commit.subject,
            author: commit.author,
            avatar: initials(commit.author),
            when: ago(commit.at),
          },
    tip: tip ?? null,
    offline: false,
    pending: false,
    reason: "",
  };
};

/**
 * Load, or reload, the whole repository view.
 *
 * A reader switching branches while the previous read resolves gets the newer
 * answer: `wantedRef` names what was last asked for and `update` drops anything
 * else, because `interrupt` registers a key rather than cancelling — nothing
 * stops unless `update` returns an Interrupt. `ref` empty means "whatever HEAD names",
 * which is the first load; `keep` names a path to stay on if it still exists,
 * so a reload after a commit shows the file just written rather than jumping
 * back to the README.
 */
export const LoadCode = Command.define("LoadCode", {
  args: { ref: Schema.String, keep: Schema.String },
  messages: [AppMessage.SucceededLoadCode, AppMessage.FellBackLoadCode],
  interrupt: true,
  execute: ({ ref, keep }) =>
    Effect.gen(function* () {
      const answer = yield* Effect.tryPromise(
        async () => await snapshotOf(ref === "" ? null : ref, keep === "" ? undefined : keep),
      );
      return answer.ok
        ? AppMessage.SucceededLoadCode({ ref, view: answer.view })
        : AppMessage.FellBackLoadCode({ ref, reason: answer.reason });
    }).pipe(
      Effect.catch((cause) =>
        Effect.succeed(AppMessage.FellBackLoadCode({ ref, reason: reasonOf(cause) })),
      ),
    ),
});

/** Show the open file as it was at `oid` — a read-only look back. */
export const LoadFileAt = Command.define("LoadFileAt", {
  args: { oid: Schema.String, path: Schema.String },
  messages: [AppMessage.SucceededLoadFileAt, AppMessage.FailedLoadFileAt],
  interrupt: true,
  execute: ({ oid, path }) =>
    Effect.gen(function* () {
      const { clientFromDocument } = yield* Effect.tryPromise(async () => await import("./api.ts"));
      const client = Repository.reading(clientFromDocument());
      const content = yield* Effect.tryPromise(async () => await client.file(oid, path));
      return AppMessage.SucceededLoadFileAt({ oid, path, content });
    }).pipe(
      Effect.catch((cause) =>
        Effect.succeed(AppMessage.FailedLoadFileAt({ oid, path, reason: reasonOf(cause) })),
      ),
    ),
});

/** Open one file at the current tip. */
export const LoadFile = Command.define("LoadFile", {
  args: { tip: Schema.String, path: Schema.String },
  messages: [AppMessage.SucceededLoadFileAt, AppMessage.FailedLoadFileAt],
  interrupt: true,
  execute: ({ tip, path }) =>
    Effect.gen(function* () {
      const { clientFromDocument } = yield* Effect.tryPromise(async () => await import("./api.ts"));
      const client = Repository.reading(clientFromDocument());
      const content = yield* Effect.tryPromise(async () => await client.file(tip, path));
      return AppMessage.SucceededLoadFileAt({ oid: "", path, content });
    }).pipe(
      Effect.catch((cause) =>
        Effect.succeed(AppMessage.FailedLoadFileAt({ oid: "", path, reason: reasonOf(cause) })),
      ),
    ),
});

/** Recent history at the tip, or one file's history. */
export const LoadHistory = Command.define("LoadHistory", {
  args: { tip: Schema.String, path: Schema.String },
  messages: [AppMessage.SucceededLoadHistory, AppMessage.FailedLoadHistory],
  interrupt: true,
  execute: ({ tip, path }) =>
    Effect.gen(function* () {
      const { clientFromDocument } = yield* Effect.tryPromise(async () => await import("./api.ts"));
      const client = Repository.reading(clientFromDocument());
      const rows: readonly HistoryRow[] = yield* Effect.tryPromise(async () => {
        if (path === "") {
          const commits = await client.recentCommits(tip, 20);
          return commits.map((commit) => ({
            oid: commit.oid,
            subject: commit.subject,
            author: commit.author,
            when: ago(commit.at),
          }));
        }
        // `/history` answers with the oid and the message alone — it is the
        // list of commits that touched a path, not their authorship. The rows
        // say so rather than inventing an author and a date for each.
        const commits = await client.history(tip, path, "20");
        return commits.map((commit) => ({
          oid: commit.oid,
          subject: commit.message.split("\n")[0] ?? commit.message,
          author: "",
          when: "",
        }));
      });
      return AppMessage.SucceededLoadHistory({ path, rows });
    }).pipe(
      Effect.catch((cause) =>
        Effect.succeed(AppMessage.FailedLoadHistory({ path, reason: reasonOf(cause) })),
      ),
    ),
});

/**
 * One commit against the tip the editor opened at, then a reload.
 *
 * `expected` pins that tip, so a commit that landed mid-edit answers
 * `RefConflict` — surfaced as an error the writer can act on — instead of
 * being silently parented over.
 */
export const CommitFile = Command.define("CommitFile", {
  args: {
    branch: Schema.String,
    path: Schema.String,
    /** `null` deletes: the same request, with no content. */
    content: Schema.NullOr(Schema.String),
    message: Schema.String,
    expected: Schema.NullOr(Schema.String),
    keep: Schema.String,
  },
  messages: [AppMessage.SucceededCommitFile, AppMessage.FailedCommitFile],
  execute: ({ branch, path, content, message, expected, keep }) =>
    Effect.gen(function* () {
      const outcome = yield* Effect.tryPromise(
        async () => await write({ branch, path, content, message, expected }),
      );
      return outcome === null
        ? AppMessage.SucceededCommitFile({ branch, keep })
        : AppMessage.FailedCommitFile({ reason: outcome });
    }).pipe(
      // `write` classifies what it recognises and rethrows the rest — a
      // chunk that will not load, a fault inside the local client. Those are
      // still this writer's problem, not the application's: without this the
      // rejection is a defect, and Foldkit's runner ends the session on the
      // crash screen with the draft still unsaved.
      Effect.catch((cause) =>
        Effect.succeed(AppMessage.FailedCommitFile({ reason: reasonOf(cause) })),
      ),
    ),
});

/** Create a branch at the current tip and switch to it. */
export const CreateBranch = Command.define("CreateBranch", {
  args: { name: Schema.String, from: Schema.String },
  messages: [AppMessage.SucceededCreateBranch, AppMessage.CompletedSync],
  execute: ({ name, from }) =>
    Effect.gen(function* () {
      const { clientFromDocument } = yield* Effect.tryPromise(async () => await import("./api.ts"));
      const client = Repository.reading(clientFromDocument());
      yield* Effect.tryPromise(async () => await client.branchCreate(name, from));
      return AppMessage.SucceededCreateBranch({ name });
    }).pipe(
      // A refusal — a name already taken, a protected ref, an outage — is
      // reported where the reader is looking: the dialog stays open with the
      // name they typed, and the notice inside it says why.
      Effect.catch((cause) =>
        Effect.succeed(
          AppMessage.CompletedSync({
            notice: `The branch was not created — ${reasonOf(cause)}.`,
            reload: false,
          }),
        ),
      ),
    ),
});

/**
 * Where the current branch stands against origin.
 *
 * Answered only by the local client; against the HTTP client there is no
 * "against", and the controls stay hidden rather than showing zeroes.
 */
export const RefreshSync = Command.define("RefreshSync", {
  args: { ref: Schema.String },
  messages: [AppMessage.SucceededRefreshSync],
  interrupt: true,
  execute: ({ ref }) =>
    Effect.gen(function* () {
      const client = Repository.syncing();
      if (client === null) return AppMessage.SucceededRefreshSync({ sync: null });
      const state = yield* Effect.tryPromise(async () => await client.sync(ref));
      return AppMessage.SucceededRefreshSync({
        sync: {
          ahead: state.ahead,
          behind: state.behind,
          canPush: state.remote !== null,
        },
      });
    }).pipe(Effect.orElseSucceed(() => AppMessage.SucceededRefreshSync({ sync: null }))),
});

export const PushBranch = Command.define("PushBranch", {
  args: { ref: Schema.String },
  messages: [AppMessage.CompletedSync],
  execute: ({ ref }) =>
    Effect.gen(function* () {
      const client = Repository.syncing();
      if (client === null) return AppMessage.CompletedSync({ notice: null, reload: false });
      const results = yield* Effect.tryPromise(async () => await client.push(ref));
      const refused = results.filter((result) => !result.ok);
      return AppMessage.CompletedSync({
        notice:
          refused.length === 0
            ? `Pushed ${ref} to origin.`
            : `origin refused ${refused[0]?.ref ?? ref}: ${refused[0]?.reason ?? "unknown"}`,
        reload: false,
      });
    }).pipe(
      Effect.catch((cause) =>
        Effect.succeed(
          AppMessage.CompletedSync({ notice: `Push failed — ${reasonOf(cause)}.`, reload: false }),
        ),
      ),
    ),
});

export const FetchOrigin = Command.define("FetchOrigin", {
  messages: [AppMessage.CompletedSync],
  execute: Effect.gen(function* () {
    const client = Repository.syncing();
    if (client === null) return AppMessage.CompletedSync({ notice: null, reload: false });
    const fetched = yield* Effect.tryPromise(async () => await client.fetchOrigin());
    return AppMessage.CompletedSync({
      notice:
        fetched.updated === 0
          ? "Already up to date with origin."
          : `Fetched origin — ${String(fetched.updated)} ref${fetched.updated === 1 ? "" : "s"} moved.`,
      reload: true,
    });
  }).pipe(
    Effect.catch((cause) =>
      Effect.succeed(
        AppMessage.CompletedSync({ notice: `Fetch failed — ${reasonOf(cause)}.`, reload: false }),
      ),
    ),
  ),
});

/**
 * Open the OPFS clone, off the boot path.
 *
 * A browser without OPFS, or one whose first load could not reach the remote,
 * simply never swaps: the HTTP client keeps answering and nothing about the
 * page changes, which is the documented behaviour rather than a failure.
 */
export const OpenLocalRepository = Command.define("OpenLocalRepository", {
  args: { subject: Schema.NullOr(Schema.String) },
  messages: [AppMessage.SettledLocalRepository],
  execute: ({ subject }) =>
    Effect.gen(function* () {
      const { clientFromDocument } = yield* Effect.tryPromise(async () => await import("./api.ts"));
      const opened = yield* Effect.tryPromise(
        async () => await Repository.open(clientFromDocument(), subject),
      );
      return AppMessage.SettledLocalRepository({ state: opened ? "Ready" : "Unavailable" });
    }).pipe(Effect.orElseSucceed(() => AppMessage.SettledLocalRepository({ state: "Failed" }))),
});

/** Author local commits as whoever `/whoami` said is asking. */
export const SignLocalAs = Command.define("SignLocalAs", {
  args: { subject: Schema.NullOr(Schema.String) },
  messages: [AppMessage.CompletedNavigate],
  execute: ({ subject }) =>
    Effect.sync(() => {
      Repository.signAs(subject);
      return AppMessage.CompletedNavigate();
    }),
});

/**
 * One commit, and the reason it was refused.
 *
 * `RefConflict` is the one worth its own sentence: it is what `expected`
 * exists to produce, and the writer's draft is still in the Model with
 * nowhere to go, so the message says what to do with it. Classified here
 * because `tryPromise` widens the thrown value and the tag is on the class.
 */
const write = async (input: {
  readonly branch: string;
  readonly path: string;
  readonly content: string | null;
  readonly message: string;
  readonly expected: string | null;
}): Promise<string | null> => {
  const api = await import("./api.ts");
  const client = Repository.reading(api.clientFromDocument());
  try {
    await client.commitFiles({
      branch: input.branch,
      message: input.message,
      files: [
        input.content === null
          ? { path: input.path, content: null }
          : { path: input.path, content: input.content },
      ],
      expected: input.expected,
    });
    return null;
  } catch (error) {
    if (!(error instanceof api.ApiError) && !(error instanceof TypeError)) throw error;
    return error instanceof api.ApiError && error.tag === "RefConflict"
      ? `someone else committed to ${input.branch} while you were editing — copy your draft, reload, and reapply it`
      : api.describe(error);
  }
};

/**
 * Open a Change Request for the current branch.
 *
 * The order is the honest one: push first, so the revision the event names
 * exists on the server, then sign and append `pr.opened`, then go look at it.
 * A failure at any step says which step, and stops there.
 */
export const Propose = Command.define("Propose", {
  args: {
    branch: Schema.String,
    head: Schema.String,
    base: Schema.String,
    title: Schema.String,
    description: Schema.String,
  },
  messages: [AppMessage.SucceededPropose, AppMessage.FailedPropose],
  execute: ({ branch, head, base, title, description }) =>
    Effect.gen(function* () {
      const sync = Repository.syncing();
      if (sync !== null) {
        const state = yield* Effect.tryPromise(async () => await sync.sync(branch));
        if (state.ahead > 0) {
          const results = yield* Effect.tryPromise(async () => await sync.push(branch));
          const refused = results.find((result) => !result.ok);
          if (refused !== undefined) {
            return AppMessage.FailedPropose({
              reason: `push refused: ${refused.reason ?? refused.ref}`,
            });
          }
        }
      }
      const hub = yield* Effect.tryPromise(async () => await import("./hub.ts"));
      const pr = yield* Effect.tryPromise(
        async () => await hub.openPull({ title, description, base, head }),
      );
      return pr === null
        ? AppMessage.FailedPropose({
            reason: "the hub refused the Change Request — is this key a member?",
          })
        : AppMessage.SucceededPropose({ id: pr });
    }).pipe(
      Effect.catch((cause) =>
        Effect.succeed(AppMessage.FailedPropose({ reason: reasonOf(cause) })),
      ),
    ),
});

/** Replay one commit onto the current branch, or say why it could not be. */
export const CherryPick = Command.define("CherryPick", {
  args: { commit: Schema.String, onto: Schema.String },
  messages: [AppMessage.CompletedSync],
  execute: ({ commit, onto }) =>
    Effect.gen(function* () {
      const { clientFromDocument } = yield* Effect.tryPromise(async () => await import("./api.ts"));
      const client = Repository.reading(clientFromDocument());
      const outcome = yield* Effect.tryPromise(async () => await client.cherryPick(commit, onto));
      return AppMessage.CompletedSync({
        notice:
          outcome.kind === "conflicted"
            ? "cherry-pick conflicted — resolve on a branch"
            : outcome.kind === "up-to-date"
              ? `${onto} already has ${commit.slice(0, 7)}`
              : `picked ${commit.slice(0, 7)} onto ${onto}`,
        reload: outcome.kind === "replayed",
      });
    }).pipe(
      Effect.catch((cause) =>
        Effect.succeed(
          AppMessage.CompletedSync({
            notice: `cherry-pick failed — ${reasonOf(cause)}`,
            reload: false,
          }),
        ),
      ),
    ),
});

export const Rebase = Command.define("Rebase", {
  args: { branch: Schema.String, onto: Schema.String },
  messages: [AppMessage.CompletedSync],
  execute: ({ branch, onto }) =>
    Effect.gen(function* () {
      const { clientFromDocument } = yield* Effect.tryPromise(async () => await import("./api.ts"));
      const client = Repository.reading(clientFromDocument());
      const outcome = yield* Effect.tryPromise(async () => await client.rebase(branch, onto));
      return AppMessage.CompletedSync({
        notice:
          outcome.kind === "conflicted"
            ? "rebase conflicted — resolve by hand"
            : outcome.kind === "up-to-date"
              ? `${branch} is already on ${onto}`
              : `rebased ${branch} onto ${onto}`,
        reload: outcome.kind === "replayed",
      });
    }).pipe(
      Effect.catch((cause) =>
        Effect.succeed(
          AppMessage.CompletedSync({ notice: `rebase failed — ${reasonOf(cause)}`, reload: false }),
        ),
      ),
    ),
});

/** One bisect step: what the marks so far imply about where to look next. */
export const Bisect = Command.define("Bisect", {
  args: { good: Schema.Array(Schema.String), bad: Schema.String },
  messages: [AppMessage.SucceededBisect, AppMessage.CompletedSync],
  execute: ({ good, bad }) =>
    Effect.gen(function* () {
      const { clientFromDocument } = yield* Effect.tryPromise(async () => await import("./api.ts"));
      const client = Repository.reading(clientFromDocument());
      const answer = yield* Effect.tryPromise(async () => await client.bisect([...good], bad));
      return AppMessage.SucceededBisect({
        answer: { kind: answer.kind, commit: answer.commit, steps: answer.steps },
      });
    }).pipe(
      Effect.catch((cause) =>
        Effect.succeed(
          AppMessage.CompletedSync({ notice: `bisect failed — ${reasonOf(cause)}`, reload: false }),
        ),
      ),
    ),
});
