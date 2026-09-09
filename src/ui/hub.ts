/**
 * The hub, read through the derived atom client.
 *
 * `GET /hub/tasks` and `GET /hub/pulls` are queried as atoms — memoized,
 * result-tracked — and every read here answers its caller with a value rather
 * than pushing anywhere. A Foldkit Command asks, and what it does with the
 * answer is fold it into the Model as a Message; that is what keeps the atoms
 * an implementation detail of this module rather than a second store beside
 * the Model.
 *
 * A repository whose hub is empty (or absent, or unreachable) keeps the
 * fixtures: the sample data is the UI's documented offline state, and an empty
 * live hub would render an empty product with nothing to review it by.
 *
 * Loaded lazily — this module pulls the derived client and the `HttpApi`
 * declaration with it, and the entry bundle should not pay for that before
 * first paint (the same argument `highlight.ts` makes for Shiki).
 *
 * Writes go through the browser's own signing key (`identity.ts`): a task
 * opened here, or a comment on a hub pull request, is signed locally and
 * appended over `POST /hub/events`, then read back by refreshing the same
 * query atoms — so what the screens show is always the server's projection,
 * never an optimistic guess. When the repository refuses the event (a fresh
 * key is not a member), the caller falls back to tab-local state and says so.
 */
import { Effect } from "effect";
import { AsyncResult } from "effect/unstable/reactivity";

import type {
  HubCheck,
  HubPullDetail,
  HubPullSummary,
  HubSessionSummary,
  HubTask,
  HubThread,
} from "../server/ApiContract.ts";

import { ApiError } from "./api.ts";
import { registry } from "./atoms.ts";
import type { Atom } from "effect/unstable/reactivity";
import { apiBase, GitPlusApi, repoFromDocument } from "./client.ts";
import {
  type ChangeRequest,
  type Comment,
  type SessionRow,
  type Status,
  type Task,
  type Thread,
} from "./model.ts";
import { ago, initials } from "./time.ts";
import { repositoryPath } from "../client/Url.ts";

const repo = repoFromDocument();

/** A fingerprint is `SHA256:…`; the reader wants something name-sized. */
const shortAuthor = (fingerprint: string | null): string =>
  fingerprint === null ? "unknown" : (fingerprint.split(":")[1] ?? fingerprint).slice(0, 8);

const taskStatus = (task: HubTask): Status =>
  task.closed !== null ? "Done" : task.claim !== null ? "In progress" : "Todo";

/**
 * A task, and what it belongs to.
 *
 * Only the edge is carried across. What it *means* — a release, an epic, a
 * parent story — is the reader's to name, and `task.ts`'s `ancestorsOf` is
 * where this UI names it.
 */
const mapTask = (task: HubTask): Task => ({
  id: task.task,
  kind: "Task",
  title: task.title === "" ? task.task : task.title,
  status: taskStatus(task),
  avatar: initials(task.title === "" ? task.task : task.title),
  desc: task.description,
  assignees: [],
  labels: task.refs.map((name) => ({ name, hue: "blue" })),
  comments: [],
  updated: "in the hub",
  hub: true,
  parent: task.parent ?? undefined,
  children: task.children,
});

const pullStatus = (pull: HubPullSummary): Status =>
  pull.state === "merged"
    ? "Merged"
    : pull.state === "closed"
      ? "Done"
      : pull.checks.total > 0 && !pull.checks.passed
        ? "Checks failing"
        : pull.approvals > 0
          ? "In review"
          : "Open";

const reviewCard = (pull: HubPullSummary): ChangeRequest["review"] => {
  if (pull.state === "merged") {
    return {
      headline: "Merged",
      detail: `into ${pull.base}`,
      ok: true,
      action: "Merged",
      merged: true,
    };
  }
  const checks =
    pull.checks.total === 0
      ? "no checks reported"
      : pull.checks.passed
        ? "checks green"
        : "checks failing";
  const approved = pull.approvals > 0;
  // Whether Merge is offered is the *server's* judgment — approvals,
  // required checks, threads, target movement, all under the published
  // rules — never a reconstruction of branch policy from these counts.
  return {
    headline: approved
      ? `${pull.approvals} approval${pull.approvals === 1 ? "" : "s"}`
      : "Review required",
    detail: pull.mergeable.ok
      ? `${checks} · ${pull.threads.unresolved} open thread${pull.threads.unresolved === 1 ? "" : "s"}`
      : (pull.mergeable.reasons[0] ??
        `${checks} · ${pull.threads.unresolved} open thread${pull.threads.unresolved === 1 ? "" : "s"}`),
    ok: pull.state === "open" && pull.mergeable.ok,
    action: "Merge",
  };
};

const mapPull = (pull: HubPullSummary): ChangeRequest => {
  const mapped: ChangeRequest = {
    id: pull.id,
    kind: "CR",
    title: pull.title,
    status: pullStatus(pull),
    avatar: initials(shortAuthor(pull.author)),
    desc: "",
    assignees: [],
    labels: [],
    comments: [],
    updated: ago(new Date(pull.at)),
    sourceRef: pull.head ?? "",
    targetRef: pull.base,
    diffStat: "",
    commitCount: "",
    diffFile: "",
    commits: [],
    checks: [],
    review: reviewCard(pull),
    diff: [],
    hub: true,
    reviewHead: pull.head ?? undefined,
  };
  return mapped;
};

const mapCheck = (check: HubCheck): ChangeRequest["checks"][number] => ({
  name: check.name,
  detail: `${check.provider} · ${check.status}`,
  ok: check.status === "success",
});

const mapThread = (thread: HubThread): Thread => ({
  id: thread.id,
  path: thread.path,
  resolved: thread.resolved,
  comments: thread.comments.map((comment) => ({
    avatar: initials(shortAuthor(comment.author)),
    author: shortAuthor(comment.author),
    when: ago(new Date(comment.at)),
    text: comment.body,
  })),
});

const threadComments = (threads: readonly HubThread[]): readonly Comment[] =>
  threads.flatMap((thread) =>
    thread.comments.map((comment) => ({
      avatar: initials(shortAuthor(comment.author)),
      author: shortAuthor(comment.author),
      when: ago(new Date(comment.at)),
      text: thread.path === null ? comment.body : `${thread.path}: ${comment.body}`,
    })),
  );

/** The ids the hub answered for, so hydration never touches a fixture. */
const fromHub = new Set<string>();

/**
 * Whether the hub's refusal was authentication, asked of the server itself.
 *
 * An authentication refusal and an unreachable server are different product
 * states — a private repository that turned this key away must not be
 * dressed up as the offline sample. Rather than dissecting the derived
 * client's failure shapes, one cheap probe re-asks the listing and reads
 * the status plainly: a 401/403 here means even the signed retry was
 * refused (the client's transport already presents the browser key), and a
 * network fault means offline, which keeps the fixtures.
 */
const deniedByServer = async (): Promise<boolean> => {
  try {
    const base = apiBase() ?? "";
    const response = await fetch(`${base}${repositoryPath(repo)}/hub/tasks?limit=1`);
    return response.status === 401 || response.status === 403;
  } catch {
    return false;
  }
};

const DENIED =
  "this repository requires authentication to read — grant this browser's key to see live state";

/** Refresh a complete listing together, including pages fetched on earlier reads. */
const allPages = Effect.fn("hub.allPages")(function* <A, E, R>(
  read: (cursor: string | undefined) => Effect.Effect<
    {
      readonly items: ReadonlyArray<A>;
      readonly has_more: boolean;
      readonly next_cursor: string | null;
    },
    E,
    R
  >,
) {
  const items: A[] = [];
  const seen = new Set<string>();
  let cursor: string | undefined;
  while (true) {
    const page = yield* read(cursor);
    items.push(...page.items);
    if (!page.has_more) return { items };
    if (page.next_cursor === null || seen.has(page.next_cursor)) {
      return yield* new ApiError({
        tag: "InvalidResponse",
        status: 200,
        message: "hub listing returned a missing or repeated pagination cursor",
      });
    }
    cursor = page.next_cursor;
    seen.add(cursor);
  }
});

const tasksAtom = GitPlusApi.runtime.atom(
  GitPlusApi.use((client) =>
    allPages((cursor) => client.hub.tasks({ params: { repo }, query: { cursor } })),
  ),
);
const pullsAtom = GitPlusApi.runtime.atom(
  GitPlusApi.use((client) =>
    allPages((cursor) => client.hub.pulls({ params: { repo }, query: { cursor } })),
  ),
);

const sessionsAtom = GitPlusApi.runtime.atom(
  GitPlusApi.use((client) =>
    allPages((cursor) => client.hub.sessions({ params: { repo }, query: { cursor } })),
  ),
);

const mapSession = (session: HubSessionSummary): SessionRow => ({
  id: session.session,
  agent:
    session.agent === null
      ? "unknown"
      : `${session.agent.kind} · ${session.agent.model} · ${session.agent.harness}`,
  refs: session.refs,
  pulls: session.pulls,
  commits: session.commits,
  openDecisions: session.decisions.open,
  tokens: session.usage.inputTokens + session.usage.outputTokens,
});

/**
 * One Change Request's detail, as the screen shows it.
 *
 * Only the revision under review: superseded heads' checks are history, not
 * current evidence, and the server's own count of the commit range replaces
 * the review-count proxy the listing carries.
 */
const detailOf = (detail: HubPullDetail): Task => ({
  ...mapPull(detail),
  desc: detail.description,
  comments: threadComments(detail.threadList),
  threads: detail.threadList.map(mapThread),
  reviewHead: detail.head ?? undefined,
  // `checkList` is the whole history, labelled with the head each run was
  // against (`src/server/ApiContract.ts`); only the current revision's runs are
  // evidence about the code being reviewed. A superseded head's red `ci` beside
  // the new green one reads as a failing proposal that is not failing.
  checks: detail.checkList
    .filter((check) => detail.head !== null && check.head === detail.head)
    .map(mapCheck),
  commitCount: String(detail.commits),
});

/**
 * Drop whatever the listing atoms are holding.
 *
 * Belt and braces rather than the mechanism: the registry carries no idle TTL
 * (`atoms.ts`), so a node is already released when the read that subscribed to
 * it returns and the next `listings()` asks the repository again. This is what
 * makes that true of a node something else is still holding.
 */
export const refreshListings = (): void => {
  registry.refresh(tasksAtom);
  registry.refresh(pullsAtom);
  registry.refresh(sessionsAtom);
};

/**
 * What the hub holds, as a value.
 *
 * This answers a caller rather than writing anywhere, so a Command can ask
 * and turn the answer into a Message — which is what keeps the Model the only
 * place the hub's answers are held. Each call is a fresh read: the registry
 * holds nothing once the read that subscribed has returned, which is what lets
 * `settled` below watch a projection catch up.
 *
 * Three outcomes, and they are not interchangeable. `Denied` is a repository
 * that turned this browser away — the caller must empty rather than fall back,
 * because showing the sample over a refusal dresses a denial up as data.
 * `Unreachable` is offline, where the fixtures are the documented behaviour.
 * `Empty` tasks *and* pulls is a repository with no hub, which is the same.
 */
export type Listings =
  | {
      readonly _tag: "Loaded";
      readonly tasks: readonly Task[];
      readonly sessions: readonly SessionRow[];
    }
  | { readonly _tag: "Denied"; readonly reason: string }
  | { readonly _tag: "Unreachable"; readonly reason: string };

/**
 * One settled answer from a listing atom, or `null` when it failed.
 *
 * Subscribes, takes the first settled result and lets go. The registry holds
 * nothing after that (`atoms.ts`), so each call is a fresh read — which is
 * what both `listings` and `settled` are relying on.
 */
const readAtom = async <A>(
  atom: Atom.Atom<AsyncResult.AsyncResult<{ readonly items: readonly A[] }, unknown>>,
): Promise<readonly A[] | null> =>
  await new Promise<readonly A[] | null>((resolve) => {
    const stop = registry.subscribe(
      atom,
      (result) => {
        if (AsyncResult.isSuccess(result)) {
          resolve(result.value.items);
          // After this turn: `subscribe` has not returned its unsubscribe
          // yet when `immediate` delivers a value that is already settled.
          queueMicrotask(stop);
        } else if (AsyncResult.isFailure(result)) {
          resolve(null);
          queueMicrotask(stop);
        }
      },
      { immediate: true },
    );
  });

export const listings = async (): Promise<Listings> => {
  const [tasks, pulls, sessions] = await Promise.all([
    readAtom(tasksAtom),
    readAtom(pullsAtom),
    readAtom(sessionsAtom),
  ]);

  if (tasks === null || pulls === null) {
    return (await deniedByServer())
      ? { _tag: "Denied", reason: DENIED }
      : { _tag: "Unreachable", reason: "the repository's hub could not be read" };
  }

  const live = tasks.filter((task) => task.exists);
  if (live.length === 0 && pulls.length === 0) {
    return { _tag: "Unreachable", reason: "this repository's hub is empty" };
  }

  const mapped: Task[] = [...pulls.map(mapPull), ...live.map(mapTask)];
  fromHub.clear();
  for (const task of mapped) fromHub.add(task.id);
  return {
    _tag: "Loaded",
    tasks: mapped,
    sessions: (sessions ?? []).map(mapSession),
  };
};

/**
 * One hub Change Request's discussion, checks and review.
 *
 * Answers with the detail rather than pushing it anywhere: the caller is a
 * Foldkit Command, and what it does with the answer is fold it into the Model
 * as a Message. `null` for a fixture id — the design's data is complete — and
 * for a detail the listing has already moved past, because a response naming a
 * superseded head cannot replace the proposal that superseded it.
 */
export const hydrated = async (id: string, head: string | null): Promise<Task | null> => {
  if (!fromHub.has(id)) return null;
  const atom = GitPlusApi.query("hub", "pull", { params: { repo, id } });
  const detail = await new Promise<HubPullDetail | null>((resolve) => {
    const stop = registry.subscribe(
      atom,
      (result) => {
        if (AsyncResult.isSuccess(result)) {
          resolve(result.value);
          queueMicrotask(stop);
        } else if (AsyncResult.isFailure(result)) {
          resolve(null);
          queueMicrotask(stop);
        }
      },
      { immediate: true },
    );
  });
  if (detail === null) return null;
  if (head !== null && (detail.head ?? null) !== head) return null;
  return detailOf(detail);
};

/**
 * Wait briefly for an id the refresh is about to deliver.
 *
 * A signed event lands, the projection is re-read, and only then does the
 * listing name what was just written — so a caller that navigated straight to
 * the new id would arrive before it exists. Polling the listing rather than a
 * store is what lets this module have no store at all.
 *
 * The whole listing, not one atom of it: the id may be a task's or a Change
 * Request's, and `listings` is where the two are folded into one set. Each
 * pass is a real read — the registry holds nothing between them — which is
 * exactly what makes the poll see the projection move.
 */
const settled = async (id: string): Promise<boolean> => {
  for (let waited = 0; waited < 4000; waited += 200) {
    const held = await listings();
    if (held._tag === "Loaded" && held.tasks.some((task) => task.id === id)) return true;
    await new Promise((resolve) => setTimeout(resolve, 200));
  }
  return false;
};

/**
 * Open a task in the hub, signed by this browser's key.
 *
 * `null` — not an error — when the event could not land (offline, or the
 * key holds no membership on a repository that requires one): the caller
 * keeps its tab-local fallback, which is the documented behaviour.
 */
export const createTask = async (input: {
  readonly title: string;
  readonly description: string;
  readonly parent?: string;
}): Promise<string | null> => {
  try {
    const { openTask } = await import("./identity.ts");
    const task = await openTask(input);
    fromHub.add(task);
    refreshListings();
    // Wait for the projection to arrive so the navigation that follows finds
    // the task in the Model rather than an empty detail screen.
    await settled(task);
    return task;
  } catch {
    return null;
  }
};

/**
 * Move a hub task under another, or out from under one.
 *
 * `false` where the repository refused the event — a key that is not a member
 * — and the caller says so rather than showing a move that did not happen.
 */
export const moveTask = async (task: string, parent: string): Promise<boolean> => {
  if (!fromHub.has(task)) return false;
  try {
    const { reparentTask } = await import("./identity.ts");
    await reparentTask({ task, parent });
    refreshListings();
    return true;
  } catch {
    return false;
  }
};

/**
 * Comment on a hub pull request, signed by this browser's key.
 *
 * `false` for a fixture id — the design's data is not a place to write — and
 * for any event the repository refused.
 */
export const commentOn = async (id: string, body: string): Promise<boolean> => {
  // Only a Change Request: pull-request and task ids share one shape, so a
  // task id reaching the pull-request comment API would *create* a ghost
  // `refs/hub/pr/<task>` ref. Live task discussion waits for a task-comment
  // event to exist in the protocol; the detail screen says so. The caller
  // holds the task and has already checked that, so this checks provenance.
  if (!fromHub.has(id)) return false;
  try {
    const { commentOnPull } = await import("./identity.ts");
    await commentOnPull({ pr: id, body });
    registry.refresh(GitPlusApi.query("hub", "pull", { params: { repo, id } }));
    refreshListings();
    return true;
  } catch {
    return false;
  }
};

const refreshPull = (id: string): void => {
  registry.refresh(GitPlusApi.query("hub", "pull", { params: { repo, id } }));
  refreshListings();
};

/** Open a Change Request for a revision the server holds. */
export const openPull = async (input: {
  readonly title: string;
  readonly description: string;
  readonly base: string;
  readonly head: string;
}): Promise<string | null> => {
  try {
    const identity = await import("./identity.ts");
    const pr = await identity.openPull(input);
    fromHub.add(pr);
    refreshListings();
    await settled(pr);
    return pr;
  } catch {
    return null;
  }
};

/** Approve or reject the revision a hub Change Request proposes. */
export const review = async (
  id: string,
  decision: "approve" | "reject",
  head: string,
): Promise<boolean> => {
  // The head is the caller's: a review approves one revision, and the one on
  // screen is the one the reader judged. Reading it here from a listing that
  // may have moved on would approve something else.
  if (!fromHub.has(id) || head === "") return false;
  try {
    const identity = await import("./identity.ts");
    await identity.reviewPull({ pr: id, head, decision });
    refreshPull(id);
    return true;
  } catch {
    return false;
  }
};

/** Reply in a thread on a hub Change Request. */
export const reply = async (id: string, thread: string, body: string): Promise<boolean> => {
  if (!fromHub.has(id)) return false;
  try {
    const identity = await import("./identity.ts");
    await identity.replyInThread({ pr: id, thread, body });
    refreshPull(id);
    return true;
  } catch {
    return false;
  }
};

/** Resolve or reopen a thread on a hub Change Request. */
export const resolveThread = async (
  id: string,
  thread: string,
  resolved: boolean,
): Promise<boolean> => {
  if (!fromHub.has(id)) return false;
  try {
    const identity = await import("./identity.ts");
    await identity.setThreadResolved({ pr: id, thread, resolved });
    refreshPull(id);
    return true;
  } catch {
    return false;
  }
};

/**
 * Settle a hub Change Request through the hub's own merge endpoint — one
 * judged server-side transition, never a generic branch merge followed by a
 * separate record. `null` on success; otherwise the reason to show, with
 * canonical state untouched: an offline or refused merge leaves the Change
 * Request open, because it *is* open.
 */
export const merge = async (id: string, head: string, base: string): Promise<string | null> => {
  if (!fromHub.has(id) || head === "") {
    return "this is not a hub Change Request the browser can settle";
  }
  try {
    const identity = await import("./identity.ts");
    await identity.mergePull({ pr: id, head, base });
    // What shows next is the projection, re-read — never an optimistic flip.
    refreshPull(id);
    return null;
  } catch (error) {
    return error instanceof ApiError
      ? error.message
      : "the hub could not be reached — the Change Request stays open";
  }
};

/** A hub task's lease and lifecycle, from the detail screen's buttons. */
export const taskAction = async (
  id: string,
  action: "claim" | "release" | "complete" | "abandon",
): Promise<boolean> => {
  if (!fromHub.has(id)) return false;
  try {
    const identity = await import("./identity.ts");
    if (action === "claim") await identity.claimTask({ task: id });
    else if (action === "release") await identity.releaseTask({ task: id });
    else {
      await identity.closeTask({
        task: id,
        outcome: action === "complete" ? "completed" : "abandoned",
      });
    }
    refreshListings();
    return true;
  } catch {
    return false;
  }
};
