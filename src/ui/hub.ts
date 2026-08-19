/**
 * The hub, read through the derived atom client and folded into the store.
 *
 * `GET /hub/tasks` and `GET /hub/pulls` are queried as atoms — memoized,
 * result-tracked — and while they answer with anything, the store's contents
 * are the projection rather than the design's fixtures. A repository whose
 * hub is empty (or absent, or unreachable) keeps the fixtures: the sample
 * data is the UI's documented offline state, and an empty live hub would
 * render an empty product with nothing to review it by.
 *
 * Loaded lazily by `store.ts` — this module pulls the derived client and the
 * `HttpApi` declaration with it, and the entry bundle should not pay for
 * that before first paint (the same argument `highlight.ts` makes for
 * Shiki).
 *
 * Writes go through the browser's own signing key (`identity.ts`): a task
 * opened here, or a comment on a hub pull request, is signed locally and
 * appended over `POST /hub/events`, then read back by refreshing the same
 * query atoms — so what the screens show is always the server's projection,
 * never an optimistic guess. When the repository refuses the event (a fresh
 * key is not a member), the caller falls back to tab-local state and says so.
 */
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
import { apiBase, GitPlusApi, repoFromDocument } from "./client.ts";
import {
  type ChangeRequest,
  type Comment,
  isChangeRequest,
  type SessionRow,
  type Status,
  type Task,
  type Thread,
} from "./model.ts";
import { ago, initials } from "./time.ts";
import { store } from "./store.ts";

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
 * parent story — is the reader's to name, and `store.ancestorsOf` is where
 * this UI names it.
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

const mapPull = (pull: HubPullSummary): ChangeRequest => ({
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
});

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
    const response = await fetch(`${base}/${encodeURIComponent(repo)}/hub/tasks?limit=1`);
    return response.status === 401 || response.status === 403;
  } catch {
    return false;
  }
};

const DENIED =
  "this repository requires authentication to read — grant this browser's key to see live state";

const tasksAtom = GitPlusApi.query("hub", "tasks", { params: { repo }, query: {} });
const pullsAtom = GitPlusApi.query("hub", "pulls", { params: { repo }, query: {} });

const sessionsAtom = GitPlusApi.query("hub", "sessions", { params: { repo }, query: {} });

/** Ask the hub again; every mounted subscription folds the answer back in. */
export const refreshListings = (): void => {
  registry.refresh(tasksAtom);
  registry.refresh(pullsAtom);
  registry.refresh(sessionsAtom);
};

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
 * Subscribe the store to the hub listings.
 *
 * Both atoms stay mounted for the life of the page; a later invalidation
 * (or refetch) folds straight back into the store, which notifies the
 * screens exactly as a local mutation would.
 */
export const seed = (): void => {
  let tasks: readonly HubTask[] | null = null;
  let pulls: readonly HubPullSummary[] | null = null;

  const apply = (): void => {
    if (tasks === null || pulls === null) return;
    if (tasks.length === 0 && pulls.length === 0) return;
    const mapped: Task[] = [...pulls.map(mapPull), ...tasks.map(mapTask)];
    fromHub.clear();
    for (const task of mapped) fromHub.add(task.id);
    store.adopt(mapped);
  };

  registry.subscribe(
    tasksAtom,
    (result) => {
      if (AsyncResult.isSuccess(result)) {
        tasks = result.value.items.filter((task) => task.exists);
        apply();
      } else if (AsyncResult.isFailure(result)) {
        void deniedByServer().then((was) => {
          if (was) store.denyLive(DENIED);
        });
      }
    },
    { immediate: true },
  );
  registry.subscribe(
    pullsAtom,
    (result) => {
      if (AsyncResult.isSuccess(result)) {
        pulls = result.value.items;
        apply();
      } else if (AsyncResult.isFailure(result)) {
        void deniedByServer().then((was) => {
          if (was) store.denyLive(DENIED);
        });
      }
    },
    { immediate: true },
  );
  registry.subscribe(
    sessionsAtom,
    (result) => {
      if (AsyncResult.isSuccess(result)) {
        store.adoptSessions(result.value.items.map(mapSession));
      }
    },
    { immediate: true },
  );
};

const hydrating = new Set<string>();

/**
 * Fill one hub Change Request's discussion, checks and review from the
 * detail endpoint. A no-op for fixture ids: the design's data is complete.
 */
export const hydrate = (id: string): void => {
  if (!fromHub.has(id) || hydrating.has(id)) return;
  hydrating.add(id);
  const atom = GitPlusApi.query("hub", "pull", { params: { repo, id } });
  registry.subscribe(
    atom,
    (result) => {
      if (!AsyncResult.isSuccess(result)) return;
      const detail: HubPullDetail = result.value;
      store.patch(id, (task) => {
        const hydrated = {
          ...task,
          desc: detail.description,
          comments: threadComments(detail.threadList),
          threads: detail.threadList.map(mapThread),
          reviewHead: detail.head ?? undefined,
        };
        if (!isChangeRequest(task)) return hydrated;
        return {
          ...hydrated,
          // Only the revision under review: superseded heads' checks are
          // history, not current evidence, and the server's own count of
          // the commit range replaces the review-count proxy that once
          // stood in for it.
          checks: detail.checkList
            .filter((check) => detail.head !== null && check.head === detail.head)
            .map(mapCheck),
          commitCount: String(detail.commits),
          // The Merge button states the server's judgment — approvals,
          // required checks, threads, target movement — never a client-side
          // reconstruction of branch policy from counts.
          review:
            task.review.merged === true
              ? task.review
              : {
                  ...task.review,
                  ok: detail.mergeable.ok,
                  detail: detail.mergeable.ok
                    ? task.review.detail
                    : (detail.mergeable.reasons[0] ?? task.review.detail),
                },
        };
      });
    },
    { immediate: true },
  );
};

/** Poll the store briefly for an id the refresh is about to deliver. */
const settled = async (id: string): Promise<boolean> => {
  for (let waited = 0; waited < 4000; waited += 200) {
    if (store.get(id) !== undefined) return true;
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
    // the task in the store rather than an empty detail screen.
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
  // event to exist in the protocol; the detail screen says so.
  const task = store.get(id);
  if (!fromHub.has(id) || task === undefined || !isChangeRequest(task)) return false;
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
export const review = async (id: string, decision: "approve" | "reject"): Promise<boolean> => {
  const task = store.get(id);
  if (!fromHub.has(id) || task?.reviewHead === undefined) return false;
  try {
    const identity = await import("./identity.ts");
    await identity.reviewPull({ pr: id, head: task.reviewHead, decision });
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
export const merge = async (id: string): Promise<string | null> => {
  const task = store.get(id);
  if (
    !fromHub.has(id) ||
    task === undefined ||
    !isChangeRequest(task) ||
    task.reviewHead === undefined
  ) {
    return "this is not a hub Change Request the browser can settle";
  }
  try {
    const identity = await import("./identity.ts");
    await identity.mergePull({ pr: id, head: task.reviewHead, base: task.targetRef });
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
