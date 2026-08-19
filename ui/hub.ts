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
} from "../src/server/ApiContract.ts";

import { registry } from "./atoms.ts";
import { GitPlusApi, repoFromDocument } from "./client.ts";
import type { ChangeRequest, Comment, SessionRow, Status, Task, Thread } from "./model.ts";
import { ago, initials } from "./time.ts";
import { store } from "./store.ts";

const repo = repoFromDocument();

/** A fingerprint is `SHA256:…`; the reader wants something name-sized. */
const shortAuthor = (fingerprint: string | null): string =>
  fingerprint === null ? "unknown" : (fingerprint.split(":")[1] ?? fingerprint).slice(0, 8);

const taskStatus = (task: HubTask): Status =>
  task.closed !== null ? "Done" : task.claim !== null ? "In progress" : "Todo";

const mapTask = (task: HubTask): Task => ({
  id: task.task,
  kind: "Task",
  title: task.title === "" ? task.task : task.title,
  status: taskStatus(task),
  avatar: initials(task.title === "" ? task.task : task.title),
  desc: task.description,
  assignees: [],
  labels: task.refs.map((name) => ({ name, hue: "blue" })),
  milestone: task.closed?.outcome ?? "—",
  comments: [],
  updated: "in the hub",
  hub: true,
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
  return {
    headline: approved
      ? `${pull.approvals} approval${pull.approvals === 1 ? "" : "s"}`
      : "Review required",
    detail: `${checks} · ${pull.threads.unresolved} open thread${pull.threads.unresolved === 1 ? "" : "s"}`,
    ok: pull.state === "open" && approved && (pull.checks.total === 0 || pull.checks.passed),
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
  milestone: "—",
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
        if (task.kind !== "CR") return hydrated;
        return {
          ...hydrated,
          checks: detail.checkList.map(mapCheck),
          commitCount: String(detail.reviews.length),
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
 * Comment on a hub pull request, signed by this browser's key.
 *
 * `false` for a fixture id — the design's data is not a place to write — and
 * for any event the repository refused.
 */
export const commentOn = async (id: string, body: string): Promise<boolean> => {
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

/** Record a hub Change Request as merged, after its branch really moved. */
export const recordMerged = async (id: string, mergeCommit: string): Promise<boolean> => {
  const task = store.get(id);
  if (!fromHub.has(id) || task?.reviewHead === undefined) return false;
  try {
    const identity = await import("./identity.ts");
    await identity.recordMerged({ pr: id, head: task.reviewHead, mergeCommit });
    refreshPull(id);
    return true;
  } catch {
    return false;
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
