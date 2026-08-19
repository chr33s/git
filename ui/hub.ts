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
 * Writes are deliberately absent. Hub events are signed by their author and
 * the server holds nobody's key, so a browser without one cannot author
 * events; `POST /hub/events` exists for callers that can sign (the CLI, an
 * agent). Until the UI holds a key, its mutations stay tab-local and say so.
 */
import { AsyncResult } from "effect/unstable/reactivity";

import type {
  HubCheck,
  HubPullDetail,
  HubPullSummary,
  HubTask,
  HubThread,
} from "../src/server/ApiContract.ts";

import { registry } from "./atoms.ts";
import { GitPlusApi, repoFromDocument } from "./client.ts";
import type { ChangeRequest, Comment, Status, Task } from "./model.ts";
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

const review = (pull: HubPullSummary): ChangeRequest["review"] => {
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
  review: review(pull),
  diff: [],
});

const mapCheck = (check: HubCheck): ChangeRequest["checks"][number] => ({
  name: check.name,
  detail: `${check.provider} · ${check.status}`,
  ok: check.status === "success",
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
 * Subscribe the store to the hub listings.
 *
 * Both atoms stay mounted for the life of the page; a later invalidation
 * (or refetch) folds straight back into the store, which notifies the
 * screens exactly as a local mutation would.
 */
export const seed = (): void => {
  const tasksAtom = GitPlusApi.query("hub", "tasks", { params: { repo } });
  const pullsAtom = GitPlusApi.query("hub", "pulls", { params: { repo } });

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
        tasks = result.value.tasks.filter((task) => task.exists);
        apply();
      }
    },
    { immediate: true },
  );
  registry.subscribe(
    pullsAtom,
    (result) => {
      if (AsyncResult.isSuccess(result)) {
        pulls = result.value.pulls;
        apply();
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
