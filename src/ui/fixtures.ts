/**
 * The Task and Change Request data from the design.
 *
 * Carried over verbatim from `git-plus light.dc.html` — same ids, titles,
 * statuses, descriptions, comments, commits, checks and diffs. It is fixture
 * data because the hub layer that would produce it for real
 * (`src/hub/Projection.ts`) is not yet reachable over HTTP; see `model.ts`.
 *
 * The hierarchy is the one the spec uses as its worked example:
 *
 *   Task: Implement authentication
 *   ├── Task: Design session model
 *   ├── Change Request: Add auth middleware
 *   ├── Change Request: Add login UI
 *   └── Task: Update documentation
 */
import type { ChangeRequest, DiffLine, Label, LabelHue, Person, Task } from "./model.ts";

const person = (name: string, avatar: string): Person => ({ name, avatar });

const label = (name: string, hue: LabelHue): Label => ({ name, hue });

const add = (n: number, text: string): DiffLine => ({ n, text, kind: "add" });
const del = (n: number, text: string): DiffLine => ({ n, text, kind: "del" });
const ctx = (n: number, text: string): DiffLine => ({ n, text, kind: "context" });

const mkessler = person("mkessler", "MK");
const rbaek = person("rbaek", "RB");
const atran = person("atran", "AT");

/**
 * The releases, as tasks.
 *
 * A milestone is not a type here: it is a task other tasks belong to, which
 * is what `src/hub/Task.ts` records and all the hub can say. Everything the
 * detail screen shows under "Milestone" is read back off that edge.
 */
const V4: Task = {
  id: "M-4",
  kind: "Task",
  title: "v0.4 — Identity",
  status: "In progress",
  avatar: "V4",
  updated: "today",
  desc: "Keys, membership and signing: the release the authentication work lands in.",
  assignees: [mkessler],
  labels: [label("release", "blue")],
  comments: [],
  children: ["T-12", "T-17", "CR-19"],
};

const V5: Task = {
  id: "M-5",
  kind: "Task",
  title: "v0.5 — Scale",
  status: "Todo",
  avatar: "V5",
  updated: "last week",
  desc: "What follows Identity: replication, packing and the numbers behind them.",
  assignees: [rbaek],
  labels: [label("release", "blue")],
  comments: [],
  children: ["T-20"],
};

const T12: Task = {
  id: "T-12",
  parent: "M-4",
  kind: "Task",
  title: "Implement authentication",
  status: "In progress",
  avatar: "MK",
  updated: "an hour ago",
  desc: "Add first-party authentication to git+ core: session-based for the web UI, token-based for the CLI. Child work is split between pure Tasks and Change Requests carrying the actual diffs.",
  assignees: [mkessler, rbaek],
  labels: [label("auth", "accent"), label("epic", "blue")],
  comments: [
    {
      avatar: "RB",
      author: "rbaek",
      when: "3d ago",
      text: "Splitting the middleware and UI into separate Change Requests so they can land independently.",
    },
    {
      avatar: "MK",
      author: "mkessler",
      when: "2d ago",
      text: "Agreed. Session model design is done — see T-13 for the notes.",
    },
  ],
  children: ["T-13", "CR-14", "CR-15", "T-16"],
};

const T13: Task = {
  id: "T-13",
  kind: "Task",
  title: "Design session model",
  status: "Done",
  avatar: "MK",
  updated: "4d ago",
  parent: "T-12",
  desc: "Decide session storage, expiry, and rotation strategy. Outcome: opaque server-side sessions with 30-day sliding expiry, rotated on privilege change.",
  assignees: [mkessler],
  labels: [label("auth", "accent"), label("design", "amber")],
  comments: [
    {
      avatar: "MK",
      author: "mkessler",
      when: "4d ago",
      text: "Wrote up the comparison of JWT vs server-side sessions in the description. Going with server-side.",
    },
  ],
};

const CR14: ChangeRequest = {
  id: "CR-14",
  kind: "CR",
  title: "Add auth middleware",
  status: "In review",
  avatar: "RB",
  updated: "20h ago",
  parent: "T-12",
  desc: "Implements the session middleware from T-13: cookie parsing, session lookup, and the auth context available to all handlers.",
  assignees: [rbaek],
  labels: [label("auth", "accent")],
  sourceRef: "rbaek/auth-middleware",
  targetRef: "main",
  diffStat: "+214 −38",
  commitCount: "3",
  diffFile: "server/middleware/auth.go",
  commits: [
    { sha: "b3f19ae", msg: "add session store interface", when: "2d ago" },
    { sha: "90c44d1", msg: "wire auth context into router", when: "1d ago" },
    { sha: "7d2e0fc", msg: "rotate session on privilege change", when: "20h ago" },
  ],
  checks: [
    { name: "build / linux-amd64", detail: "passed in 2m 14s", ok: true },
    { name: "test / unit", detail: "passed in 4m 02s", ok: true },
    { name: "lint / govet", detail: "passed in 41s", ok: true },
  ],
  review: {
    headline: "Approved — mergeable",
    detail: "1 approval from mkessler · all checks passing",
    ok: true,
    action: "Squash and merge",
  },
  diff: [
    ctx(12, " func Router(s *Store) http.Handler {"),
    del(13, "-\tr := chi.NewRouter()"),
    add(14, "+\tr := chi.NewRouter()"),
    add(15, "+\tr.Use(SessionMiddleware(s.Sessions))"),
    add(16, "+\tr.Use(AuthContext)"),
    ctx(17, ' \tr.Get("/", handleIndex)'),
    ctx(18, ' \tr.Post("/tasks", requireAuth(handleCreateTask))'),
  ],
  comments: [
    {
      avatar: "MK",
      author: "mkessler",
      when: "1d ago",
      text: "Reviewed — the rotation-on-privilege-change handling is clean. Approving.",
    },
  ],
};

const CR15: ChangeRequest = {
  id: "CR-15",
  kind: "CR",
  title: "Add login UI",
  status: "Checks failing",
  avatar: "AT",
  updated: "3h ago",
  parent: "T-12",
  desc: "Login and logout pages for the web UI, using the middleware from CR-14. Blocked on a flaky e2e check.",
  assignees: [atran],
  labels: [label("auth", "accent"), label("frontend", "purple")],
  sourceRef: "atran/login-ui",
  targetRef: "main",
  diffStat: "+402 −12",
  commitCount: "5",
  diffFile: "web/pages/login.tsx",
  commits: [
    { sha: "44a01be", msg: "login page layout", when: "3d ago" },
    { sha: "c81f2d9", msg: "wire session cookie flow", when: "2d ago" },
    { sha: "f00b3aa", msg: "logout + redirect handling", when: "1d ago" },
    { sha: "2be91c4", msg: "error states", when: "22h ago" },
    { sha: "9d17e05", msg: "fix e2e selector", when: "3h ago" },
  ],
  checks: [
    { name: "build / linux-amd64", detail: "passed in 2m 31s", ok: true },
    { name: "test / unit", detail: "passed in 3m 48s", ok: true },
    { name: "test / e2e-login", detail: "failed — timeout waiting for #session-ready", ok: false },
  ],
  review: {
    headline: "Blocked — checks failing",
    detail: "e2e-login failing · merge disabled until checks pass",
    ok: false,
    action: "Merge blocked",
  },
  diff: [
    add(1, "+export function LoginPage() {"),
    add(2, "+  const [error, setError] = useState<string | null>(null)"),
    add(3, "+  return ("),
    add(4, '+    <form method="post" action="/session">'),
    add(5, '+      <TokenField name="password" />'),
    add(6, "+    </form>"),
  ],
  comments: [
    {
      avatar: "AT",
      author: "atran",
      when: "5h ago",
      text: "The e2e failure looks like a race in the test harness, not the UI. Re-running with the new selector.",
    },
  ],
};

const T16: Task = {
  id: "T-16",
  kind: "Task",
  title: "Update documentation",
  status: "Todo",
  avatar: "AT",
  updated: "5d ago",
  parent: "T-12",
  desc: "Document the auth setup flow for self-hosted installs: session config, token creation, and CLI login.",
  assignees: [atran],
  labels: [label("docs", "amber")],
  comments: [],
};

const T17: Task = {
  id: "T-17",
  parent: "M-4",
  kind: "Task",
  title: "Migrate CI to runners v2",
  status: "In progress",
  avatar: "RB",
  updated: "2h ago",
  desc: "Move all pipelines to the v2 runner fleet before the v1 fleet is decommissioned at the end of the quarter.",
  assignees: [rbaek],
  labels: [label("infra", "orange")],
  comments: [
    {
      avatar: "RB",
      author: "rbaek",
      when: "1w ago",
      text: "Pipeline config CR is merged; remaining work is cache migration.",
    },
  ],
  children: ["CR-18"],
};

const CR18: ChangeRequest = {
  id: "CR-18",
  kind: "CR",
  title: "Update pipeline config",
  status: "Merged",
  avatar: "RB",
  updated: "2h ago",
  parent: "T-17",
  desc: "Points all pipeline definitions at the v2 runner labels and bumps the cache key format.",
  assignees: [rbaek],
  labels: [label("infra", "orange")],
  sourceRef: "rbaek/runners-v2",
  targetRef: "main",
  diffStat: "+61 −59",
  commitCount: "2",
  diffFile: ".gitp/pipelines.yml",
  commits: [
    { sha: "e4a91c2", msg: "point pipelines at v2 runner labels", when: "2h ago" },
    { sha: "1c0d9f7", msg: "bump cache key format", when: "2h ago" },
  ],
  checks: [
    { name: "build / linux-amd64", detail: "passed in 1m 58s", ok: true },
    { name: "test / unit", detail: "passed in 3m 40s", ok: true },
  ],
  review: {
    headline: "Merged",
    detail: "Squashed into main as e4a91c2 · 2h ago",
    ok: true,
    action: "Merged",
    merged: true,
  },
  diff: [
    del(4, "-  runs-on: fleet-v1"),
    add(5, "+  runs-on: fleet-v2"),
    del(8, "-  cache-key: v1-{{ checksum }}"),
    add(9, "+  cache-key: v2-{{ checksum }}"),
  ],
  comments: [],
};

const CR19: ChangeRequest = {
  id: "CR-19",
  parent: "M-4",
  kind: "CR",
  title: "Fix flaky clone test",
  status: "Open",
  avatar: "MK",
  updated: "6h ago",
  desc: "test/clone_test.go intermittently fails on slow disks; this pins the fixture repo size and adds a retry.",
  assignees: [mkessler],
  labels: [label("tests", "blue")],
  sourceRef: "mkessler/fix-clone-test",
  targetRef: "main",
  diffStat: "+18 −6",
  commitCount: "1",
  diffFile: "test/clone_test.go",
  commits: [{ sha: "a91bc03", msg: "pin fixture size, add retry", when: "6h ago" }],
  checks: [
    { name: "build / linux-amd64", detail: "passed in 2m 05s", ok: true },
    { name: "test / unit", detail: "passed in 3m 51s", ok: true },
  ],
  review: {
    headline: "Awaiting review",
    detail: "0 of 1 required approvals · checks passing",
    ok: true,
    action: "Squash and merge",
  },
  diff: [
    del(22, "-\trepo := makeFixture(t, 500*MB)"),
    add(23, "+\trepo := makeFixture(t, 50*MB)"),
    add(24, "+\tretry(t, 3, func() error {"),
    ctx(25, " \t\treturn clone(repo)"),
    add(26, "+\t})"),
  ],
  comments: [],
};

const T20: Task = {
  id: "T-20",
  parent: "M-5",
  kind: "Task",
  title: "Q3 performance audit",
  status: "Todo",
  avatar: "AT",
  updated: "1w ago",
  desc: "Profile pack transfer and task-tree queries under load; produce a prioritized list of follow-up Tasks.",
  assignees: [atran],
  labels: [label("perf", "red")],
  comments: [],
};

export const tasks: readonly Task[] = [V4, V5, T12, T13, CR14, CR15, T16, T17, CR18, CR19, T20];

export const byId: ReadonlyMap<string, Task> = new Map(tasks.map((task) => [task.id, task]));

/** Where each timeline card sits: 14 columns, one per day of the fortnight. */
export interface TimelineEvent {
  readonly id: string;
  readonly column: number;
  readonly span: number;
  readonly row: number;
  readonly epic?: boolean;
}

export const timeline: readonly TimelineEvent[] = [
  { id: "T-13", column: 1, span: 3, row: 1 },
  { id: "T-12", column: 3, span: 8, row: 2, epic: true },
  { id: "CR-14", column: 5, span: 4, row: 3 },
  { id: "CR-15", column: 7, span: 5, row: 4 },
  { id: "CR-18", column: 2, span: 4, row: 5 },
  { id: "T-20", column: 10, span: 5, row: 5 },
  { id: "CR-19", column: 8, span: 3, row: 6 },
];
