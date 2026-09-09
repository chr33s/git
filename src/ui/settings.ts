/**
 * What the Settings screen can ask the repository to do.
 *
 * One union rather than sixteen Messages: every administrative action has the
 * same shape — do a thing, say in one line what happened, reload what changed —
 * so the Message that carries them is one, and this says which.
 *
 * Its own module so the Message union and the Command that runs it can both
 * name it without importing each other.
 */
import { Schema as FoldkitSchema } from "foldkit";
import { Schema } from "effect";

const HEADS = "refs/heads/";
const TAGS = "refs/tags/";

/** `refs/heads/main` → `main`; a tag likewise. Names, not paths, on screen. */
export const short = (name: string): string => {
  if (name.startsWith(HEADS)) return name.slice(HEADS.length);
  return name.startsWith(TAGS) ? name.slice(TAGS.length) : name;
};

/** The full ref a branch name stands for. */
export const headRef = (branch: string): string => `${HEADS}${branch}`;

/** Comma-separated input, as the list the policy endpoint wants. */
export const list = (value: string): readonly string[] =>
  value
    .split(",")
    .map((entry) => entry.trim())
    .filter((entry) => entry !== "");

export const AdminAction = FoldkitSchema.defineTaggedUnion({
  DeleteBranch: { name: Schema.String },
  ResetBranch: { ref: Schema.String, to: Schema.String },
  DeleteTag: { name: Schema.String },
  CreateTag: { name: Schema.String, target: Schema.String, message: Schema.String },
  FetchRemote: { name: Schema.String },
  PushRemote: { name: Schema.String, branch: Schema.String },
  PullRemote: { name: Schema.String, branch: Schema.String },
  DeleteRemote: { name: Schema.String },
  AddRemote: { name: Schema.String, url: Schema.String, credential: Schema.String },
  DeleteWebhook: { id: Schema.String },
  AddWebhook: { url: Schema.String, secret: Schema.String },
  Fsck: {},
  PreviewGc: {},
  Gc: {},
  ShowReflog: { branch: Schema.String },
  WritePolicy: {
    protectedRefs: Schema.String,
    approvals: Schema.String,
    checks: Schema.String,
    requirePullRequest: Schema.Boolean,
    requireResolvedThreads: Schema.Boolean,
  },
});
export type AdminAction = typeof AdminAction.Type;

/** Which card an action reports into. */
export const cardOf = (action: AdminAction): string =>
  AdminAction.match(action, {
    DeleteBranch: () => "branches",
    ResetBranch: () => "branches",
    DeleteTag: () => "tags",
    CreateTag: () => "tags",
    FetchRemote: () => "remotes",
    PushRemote: () => "remotes",
    PullRemote: () => "remotes",
    DeleteRemote: () => "remotes",
    AddRemote: () => "remotes",
    DeleteWebhook: () => "webhooks",
    AddWebhook: () => "webhooks",
    Fsck: () => "maintenance",
    PreviewGc: () => "maintenance",
    Gc: () => "maintenance",
    ShowReflog: () => "maintenance",
    WritePolicy: () => "policy",
  });
