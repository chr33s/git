/**
 * The browser's signing identity — the key that lets this UI author hub
 * events.
 *
 * Hub events are signed by their author and judged by the projection, so a
 * browser that wants to *write* needs a key of its own. This module
 * generates one (WebCrypto Ed25519, through the same `SshSignature` module
 * every other author uses), keeps it in OPFS beside the repository clone,
 * and signs event payloads with it. The seed never leaves this origin: OPFS
 * is origin-private, and the only thing ever shown to a person is the
 * *public* half, so an operator can grant it membership with
 * `chr33s-git hub grant`.
 *
 * A fresh key is nobody: until it is granted, a repository with a genesis
 * refuses its events (and the UI falls back to tab-local state, saying so).
 * A repository served `--open` accepts them at once — which is what the
 * end-to-end suite drives.
 *
 * Writes go through `POST /hub/events` with the exact signed bytes. When
 * the server answers 401 with a nonce challenge, the request is retried
 * once under a signed `auth.request` envelope (`Auth.signEnvelope`) — the
 * same native scheme the CLI presents.
 */
import { Effect, Result } from "effect";

import {
  fingerprint,
  formatPublicKey,
  generate,
  NAMESPACE,
  parsePublicKey,
  sign,
  type PrivateKey,
} from "../src/crypto/SshSignature.ts";
import * as HubEvent from "../src/hub/Event.ts";
import * as HubTask from "../src/hub/Task.ts";
import { HubEventAppended, WhoamiAnswer } from "../src/server/ApiContract.ts";
import { signEnvelope } from "../src/server/Auth.ts";
import { Schema } from "effect";

import { ApiError } from "./api.ts";
import { apiBase, repoFromDocument } from "./client.ts";

const repo = repoFromDocument();

const urlOf = (path: string): string => `${apiBase() ?? ""}/${encodeURIComponent(repo)}${path}`;

// -- the key --------------------------------------------------------------

/**
 * The stored halves: the 32-byte seed, and the public key as its OpenSSH
 * line — the same line `hub grant` accepts, and enough to rebuild the
 * `PrivateKey` value without this module knowing the wire encoding.
 */
const store = async (create: boolean): Promise<FileSystemDirectoryHandle | null> => {
  if (globalThis.navigator?.storage?.getDirectory === undefined) return null;
  try {
    const origin = await navigator.storage.getDirectory();
    const scope = await origin.getDirectoryHandle("git-plus", { create });
    return await scope.getDirectoryHandle("identity", { create });
  } catch {
    return null;
  }
};

const load = async (): Promise<PrivateKey | null> => {
  const directory = await store(false);
  if (directory === null) return null;
  try {
    const seed = new Uint8Array(
      await (await (await directory.getFileHandle("seed")).getFile()).arrayBuffer(),
    );
    const line = (await (await (await directory.getFileHandle("public")).getFile()).text()).trim();
    const parsed = parsePublicKey(line);
    if (Result.isFailure(parsed) || seed.length !== 32) return null;
    return { publicKey: parsed.success, seed };
  } catch {
    return null;
  }
};

const persist = async (key: PrivateKey): Promise<void> => {
  const directory = await store(true);
  if (directory === null) return;
  const write = async (name: string, bytes: Uint8Array) => {
    const handle = await directory.getFileHandle(name, { create: true });
    const writable = await handle.createWritable();
    // SAFETY: both payloads are freshly allocated, never shared memory.
    await writable.write(bytes as Uint8Array<ArrayBuffer>);
    await writable.close();
  };
  await write("seed", key.seed);
  await write("public", new TextEncoder().encode(`${formatPublicKey(key.publicKey)}\n`));
};

let held: Promise<PrivateKey> | null = null;

/**
 * This browser's key: loaded from OPFS, or generated and persisted on first
 * use. Without OPFS the key is ephemeral — usable this visit, gone the
 * next — which is the honest ceiling of what such a browser can hold.
 */
export const identity = (): Promise<PrivateKey> => {
  held ??= (async () => {
    const stored = await load();
    if (stored !== null) return stored;
    const fresh = await Effect.runPromise(generate(`git-plus browser @ ${location.hostname}`));
    await persist(fresh);
    return fresh;
  })();
  return held;
};

/** What Settings shows, and what an operator pastes into `hub grant`. */
export const describeIdentity = async (): Promise<{
  readonly fingerprint: string;
  readonly publicKey: string;
}> => {
  const key = await identity();
  return {
    fingerprint: await Effect.runPromise(fingerprint(key.publicKey)),
    publicKey: formatPublicKey(key.publicKey),
  };
};

// -- the wire -------------------------------------------------------------

const decodeAppended = Schema.decodeUnknownSync(HubEventAppended);
const decodeWhoami = Schema.decodeUnknownSync(WhoamiAnswer);

const toBase64 = (bytes: Uint8Array): string => {
  let binary = "";
  for (const byte of bytes) binary += String.fromCharCode(byte);
  return btoa(binary);
};

/** The repository's identity, for envelopes; `null` before it has one. */
let knownRepoId: Promise<string | null> | null = null;
const repoIdOf = (): Promise<string | null> => {
  knownRepoId ??= fetch(urlOf("/whoami"))
    .then(async (response) => (response.ok ? decodeWhoami(await response.json()).repo : null))
    .catch(() => null);
  return knownRepoId;
};

/**
 * Retry one request under a signed `auth.request` envelope.
 *
 * The generic half of the native scheme: any JSON verb the server answered
 * 401 with a nonce challenge can be re-presented once, signed by this
 * browser's key. `ui/api.ts` routes every write through this, so a
 * repository that requires authentication asks for it exactly once per
 * request — and a key the repository has not granted simply gets the same
 * refusal back, which is the honest answer.
 */
export const retryAuthorized = async (
  url: string,
  init: RequestInit,
  denied: Response,
): Promise<Response | null> => {
  const nonce = /nonce="([^"]+)"/.exec(denied.headers.get("www-authenticate") ?? "")?.[1];
  if (nonce === undefined) return null;
  const repoId = await repoIdOf();
  if (repoId === null) return null;
  const key = await identity();
  const absolute = new URL(url, location.origin);
  const header = await Effect.runPromise(
    signEnvelope(key, {
      type: "auth.request",
      version: 1,
      repo: repoId,
      operation: `${init.method ?? "GET"} ${absolute.pathname}`,
      commands: [],
      nonce,
      expiresAt: new Date(Date.now() + 60_000).toISOString(),
    }),
  );
  const headers = new Headers(init.headers);
  headers.set("authorization", header);
  return await fetch(url, { ...init, headers });
};

/**
 * One append: POST the signed bytes, retrying once under an envelope when
 * challenged. The failure modes a caller can act on arrive as `ApiError`,
 * tag and all.
 */
const append = async (bytes: Uint8Array, key: PrivateKey): Promise<HubEventAppended> => {
  const armored = await Effect.runPromise(sign(key, bytes, NAMESPACE));
  const url = urlOf("/hub/events");
  const init: RequestInit = {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ payload: toBase64(bytes), signatures: [armored] }),
  };

  let response = await fetch(url, init);
  if (response.status === 401) {
    response = (await retryAuthorized(url, init, response)) ?? response;
  }

  if (!response.ok) {
    const failure: unknown = await response.json().catch((): undefined => undefined);
    // SAFETY: every field is read optionally and defaulted; the shape is the
    // same loose error body `ui/api.ts` decodes at its own boundary.
    const body_ = failure as { _tag?: string; message?: string; reason?: string } | undefined;
    throw new ApiError(
      body_?._tag ?? "HttpError",
      response.status,
      body_?.message ?? body_?.reason ?? `${response.status} ${response.statusText}`,
    );
  }
  return decodeAppended(await response.json());
};

/** The envelope fields every hub event shares, freshly stamped. */
const stamp = async (): Promise<{
  readonly version: 1;
  readonly repo: string;
  readonly id: string;
  readonly issuedAt: string;
  readonly trustHead: null;
}> => ({
  version: 1,
  repo: (await repoIdOf()) ?? repo,
  id: HubEvent.newId(),
  issuedAt: new Date().toISOString(),
  trustHead: null,
});

// -- the verbs ------------------------------------------------------------

/**
 * Open a task, signed by this browser. Returns the task's id once the
 * server has appended the event — the projection is the reader's to
 * re-query, which `hub.ts` does.
 */
export const openTask = async (input: {
  readonly title: string;
  readonly description: string;
}): Promise<string> => {
  const key = await identity();
  const task = HubTask.newId();
  const payload: HubTask.TaskPayload = {
    type: "task.opened",
    version: 1,
    // A repository without a genesis has no id to name; its own name is the
    // honest stand-in, and nothing folds the field until a genesis exists.
    repo: (await repoIdOf()) ?? repo,
    task,
    id: HubEvent.newId(),
    issuedAt: new Date().toISOString(),
    // The trust head this signer had seen — none; the verifier's safe
    // reading of `null` (see `hub/Event.ts`) is exactly right for a browser.
    trustHead: null,
    title: input.title,
    description: input.description,
    refs: [],
    pulls: [],
  };
  await append(HubTask.encode(payload), key);
  return task;
};

/** Comment on a pull request, signed by this browser. */
export const commentOnPull = async (input: {
  readonly pr: string;
  readonly body: string;
}): Promise<void> => {
  const key = await identity();
  const payload: HubEvent.HubPayload = {
    type: "comment.created",
    version: 1,
    repo: (await repoIdOf()) ?? repo,
    pr: input.pr,
    id: HubEvent.newId(),
    issuedAt: new Date().toISOString(),
    trustHead: null,
    body: input.body,
    head: null,
    path: null,
    side: null,
    line: null,
    contextHash: null,
  };
  await append(HubEvent.encode(payload), key);
};

/** Open a pull request for a revision that exists on the server. */
export const openPull = async (input: {
  readonly title: string;
  readonly description: string;
  readonly base: string;
  /** The proposed revision's oid. */
  readonly head: string;
}): Promise<string> => {
  const key = await identity();
  const pr = HubEvent.newId();
  const payload: HubEvent.HubPayload = {
    type: "pr.opened",
    ...(await stamp()),
    pr,
    title: input.title,
    description: input.description,
    base: input.base.startsWith("refs/") ? input.base : `refs/heads/${input.base}`,
    head: `sha1:${input.head}`,
    id: HubEvent.newId(),
  };
  await append(HubEvent.encode(payload), key);
  return pr;
};

/** Approve, reject, or comment on the exact revision under review. */
export const reviewPull = async (input: {
  readonly pr: string;
  readonly head: string;
  readonly decision: "approve" | "reject" | "comment";
  readonly body?: string;
}): Promise<void> => {
  const key = await identity();
  const payload: HubEvent.HubPayload = {
    type: "review.submitted",
    ...(await stamp()),
    pr: input.pr,
    head: `sha1:${input.head}`,
    decision: input.decision,
    body: input.body ?? "",
  };
  await append(HubEvent.encode(payload), key);
};

/** Reply in an existing thread. */
export const replyInThread = async (input: {
  readonly pr: string;
  readonly thread: string;
  readonly body: string;
}): Promise<void> => {
  const key = await identity();
  const payload: HubEvent.HubPayload = {
    type: "comment.replied",
    ...(await stamp()),
    pr: input.pr,
    thread: input.thread,
    body: input.body,
  };
  await append(HubEvent.encode(payload), key);
};

/** Mark a thread resolved, or reopen it. */
export const setThreadResolved = async (input: {
  readonly pr: string;
  readonly thread: string;
  readonly resolved: boolean;
}): Promise<void> => {
  const key = await identity();
  const payload: HubEvent.HubPayload = {
    type: input.resolved ? "comment.resolved" : "comment.reopened",
    ...(await stamp()),
    pr: input.pr,
    thread: input.thread,
  };
  await append(HubEvent.encode(payload), key);
};

/**
 * Record that a pull request merged — after the branch moved, never before:
 * the caller performs the merge through the JSON API and only then states
 * what happened, naming what was merged and what it became.
 */
export const recordMerged = async (input: {
  readonly pr: string;
  readonly head: string;
  readonly mergeCommit: string;
}): Promise<void> => {
  const key = await identity();
  const payload: HubEvent.HubPayload = {
    type: "pr.merged",
    ...(await stamp()),
    pr: input.pr,
    head: `sha1:${input.head}`,
    mergeCommit: `sha1:${input.mergeCommit}`,
  };
  await append(HubEvent.encode(payload), key);
};

/** Take a task's advisory lease, until `ttlSeconds` from now. */
export const claimTask = async (input: {
  readonly task: string;
  readonly ttlSeconds?: number;
}): Promise<void> => {
  const key = await identity();
  const base = await stamp();
  const payload: HubTask.TaskPayload = {
    type: "task.claimed",
    ...base,
    task: input.task,
    expiresAt: new Date(Date.now() + (input.ttlSeconds ?? 3600) * 1000).toISOString(),
  };
  await append(HubTask.encode(payload), key);
};

/** Let a task's lease go, so somebody else can pick it up. */
export const releaseTask = async (input: { readonly task: string }): Promise<void> => {
  const key = await identity();
  const payload: HubTask.TaskPayload = {
    type: "task.released",
    ...(await stamp()),
    task: input.task,
  };
  await append(HubTask.encode(payload), key);
};

/** Close a task, saying how it ended. */
export const closeTask = async (input: {
  readonly task: string;
  readonly outcome: "completed" | "abandoned" | "superseded";
  readonly pulls?: readonly string[];
}): Promise<void> => {
  const key = await identity();
  const payload: HubTask.TaskPayload = {
    type: "task.closed",
    ...(await stamp()),
    task: input.task,
    outcome: input.outcome,
    pulls: [...(input.pulls ?? [])],
    sessions: [],
  };
  await append(HubTask.encode(payload), key);
};
