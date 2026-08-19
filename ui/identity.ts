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
  fromSeed,
  generate,
  NAMESPACE,
  parsePublicKey,
  sign,
  type PrivateKey,
} from "../src/crypto/SshSignature.ts";
import * as HubEvent from "../src/hub/Event.ts";
import * as HubTask from "../src/hub/Task.ts";
import {
  HubEventAppended,
  HubMerged,
  RefsResponse,
  WhoamiAnswer,
} from "../src/server/ApiContract.ts";
import { signEnvelope } from "../src/server/Auth.ts";
import { Schema } from "effect";

import { type Authorize, nonceOf, repoOf, type SignedCommand } from "../src/client/Authorize.ts";
import { ApiError } from "./api.ts";
import { apiBase, repoFromDocument } from "./client.ts";

const repo = repoFromDocument();

const urlOf = (path: string): string => `${apiBase() ?? ""}/${encodeURIComponent(repo)}${path}`;

// -- the key --------------------------------------------------------------

/**
 * The identity lives as *one* versioned record (`identity.json`): the seed,
 * the public line derived from it, and the format version — written in a
 * single OPFS file so an interrupted write can never leave a seed from one
 * key beside the public half of another. The seed is always the authority:
 * every load re-derives the public point and compares, and a disagreement
 * is repaired from the seed with a visible note rather than signed over.
 */
const RECORD = "identity.json";

const StoredIdentity = Schema.Struct({
  version: Schema.Literal(1),
  algorithm: Schema.Literal("ssh-ed25519"),
  /** The 32-byte seed, base64. */
  seed: Schema.String,
  /** The OpenSSH public line — what `hub grant` accepts. */
  publicKey: Schema.String,
});
const decodeStored = Schema.decodeUnknownResult(StoredIdentity);

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

const seedFromBase64 = (encoded: string): Uint8Array | null => {
  try {
    const binary = atob(encoded);
    const bytes = new Uint8Array(binary.length);
    for (let index = 0; index < binary.length; index += 1) bytes[index] = binary.charCodeAt(index);
    return bytes;
  } catch {
    return null;
  }
};

const seedToBase64 = (seed: Uint8Array): string => {
  let binary = "";
  for (const byte of seed) binary += String.fromCharCode(byte);
  return btoa(binary);
};

/** Why the identity is not simply "loaded" — repaired, fresh, or ephemeral. */
let identityNote: string | null = null;

/**
 * The key a seed determines, checked against the public line stored beside
 * it. Agreement returns the stored line's key (its comment included);
 * disagreement is repaired from the seed — the private authority — with the
 * note that a re-grant may be needed. `null` when the seed itself is
 * unusable.
 */
const keyOfSeed = async (seed: Uint8Array, storedLine: string): Promise<PrivateKey | null> => {
  const derived = await Effect.runPromise(
    fromSeed(seed, `git-plus browser @ ${location.hostname}`).pipe(
      Effect.map((key): PrivateKey | null => key),
      Effect.orElseSucceed((): PrivateKey | null => null),
    ),
  );
  if (derived === null) return null;
  const parsed = parsePublicKey(storedLine.trim());
  if (
    Result.isSuccess(parsed) &&
    parsed.success.point.length === derived.publicKey.point.length &&
    parsed.success.point.every((byte, index) => byte === derived.publicKey.point[index])
  ) {
    return { publicKey: parsed.success, seed };
  }
  identityNote =
    "the stored identity's halves disagreed; the public key was re-derived from the seed — if the fingerprint changed, the key needs granting again";
  return derived;
};

const load = async (): Promise<PrivateKey | null> => {
  const directory = await store(false);
  if (directory === null) return null;

  // The atomic record, when one exists.
  try {
    const raw: unknown = JSON.parse(
      await (await (await directory.getFileHandle(RECORD)).getFile()).text(),
    );
    const decoded = decodeStored(raw);
    if (Result.isSuccess(decoded)) {
      const seed = seedFromBase64(decoded.success.seed);
      if (seed !== null) {
        const key = await keyOfSeed(seed, decoded.success.publicKey);
        if (key !== null) {
          if (identityNote !== null) await persist(key);
          return key;
        }
      }
    }
    // A record that exists but cannot be used is said out loud: continuing
    // to a fresh key silently would strand every grant made to the old one.
    identityNote =
      "the stored identity could not be read; a fresh key was generated and needs granting";
    return null;
  } catch {
    // No record: fall through to the legacy two-file layout, if any.
  }

  // One-time migration from the two independently written files. When the
  // halves agree the fingerprint is preserved exactly; when they disagree
  // the seed wins, visibly.
  try {
    const seed = new Uint8Array(
      await (await (await directory.getFileHandle("seed")).getFile()).arrayBuffer(),
    );
    const line = (await (await (await directory.getFileHandle("public")).getFile()).text()).trim();
    const key = await keyOfSeed(seed, line);
    if (key === null) return null;
    await persist(key);
    await directory.removeEntry("seed").catch(() => {});
    await directory.removeEntry("public").catch(() => {});
    return key;
  } catch {
    return null;
  }
};

const persist = async (key: PrivateKey): Promise<void> => {
  const directory = await store(true);
  if (directory === null) return;
  const record = JSON.stringify(
    {
      version: 1,
      algorithm: "ssh-ed25519",
      seed: seedToBase64(key.seed),
      publicKey: formatPublicKey(key.publicKey),
    } satisfies (typeof StoredIdentity)["Type"],
    null,
    2,
  );
  // One file, written through OPFS's create-then-atomically-swap writable:
  // a crash mid-write leaves the previous record, never half of a new one.
  const handle = await directory.getFileHandle(RECORD, { create: true });
  const writable = await handle.createWritable();
  await writable.write(new TextEncoder().encode(record));
  await writable.close();
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
  /** A repair or regeneration worth telling the operator about; `null` when healthy. */
  readonly note: string | null;
}> => {
  const key = await identity();
  return {
    fingerprint: await Effect.runPromise(fingerprint(key.publicKey)),
    publicKey: formatPublicKey(key.publicKey),
    note: identityNote,
  };
};

// -- the wire -------------------------------------------------------------

/**
 * Boundary decoding, without throwing at the parser: a live answer that
 * does not match the wire contract becomes a typed `ApiError` naming the
 * endpoint, so drift reads as drift in diagnostics rather than as an
 * anonymous crash. Each decoder reads the body itself — the response is the
 * I/O boundary, and this is where its bytes become (or fail to become) a
 * domain value.
 */
const decoded = <S extends Schema.ConstraintDecoder<unknown>>(schema: S, endpoint: string) => {
  const parse = Schema.decodeUnknownResult(schema);
  return async (response: Response): Promise<S["Type"]> => {
    const body: unknown = await response.json().catch((): undefined => undefined);
    const result = parse(body);
    if (Result.isFailure(result)) {
      throw new ApiError("SchemaError", 502, `${endpoint} answered outside its wire contract`);
    }
    return result.success;
  };
};
const decodeAppended = decoded(HubEventAppended, "POST /hub/events");
const decodeWhoami = decoded(WhoamiAnswer, "GET /whoami");

const toBase64 = (bytes: Uint8Array): string => {
  let binary = "";
  for (const byte of bytes) binary += String.fromCharCode(byte);
  return btoa(binary);
};

/**
 * The repository's identity, for envelopes; `null` before it is known.
 *
 * Cached only on success: a network fault or an unreadable answer keeps
 * nothing, so the next action asks again instead of inheriting the failure
 * for the life of the page. On a *private* repository the unauthenticated
 * `/whoami` is itself refused — and the refusal's own challenge names the
 * RepoID, which is exactly why the server puts it there.
 */
let knownRepoId: string | null = null;
let resolving: Promise<string | null> | null = null;

const resolveRepoId = async (): Promise<string | null> => {
  try {
    const response = await fetch(urlOf("/whoami"));
    if (response.ok) return (await decodeWhoami(response)).repo;
    if (response.status === 401) return repoOf(response);
    return null;
  } catch {
    return null;
  }
};

const repoIdOf = (): Promise<string | null> => {
  if (knownRepoId !== null) return Promise.resolve(knownRepoId);
  resolving ??= resolveRepoId().then((repo) => {
    resolving = null;
    if (repo !== null) knownRepoId = repo;
    return repo;
  });
  return resolving;
};

/**
 * The signed `authorization` header a challenge asks for, or `null` when
 * the challenge is malformed — a caller then reports the refusal it holds.
 */
const envelopeFor = async (
  denied: Response,
  operation: string,
  commands: ReadonlyArray<SignedCommand>,
): Promise<string | null> => {
  const nonce = nonceOf(denied);
  if (nonce === null) return null;
  // The challenge's own RepoID wins — it is the server's statement about
  // this very repository — and is remembered for the requests that follow.
  const challenged = repoOf(denied);
  if (challenged !== null) knownRepoId ??= challenged;
  const repoId = challenged ?? (await repoIdOf());
  if (repoId === null) return null;
  const key = await identity();
  return await Effect.runPromise(
    signEnvelope(key, {
      type: "auth.request",
      version: 1,
      repo: repoId,
      operation,
      commands: [...commands],
      nonce,
      expiresAt: new Date(Date.now() + 60_000).toISOString(),
    }),
  );
};

/**
 * Retry one request under a signed `auth.request` envelope.
 *
 * The generic half of the native scheme: any JSON verb the server answered
 * 401 with a nonce challenge can be re-presented once, signed by this
 * browser's key. `ui/api.ts` routes every request through this, so a
 * repository that requires authentication asks for it exactly once per
 * request — and a key the repository has not granted simply gets the same
 * refusal back, which is the honest answer.
 */
export const retryAuthorized = async (
  url: string,
  init: RequestInit,
  denied: Response,
): Promise<Response | null> => {
  const absolute = new URL(url, location.origin);
  const header = await envelopeFor(denied, `${init.method ?? "GET"} ${absolute.pathname}`, []);
  if (header === null) return null;
  const headers = new Headers(init.headers);
  headers.set("authorization", header);
  return await fetch(url, { ...init, headers });
};

/**
 * The same discipline for smart HTTP — clone, fetch and push hand their
 * challenge here, so a private repository's advertisement, pack and
 * receive-pack all answer to the one browser key. The push's ref commands
 * ride in the envelope, which is what lets the policy boundary hold the
 * push to exactly what was signed.
 */
export const authorizeSmartHttp: Authorize = ({ commands, operation, response }) =>
  envelopeFor(response, operation, commands);

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
  return await decodeAppended(response);
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

const decodeMerged = decoded(HubMerged, "POST /hub/pulls/:id/merge");
const decodeRefs = decoded(RefsResponse, "GET /refs");

/** Where a ref stands on the server now — the compare-and-swap a merge names. */
const tipOf = async (ref: string): Promise<string | null> => {
  const url = urlOf("/refs");
  let response = await fetch(url);
  if (response.status === 401) {
    response = (await retryAuthorized(url, {}, response)) ?? response;
  }
  if (!response.ok) {
    throw new ApiError("HttpError", response.status, `${response.status} ${response.statusText}`);
  }
  return (await decodeRefs(response)).refs.find((held) => held.name === ref)?.oid ?? null;
};

/**
 * Settle a pull request through the hub's own merge: one server-side
 * transition that advances the base to the exact approved head and appends
 * this browser's signed `pr.merged` beside it — or refuses, leaving both
 * refs exactly where they were. Never two requests, never an optimistic
 * "merged": the caller re-reads the projection to learn what happened.
 */
export const mergePull = async (input: {
  readonly pr: string;
  /** The approved revision — the base advances to exactly this. */
  readonly head: string;
  /** The base ref being advanced, e.g. `refs/heads/main`. */
  readonly base: string;
}): Promise<HubMerged> => {
  const key = await identity();
  const expected = await tipOf(input.base);
  const payload: HubEvent.HubPayload = {
    type: "pr.merged",
    ...(await stamp()),
    pr: input.pr,
    head: `sha1:${input.head}`,
    mergeCommit: `sha1:${input.head}`,
  };
  const bytes = HubEvent.encode(payload);
  const armored = await Effect.runPromise(sign(key, bytes, NAMESPACE));
  const url = urlOf(`/hub/pulls/${encodeURIComponent(input.pr)}/merge`);
  const init: RequestInit = {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      head: input.head,
      expected,
      payload: toBase64(bytes),
      signatures: [armored],
    }),
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
  return await decodeMerged(response);
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
