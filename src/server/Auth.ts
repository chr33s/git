/**
 * Authentication: proof of an SSH private key, and nothing minted by a server.
 *
 * The model this replaces issued HMAC tokens from a shared secret, which made
 * the server the source of repository authority — lose the secret and you lose
 * the repository; move the repository and the tokens mean nothing. Here
 * authority comes from the trust graph, and a request's job is only to prove
 * which key is making it.
 *
 * Two paths, because two kinds of client exist and both have to work:
 *
 *   - **native** (`Hub-SSH-v1`): the client signs an envelope naming this
 *     repository, this operation and these ref commands, with a server nonce
 *     and an expiry. Nothing replays: not to another repository, not to
 *     another operation, not twice.
 *   - **delegated** (`hub1.`): a member signs a short-lived capability
 *     attestation, and stock `git` presents it as a Basic password. It is
 *     verified against the trust graph like anything else — no server secret,
 *     no registry, no revocation list beyond membership itself. Stock `git`
 *     cannot do challenge-response over smart HTTP, and refusing to serve it
 *     would make every ordinary `git clone` fail.
 *
 * A delegated credential can never exceed what its issuer holds, and revoking
 * the issuer revokes every credential they minted. Within its lifetime it *is*
 * a bearer credential; the short expiry and the capability scoping are the
 * containment, and that is stated rather than hidden.
 *
 * The envelope signs the **ref command list**, not the request body. A push
 * body is a pack that streams, and hashing it before sending would make every
 * client buffer an entire repository to authenticate. It does not need to: each
 * command names the exact new oid, and the pack is checked against those oids
 * on the way in, so the body is bound transitively by content addressing.
 */
import { Context, Effect, Layer, Schema } from "effect";

import {
  type Fingerprint,
  fingerprint,
  NAMESPACE,
  type PrivateKey,
  sign,
  verify,
} from "../crypto/SshSignature.ts";
import { Invalid } from "../git/Error.ts";
import { Repository } from "../git/Repository.ts";
import { readGenesis, type RepoId } from "../trust/Genesis.ts";
import { type Member, project, type Projection } from "../trust/Projection.ts";
import * as Verify from "../trust/Verify.ts";

const encoder = new TextEncoder();
const decoder = new TextDecoder();

// -- what an operation costs ---------------------------------------------------

/**
 * The capability a request needs before anything else is checked.
 *
 * Receive-pack is charged `source.push` from the advertisement on, because a
 * client that cannot push has no business learning the ref layout through the
 * push endpoint. Whether a *particular* push additionally needs
 * `source.force-push` or `source.delete` is a question about its commands, and
 * it belongs to the policy boundary, which can see them.
 */
export const requiredCapability = (request: Request): string => {
  const url = new URL(request.url);
  const last = url.pathname.split("/").at(-1);

  if (last === "git-receive-pack") return "source.push";
  if (last === "refs" && url.searchParams.get("service") === "git-receive-pack") {
    return "source.push";
  }
  if (last === "git-upload-pack") return "repo.read";
  // The LFS batch endpoint negotiates downloads as well as uploads, so method
  // alone would lock a reader out of cloning any repository that uses LFS. The
  // upload it hands back is a separate PUT, and that one is charged as a write.
  if (last === "batch" && url.pathname.includes("/info/lfs/")) return "repo.read";
  return request.method === "GET" || request.method === "HEAD" ? "repo.read" : "source.push";
};

// -- nonces ---------------------------------------------------------------------

/**
 * Server-issued, single-use nonces.
 *
 * Single use is what stops a captured envelope being replayed inside its own
 * lifetime. The store only has to remember a nonce until it expires, so it
 * stays small however long the server runs.
 *
 * A server that forgets a nonce — an evicted Durable Object, a restart —
 * rejects the request that carried it and the client asks for another. That is
 * a retry, not a failure mode worth persisting through.
 */
export class Nonces extends Context.Service<
  Nonces,
  {
    readonly issue: (ttlSeconds: number) => Effect.Effect<string>;
    /** `true` the first time; `false` for an unknown, spent or expired nonce. */
    readonly consume: (nonce: string) => Effect.Effect<boolean>;
  }
>()("server/Nonces") {}

export const noncesInMemory: Layer.Layer<Nonces> = Layer.sync(Nonces, () => {
  const issued = new Map<string, number>();

  const prune = (now: number) => {
    for (const [nonce, expiry] of issued) if (expiry <= now) issued.delete(nonce);
  };

  return Nonces.of({
    issue: (ttlSeconds) =>
      Effect.sync(() => {
        const now = Date.now();
        prune(now);
        const nonce = crypto.randomUUID();
        issued.set(nonce, now + ttlSeconds * 1000);
        return nonce;
      }),
    consume: (nonce) =>
      Effect.sync(() => {
        const now = Date.now();
        prune(now);
        const expiry = issued.get(nonce);
        if (expiry === undefined || expiry <= now) return false;
        issued.delete(nonce);
        return true;
      }),
  });
});

// -- payloads --------------------------------------------------------------------

/** One ref command: the name, what it was, and what it is to become. */
export const Command = Schema.Struct({
  ref: Schema.String,
  from: Schema.NullOr(Schema.String),
  to: Schema.NullOr(Schema.String),
});

export const Envelope = Schema.Struct({
  type: Schema.tag("auth.request"),
  version: Schema.Literal(1),
  repo: Schema.String,
  operation: Schema.String,
  commands: Schema.Array(Command),
  nonce: Schema.String,
  expiresAt: Schema.String,
});

export const Delegation = Schema.Struct({
  type: Schema.tag("auth.delegate"),
  version: Schema.Literal(1),
  repo: Schema.String,
  capabilities: Schema.Array(Schema.String),
  expiresAt: Schema.String,
  /** Distinguishes two credentials minted in the same second. */
  nonce: Schema.String,
});

export type Envelope = (typeof Envelope)["Type"];
export type Delegation = (typeof Delegation)["Type"];

const decodeEnvelope = Schema.decodeUnknownEffect(Envelope);
const decodeDelegation = Schema.decodeUnknownEffect(Delegation);

/**
 * The longest a delegated credential may live.
 *
 * A day is the outer bound rather than a suggestion: it is a bearer credential
 * for as long as it is valid, and the only thing keeping that acceptable is
 * that "as long as it is valid" is short.
 */
export const MAX_DELEGATION_SECONDS = 86_400;

const encodePayload = <A>(ordered: A): Uint8Array =>
  encoder.encode(`${JSON.stringify(ordered, null, 2)}\n`);

export const encodeEnvelope = (envelope: Envelope): Uint8Array =>
  encodePayload({
    version: envelope.version,
    type: envelope.type,
    repo: envelope.repo,
    operation: envelope.operation,
    commands: envelope.commands,
    nonce: envelope.nonce,
    expiresAt: envelope.expiresAt,
  });

export const encodeDelegation = (delegation: Delegation): Uint8Array =>
  encodePayload({
    version: delegation.version,
    type: delegation.type,
    repo: delegation.repo,
    capabilities: delegation.capabilities,
    expiresAt: delegation.expiresAt,
    nonce: delegation.nonce,
  });

// -- base64url ---------------------------------------------------------------------

const toBase64Url = (bytes: Uint8Array): string => {
  let binary = "";
  for (const byte of bytes) binary += String.fromCharCode(byte);
  return btoa(binary).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
};

const fromBase64Url = (text: string): Uint8Array | null => {
  try {
    const padded = text.replace(/-/g, "+").replace(/_/g, "/");
    const binary = atob(padded.padEnd(Math.ceil(padded.length / 4) * 4, "="));
    const out = new Uint8Array(binary.length);
    for (let index = 0; index < binary.length; index++) out[index] = binary.charCodeAt(index);
    return out;
  } catch {
    return null;
  }
};

// -- what arrived ---------------------------------------------------------------------

export type Presented =
  | { readonly kind: "none" }
  | { readonly kind: "delegated"; readonly credential: string }
  | { readonly kind: "native"; readonly payload: string; readonly signature: string };

const DELEGATION_PREFIX = "hub1.";
const NATIVE_SCHEME = "Hub-SSH-v1 ";

/**
 * The credential a request carries, whichever way the client sent it.
 *
 * `git` sends credentials as HTTP Basic, so a delegated credential is accepted
 * as the *password* field — and as the username when the password is empty,
 * because that is how `https://<token>@host/repo` arrives. Missing that is how
 * you get a server where `curl` works and `git clone` does not.
 */
export const credentialOf = (request: Request): Presented => {
  const header = request.headers.get("authorization");
  if (header === null) return { kind: "none" };

  if (header.startsWith(NATIVE_SCHEME)) {
    const [payload, signature] = header.slice(NATIVE_SCHEME.length).trim().split(".");
    return payload === undefined || signature === undefined
      ? { kind: "none" }
      : { kind: "native", payload, signature };
  }

  const bearer = header.startsWith("Bearer ") ? header.slice(7).trim() : null;
  if (bearer !== null) return { kind: "delegated", credential: bearer };

  if (header.startsWith("Basic ")) {
    try {
      const decoded = atob(header.slice(6));
      const colon = decoded.indexOf(":");
      if (colon === -1) return { kind: "delegated", credential: decoded };
      const password = decoded.slice(colon + 1);
      return {
        kind: "delegated",
        credential: password.length > 0 ? password : decoded.slice(0, colon),
      };
    } catch {
      return { kind: "none" };
    }
  }

  return { kind: "none" };
};

// -- delegated credentials ---------------------------------------------------------

/**
 * Mint a credential stock `git` can present.
 *
 * The capabilities are what the holder asked for, and verification intersects
 * them with what the *issuer* holds at the time the credential is used — so
 * asking for more than you have produces a credential that authorizes less
 * than it claims, rather than one that authorizes more than you had.
 */
export const mintDelegation = Effect.fn("Auth.mintDelegation")(function* (input: {
  readonly key: PrivateKey;
  readonly repo: RepoId;
  readonly capabilities: ReadonlyArray<string>;
  readonly ttlSeconds: number;
  readonly now?: Date;
}) {
  if (input.ttlSeconds <= 0 || input.ttlSeconds > MAX_DELEGATION_SECONDS) {
    return yield* new Invalid({
      field: "ttl",
      reason: `a delegated credential lives between 1 and ${MAX_DELEGATION_SECONDS} seconds`,
    });
  }

  const now = input.now ?? new Date();
  const delegation = {
    type: "auth.delegate",
    version: 1,
    repo: input.repo,
    capabilities: input.capabilities,
    expiresAt: new Date(now.getTime() + input.ttlSeconds * 1000).toISOString(),
    nonce: crypto.randomUUID(),
  } satisfies Delegation;

  const bytes = encodeDelegation(delegation);
  const signature = yield* sign(input.key, bytes, NAMESPACE);
  return `${DELEGATION_PREFIX}${toBase64Url(bytes)}.${toBase64Url(encoder.encode(signature))}`;
});

export interface Delegated {
  readonly signer: Fingerprint;
  readonly delegation: Delegation;
}

/**
 * Check a delegated credential's shape and signature.
 *
 * Says nothing about membership — that is the trust graph's answer, and it is
 * asked separately in `authenticate` so that "this signature is real" and
 * "this signer is allowed" stay two questions.
 */
export const openDelegation = Effect.fn("Auth.openDelegation")(function* (
  credential: string,
  repo: RepoId,
  now: Date,
) {
  if (!credential.startsWith(DELEGATION_PREFIX)) return null;

  const [payload, signature] = credential.slice(DELEGATION_PREFIX.length).split(".");
  if (payload === undefined || signature === undefined) return null;

  const bytes = fromBase64Url(payload);
  const armored = fromBase64Url(signature);
  if (bytes === null || armored === null) return null;

  const json = yield* Effect.try({
    try: () => JSON.parse(decoder.decode(bytes)),
    catch: () => new Invalid({ field: "credential", reason: "not JSON" }),
  }).pipe(Effect.orElseSucceed(() => null));
  if (json === null) return null;

  const delegation = yield* decodeDelegation(json).pipe(Effect.orElseSucceed(() => null));
  if (delegation === null) return null;

  // The repository is inside the signed bytes, so a credential minted for one
  // repository presents as garbage at any other.
  if (delegation.repo !== repo) return null;
  const expiry = Date.parse(delegation.expiresAt);
  if (Number.isNaN(expiry) || expiry <= now.getTime()) return null;

  const key = yield* verify(decoder.decode(armored), bytes, NAMESPACE).pipe(
    Effect.catchTag("Invalid", () => Effect.succeed(null)),
  );
  if (key === null) return null;

  return { signer: yield* fingerprint(key), delegation };
});

// -- native envelopes ---------------------------------------------------------------

export const signEnvelope = Effect.fn("Auth.signEnvelope")(function* (
  key: PrivateKey,
  envelope: Envelope,
) {
  const bytes = encodeEnvelope(envelope);
  const signature = yield* sign(key, bytes, NAMESPACE);
  return `${NATIVE_SCHEME}${toBase64Url(bytes)}.${toBase64Url(encoder.encode(signature))}`;
});

export interface Proved {
  readonly signer: Fingerprint;
  readonly envelope: Envelope;
}

/**
 * Check an envelope: signature, repository, operation, freshness and nonce.
 *
 * The nonce is consumed here, which is why this cannot be a pure function —
 * and why a valid envelope presented twice fails the second time.
 */
export const openEnvelope = Effect.fn("Auth.openEnvelope")(function* (input: {
  readonly payload: string;
  readonly signature: string;
  readonly repo: RepoId;
  readonly operation: string;
  readonly now: Date;
}) {
  const bytes = fromBase64Url(input.payload);
  const armored = fromBase64Url(input.signature);
  if (bytes === null || armored === null) return null;

  const json = yield* Effect.try({
    try: () => JSON.parse(decoder.decode(bytes)),
    catch: () => new Invalid({ field: "credential", reason: "not JSON" }),
  }).pipe(Effect.orElseSucceed(() => null));
  if (json === null) return null;

  const envelope = yield* decodeEnvelope(json).pipe(Effect.orElseSucceed(() => null));
  if (envelope === null) return null;

  if (envelope.repo !== input.repo) return null;
  // Binding the operation is what stops a signature for `git-upload-pack`
  // being presented at `git-receive-pack`.
  if (envelope.operation !== input.operation) return null;

  const expiry = Date.parse(envelope.expiresAt);
  if (Number.isNaN(expiry) || expiry <= input.now.getTime()) return null;

  const key = yield* verify(decoder.decode(armored), bytes, NAMESPACE).pipe(
    Effect.catchTag("Invalid", () => Effect.succeed(null)),
  );
  if (key === null) return null;

  const nonces = yield* Nonces;
  if (!(yield* nonces.consume(envelope.nonce))) return null;

  return { signer: yield* fingerprint(key), envelope };
});

// -- the guard ------------------------------------------------------------------------

export interface Authenticated {
  /** `null` when the request is anonymous, which only a public repository allows. */
  readonly principal: Member | null;
  readonly signer: Fingerprint | null;
  /** What this request may do — narrowed by a delegated credential's scope. */
  readonly capabilities: ReadonlyArray<string>;
  readonly projection: Projection;
}

export type Outcome =
  | { readonly ok: true; readonly authenticated: Authenticated }
  | { readonly ok: false; readonly status: 401 | 403; readonly reason: string };

const challenge = (nonce: string) => ({
  "www-authenticate": `Basic realm="git", Hub-SSH-v1 realm="git", nonce="${nonce}"`,
});

/**
 * Whether this repository lets anonymous readers in.
 *
 * A repository with no genesis is not hub-enabled and is left exactly as it
 * was: open. Turning every existing repository private the moment this module
 * shipped would be a migration nobody asked for.
 */
export const isPublic = (projection: Projection | null): boolean => projection === null;

/**
 * Identify the requester and decide whether they may proceed.
 *
 * Returns the principal rather than a bare yes: the policy boundary needs to
 * know *who*, and recovering that with a second lookup is how the two come
 * apart.
 */
export const authenticate = Effect.fn("Auth.authenticate")(function* (input: {
  readonly request: Request;
  readonly capability: string;
  readonly now?: Date;
}) {
  const now = input.now ?? new Date();

  const stored = yield* readGenesis().pipe(Effect.orElseSucceed(() => null));
  if (stored === null) {
    // Not hub-enabled: no identity, no membership, nothing to check against.
    return {
      ok: true,
      authenticated: { principal: null, signer: null, capabilities: [], projection: EMPTY },
    } as const;
  }

  const projection = yield* project(stored.genesis);
  const presented = credentialOf(input.request);
  const nonces = yield* Nonces;

  if (presented.kind === "none") {
    // Anonymous reads are a repository policy question; anonymous writes never
    // are. Either way the answer carries a fresh nonce, so a native client
    // learns how to sign its retry from the rejection itself.
    return input.capability === "repo.read" && anonymousReadAllowed(projection)
      ? ({
          ok: true,
          authenticated: { principal: null, signer: null, capabilities: ["repo.read"], projection },
        } as const)
      : ({
          ok: false,
          status: 401,
          reason: "authentication required",
          nonce: yield* nonces.issue(300),
        } as const);
  }

  const identified =
    presented.kind === "delegated"
      ? yield* openDelegation(presented.credential, projection.repoId, now)
      : yield* openEnvelope({
          payload: presented.payload,
          signature: presented.signature,
          repo: projection.repoId,
          operation: operationOf(input.request),
          now,
        });

  if (identified === null) {
    return {
      ok: false,
      status: 401,
      reason: "credential did not verify",
      nonce: yield* nonces.issue(300),
    } as const;
  }

  const authorized = yield* Verify.authorizeKey({
    projection,
    signer: identified.signer,
    capability: input.capability,
    at: now,
  });
  if (!authorized.ok) {
    return { ok: false, status: 403, reason: authorized.reason } as const;
  }

  // A delegated credential narrows: the holder gets the intersection of what
  // the issuer holds and what the credential claims, so a stolen credential
  // scoped to `repo.read` cannot push even if its issuer could.
  const scoped =
    "delegation" in identified
      ? identified.delegation.capabilities.filter((capability) =>
          permitsCapability(authorized.principal, capability),
        )
      : authorized.principal.capabilities;

  if ("delegation" in identified && !scoped.some((held) => held === input.capability)) {
    return {
      ok: false,
      status: 403,
      reason: `this credential is not scoped for ${input.capability}`,
    } as const;
  }

  return {
    ok: true,
    authenticated: {
      principal: authorized.principal,
      signer: identified.signer,
      capabilities: scoped,
      projection,
    },
  } as const;
});

/** The projection a repository with no genesis presents: nothing is known. */
const EMPTY: Projection = {
  // SAFETY: a repository with no genesis has no identity; this value is never
  // compared against a real one, because `authenticate` returns before any
  // membership check when the genesis is absent.
  repoId: "" as RepoId,
  head: null,
  members: new Map(),
  former: new Map(),
  revoked: new Map(),
  roots: [],
  threshold: 0,
  checkpoint: null,
  rejected: [],
};

/**
 * Who is making the request being handled.
 *
 * A service rather than an argument threaded through every handler: the guard
 * runs at the host's edge and the policy boundary runs deep inside a push, and
 * the two would otherwise have to agree about a parameter on everything in
 * between. Provided per request by whichever host authenticated it.
 */
export class Requester extends Context.Service<Requester, Authenticated>()("server/Requester") {}

/** Anonymous, for the paths that never authenticated anybody. */
export const anonymous: Authenticated = {
  principal: null,
  signer: null,
  capabilities: [],
  projection: EMPTY,
};

export const requester = (authenticated: Authenticated): Layer.Layer<Requester> =>
  Layer.succeed(Requester)(authenticated);

/**
 * The guard both HTTP surfaces call.
 *
 * Returns the refusal *or* who the requester turned out to be, because the
 * second is what the rest of the request needs: a push is judged against the
 * pusher, and re-deriving that later would mean authenticating twice — which,
 * with single-use nonces, means failing the second time.
 */
export const guard = Effect.fn("Auth.guard")(function* (request: Request) {
  const outcome = yield* authenticate({ request, capability: requiredCapability(request) });
  if (outcome.ok) return { denied: null, authenticated: outcome.authenticated };

  return {
    denied: new Response(outcome.reason, {
      status: outcome.status,
      headers: outcome.status === 401 && "nonce" in outcome ? challenge(outcome.nonce) : {},
    }),
    authenticated: anonymous,
  };
});

const operationOf = (request: Request): string => {
  const last = new URL(request.url).pathname.split("/").at(-1);
  return last === "git-receive-pack" || last === "git-upload-pack"
    ? last
    : `${request.method} ${new URL(request.url).pathname}`;
};

const permitsCapability = (member: Member, capability: string): boolean =>
  member.capabilities.includes(capability) || member.capabilities.includes("repo.admin");

/**
 * Whether anonymous readers may clone.
 *
 * A repository that has never granted `repo.read` to anybody is treated as
 * public, which is what an open-source project's repository is: the grants
 * exist to *restrict*, and a repository with no read grants has restricted
 * nothing.
 */
const anonymousReadAllowed = (projection: Projection): boolean => {
  for (const member of projection.members.values()) {
    if (member.capabilities.includes("repo.read")) return false;
  }
  return true;
};

export type AuthError = Invalid;
export type AuthRequirements = Repository | Nonces;
