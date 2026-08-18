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
import { Context, Effect, Layer, Option, Schema } from "effect";

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
import type { Oid } from "../git/Store.ts";
import { type Genesis, readGenesis, type RepoId } from "../trust/Genesis.ts";
import { LOG_REF } from "../trust/Log.ts";
import { type Member, project, type Projection } from "../trust/Projection.ts";
import { permits } from "../trust/Certificate.ts";
import * as Verify from "../trust/Verify.ts";

const encoder = new TextEncoder();
const decoder = new TextDecoder();

// -- what an operation costs ---------------------------------------------------

/**
 * The capability a request needs before anything else is checked.
 *
 * Receive-pack is charged a write from the advertisement on, because a client
 * that cannot write has no business learning the ref layout through the push
 * endpoint. Which write it is — whether a *particular* push also needs
 * `source.force-push`, or is a deletion needing `source.delete` — is a question
 * about its commands, and it belongs to the policy boundary, which can see
 * them. So the guard asks for `source.push` *or* `source.delete`: charging
 * `source.push` alone made `source.delete` unusable as a standalone
 * capability, refusing at the door the very holder the policy boundary was
 * written to admit.
 */
const WRITE = ["source.push", "source.delete"];

export const requiredCapability = (request: Request): ReadonlyArray<string> => {
  const url = new URL(request.url);
  const last = url.pathname.split("/").at(-1);

  if (last === "git-receive-pack") return WRITE;
  if (last === "refs" && url.searchParams.get("service") === "git-receive-pack") {
    return WRITE;
  }
  if (last === "git-upload-pack") return READ;
  // The LFS batch endpoint negotiates downloads as well as uploads, so method
  // alone would lock a reader out of cloning any repository that uses LFS. The
  // upload it hands back is a separate PUT, and that one is charged as a write.
  if (last === "batch" && url.pathname.includes("/info/lfs/")) return READ;
  // POST is not the same thing as "writes". These take a body because their
  // inputs do not fit in a URL, and they change nothing — charging them a
  // write locks a reader out of `diff` and `grep`, and makes an otherwise
  // public repository refuse them anonymously.
  //
  // The method is checked as well as the name, and that is not belt-and-braces:
  // a URL's last segment is also a *resource name*, so `DELETE /remotes/log`
  // and `DELETE /webhooks/log` end in one of these words while being deletions.
  // Matching on the word alone charged them `repo.read`, and neither endpoint
  // has a policy gate behind it to catch what got through.
  if (request.method === "POST" && last !== undefined && READ_ONLY_POSTS.has(last)) {
    return READ;
  }
  return request.method === "GET" || request.method === "HEAD" ? READ : WRITE;
};

const READ = ["repo.read"];

/** Whether a request asked for nothing more than to read. */
const readOnly = (capabilities: ReadonlyArray<string>): boolean =>
  capabilities.length === 1 && capabilities[0] === "repo.read";

/**
 * POST endpoints that read and never write.
 *
 * `history` and `log` are GET endpoints and so are not here: the last line
 * already charges a GET `repo.read`, and naming them would only widen the set
 * of words a future DELETE route could collide with.
 */
const READ_ONLY_POSTS = new Set(["diff", "grep", "bisect", "fsck"]);

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

/**
 * One nonce store, as a value.
 *
 * `Layer.sync` would be a *description* of how to build one, and every
 * `Effect.provide` would run it again — a fresh `Map` per request, so the
 * nonce a challenge issued is unknown by the time the signed retry arrives and
 * native authentication can never succeed. The store has to outlive the
 * provide, so it is constructed here and handed over by `Layer.succeed`.
 */
export const nonceStore = (): Nonces["Service"] => {
  /**
   * Nonces that have been *spent*, until they would have expired anyway.
   *
   * Issuance writes nothing. Every 401 issues a nonce, and 401s are what
   * unauthenticated traffic produces — so a store that remembered every issued
   * one could be turned over at will by noise, evicting the challenges honest
   * clients were about to answer and starving native authentication host-wide.
   * A nonce carries its own expiry and a tag only this store can make, so
   * "did I issue this, and is it still good?" needs no memory; only "has it
   * been used already?" does, and an entry lands here only after a signature
   * has already been verified against it.
   */
  const spent = new Map<string, number>();

  /**
   * A ceiling on the spent set.
   *
   * Reached only by genuinely authenticated requests, which is what makes it
   * safe to evict the oldest: an attacker who can fill this can already
   * authenticate, and the cost of eviction is one client's retry.
   */
  const CAPACITY = 4096;

  /**
   * The tag that makes a nonce self-certifying.
   *
   * Random per store and never leaves it, so a nonce cannot be forged; and
   * because it is per store, a restart invalidates outstanding challenges,
   * which is the retry the service already documents.
   */
  const secret = crypto.getRandomValues(new Uint8Array(32));
  // Imported on first use, not here. A promise created at construction with no
  // handler until the first challenge is an unhandled rejection if the import
  // fails — which takes the host down at start-up, before it has served
  // anything, for a key it may never need.
  let key: Promise<CryptoKey> | null = null;

  const tag = async (body: string): Promise<string> => {
    key ??= crypto.subtle.importKey(
      "raw",
      secret.slice().buffer,
      { name: "HMAC", hash: "SHA-256" },
      false,
      ["sign"],
    );
    const mac = await crypto.subtle.sign("HMAC", await key, encoder.encode(body));
    return [...new Uint8Array(mac)].map((byte) => byte.toString(16).padStart(2, "0")).join("");
  };

  const prune = (now: number) => {
    for (const [nonce, expiry] of spent) if (expiry <= now) spent.delete(nonce);
    // Insertion-ordered, so the front of the map is the oldest.
    while (spent.size > CAPACITY) {
      const oldest = spent.keys().next();
      if (oldest.done === true) break;
      spent.delete(oldest.value);
    }
  };

  return Nonces.of({
    issue: (ttlSeconds) =>
      Effect.promise(async () => {
        const body = `${Date.now() + ttlSeconds * 1000}.${crypto.randomUUID()}`;
        return `${body}.${await tag(body)}`;
      }),
    consume: (nonce) =>
      Effect.promise(async () => {
        const cut = nonce.lastIndexOf(".");
        if (cut <= 0) return false;
        const body = nonce.slice(0, cut);
        // Constant-time is not the property that matters here: forging the tag
        // needs the secret, and a wrong one is rejected whatever the timing
        // tells the caller about how wrong it was.
        if (nonce.slice(cut + 1) !== (await tag(body))) return false;

        const expiry = Number(body.slice(0, body.indexOf(".")));
        const now = Date.now();
        if (!Number.isFinite(expiry) || expiry <= now) return false;

        prune(now);
        if (spent.has(nonce)) return false;
        spent.set(nonce, expiry);
        return true;
      }),
  });
};

/**
 * A store and the layer carrying it.
 *
 * A function returning a Layer, which normally is redundant — a Layer is
 * already lazy — and here is the point: each call makes a *distinct* store, so
 * a host gets one for its lifetime rather than sharing one with every other
 * host in the isolate. `Layer.succeed` over a value built here is what makes
 * the store outlive the provide; `Layer.sync` would rebuild it per request and
 * no challenge could ever be answered.
 */
export const noncesInMemory = (): Layer.Layer<Nonces> => Layer.succeed(Nonces)(nonceStore());

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

/**
 * How far apart two machines' clocks may be before this refuses to care.
 *
 * Only ever widens the lifetime *ceiling*, never the expiry itself: an expired
 * credential is expired.
 */
const CLOCK_SKEW_SECONDS = 300;

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
  // Enforced on the verifying side as well as the minting side. The holder
  // signs these themselves, so a cap only this server's `mintDelegation`
  // applied would be a cap anybody could opt out of by writing their own.
  //
  // The skew allowance is what stops a credential minted at exactly the
  // documented maximum from being refused by a server whose clock is a second
  // behind the minter's — a rejection nobody could diagnose from either side.
  if (expiry - now.getTime() > (MAX_DELEGATION_SECONDS + CLOCK_SKEW_SECONDS) * 1000) return null;

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
 * Check an envelope: signature, repository, operation and freshness.
 *
 * The nonce is *not* spent here. Spending it is what enforces single use, and
 * the store that records a spent one is bounded — so spending on a signature
 * alone let anybody with a throwaway key turn that store over and evict the
 * record of a genuine spend, re-opening the replay window it exists to close.
 * A key this repository never granted can now burn nothing: the caller spends
 * the nonce once membership has been established.
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
  /**
   * The envelope a native client signed, when it signed one.
   *
   * Kept because the commands inside it are the point: the guard cannot check
   * them — the push body has not been read yet — so the promise that a
   * signature covers *these refs moving to these oids* is only kept if the
   * policy boundary holds the push to them later. `null` for a delegated
   * credential, which makes no claim about particular refs.
   */
  readonly envelope: Envelope | null;
}

export type Outcome =
  | { readonly ok: true; readonly authenticated: Authenticated }
  | { readonly ok: false; readonly status: 401 | 403; readonly reason: string };

const challenge = (nonce: string) => ({
  "www-authenticate": `Basic realm="git", Hub-SSH-v1 realm="git", nonce="${nonce}"`,
});

/**
 * Identify the requester and decide whether they may proceed.
 *
 * Returns the principal rather than a bare yes: the policy boundary needs to
 * know *who*, and recovering that with a second lookup is how the two come
 * apart.
 */
export const authenticate = Effect.fn("Auth.authenticate")(function* (input: {
  readonly request: Request;
  /**
   * What the request needs — any *one* of these, not all of them.
   *
   * A list because a push is charged `source.push` or `source.delete` and the
   * guard cannot see which it is; the policy boundary, which reads the
   * commands, makes the precise charge.
   */
  readonly capability: ReadonlyArray<string>;
  readonly now?: Date;
}) {
  const now = input.now ?? new Date();

  // Not swallowed: `readGenesis` already answers `null` for a repository that
  // has none, so a *failure* here means the repository's identity could not be
  // read at all. Treating that as "no members" would open a private repository
  // to anybody the moment its storage hiccuped.
  const stored = yield* readGenesis().pipe(
    Effect.catchTag("Invalid", (error) => Effect.fail(error)),
  );
  if (stored === null) {
    // Not hub-enabled: no identity, and so no membership to grant anything.
    // Reads are served as a plain git repository's always have been; writes
    // are not, unless the host has said otherwise — and this is the one place
    // that covers *every* write, not only the ones that move a ref. Checked at
    // the ref boundary alone, it left webhook and remote registration, the
    // remote-push verb and LFS uploads reachable by anybody.
    const open = yield* Effect.serviceOption(AnonymousWrites);
    if (!readOnly(input.capability) && !Option.getOrElse(open, () => false)) {
      return {
        ok: false,
        status: 403,
        reason:
          "this repository has no membership to authorize a write; run `hub init` to give it one",
      } as const;
    }
    return {
      ok: true,
      authenticated: {
        principal: null,
        signer: null,
        capabilities: input.capability,
        projection: EMPTY,
        envelope: null,
      },
    } as const;
  }

  const projection = yield* folded(stored.genesis);
  const presented = credentialOf(input.request);
  const nonces = yield* Nonces;

  if (presented.kind === "none") {
    // Anonymous reads are a repository policy question; anonymous writes never
    // are. Either way the answer carries a fresh nonce, so a native client
    // learns how to sign its retry from the rejection itself.
    return readOnly(input.capability) && anonymousReadAllowed(projection)
      ? ({
          ok: true,
          authenticated: {
            principal: null,
            signer: null,
            capabilities: ["repo.read"],
            projection,
            envelope: null,
          },
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
    // A repository anonymous readers may clone stays readable even when the
    // credential presented is nonsense: `git` sends whatever a credential
    // helper has for the host, and refusing here would break clones for
    // people whose only mistake was having an unrelated entry.
    return readOnly(input.capability) && anonymousReadAllowed(projection)
      ? ({
          ok: true,
          authenticated: {
            principal: null,
            signer: null,
            capabilities: ["repo.read"],
            projection,
            envelope: null,
          },
        } as const)
      : ({
          ok: false,
          status: 401,
          reason: "credential did not verify",
          nonce: yield* nonces.issue(300),
        } as const);
  }

  // Any one of them: a holder of `source.delete` and a holder of
  // `source.push` are both entitled to reach receive-pack, and which of the
  // two a given command needs is settled behind the guard.
  let authorized = yield* Verify.authorizeKey({
    projection,
    signer: identified.signer,
    capability: input.capability[0] ?? "repo.read",
    at: now,
  });
  for (const capability of input.capability.slice(1)) {
    if (authorized.ok) break;
    authorized = yield* Verify.authorizeKey({
      projection,
      signer: identified.signer,
      capability,
      at: now,
    });
  }
  if (!authorized.ok) {
    // A repository anonymous readers may clone must not become *less*
    // readable because a credential was presented: `git` sends one on every
    // request once it has any, and refusing here would break a clone that
    // works without it.
    if (readOnly(input.capability) && anonymousReadAllowed(projection)) {
      return {
        ok: true,
        authenticated: {
          principal: null,
          signer: identified.signer,
          capabilities: ["repo.read"],
          projection,
          envelope: null,
        },
      } as const;
    }
    return { ok: false, status: 403, reason: authorized.reason } as const;
  }

  // Spent here, and not where the envelope was opened: single use is enforced
  // by a bounded store, and a store an unauthenticated caller can fill is one
  // whose oldest entries fall out — taking with them the record that a genuine
  // nonce had been used, inside its own lifetime. Behind the membership check
  // the only keys that can fill it are keys this repository granted.
  if ("envelope" in identified) {
    const spent = yield* nonces.consume(identified.envelope.nonce);
    if (!spent) {
      return {
        ok: false,
        status: 401,
        reason: "this request has already been made",
        nonce: yield* nonces.issue(300),
      } as const;
    }
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

  // `permits`, not an exact match: a credential scoped `repo.admin` covers
  // `repo.read`, and `hub.check:*` covers `hub.check:test`.
  if (
    "delegation" in identified &&
    !input.capability.some((capability) => permits(scoped, capability))
  ) {
    return {
      ok: false,
      status: 403,
      reason: `this credential is not scoped for ${input.capability.join(" or ")}`,
    } as const;
  }

  return {
    ok: true,
    authenticated: {
      principal: authorized.principal,
      signer: identified.signer,
      capabilities: scoped,
      projection,
      envelope: "envelope" in identified ? identified.envelope : null,
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
  checkpoints: [],
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

/**
 * The trust fold, remembered by the log head it was folded from.
 *
 * Every request runs it, and it is an Ed25519 verification per signature per
 * record — so an anonymous `GET /info/refs` loop is an amplifier without this,
 * paying nothing to make the server pay for a full fold each time. The result
 * is a pure function of the log head, which the memo is keyed by, so a stale
 * answer is not possible: the moment anything appends, the key changes.
 *
 * One entry per *repository*, replaced when its log moves — not one per state.
 * Keyed by the state, a repository that appends often (hourly checkpoints are
 * enough) filled the whole map with its own history and evicted every other
 * repository the host serves, reinstating the per-request fold this exists to
 * prevent. Nothing ever wants two folds of one repository at two heads, so a
 * moved head replaces rather than adds, and the bound counts repositories.
 *
 * Least-recently-used, not first-in: a host with more repositories than the
 * bound serves them in some order, and evicting by insertion evicts whichever
 * happens to be oldest rather than whichever is idle.
 */
const FOLDS = 256;
const folds = new Map<RepoId, { readonly head: Oid | null; readonly projection: Projection }>();

const folded = Effect.fn("Auth.folded")(function* (genesis: Genesis) {
  const repository = yield* Repository;
  const head = yield* repository.resolve(LOG_REF);

  const known = folds.get(genesis.repoId);
  if (known !== undefined && known.head === head) {
    // Re-inserted so iteration order is least-recently-used first.
    folds.delete(genesis.repoId);
    folds.set(genesis.repoId, known);
    return known.projection;
  }

  const projection = yield* project(genesis);
  folds.delete(genesis.repoId);
  folds.set(genesis.repoId, { head, projection });
  while (folds.size > FOLDS) {
    const oldest = folds.keys().next();
    if (oldest.done === true) break;
    folds.delete(oldest.value);
  }
  return projection;
});

/**
 * Whether this host serves writes to repositories that have no identity.
 *
 * §14 is unconditional that anonymous does not get `source.push`, and a
 * repository with no genesis has no membership to grant it — so the default is
 * to refuse, which is also what every stock git host does with an unconfigured
 * bare repository. It is a *host* decision rather than a repository one for the
 * only reason a host ever gets to decide anything here: a repository with no
 * identity has no way to state a policy of its own.
 *
 * It lives beside the guard rather than beside the policy boundary because the
 * guard is what every surface passes through — smart-HTTP, the JSON verbs, LFS
 * uploads and webhook and remote registration alike. Checked only at the ref
 * boundary, it left every write that does not move a ref wide open.
 */
export class AnonymousWrites extends Context.Service<AnonymousWrites, boolean>()(
  "server/AnonymousWrites",
) {}

export const anonymousWrites = (allowed: boolean): Layer.Layer<AnonymousWrites> =>
  Layer.succeed(AnonymousWrites)(allowed);

/** Anonymous, for the paths that never authenticated anybody. */
export const anonymous: Authenticated = {
  principal: null,
  signer: null,
  capabilities: [],
  projection: EMPTY,
  envelope: null,
};

export const requester = (authenticated: Authenticated): Layer.Layer<Requester> =>
  Layer.succeed(Requester)(authenticated);

/**
 * The same value as a per-request context rather than a layer.
 *
 * A layer has to be part of the graph a router is built from, which means one
 * router per request — and the router carries the whole API's handler tree and
 * a `Scope` nobody closes. `HttpRouter.toWebHandler` takes a context per call
 * for exactly this: the router is built once, and who is asking arrives with
 * the request instead of with the graph.
 */
export const requesterContext = (authenticated: Authenticated): Context.Context<Requester> =>
  Context.make(Requester, authenticated);

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
  permits(member.capabilities, capability);

/**
 * Whether anonymous readers may clone.
 *
 * A repository that has never granted `repo.read` to anybody is treated as
 * public, which is what an open-source project's repository is: the grants
 * exist to *restrict*, and a repository with no read grants has restricted
 * nothing.
 */
export const anonymousReadAllowed = (projection: Projection): boolean => {
  // `former` as well as `members`: revocation moves a member out of the one
  // and into the other, so looking only at current members would make a
  // private repository world-readable the moment its last reader was revoked
  // — the exact opposite of what revoking them was for.
  //
  // `permits`, not `includes`: `repo.admin` carries `repo.read`, and a
  // repository whose members are all admins had restricted reading just as
  // surely as one that granted `repo.read` by name.
  for (const member of projection.members.values()) {
    if (permitsCapability(member, "repo.read")) return false;
  }
  for (const member of projection.former.values()) {
    if (permitsCapability(member, "repo.read")) return false;
  }
  return true;
};

export type AuthError = Invalid;
export type AuthRequirements = Repository | Nonces;
