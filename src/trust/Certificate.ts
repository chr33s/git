/**
 * What a trust record says: capabilities, and the four payloads that move
 * them around.
 *
 * Every payload carries the `RepoID` it was written for. That field is inside
 * the signed bytes, so a grant lifted out of one repository and replayed into
 * another does not verify there — which matters more than it looks, because
 * the alternative is that anyone who can read a public repository can harvest
 * its membership records.
 *
 * Capabilities rather than roles, for the reason the spec gives: a role is a
 * name for a set of capabilities, and names drift. `hub.check` is scoped by
 * check name because an unscoped one would make "required checks" only as
 * strong as the least trusted CI bot — any holder could sign a green `test`.
 */
import { Effect, Result, Schema } from "effect";

import {
  type Fingerprint,
  fingerprint,
  isFingerprint,
  parsePublicKey,
} from "../crypto/SshSignature.ts";
import { Invalid } from "../git/Error.ts";
import type { RepoId } from "./Genesis.ts";

/**
 * The capabilities this version knows, minus the scoped ones.
 *
 * A closed list rather than free strings: a typo in a grant would otherwise
 * be a capability that exists, is held by somebody, and authorizes nothing —
 * discovered when a merge is refused for a reason nobody can find.
 */
/**
 * How many signatures on one record are ever worth verifying.
 *
 * Every entry costs an Ed25519 verification, and the list is attacker-chosen:
 * anybody who may write a record can attach a hundred thousand well-formed
 * armors to it and multiply the cost of every later fold — which is on the
 * path of every protected-branch push. Far above any real quorum, so no honest
 * record is truncated; a record needing more than this is one nobody can
 * afford to check.
 */
export const MAX_SIGNATURES = 64;

export const CAPABILITIES = [
  "repo.read",
  "source.push",
  "source.force-push",
  "source.delete",
  "hub.create-pr",
  "hub.comment",
  "hub.review",
  "hub.approve",
  "hub.merge",
  "hub.redact",
  "hub.session",
  "hub.task",
  "member.invite",
  "member.revoke",
  "policy.write",
  "repo.admin",
] as const;

/** `hub.check:<name>` — one check name, or `*` for any. */
const CHECK_PREFIX = "hub.check:";

const known = new Set<string>(CAPABILITIES);

export const isCapability = (value: string): boolean => {
  if (known.has(value)) return true;
  if (!value.startsWith(CHECK_PREFIX)) return false;
  const name = value.slice(CHECK_PREFIX.length);
  return name.length > 0 && !name.includes(" ");
};

/**
 * Whether a set of held capabilities authorizes an operation.
 *
 * `repo.admin` implies everything. That is a real decision and not an
 * oversight: the spec's own authorization graph has a root quorum granting one
 * member `repo.admin` and that member then issuing grants, which only works if
 * admin carries `member.invite` — and once it carries one thing it did not
 * name, "which ones?" is a question with no principled answer. So: all of
 * them, said once, here.
 *
 * `hub.check:*` matches any check name; `hub.check:test` matches only `test`.
 */
export const permits = (held: ReadonlyArray<string>, required: string): boolean => {
  for (const capability of held) {
    if (capability === "repo.admin") return true;
    if (capability === required) return true;
    if (
      capability === `${CHECK_PREFIX}*` &&
      required.startsWith(CHECK_PREFIX) &&
      required.length > CHECK_PREFIX.length
    ) {
      return true;
    }
  }
  return false;
};

/** The capability a check event of this name requires of its signer. */
export const checkCapability = (name: string): string => `${CHECK_PREFIX}${name}`;

// -- payloads ------------------------------------------------------------------

const Timestamp = Schema.String;

/**
 * Fields every trust payload carries.
 *
 * `id` is a UUIDv7 so that two grants with otherwise identical contents are
 * distinct records; without it, re-granting exactly what somebody already has
 * would content-address to the record that is already there and silently do
 * nothing.
 */
const envelope = {
  version: Schema.Literal(1),
  repo: Schema.String,
  id: Schema.String,
  issuedAt: Timestamp,
};

export const Grant = Schema.Struct({
  type: Schema.tag("trust.grant"),
  ...envelope,
  /** The subject key's fingerprint: what every later record names it by. */
  subject: Schema.String,
  /** The `authorized_keys` line, so a verifier needs nothing else to check it. */
  publicKey: Schema.String,
  capabilities: Schema.Array(Schema.String),
  /** `null` never expires; collaborative repositories should set one. */
  expiresAt: Schema.NullOr(Timestamp),
});

/**
 * Why a key stopped being trusted — and, for one value, since when.
 *
 * `compromised` is the retroactive class: it invalidates what the key signed
 * before anyone noticed, because the premise of a compromise is that the
 * signatures were never the subject's in the first place. Every other reason
 * is forward-only, so a member who leaves does not take their review history
 * with them.
 */
export const RevocationReason = Schema.Literals(["rotated", "left", "compromised", "superseded"]);

export const Revoke = Schema.Struct({
  type: Schema.tag("trust.revoke"),
  ...envelope,
  subject: Schema.String,
  reason: RevocationReason,
  /**
   * When the compromise is believed to have started.
   *
   * `null` with `reason: "compromised"` means "assume from the grant" — the
   * safe reading, because a compromise of unknown age is not a compromise of
   * no age.
   */
  compromisedAt: Schema.NullOr(Timestamp),
});

export const RootChange = Schema.Struct({
  type: Schema.tag("trust.root-change"),
  ...envelope,
  rootKeys: Schema.Array(Schema.String),
  threshold: Schema.Int,
});

export const Checkpoint = Schema.Struct({
  type: Schema.tag("trust.checkpoint"),
  ...envelope,
  /** The log commits this checkpoint attests to having seen. */
  frontier: Schema.Array(Schema.String),
});

export const TrustPayload = Schema.Union([Grant, Revoke, RootChange, Checkpoint]).pipe(
  Schema.toTaggedUnion("type"),
);

export type Grant = (typeof Grant)["Type"];
export type Revoke = (typeof Revoke)["Type"];
export type RootChange = (typeof RootChange)["Type"];
export type Checkpoint = (typeof Checkpoint)["Type"];
export type TrustPayload = (typeof TrustPayload)["Type"];

const decodePayload = Schema.decodeUnknownEffect(TrustPayload);

const decoder = new TextDecoder();
const encoder = new TextEncoder();

/**
 * Payload bytes to a checked payload.
 *
 * The bytes are the argument rather than the decoded object because they are
 * what signatures cover: a caller that decodes first and verifies later can
 * verify a re-encoding of what it read, which is a different thing.
 */
export const decode = Effect.fn("Certificate.decode")(function* (bytes: Uint8Array) {
  const json = yield* Effect.try({
    try: () => JSON.parse(decoder.decode(bytes)),
    catch: () => new Invalid({ field: "payload", reason: "trust payload is not valid JSON" }),
  });

  return yield* decodePayload(json).pipe(
    Effect.mapError(
      (issue) =>
        new Invalid({ field: "payload", reason: `malformed trust payload: ${issue.message}` }),
    ),
  );
});

/**
 * The canonical bytes for a payload.
 *
 * Field order is spelled out per variant for the reason the genesis spells its
 * out: the canonical form is what signatures are over, and it should be
 * readable in one place rather than inferred from a construction order.
 */
export const encode = (payload: TrustPayload): Uint8Array => {
  const common = {
    version: payload.version,
    type: payload.type,
    repo: payload.repo,
    id: payload.id,
    issuedAt: payload.issuedAt,
  };

  const ordered =
    payload.type === "trust.grant"
      ? {
          ...common,
          subject: payload.subject,
          publicKey: payload.publicKey,
          capabilities: payload.capabilities,
          expiresAt: payload.expiresAt,
        }
      : payload.type === "trust.revoke"
        ? {
            ...common,
            subject: payload.subject,
            reason: payload.reason,
            compromisedAt: payload.compromisedAt,
          }
        : payload.type === "trust.root-change"
          ? { ...common, rootKeys: payload.rootKeys, threshold: payload.threshold }
          : { ...common, frontier: payload.frontier };

  return encoder.encode(`${JSON.stringify(ordered, null, 2)}\n`);
};

// -- constructors --------------------------------------------------------------

/**
 * Build a grant from the subject's public key line.
 *
 * The fingerprint is computed here rather than taken from the caller: it is
 * derived data, and a caller that could supply it could supply one that names
 * a different key than the one in the record — which `validate` would then
 * refuse, having wasted everyone's time.
 */
export const grant = Effect.fn("Certificate.grant")(function* (input: {
  readonly repo: RepoId;
  readonly publicKey: string;
  readonly capabilities: ReadonlyArray<string>;
  readonly expiresAt?: Date | null;
  readonly id: string;
  readonly at?: Date;
}) {
  const key = parsePublicKey(input.publicKey);
  if (Result.isFailure(key)) {
    return yield* new Invalid({ field: "publicKey", reason: key.failure.reason });
  }

  // Here as well as in `validate`, and the difference is where the answer
  // lands. `validate` runs during the fold, which is *after* the record has
  // been written — and the trust log is append-only, so a capability somebody
  // typed wrong was pinned on a ref nothing can rewind, rejected for ever and
  // re-read on every membership check. A payload that can never be valid is
  // one this refuses to build.
  for (const capability of input.capabilities) {
    if (!isCapability(capability)) {
      return yield* new Invalid({
        field: "capabilities",
        reason: `unknown capability '${capability}'`,
      });
    }
  }

  return {
    type: "trust.grant",
    version: 1,
    repo: input.repo,
    id: input.id,
    issuedAt: (input.at ?? new Date()).toISOString(),
    subject: yield* fingerprint(key.success),
    publicKey: input.publicKey,
    capabilities: input.capabilities,
    expiresAt:
      input.expiresAt === undefined || input.expiresAt === null
        ? null
        : input.expiresAt.toISOString(),
  } satisfies Grant;
});

export const revoke = (input: {
  readonly repo: RepoId;
  readonly subject: Fingerprint;
  readonly reason: Revoke["reason"];
  readonly compromisedAt?: Date | null;
  readonly id: string;
  readonly at?: Date;
}): Revoke => ({
  type: "trust.revoke",
  version: 1,
  repo: input.repo,
  id: input.id,
  issuedAt: (input.at ?? new Date()).toISOString(),
  subject: input.subject,
  reason: input.reason,
  compromisedAt:
    input.compromisedAt === undefined || input.compromisedAt === null
      ? null
      : input.compromisedAt.toISOString(),
});

export const rootChange = (input: {
  readonly repo: RepoId;
  readonly rootKeys: ReadonlyArray<string>;
  readonly threshold: number;
  readonly id: string;
  readonly at?: Date;
}): RootChange => ({
  type: "trust.root-change",
  version: 1,
  repo: input.repo,
  id: input.id,
  issuedAt: (input.at ?? new Date()).toISOString(),
  rootKeys: input.rootKeys,
  threshold: input.threshold,
});

export const checkpoint = (input: {
  readonly repo: RepoId;
  readonly frontier: ReadonlyArray<string>;
  readonly id: string;
  readonly at?: Date;
}): Checkpoint => ({
  type: "trust.checkpoint",
  version: 1,
  repo: input.repo,
  id: input.id,
  issuedAt: (input.at ?? new Date()).toISOString(),
  frontier: input.frontier,
});

/**
 * Structural checks, before a payload is treated as authority.
 *
 * Separate from schema decoding because these are not shape questions: that
 * the subject fingerprint is the fingerprint of the key beside it is a claim
 * the record makes about itself, and a record whose two halves disagree is one
 * a verifier must not accept — it would authorize a key nobody named.
 */
export const validate = Effect.fn("Certificate.validate")(function* (
  payload: TrustPayload,
  repo: RepoId,
) {
  if (payload.repo !== repo) {
    return yield* new Invalid({
      field: "repo",
      reason: `record is for ${payload.repo}, not ${repo}`,
    });
  }
  if (Number.isNaN(Date.parse(payload.issuedAt))) {
    return yield* new Invalid({ field: "issuedAt", reason: `not a date: '${payload.issuedAt}'` });
  }

  if (payload.type === "trust.grant") {
    const key = parsePublicKey(payload.publicKey);
    if (Result.isFailure(key)) {
      return yield* new Invalid({ field: "publicKey", reason: key.failure.reason });
    }
    const print = yield* fingerprint(key.success);
    if (print !== payload.subject) {
      return yield* new Invalid({
        field: "subject",
        reason: `subject ${payload.subject} is not the fingerprint of the key given (${print})`,
      });
    }
    for (const capability of payload.capabilities) {
      if (!isCapability(capability)) {
        return yield* new Invalid({
          field: "capabilities",
          reason: `unknown capability '${capability}'`,
        });
      }
    }
    if (payload.expiresAt !== null && Number.isNaN(Date.parse(payload.expiresAt))) {
      return yield* new Invalid({
        field: "expiresAt",
        reason: `not a date: '${payload.expiresAt}'`,
      });
    }
    return;
  }

  if (payload.type === "trust.revoke") {
    if (!isFingerprint(payload.subject)) {
      return yield* new Invalid({
        field: "subject",
        reason: `not a fingerprint: '${payload.subject}'`,
      });
    }
    if (payload.compromisedAt !== null && Number.isNaN(Date.parse(payload.compromisedAt))) {
      return yield* new Invalid({
        field: "compromisedAt",
        reason: `not a date: '${payload.compromisedAt}'`,
      });
    }
    return;
  }

  if (payload.type === "trust.root-change") {
    if (payload.rootKeys.length === 0) {
      return yield* new Invalid({ field: "rootKeys", reason: "a repository needs a root key" });
    }
    if (payload.threshold < 1 || payload.threshold > payload.rootKeys.length) {
      return yield* new Invalid({
        field: "threshold",
        reason: `threshold ${payload.threshold} cannot be met by ${payload.rootKeys.length} keys`,
      });
    }
    const seen = new Set<Fingerprint>();
    for (const line of payload.rootKeys) {
      const key = parsePublicKey(line);
      if (Result.isFailure(key)) {
        return yield* new Invalid({ field: "rootKeys", reason: key.failure.reason });
      }
      const print = yield* fingerprint(key.success);
      if (seen.has(print)) {
        return yield* new Invalid({ field: "rootKeys", reason: `duplicate root key ${print}` });
      }
      seen.add(print);
    }
  }
});
