/** Signed statements carried by a principal's append-only social log. */
import { Effect, Result, Schema } from "effect";

import { parsePublicKey } from "../crypto/SshSignature.ts";
import { Invalid } from "../git/Error.ts";
import { isOid } from "../git/Store.ts";
import { isRepoId, type RepoId } from "../trust/Genesis.ts";
import {
  isPrincipalId,
  principalOf,
  principalSubject,
  type PrincipalId,
} from "../trust/Principal.ts";

const encoder = new TextEncoder();
const decoder = new TextDecoder();

export const SOCIAL_SCOPES = ["introduce.repo", "introduce.key", "review", "vouch"] as const;
export type SocialScope = (typeof SOCIAL_SCOPES)[number];
const socialScopes = new Set<string>(SOCIAL_SCOPES);

const envelope = {
  version: Schema.Literal(1),
  author: Schema.String,
  id: Schema.String,
  issuedAt: Schema.String,
  socialHead: Schema.NullOr(Schema.String),
  trustHead: Schema.NullOr(Schema.String),
};

export const AttestRepo = Schema.Struct({
  type: Schema.tag("social.attest.repo"),
  ...envelope,
  repo: Schema.String,
  urls: Schema.Array(Schema.String),
  role: Schema.Literals(["origin", "mirror", "fork"]),
  forkOf: Schema.NullOr(Schema.String),
  lineage: Schema.NullOr(Schema.String),
  inbox: Schema.NullOr(Schema.String),
});

/**
 * One schema for both claim kinds keeps `type` globally unique in the tagged
 * union. `validate` is where the mutually-exclusive fields are enforced.
 */
export const AttestPrincipal = Schema.Struct({
  type: Schema.tag("social.attest.principal"),
  ...envelope,
  subject: Schema.String,
  claim: Schema.Literals(["key-of", "external-identity"]),
  publicKey: Schema.optional(Schema.String),
  identity: Schema.optional(Schema.String),
  proof: Schema.optional(Schema.String),
});

export const MirrorLocation = Schema.Struct({
  url: Schema.String,
  mode: Schema.Literals(["read", "write"]),
});

export const Mirrors = Schema.Struct({
  type: Schema.tag("social.mirrors"),
  ...envelope,
  repo: Schema.String,
  urls: Schema.Array(MirrorLocation),
});

export const Vouch = Schema.Struct({
  type: Schema.tag("social.vouch"),
  ...envelope,
  subject: Schema.String,
  scope: Schema.Array(Schema.String),
  depth: Schema.Int,
  expiresAt: Schema.NullOr(Schema.String),
});

export const Follow = Schema.Struct({
  type: Schema.tag("social.follow"),
  ...envelope,
  subject: Schema.String,
  petname: Schema.String,
});

export const Label = Schema.Struct({
  type: Schema.tag("social.label"),
  ...envelope,
  subject: Schema.String,
  namespace: Schema.String,
  label: Schema.String,
});

export const Revoke = Schema.Struct({
  type: Schema.tag("social.revoke"),
  ...envelope,
  target: Schema.String,
  reason: Schema.Literals(["withdrawn", "superseded", "compromised"]),
  compromisedAt: Schema.NullOr(Schema.String),
});

export const Checkpoint = Schema.Struct({
  type: Schema.tag("social.checkpoint"),
  ...envelope,
  frontier: Schema.Array(Schema.String),
});

export const SocialStatement = Schema.Union([
  AttestRepo,
  AttestPrincipal,
  Mirrors,
  Vouch,
  Follow,
  Label,
  Revoke,
  Checkpoint,
]).pipe(Schema.toTaggedUnion("type"));

export type AttestRepo = (typeof AttestRepo)["Type"];
export type AttestPrincipal = (typeof AttestPrincipal)["Type"];
export type Mirrors = (typeof Mirrors)["Type"];
export type Vouch = (typeof Vouch)["Type"];
export type Follow = (typeof Follow)["Type"];
export type Label = (typeof Label)["Type"];
export type Revoke = (typeof Revoke)["Type"];
export type Checkpoint = (typeof Checkpoint)["Type"];
export type SocialStatement = (typeof SocialStatement)["Type"];

const decodeStatement = Schema.decodeUnknownEffect(SocialStatement);

export interface StatementContext {
  readonly author: PrincipalId;
  readonly id: string;
  readonly socialHead: string | null;
  readonly trustHead: string | null;
  readonly at?: Date;
}

const common = (input: StatementContext) => ({
  version: 1 as const,
  author: input.author,
  id: input.id,
  issuedAt: (input.at ?? new Date()).toISOString(),
  socialHead: input.socialHead,
  trustHead: input.trustHead,
});

export const attestRepo = (
  input: StatementContext & {
    readonly repo: RepoId;
    readonly urls: ReadonlyArray<string>;
    readonly role: AttestRepo["role"];
    readonly forkOf?: RepoId | null;
    readonly lineage?: string | null;
    readonly inbox?: string | null;
  },
): AttestRepo => ({
  type: "social.attest.repo",
  ...common(input),
  repo: input.repo,
  urls: input.urls,
  role: input.role,
  forkOf: input.forkOf ?? null,
  lineage: input.lineage ?? null,
  inbox: input.inbox ?? null,
});

export const attestPrincipalKey = (
  input: StatementContext & { readonly subject: PrincipalId; readonly publicKey: string },
): AttestPrincipal => ({
  type: "social.attest.principal",
  ...common(input),
  subject: principalSubject(input.subject),
  claim: "key-of",
  publicKey: input.publicKey,
});

export const attestExternalIdentity = (
  input: StatementContext & {
    readonly subject: PrincipalId;
    readonly identity: string;
    readonly proof: string;
  },
): AttestPrincipal => ({
  type: "social.attest.principal",
  ...common(input),
  subject: principalSubject(input.subject),
  claim: "external-identity",
  identity: input.identity,
  proof: input.proof,
});

export const mirrors = (
  input: StatementContext & {
    readonly repo: "self" | RepoId;
    readonly urls: Mirrors["urls"];
  },
): Mirrors => ({ type: "social.mirrors", ...common(input), repo: input.repo, urls: input.urls });

export const vouch = (
  input: StatementContext & {
    readonly subject: PrincipalId;
    readonly scope: ReadonlyArray<SocialScope>;
    readonly depth: number;
    readonly expiresAt?: Date | null;
  },
): Vouch => ({
  type: "social.vouch",
  ...common(input),
  subject: principalSubject(input.subject),
  scope: input.scope,
  depth: input.depth,
  expiresAt: input.expiresAt?.toISOString() ?? null,
});

export const follow = (
  input: StatementContext & { readonly subject: PrincipalId; readonly petname: string },
): Follow => ({
  type: "social.follow",
  ...common(input),
  subject: principalSubject(input.subject),
  petname: input.petname,
});

export const label = (
  input: StatementContext & {
    readonly subject: string;
    readonly namespace: string;
    readonly label: string;
  },
): Label => ({
  type: "social.label",
  ...common(input),
  subject: input.subject,
  namespace: input.namespace,
  label: input.label,
});

export const revoke = (
  input: StatementContext & {
    readonly target: string;
    readonly reason?: Revoke["reason"];
    readonly compromisedAt?: Date | null;
  },
): Revoke => ({
  type: "social.revoke",
  ...common(input),
  target: input.target,
  reason: input.reason ?? "withdrawn",
  compromisedAt: input.compromisedAt?.toISOString() ?? null,
});

export const checkpoint = (
  input: StatementContext & { readonly frontier: ReadonlyArray<string> },
): Checkpoint => ({
  type: "social.checkpoint",
  ...common(input),
  frontier: input.frontier,
});

/** Canonical bytes: the common envelope first, then variant fields. */
export const encode = (statement: SocialStatement): Uint8Array => {
  const base = {
    version: statement.version,
    type: statement.type,
    author: statement.author,
    id: statement.id,
    issuedAt: statement.issuedAt,
    socialHead: statement.socialHead,
    trustHead: statement.trustHead,
  };

  const ordered =
    statement.type === "social.attest.repo"
      ? {
          ...base,
          repo: statement.repo,
          urls: statement.urls,
          role: statement.role,
          forkOf: statement.forkOf,
          lineage: statement.lineage,
          inbox: statement.inbox,
        }
      : statement.type === "social.attest.principal"
        ? statement.claim === "key-of"
          ? {
              ...base,
              subject: statement.subject,
              claim: statement.claim,
              publicKey: statement.publicKey,
            }
          : {
              ...base,
              subject: statement.subject,
              claim: statement.claim,
              identity: statement.identity,
              proof: statement.proof,
            }
        : statement.type === "social.mirrors"
          ? { ...base, repo: statement.repo, urls: statement.urls }
          : statement.type === "social.vouch"
            ? {
                ...base,
                subject: statement.subject,
                scope: statement.scope,
                depth: statement.depth,
                expiresAt: statement.expiresAt,
              }
            : statement.type === "social.follow"
              ? { ...base, subject: statement.subject, petname: statement.petname }
              : statement.type === "social.label"
                ? {
                    ...base,
                    subject: statement.subject,
                    namespace: statement.namespace,
                    label: statement.label,
                  }
                : statement.type === "social.revoke"
                  ? {
                      ...base,
                      target: statement.target,
                      reason: statement.reason,
                      compromisedAt: statement.compromisedAt,
                    }
                  : { ...base, frontier: statement.frontier };

  return encoder.encode(`${JSON.stringify(ordered, null, 2)}\n`);
};

export const decode = Effect.fn("social.Statement.decode")(function* (bytes: Uint8Array) {
  const json = yield* Effect.try({
    try: () => JSON.parse(decoder.decode(bytes)),
    catch: () => new Invalid({ field: "statement", reason: "social statement is not valid JSON" }),
  });
  return yield* decodeStatement(json).pipe(
    Effect.mapError(
      (issue) =>
        new Invalid({
          field: "statement",
          reason: `malformed social statement: ${issue.message}`,
        }),
    ),
  );
});

const validUrl = (value: string): boolean => {
  try {
    const url = new URL(value);
    return url.protocol === "https:" || url.protocol === "http:";
  } catch {
    return false;
  }
};

const validId = (value: string): boolean =>
  /^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/.test(value);

/** Structural checks before a statement can affect any projection. */
export const validate = Effect.fn("social.Statement.validate")(function* (
  statement: SocialStatement,
  identity: PrincipalId,
) {
  if (!isPrincipalId(statement.author) || statement.author !== identity) {
    return yield* new Invalid({
      field: "author",
      reason: `author ${statement.author} is not this identity repository (${identity})`,
    });
  }
  if (!validId(statement.id)) {
    return yield* new Invalid({ field: "id", reason: `'${statement.id}' is not a UUIDv7` });
  }
  if (Number.isNaN(Date.parse(statement.issuedAt))) {
    return yield* new Invalid({
      field: "issuedAt",
      reason: `not a date: '${statement.issuedAt}'`,
    });
  }
  for (const [field, head] of [
    ["socialHead", statement.socialHead],
    ["trustHead", statement.trustHead],
  ] as const) {
    if (head !== null && !isOid(head)) {
      return yield* new Invalid({ field, reason: `'${head}' is not an object id` });
    }
  }

  if (statement.type === "social.attest.repo") {
    if (!isRepoId(statement.repo)) {
      return yield* new Invalid({ field: "repo", reason: `'${statement.repo}' is not a RepoID` });
    }
    if (statement.urls.length === 0 || statement.urls.some((url) => !validUrl(url))) {
      return yield* new Invalid({ field: "urls", reason: "repo attestations need valid URLs" });
    }
    if (statement.role === "fork" ? !isRepoId(statement.forkOf ?? "") : statement.forkOf !== null) {
      return yield* new Invalid({
        field: "forkOf",
        reason: "a fork must name its parent, and only a fork may name one",
      });
    }
    if (statement.lineage !== null && !/^sha1:[0-9a-f]{40}$/.test(statement.lineage)) {
      return yield* new Invalid({ field: "lineage", reason: "lineage must be a sha1 identifier" });
    }
    if (statement.inbox !== null && !validUrl(statement.inbox)) {
      return yield* new Invalid({ field: "inbox", reason: "inbox must be a URL" });
    }
    return;
  }

  if (statement.type === "social.attest.principal") {
    if (principalOf(statement.subject) === null) {
      return yield* new Invalid({
        field: "subject",
        reason: `'${statement.subject}' is not a principal`,
      });
    }
    if (statement.claim === "key-of") {
      if (
        statement.publicKey === undefined ||
        statement.identity !== undefined ||
        statement.proof !== undefined
      ) {
        return yield* new Invalid({
          field: "claim",
          reason: "a key-of claim carries only a public key",
        });
      }
      if (Result.isFailure(parsePublicKey(statement.publicKey))) {
        return yield* new Invalid({ field: "publicKey", reason: "not an SSH public key" });
      }
      return;
    }
    if (
      statement.identity === undefined ||
      !statement.identity.includes(":") ||
      statement.proof === undefined ||
      !validUrl(statement.proof) ||
      statement.publicKey !== undefined
    ) {
      return yield* new Invalid({
        field: "claim",
        reason: "an external-identity claim needs a platform identity and proof URL",
      });
    }
    return;
  }

  if (statement.type === "social.mirrors") {
    if (statement.repo !== "self" && !isRepoId(statement.repo)) {
      return yield* new Invalid({ field: "repo", reason: "a mirror must name self or a RepoID" });
    }
    if (
      statement.urls.length === 0 ||
      statement.urls.length > 8 ||
      statement.urls.some(({ url }) => !validUrl(url))
    ) {
      return yield* new Invalid({ field: "urls", reason: "mirrors need one to eight valid URLs" });
    }
    return;
  }

  if (statement.type === "social.vouch") {
    if (principalOf(statement.subject) === null) {
      return yield* new Invalid({ field: "subject", reason: "a vouch must name a principal" });
    }
    const unknown = statement.scope.find((scope) => !socialScopes.has(scope));
    if (unknown !== undefined) {
      return yield* new Invalid({
        field: "scope",
        reason: `unknown social scope '${unknown}'`,
      });
    }
    if (statement.scope.length === 0 || statement.depth < 0 || statement.depth > 16) {
      return yield* new Invalid({
        field: "depth",
        reason: "a vouch needs a scope and a depth from 0 to 16",
      });
    }
    if (statement.expiresAt !== null && Number.isNaN(Date.parse(statement.expiresAt))) {
      return yield* new Invalid({ field: "expiresAt", reason: "not a date" });
    }
    return;
  }

  if (statement.type === "social.follow") {
    if (principalOf(statement.subject) === null || statement.petname.trim() === "") {
      return yield* new Invalid({
        field: "follow",
        reason: "a follow needs a principal and a non-empty petname",
      });
    }
    return;
  }

  if (statement.type === "social.label") {
    if (
      !/^[a-z0-9-]+(?:\.[a-z0-9-]+)+$/i.test(statement.namespace) ||
      statement.label.trim() === ""
    ) {
      return yield* new Invalid({
        field: "label",
        reason: "a label needs a reverse-domain namespace and a value",
      });
    }
    return;
  }

  if (statement.type === "social.revoke") {
    if (!validId(statement.target)) {
      return yield* new Invalid({ field: "target", reason: "a revoke must name a statement id" });
    }
    if (statement.compromisedAt !== null && Number.isNaN(Date.parse(statement.compromisedAt))) {
      return yield* new Invalid({ field: "compromisedAt", reason: "not a date" });
    }
    return;
  }

  if (statement.frontier.some((head) => !isOid(head))) {
    return yield* new Invalid({ field: "frontier", reason: "a checkpoint frontier is object ids" });
  }
});
