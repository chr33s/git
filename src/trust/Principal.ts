/**
 * Stable identities built from identity repositories.
 *
 * A principal identifier has the same bytes as a repository identifier. The
 * separate brand prevents an API that expects "who" from silently accepting
 * "which project"; payloads that name a principal use the explicit
 * `principal:` subject spelling.
 */
import { Context, Effect, Layer, Option } from "effect";

import type { Fingerprint } from "../crypto/SshSignature.ts";
import type { Oid } from "../git/Store.ts";
import { permits } from "./Capability.ts";
import { isRepoId, type RepoId } from "./Genesis.ts";
import type { Member, PrincipalMember, Projection } from "./Projection.ts";

export type PrincipalId = RepoId & { readonly PrincipalId: unique symbol };
export type PrincipalSubject = string & { readonly PrincipalSubject: unique symbol };

/** A repository identity viewed as the identity repository of a principal. */
export const principalId = (repo: RepoId): PrincipalId => {
  // SAFETY: a PrincipalID is defined as the RepoID of an identity repository;
  // the caller is making that semantic choice explicitly at this boundary.
  return repo as PrincipalId;
};

export const isPrincipalId = (value: string): value is PrincipalId => isRepoId(value);

export const principalSubject = (principal: PrincipalId): PrincipalSubject => {
  // SAFETY: the prefix and the validated PrincipalID are exactly the branded
  // subject representation this module owns.
  return `principal:${principal}` as PrincipalSubject;
};

export const principalOf = (subject: string): PrincipalId | null => {
  if (!subject.startsWith("principal:")) return null;
  const value = subject.slice("principal:".length);
  return isPrincipalId(value) ? value : null;
};

export interface ResolvedIdentity {
  readonly principal: PrincipalId;
  readonly projection: Projection;
  /** The identity trust-log head the projection was built from. */
  readonly head: Oid | null;
}

/** Identity repositories already fetched and verified by the caller. */
export class Identities extends Context.Service<
  Identities,
  {
    readonly resolve: (principal: PrincipalId) => Effect.Effect<ResolvedIdentity | null>;
  }
>()("trust/Identities") {}

export const identitiesInMemory = (
  identities: ReadonlyArray<ResolvedIdentity>,
): Layer.Layer<Identities> => {
  const byPrincipal = new Map(
    identities.map((identity) => [identity.principal, identity] as const),
  );
  return Layer.succeed(Identities)({
    resolve: (principal) => Effect.succeed(byPrincipal.get(principal) ?? null),
  });
};

/** Resolve one identity and reject a provider answer that does not match its pin. */
const resolveVerified = Effect.fn("trust.Principal.resolveVerified")(function* (
  service: Identities["Service"],
  principal: PrincipalId,
) {
  const identity = yield* service.resolve(principal);
  if (
    identity === null ||
    identity.principal !== principal ||
    identity.projection.repoId !== principal ||
    identity.projection.head !== identity.head
  ) {
    return null;
  }
  return identity;
});

export type PrincipalAuthorization =
  | {
      readonly ok: true;
      /** A Member-shaped view for existing policy and hub consumers. */
      readonly principal: Member;
      readonly grant: PrincipalMember;
      readonly identity: ResolvedIdentity;
    }
  | { readonly ok: false; readonly reason: string; readonly quarantined: boolean };

const denied = (reason: string, quarantined = false): PrincipalAuthorization => ({
  ok: false,
  reason,
  quarantined,
});

export type IdentityAuthorization =
  | {
      readonly ok: true;
      readonly principal: PrincipalId;
      readonly member: Member;
      readonly identity: ResolvedIdentity;
    }
  | { readonly ok: false; readonly reason: string; readonly quarantined: boolean };

export type IdentityKeyRelation =
  | { readonly available: true; readonly belongs: boolean }
  | { readonly available: false; readonly belongs: false };

/** Whether a key has ever been a device of this stable identity. */
export const containsIdentityKey = Effect.fn("trust.Principal.containsIdentityKey")(function* (
  principal: PrincipalId,
  signer: Fingerprint,
) {
  const service = yield* Effect.serviceOption(Identities);
  if (Option.isNone(service)) return { available: false, belongs: false } as const;
  const identity = yield* resolveVerified(service.value, principal);
  if (identity === null) return { available: false, belongs: false } as const;
  return {
    available: true,
    belongs: identity.projection.members.has(signer) || identity.projection.former.has(signer),
  } as const;
});

/** Prove that a signing key is current for one specifically named identity. */
export const identifyKey = Effect.fn("trust.Principal.identifyKey")(function* (input: {
  readonly principal: PrincipalId;
  readonly signer: Fingerprint;
  readonly at?: Date;
}) {
  const service = yield* Effect.serviceOption(Identities);
  if (Option.isNone(service)) {
    return {
      ok: false,
      reason: "the referenced identity repository has not been synchronized",
      quarantined: true,
    } satisfies IdentityAuthorization;
  }

  const identity = yield* resolveVerified(service.value, input.principal);
  if (identity === null) {
    return {
      ok: false,
      reason: "the referenced identity repository has not been synchronized",
      quarantined: true,
    } satisfies IdentityAuthorization;
  }

  const member = identity.projection.members.get(input.signer);
  const at = input.at ?? new Date();
  if (
    member === undefined ||
    (member.expiresAt !== null && member.expiresAt.getTime() <= at.getTime())
  ) {
    return {
      ok: false,
      reason: `${input.signer} is not a current key of ${input.principal}`,
      quarantined: false,
    } satisfies IdentityAuthorization;
  }

  return {
    ok: true,
    principal: input.principal,
    member,
    identity,
  } satisfies IdentityAuthorization;
});

export interface ResolvedPrincipalKey {
  /** Member-shaped target-repository authority for existing consumers. */
  readonly principal: Member;
  readonly grant: PrincipalMember;
  readonly identity: ResolvedIdentity;
}

export interface PrincipalKeyCandidates {
  readonly matches: ReadonlyArray<ResolvedPrincipalKey>;
  /** At least one relevant identity repository was not available or valid. */
  readonly unavailable: boolean;
}

/**
 * Resolve a key through every matching stable-identity grant.
 *
 * `includeFormer` is for a stored-event fold. It does not itself authorize a
 * revoked grant; the caller still applies that grant's revocation window and
 * trust-head ancestry. Returning all matches is important: one revoked
 * principal must not hide a second, valid grant for the same device key.
 */
export const resolveKeyCandidates = Effect.fn("trust.Principal.resolveKeyCandidates")(
  function* (input: {
    readonly projection: Projection;
    readonly signer: Fingerprint;
    readonly includeFormer?: boolean;
    readonly at?: Date;
  }) {
    const service = yield* Effect.serviceOption(Identities);
    if (Option.isNone(service)) {
      return { matches: [], unavailable: input.projection.principals.size > 0 };
    }

    const grants = new Map(input.projection.principals);
    if (input.includeFormer === true) {
      for (const [principal, grant] of input.projection.formerPrincipals) {
        if (!grants.has(principal)) grants.set(principal, grant);
      }
    }

    const at = input.at ?? new Date();
    const matches: ResolvedPrincipalKey[] = [];
    let unavailable = false;
    for (const grant of grants.values()) {
      const identity = yield* resolveVerified(service.value, grant.principal);
      if (identity === null) {
        unavailable = true;
        continue;
      }

      // A current device is the live path. For a stored event, a forward-only
      // rotation may preserve what the device signed; compromise never does.
      let device = identity.projection.members.get(input.signer);
      if (device === undefined && input.includeFormer === true) {
        const windows = identity.projection.revoked.get(input.signer);
        const open = windows?.at(-1);
        if (open !== undefined && open.supersededBy === null && open.reason !== "compromised") {
          device = identity.projection.former.get(input.signer);
        }
      }
      if (device === undefined) continue;
      if (device.expiresAt !== null && device.expiresAt.getTime() <= at.getTime()) continue;

      const expiresAt =
        grant.expiresAt === null
          ? device.expiresAt
          : device.expiresAt === null || grant.expiresAt <= device.expiresAt
            ? grant.expiresAt
            : device.expiresAt;
      matches.push({
        grant,
        identity,
        principal: {
          fingerprint: input.signer,
          publicKey: device.publicKey,
          capabilities: grant.capabilities,
          grantedAt: grant.grantedAt,
          expiresAt,
          grant: grant.grant,
          history: grant.history,
        },
      });
    }
    return { matches, unavailable } satisfies PrincipalKeyCandidates;
  },
);

/**
 * Resolve one presenting key through the stable identities granted here.
 *
 * An unavailable foreign log is quarantine: replication can bring it later
 * and the same deterministic question can then be asked again. A resolved log
 * in which the key is absent is a real denial.
 */
export const authorizeKey = Effect.fn("trust.Principal.authorizeKey")(function* (input: {
  readonly projection: Projection;
  readonly signer: Fingerprint;
  readonly capability: string;
  readonly at?: Date;
}) {
  const at = input.at ?? new Date();
  const eligible = [...input.projection.principals.values()].filter((grant) =>
    permits(grant.capabilities, input.capability),
  );
  if (eligible.length === 0) {
    return denied(`${input.signer} is not a member of this repository`);
  }

  const resolved = yield* resolveKeyCandidates({
    projection: input.projection,
    signer: input.signer,
    at,
  });
  for (const match of resolved.matches) {
    if (!permits(match.grant.capabilities, input.capability)) continue;
    if (match.grant.expiresAt !== null && match.grant.expiresAt.getTime() <= at.getTime()) continue;
    return { ok: true, ...match } as const;
  }

  return resolved.unavailable
    ? denied("a referenced identity repository has not been synchronized", true)
    : denied(`${input.signer} is not a current key of any granted principal`);
});
