/**
 * The capability vocabulary, and what one capability implies about another.
 *
 * Its own module because both `Certificate` — which decides what a grant may
 * say — and `Principal` — which decides what a resolved stable identity may do
 * — have to answer the implication question, and `Certificate` already imports
 * `Principal`. Left in `Certificate`, the second caller could not import it
 * without a cycle and kept a copy instead: two answers to "does this hold that",
 * one of them reached only through stable-identity authorization, drifting
 * apart the first time either the admin override or the wildcard rule changed.
 */

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
  "hub.queue",
  "social.write",
  "member.invite",
  "member.revoke",
  "policy.write",
  "repo.admin",
] as const;

/** `hub.check:<name>` — one check name, or `*` for any. */
export const CHECK_PREFIX = "hub.check:";

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
