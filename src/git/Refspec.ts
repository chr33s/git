/**
 * Refspecs: which refs a transfer carries, and what they are called at the
 * other end.
 *
 * The transport already does not care what an oid was reached through — a
 * branch, a tag, a review, a membership grant are all commits — but the ref
 * *selection* was hard-coded to heads and tags, which is what made
 * `refs/hub/*` unreachable without touching the pack layer. This is that
 * selection as data, so a namespace becomes a configuration line rather than
 * a code path.
 *
 * git's own syntax, and only the parts that mean something here:
 *
 * ```text
 * +refs/heads/*:refs/remotes/origin/*   forced, one wildcard each side
 * refs/heads/main:refs/heads/main       exact
 * +refs/hub/*:refs/hub/*                same name both ends
 * ```
 *
 * Pure: parsing and matching are byte work, so they stay synchronous and
 * return `Result`, the same seam `Format.ts` draws.
 */
import { Result } from "effect";

import { Invalid } from "./Error.ts";

export interface Refspec {
  /**
   * Whether the update may discard commits.
   *
   * `+` in git's syntax. It is advice to the caller applying the update, not
   * something matching enforces — `Fetch.ts` and the policy boundary are
   * where a non-fast-forward is refused, and they have the history to judge
   * it with.
   */
  readonly force: boolean;
  readonly source: string;
  readonly destination: string;
}

const invalid = (reason: string) => Result.fail(new Invalid({ field: "refspec", reason }));

/**
 * One refspec, as it would appear in a config file.
 *
 * Both halves must agree about wildcards: a pattern source with an exact
 * destination would map every matched ref onto one name, which is not a
 * transfer but a race between the refs it matched.
 */
export const parse = (spec: string): Result.Result<Refspec, Invalid> => {
  const force = spec.startsWith("+");
  const body = force ? spec.slice(1) : spec;

  const colon = body.indexOf(":");
  if (colon === -1) return invalid(`'${spec}' has no destination`);

  const source = body.slice(0, colon);
  const destination = body.slice(colon + 1);
  if (source === "" || destination === "") return invalid(`'${spec}' has an empty half`);

  const sourceStars = source.split("*").length - 1;
  const destinationStars = destination.split("*").length - 1;
  if (sourceStars > 1 || destinationStars > 1) {
    return invalid(`'${spec}' may use at most one '*' per half`);
  }
  if (sourceStars !== destinationStars) {
    return invalid(`'${spec}' must use '*' on both sides or neither`);
  }

  return Result.succeed({ force, source, destination });
};

export const format = (spec: Refspec): string =>
  `${spec.force ? "+" : ""}${spec.source}:${spec.destination}`;

/**
 * What a ref is called at the other end, or `null` when this spec does not
 * cover it.
 */
export const map = (spec: Refspec, ref: string): string | null => {
  const star = spec.source.indexOf("*");
  if (star === -1) return ref === spec.source ? spec.destination : null;

  const prefix = spec.source.slice(0, star);
  const suffix = spec.source.slice(star + 1);
  if (!ref.startsWith(prefix) || !ref.endsWith(suffix)) return null;
  // A ref that is exactly the prefix and suffix matched an empty `*`, which
  // would produce a destination with an empty path component.
  if (ref.length < prefix.length + suffix.length) return null;

  const middle = ref.slice(prefix.length, ref.length - suffix.length);
  if (middle === "") return null;
  // A replacer function, not a replacement string: `String.replace` expands
  // `$&`, `` $` ``, `$'` and `$$` inside the second argument, and the second
  // argument here is part of a ref name the remote chose. `refs/heads/x$`y`
  // would otherwise map to a destination with the whole prefix spliced into
  // the middle of it, and `refs/heads/a$&b` to one containing a `*`.
  return spec.destination.replace("*", () => middle);
};

/** The first spec that covers this ref, and the name it maps to. */
export const resolve = (
  specs: ReadonlyArray<Refspec>,
  ref: string,
): { readonly spec: Refspec; readonly destination: string } | null => {
  for (const spec of specs) {
    const destination = map(spec, ref);
    if (destination !== null) return { spec, destination };
  }
  return null;
};

/**
 * What a plain clone asks for: branches and tags, under their own names.
 *
 * The historical default, spelled out, so that a caller that passes nothing
 * gets exactly what it used to and a caller that wants more says so.
 */
export const DEFAULT_FETCH: ReadonlyArray<Refspec> = [
  { force: false, source: "refs/heads/*", destination: "refs/heads/*" },
  { force: false, source: "refs/tags/*", destination: "refs/tags/*" },
];

/**
 * The hub's namespaces, fetched under the names they already have.
 *
 * `refs/meta/policy` is here beside the trust log because a replica that has
 * the membership but not the branch rules answers `OPEN` to every question
 * `Policy.rulesOf` asks — so a mirror sharing the same trust graph would let
 * through exactly the pushes the origin protects. Rules that do not replicate
 * are rules with a second host they do not apply on.
 */
export const HUB_FETCH: ReadonlyArray<Refspec> = [
  { force: false, source: "refs/meta/trust/*", destination: "refs/meta/trust/*" },
  { force: false, source: "refs/meta/policy", destination: "refs/meta/policy" },
  { force: false, source: "refs/social/log", destination: "refs/social/log" },
  { force: false, source: "refs/hub/*", destination: "refs/hub/*" },
];

/**
 * Whether a ref lives in a namespace that only ever grows.
 *
 * Hub and trust refs are append-only: an update must contain what it replaces.
 * That is a different rule from a branch (which may move to anything that
 * keeps its commits) and from a tag (which may not move at all), and every
 * surface that applies a ref update needs to know which of the three it has.
 *
 * The trust log is one ref, not a namespace. Matching `refs/meta/trust/log/*`
 * as well let a name nothing reads through the check that closes that
 * namespace: unbounded in count, since the population bound covers only
 * `refs/hub/`, undeletable, hidden from the advertisement, copied to every
 * mirror and rooted by collection on each of them — a permanent, invisible pin
 * on the object graph, for a ref no fold ever opens.
 */
export const isAppendOnly = (ref: string): boolean =>
  ref.startsWith("refs/hub/") || ref === TRUST_LOG || ref === SOCIAL_LOG;

/** The one trust ref that moves; the genesis never does. */
export const TRUST_LOG = "refs/meta/trust/log";
export const TRUST_GENESIS = "refs/meta/trust/genesis";
/** The one social ref that moves; every statement is a commit in this log. */
export const SOCIAL_LOG = "refs/social/log";

/**
 * Refs a plain `git clone` should not be sent.
 *
 * Hub state is large — an event per comment — and a stock client asked for
 * none of it. Sending it on every advertisement would make every clone pay
 * for a feature it is not using, so these are advertised only to clients that
 * name the namespace.
 *
 * The genesis is the exception, and deliberately: it is one small commit, and
 * it is what lets any client compute the `RepoID` and check it against what it
 * trusts. Hiding identity would make verification need permission.
 */
export const hiddenFromAdvertisement = (ref: string): boolean =>
  ref !== TRUST_GENESIS &&
  (ref.startsWith("refs/hub/") ||
    ref.startsWith("refs/meta/") ||
    ref.startsWith("refs/quarantine/") ||
    ref.startsWith("refs/social/"));

/**
 * Whether a `ref-prefix` is asking for a hidden namespace.
 *
 * Not the same question as `hiddenFromAdvertisement`, and conflating them cost
 * clients their answer: `ref-prefix refs/hub` — no trailing slash, which git
 * writes whenever the configured refspec has none — is not itself a ref in a
 * hidden namespace, so it matched nothing and the hub refs were withheld from
 * a client that had asked for them by name. Since v0 hides them too, that
 * client could never discover them at all, and a mirror reported a successful
 * replication having fetched nothing.
 *
 * The prefix has to *name* the namespace, though, not merely start the same
 * way. Accepting anything the namespace begins with let `ls-remote 'refs/h*'`
 * — three characters, naming nothing — return the whole of `refs/hub/`, which
 * is the state the hiding exists to withhold from a client that did not ask
 * for it. The one shortfall allowed is the trailing slash, because that is the
 * one git itself writes.
 */
export const namesHiddenNamespace = (prefix: string): boolean =>
  HIDDEN.some((namespace) => prefix.startsWith(namespace) || `${prefix}/` === namespace);

const HIDDEN = ["refs/hub/", "refs/meta/", "refs/quarantine/", "refs/social/"];

/**
 * The prefixes a client must ask for by name to reach a hidden namespace.
 *
 * The mirror of `namesHiddenNamespace`, for the side doing the asking. A
 * refspec covers a namespace whenever either name is a prefix of the other,
 * but the *ask* has to name the namespace: `refs/` is what "everything" looks
 * like and is answered with the default. So `refs/hub/*` asks for `refs/hub/`
 * and `refs/*` asks for both namespaces by name — which is the case that used
 * to fetch nothing, because a probe built from the refspec's own prefix was
 * too short to name anything and the client never sent an `ls-refs` at all.
 * v0 hides these too, so that fetch reported a replication it had not done.
 */
export const hiddenPrefixes = (prefix: string): ReadonlyArray<string> =>
  HIDDEN.flatMap((namespace) =>
    prefix.startsWith(namespace) ? [prefix] : namespace.startsWith(prefix) ? [namespace] : [],
  );

/**
 * The `ls-refs` prefixes one refspec has to ask for by name.
 *
 * The literal head of the source, and then whichever hidden namespaces that
 * head touches. Cut at the **first** wildcard, not at a trailing one: a source
 * may put its `*` in the middle — `map` supports exactly that — and a prefix
 * that kept the `*` in it matched nothing, because `ls-refs` compares a prefix
 * with `startsWith`. The ask came back empty and the fetch reported a
 * replication of zero refs as a success, which is the one failure this whole
 * path exists to make visible.
 */
export const probes = (spec: Refspec): ReadonlyArray<string> =>
  hiddenPrefixes(spec.source.split("*")[0] ?? spec.source);
