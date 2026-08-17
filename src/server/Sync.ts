/**
 * This repository acting as a client of another — the server side of
 * `fetch`, `push` and `pull` against a named remote.
 *
 * Its own module rather than handler bodies in `Api.ts`: the API file is the
 * declaration of the HTTP surface, and this is the machinery behind three of
 * its endpoints. The split is also what keeps the layering readable — this
 * file is the one place the server reaches for the client transport
 * (`client/Fetch.ts`'s `requestPack`), and nothing in it knows about HTTP
 * schemas or routes.
 */
import { Effect, Stream } from "effect";

import { lsRemote, requestPack } from "../client/Fetch.ts";
import { Invalid } from "../git/Error.ts";
import { Repository } from "../git/Repository.ts";
import type { Oid } from "../git/Store.ts";
import { Remotes, validate as validateRemote } from "./Remotes.ts";

/**
 * The remote a request names: one this repository stores, credential
 * included, or a URL taken as it stands. A URL carries no credential — one in
 * a request body is one in an access log — so an authenticated remote is one
 * that has been registered.
 */
export const remoteFor = Effect.fn("Sync.remoteFor")(function* (payload: {
  readonly name?: string | undefined;
  readonly url?: string | undefined;
}) {
  const { name, url } = payload;
  if (name !== undefined && url !== undefined) {
    return yield* new Invalid({
      field: "remote",
      reason: "name a stored remote or give a url, not both",
    });
  }
  if (url !== undefined) {
    // `origin` is what git calls the remote you cloned from, and a URL used
    // without a stored name still has to track somewhere.
    const checked = yield* validateRemote({ name: "origin", url });
    return { name: checked.name, url: checked.url, credential: null };
  }
  if (name === undefined) {
    return yield* new Invalid({
      field: "remote",
      reason: "give a stored remote 'name' or a 'url'",
    });
  }

  const remotes = yield* Remotes;
  const stored = yield* remotes.get(name).pipe(Effect.catchTag("StorageFailure", Effect.die));
  if (stored === null) {
    return yield* new Invalid({ field: "name", reason: `unknown remote '${name}'` });
  }
  return { name: stored.name, url: stored.url, credential: stored.credential };
});

/** What `remoteFor` resolves to, and what every operation below acts on. */
export interface Target {
  readonly name: string;
  readonly url: string;
  readonly credential: string | null;
}

/**
 * Which remote refs a request asked for. An entry is a full ref name
 * (`refs/heads/main`), its short form (`main`, `v1.0`), or a prefix with a
 * trailing `*`. Absent means every branch and tag the remote advertises.
 */
const selects = (filter: ReadonlyArray<string> | undefined, name: string): boolean => {
  if (filter === undefined) return true;
  const short = name.replace(/^refs\/(?:heads|tags)\//, "");
  return filter.some((entry) =>
    entry.endsWith("*") ? name.startsWith(entry.slice(0, -1)) : entry === name || entry === short,
  );
};

/**
 * Where a fetched ref lands. A branch becomes a remote-tracking ref, so a
 * fetch never moves a local branch — `pull` is the operation that does that,
 * and only once it can say the move is a fast-forward. A tag keeps its own
 * name, because a tag is not per-remote.
 */
const trackingOf = (remote: string, name: string): string =>
  name.startsWith("refs/heads/")
    ? `refs/remotes/${remote}/${name.slice("refs/heads/".length)}`
    : name;

/**
 * A fetch into this repository: advertisement, one pack, then the tracking
 * refs.
 *
 * `Client.fetchRepository` writes through an `ObjectStore` and `RefStore` it
 * is handed; this layer carries `Repository` and not the stores underneath
 * it, so the pack goes in through `Repository.unpack` — receive-pack's own
 * ingest, and the reason this can report how many objects arrived rather than
 * only which refs moved.
 *
 * `depth` is passed through as `deepen` and nothing more: the boundary
 * commits' parents stay on the remote and there is no shallow list in these
 * stores to record that in, so a depth-limited fetch leaves commits whose
 * parents are absent — which `fsck` will report. It is here so a caller after
 * the last few commits of a large history need not take all of it; it is not
 * an equivalent of `git clone --depth`.
 */
export const fetchFrom = Effect.fn("Sync.fetchFrom")(function* (input: {
  readonly remote: string;
  readonly url: string;
  readonly credential: string | null;
  readonly refs?: ReadonlyArray<string> | undefined;
  readonly depth?: number | undefined;
}) {
  const repository = yield* Repository;
  const token = input.credential ?? undefined;

  if (input.depth !== undefined && (!Number.isInteger(input.depth) || input.depth < 1)) {
    return yield* new Invalid({
      field: "depth",
      reason: `depth must be a positive integer, not '${input.depth}'`,
    });
  }

  const advertised = yield* lsRemote(input.url, { token });
  const local = new Map(yield* repository.refs);

  const wanted = advertised
    .filter(
      (ref) =>
        // `refs/tags/v1^{}` is the tag's target, not a ref to hold, and
        // `HEAD` is a symbolic ref this repository has one of already.
        !ref.name.endsWith("^{}") &&
        (ref.name.startsWith("refs/heads/") || ref.name.startsWith("refs/tags/")) &&
        selects(input.refs, ref.name),
    )
    .map((ref) => ({ name: trackingOf(input.remote, ref.name), oid: ref.oid }))
    .filter(
      (ref) =>
        local.get(ref.name) !== ref.oid &&
        // A tag is a name that does not move: re-pointing one on a fetch
        // would rewrite what this repository has already published under it.
        !(ref.name.startsWith("refs/tags/") && local.has(ref.name)),
    );

  if (wanted.length === 0) return { refs: [], objects: 0 };

  const wants: Array<Oid> = [];
  for (const oid of new Set(wanted.map((ref) => ref.oid))) {
    if (!(yield* repository.contains(oid))) wants.push(oid);
  }

  /** One negotiation round, unpacked. */
  const round = (haves: ReadonlyArray<Oid>) =>
    Effect.gen(function* () {
      const pack = yield* requestPack({
        // The shared client transport, not a local copy: its prelude reader is
        // the one that survives a server acknowledging more than one have
        // before the pack.
        url: input.url,
        token,
        wants,
        haves,
        depth: input.depth,
      });
      return yield* repository.unpack(
        Stream.fromAsyncIterable(
          pack,
          (cause) => new Invalid({ field: "remote", reason: String(cause) }),
        ),
      );
    });

  // Every wanted object is already here — a branch that was fetched under
  // another name, or a ref moved back to where it was. There is nothing to
  // ask for, and an empty `want` list is a request the server rejects.
  const arrived: Oid[] = [];
  if (wants.length > 0) {
    arrived.push(...(yield* round([...new Set(local.values())])));

    // A `have` claims that commit *and everything behind it*, which is what
    // lets the remote leave that history out of the pack. After a depth-limited
    // fetch this repository holds tips whose parents are absent, and there is
    // no shallow list in these stores to declare that with — so the offer can
    // be a lie, and the symptom is a pack that does not contain what was
    // asked for. Cheaper to notice that than to prove the offer honest: the
    // check is one `has` per want, and only the rare bad round pays for a
    // second, which claims nothing and therefore cannot lie.
    const held = yield* Effect.forEach(wants, (oid) => repository.contains(oid));
    if (held.includes(false)) arrived.push(...(yield* round([])));
  }

  const refs = yield* Effect.forEach(wanted, (ref) =>
    repository
      .setRef({ name: ref.name, to: ref.oid })
      .pipe(Effect.map((moved) => ({ name: moved.ref, oid: moved.oid, from: moved.previous }))),
  );

  return { refs, objects: arrived.length };
});

/**
 * Fetch, then move the local branch — but only when that move loses nothing.
 *
 * The outcomes are the point of the function: `created` and `fast-forward`
 * move the branch, `up-to-date` includes a remote that is merely *behind*
 * (moving the branch back would drop commits only this side has), and
 * `non-fast-forward` is reported rather than resolved. `/merge` and
 * `/rebase` are where a caller says which resolution it meant, and both can
 * start from `tracking`, which this pull has already moved — guessing here
 * would write a merge commit nobody asked for into a branch.
 */
export const pull = Effect.fn("Sync.pull")(function* (input: {
  readonly target: Target;
  readonly branch: string;
  readonly depth?: number | undefined;
}) {
  const repository = yield* Repository;

  // The same normalization the policy gate applied, and then a check that the
  // two agree. Stripping only a `refs/heads/` prefix meant `refs/tags/x` was
  // *judged* as `refs/tags/x` and *written* as `refs/heads/refs/tags/x` — so a
  // `source.push` holder could create a ref inside a fully protected
  // `refs/heads/*` namespace without the protected-branch rules ever seeing
  // it. A pull moves a branch; anything else is a request this cannot serve.
  const branch = input.branch.startsWith("refs/") ? input.branch : `refs/heads/${input.branch}`;
  if (!branch.startsWith("refs/heads/") || branch === "refs/heads/") {
    return yield* new Invalid({
      field: "branch",
      reason: `'${input.branch}' is not a branch; a pull moves refs/heads/*`,
    });
  }
  const short = branch.slice("refs/heads/".length);
  const tracking = `refs/remotes/${input.target.name}/${short}`;

  const fetched = yield* fetchFrom({
    remote: input.target.name,
    url: input.target.url,
    credential: input.target.credential,
    refs: [`refs/heads/${short}`],
    depth: input.depth,
  });

  // Absent from the fetch's own report means the tracking ref was
  // already where the remote is, not that the remote has no such branch.
  const moved = fetched.refs.find((ref) => ref.name === tracking);
  const to = moved?.oid ?? (yield* repository.resolve(tracking));
  if (to === null) {
    return yield* new Invalid({
      field: "branch",
      reason: `remote has no branch '${short}'`,
    });
  }

  const from = yield* repository.resolve(branch);
  const outcome = { branch, tracking, from, to, objects: fetched.objects };

  if (from === null) {
    yield* repository.setRef({ name: branch, to, expected: null });
    return { kind: "created" as const, ...outcome };
  }
  if (from === to) return { kind: "up-to-date" as const, ...outcome };
  if (yield* repository.isAncestor(to, from)) {
    return { kind: "up-to-date" as const, ...outcome };
  }
  if (!(yield* repository.isAncestor(from, to))) {
    return { kind: "non-fast-forward" as const, ...outcome };
  }

  // A compare-and-swap, because the fetch above was not instantaneous and
  // this branch is one a push can move.
  yield* repository.setRef({ name: branch, to, expected: from });
  return { kind: "fast-forward" as const, ...outcome };
});
