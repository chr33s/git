/**
 * Trust on first use, for repositories rather than hosts.
 *
 * SSH answers "is this the same server?" from `~/.ssh/known_hosts`. This
 * answers a different question — "is this the same repository?" — and keeps
 * its own store for it, because the two are genuinely independent: a
 * repository moves between hosts without changing identity, and one host
 * serves thousands of repositories that have nothing to do with each other.
 * Writing repository identities into `known_hosts` would conflate them.
 *
 * Entries are keyed by URL and carry a `RepoID` and nothing else. An earlier
 * draft stored a public key beside it, which cannot be right: a genesis holds
 * several root keys and no single one of them identifies the repository. The
 * `RepoID` is the identity; the keys are what it is made of.
 *
 * The file format is `known_hosts`-shaped on purpose — one entry per line,
 * `#` comments, appended to rather than rewritten — so that the thing a user
 * already knows how to inspect, grep and hand-edit behaves the way they
 * expect.
 */
import { Context, Effect } from "effect";

import { Invalid, type StorageFailure } from "../git/Error.ts";
import { isRepoId, type RepoId } from "./Genesis.ts";

export interface KnownRepo {
  readonly url: string;
  readonly repoId: RepoId;
}

export class KnownRepos extends Context.Service<
  KnownRepos,
  {
    readonly list: Effect.Effect<ReadonlyArray<KnownRepo>, StorageFailure>;
    /** The pinned identity for a URL, or `null` when it has never been seen. */
    readonly lookup: (url: string) => Effect.Effect<RepoId | null, StorageFailure>;
    /**
     * Pin an identity.
     *
     * Replaces the entry for that URL: a caller reaches here only after the
     * user has resolved a mismatch, and refusing at this level would mean the
     * only way to accept a re-created repository is to hand-edit the file.
     */
    readonly remember: (entry: KnownRepo) => Effect.Effect<void, StorageFailure>;
    /** `false` when there was nothing pinned for that URL. */
    readonly forget: (url: string) => Effect.Effect<boolean, StorageFailure>;
  }
>()("trust/KnownRepos") {}

/**
 * What the client should do about the identity a repository just presented.
 *
 * `changed` is the case the whole mechanism exists for, and it is deliberately
 * not a boolean beside `trusted`: the two carry different information, and a
 * caller that has to reconstruct "what did we expect?" from somewhere else in
 * order to warn is a caller that will warn badly.
 */
export type Decision =
  | { readonly kind: "trusted"; readonly repoId: RepoId }
  | {
      readonly kind: "new";
      readonly repoId: RepoId;
      /**
       * A URL where this same identity is already pinned, when there is one.
       *
       * A repository that moved is not a repository nobody has ever seen, and
       * saying so turns a fresh trust decision into a recognition.
       */
      readonly alias: string | null;
    }
  | {
      readonly kind: "changed";
      readonly expected: RepoId;
      readonly presented: RepoId;
    };

/**
 * Compare what a repository presented against what is pinned for its URL.
 *
 * Pure over the store's answers rather than a method on the port, so the
 * policy is one function every surface shares — the CLI's prompt, a server's
 * replication check and a test all reach the same verdict.
 */
export const decide = Effect.fn("KnownRepos.decide")(function* (url: string, presented: RepoId) {
  const store = yield* KnownRepos;

  const pinned = yield* store.lookup(url);
  if (pinned !== null) {
    return pinned === presented
      ? ({ kind: "trusted", repoId: presented } as const)
      : ({ kind: "changed", expected: pinned, presented } as const);
  }

  const alias = (yield* store.list).find((entry) => entry.repoId === presented);
  return { kind: "new", repoId: presented, alias: alias?.url ?? null } as const;
});

/**
 * The warning SSH taught everyone to read, for repositories.
 *
 * Rendered here rather than in the CLI so the server and the browser client
 * say the same thing about the same event.
 */
export const mismatchMessage = (url: string, expected: RepoId, presented: RepoId): string =>
  [
    "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@",
    "@    WARNING: REPOSITORY IDENTITY HAS CHANGED!             @",
    "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@",
    "",
    `The repository at ${url} is not the one you trusted.`,
    "",
    `  pinned:    ${expected}`,
    `  presented: ${presented}`,
    "",
    "Either the repository was re-created with a new identity, or",
    "something is answering in its place. Hub operations will fail",
    "until this is resolved:",
    "",
    `  chr33s-git hub forget ${url}`,
    "",
  ].join("\n");

/** The prompt a first sighting produces. */
export const firstUseMessage = (url: string, repoId: RepoId, alias: string | null): string =>
  [
    `The authenticity of repository`,
    `  ${url}`,
    "cannot be established.",
    "",
    `Repository fingerprint:`,
    `  ${repoId}`,
    ...(alias === null ? [] : ["", `This is the repository you already trust as`, `  ${alias}`]),
    "",
  ].join("\n");

// -- serialization ------------------------------------------------------------

/**
 * One entry: `<url> <RepoID>`.
 *
 * Unparseable lines are skipped rather than fatal. This file is hand-editable
 * by design, and refusing to answer any question because line 12 has a typo
 * would take out every other repository a user trusts.
 */
export const parseLine = (line: string): KnownRepo | null => {
  const trimmed = line.trim();
  if (trimmed === "" || trimmed.startsWith("#")) return null;

  const [url, repoId] = trimmed.split(/\s+/);
  if (url === undefined || repoId === undefined) return null;
  return isRepoId(repoId) ? { url, repoId } : null;
};

export const formatLine = (entry: KnownRepo): string => `${entry.url} ${entry.repoId}`;

export const parseFile = (contents: string): ReadonlyArray<KnownRepo> => {
  const entries: KnownRepo[] = [];
  for (const line of contents.split("\n")) {
    const entry = parseLine(line);
    if (entry !== null) entries.push(entry);
  }
  return entries;
};

export const formatFile = (entries: ReadonlyArray<KnownRepo>): string =>
  entries.length === 0 ? "" : `${entries.map(formatLine).join("\n")}\n`;

/**
 * A URL reduced to what identity is keyed by.
 *
 * `https://host/repo`, `https://host/repo/` and `https://host/repo.git` are
 * the same repository, and pinning them separately would prompt a user three
 * times for one repository and then fail to warn when one of the three
 * changed. The scheme and host are kept: the same path on another host is not
 * the same URL, even when it turns out to be the same repository — which is
 * what `alias` is for.
 */
export const canonicalUrl = (url: string): Effect.Effect<string, Invalid> =>
  Effect.try({
    try: () => {
      const value = new URL(url);
      const path = value.pathname.replace(/\.git$/, "").replace(/\/+$/, "");
      return `${value.protocol}//${value.host}${path}`;
    },
    catch: () => new Invalid({ field: "url", reason: `not a URL: '${url}'` }),
  });
