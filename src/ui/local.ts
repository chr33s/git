/**
 * The repository, locally: OPFS as the object and ref stores, the same
 * `Repository` service the server runs, and the server itself demoted to a
 * remote called `origin`.
 *
 * On first load the repository is cloned over smart HTTP into the Origin
 * Private File System (`src/adapters/Opfs.ts` writes the same loose-object
 * layout `git/Node.ts` does), and from then on the Code screen's reads and
 * commits are local: refs, trees, blobs, history and new commits never touch
 * the network. Sync is deliberate — `push()` sends a branch with the same
 * client the CLI uses (`src/client/Push.ts`), `fetchOrigin()` brings the
 * remote's movement in — so "offline" stops being a fallback and becomes the
 * ordinary state between syncs.
 *
 * The class implements the same surface the Code screen already consumes
 * from the HTTP client (`CodeApi` in `api.ts`), so the screen cannot tell
 * which it holds — except through `sync`, which only this one answers.
 *
 * Loaded lazily by the shell: OPFS, the pack machinery and the Effect
 * runtime stay off the boot path, and a browser without OPFS (or a page not
 * in a secure context) simply never swaps away from the HTTP client.
 */
import { Effect, Layer, Stream } from "effect";

import * as Opfs from "../adapters/Opfs.ts";
import * as Client from "../client/Client.ts";
import { fetchRepository } from "../client/Fetch.ts";
import { push as pushBranches, type PushResult } from "../client/Push.ts";
import { isBinary, unified } from "../git/Diff.ts";
import { isGitlink, isTree, type Signature } from "../git/Format.ts";
import { cherryPick as replayCommit, rebase as replayBranch } from "../git/Rebase.ts";
import { next as bisectNext } from "../git/Bisect.ts";
import { forPath as pathHistory } from "../git/History.ts";
import { Repository, treeAt } from "../git/Repository.ts";
import { ObjectStore, RefStore, type Oid } from "../git/Store.ts";

import {
  ApiError,
  type CommitDetail,
  type CommitFilesRequest,
  qualify,
  type CommitCreated,
  type CommitSummary,
  type DiffFile,
  type FileEntry,
  type GrepResponse,
  type Ref,
} from "./api.ts";
import type { BisectAnswer, ReplayResult } from "../server/ApiContract.ts";

import { authorizeSmartHttp } from "./identity.ts";
import type { SyncState } from "./api.ts";

export type { PushResult, SyncState };

const HEADS = "refs/heads/";
const REMOTE = "refs/remotes/origin/";

const encoder = new TextEncoder();
const decoder = new TextDecoder();

const bytesOf = (content: string, encoding: "utf8" | "base64" | undefined): Uint8Array => {
  if (encoding !== "base64") return encoder.encode(content);
  const binary = atob(content);
  const bytes = new Uint8Array(binary.length);
  for (let index = 0; index < binary.length; index += 1) bytes[index] = binary.charCodeAt(index);
  return bytes;
};

const STATUS = {
  Invalid: 400,
  ObjectNotFound: 404,
  RefConflict: 409,
  PackCorrupt: 502,
  StorageFailure: 500,
} satisfies Record<string, number>;

const statusFor = (tag: string): number =>
  // SAFETY: guarded by the `in` check on the very literal being indexed.
  tag in STATUS ? STATUS[tag as keyof typeof STATUS] : 500;

/**
 * A local failure in the shape every screen already handles.
 *
 * The screens narrow on `ApiError` and switch on its `tag` — a local
 * `RefConflict` must read exactly like the server's, because it means the
 * same thing: somebody moved the branch while the editor was open.
 */
/**
 * The shape every failure in this repository's error channel shares: a
 * `Data.TaggedError` with its tag, an `Error` message, and — on `Invalid`
 * and its relatives — a `reason` carrying the human detail.
 */
interface TaggedFailure {
  readonly _tag: string;
  readonly message?: string;
  readonly reason?: string;
}

const toApiError = (error: TaggedFailure): ApiError => {
  const message =
    error.reason ??
    (error.message === undefined || error.message === "" ? error._tag : error.message);
  return new ApiError({ tag: error._tag, status: statusFor(error._tag), message });
};

export class LocalGitApi {
  readonly repo: string;
  readonly cloneUrl: string;

  /** Who local commits are authored as; the shell updates it from `/whoami`. */
  author: { name: string; email: string };

  readonly #root: FileSystemDirectoryHandle;

  private constructor(options: {
    readonly repo: string;
    readonly cloneUrl: string;
    readonly root: FileSystemDirectoryHandle;
  }) {
    this.repo = options.repo;
    this.cloneUrl = options.cloneUrl;
    this.#root = options.root;
    this.author = { name: "git+ browser", email: `local@${location.hostname}` };
  }

  /**
   * Open (and on first load, clone) the repository in OPFS.
   *
   * `null` — not an error — when the browser offers no OPFS, when the clone
   * finds an unreachable remote on an empty store, or when anything else
   * keeps the local copy from being trustworthy: the caller keeps the HTTP
   * client and the page behaves as it always has.
   */
  static async open(options: {
    readonly repo: string;
    readonly cloneUrl: string;
  }): Promise<LocalGitApi | null> {
    if (globalThis.navigator?.storage?.getDirectory === undefined) {
      return null;
    }
    try {
      const origin = await navigator.storage.getDirectory();
      const scope = await origin.getDirectoryHandle("git-plus", { create: true });
      const root = await scope.getDirectoryHandle(options.repo, { create: true });
      const local = new LocalGitApi({ ...options, root });
      // Ask the browser to keep this origin's storage out of eviction's
      // reach: an evicted un-pushed commit is real data loss. Advisory — a
      // refusal changes nothing about how the page behaves.
      void navigator.storage.persist?.().catch(() => false);

      const cloned = (await local.refs()).some((ref) => ref.name.startsWith(HEADS));
      if (!cloned) {
        // First load: clone. An empty answer from an empty repository is
        // fine — local commits can still be made and pushed — but a remote
        // that cannot be reached leaves nothing to work on, so stay remote.
        const fetched = await local.#run(
          Effect.gen(function* () {
            const target = { objects: yield* ObjectStore, refs: yield* RefStore };
            return yield* fetchRepository({
              url: options.cloneUrl,
              stores: target,
              authorize: authorizeSmartHttp,
            });
          }),
        );
        const head = fetched.defaultBranch ?? "main";
        await local.#run(
          Effect.gen(function* () {
            const repository = yield* Repository;
            yield* repository.setHead(`${HEADS}${head}`);
          }),
        );
        // Only here, off the clone: the branches just written *are* origin's,
        // from the very advertisement that produced them. On every later
        // open the tracking refs are left exactly where the last clone,
        // fetch or push observed origin — copying local heads over them on
        // reload was how an unpushed commit read as "nothing to push" after
        // every refresh, with Push disabled over exactly the work that
        // needed it.
        await local.#mirror();
      }
      return local;
    } catch {
      return null;
    }
  }

  /**
   * The repository service plus the bare stores, per call — `Repository` for
   * the read/write surface, the stores themselves for `fetchRepository`,
   * which streams a pack straight into them.
   */
  get #layer() {
    const stores = Opfs.stores(this.#root);
    return Layer.mergeAll(Client.local(stores), stores);
  }

  /**
   * One writer at a time, across tabs.
   *
   * OPFS writes are atomic per file, but a ref update is a read-check-write
   * — two tabs committing at once can race it. The Web Locks API serializes
   * the mutating operations under one origin-wide name; a browser without
   * it falls through to the direct call, which is no worse than before.
   */
  async #locked<A>(work: () => Promise<A>): Promise<A> {
    const locks = globalThis.navigator?.locks;
    if (locks === undefined) return await work();
    return await locks.request(`git-plus:${this.repo}`, work);
  }

  async #run<A, E extends TaggedFailure>(
    effect: Effect.Effect<A, E, Repository | ObjectStore | RefStore>,
  ): Promise<A> {
    const outcome = await Effect.runPromise(
      effect.pipe(
        Effect.provide(this.#layer),
        Effect.map((value) => ({ ok: true as const, value })),
        Effect.catch((error: E) => Effect.succeed({ ok: false as const, error })),
      ),
    );
    if (!outcome.ok) throw toApiError(outcome.error);
    return outcome.value;
  }

  /**
   * Record origin's view of each local branch, for ahead/behind.
   *
   * Called exactly once, after the first clone — the moment local branches
   * and origin's are the same thing by construction. Afterwards a tracking
   * ref is an *observation* of origin and moves only when origin was
   * actually observed: a successful push (`#push`) and a fetch
   * (`#fetchOrigin`) each record what the wire established, and nothing
   * ever copies a local head over one.
   */
  async #mirror(): Promise<void> {
    await this.#run(
      Effect.gen(function* () {
        const repository = yield* Repository;
        const refs = yield* repository.refs;
        for (const [name, oid] of refs) {
          if (!name.startsWith(HEADS)) continue;
          yield* repository
            .setRef({ name: `${REMOTE}${name.slice(HEADS.length)}`, to: oid })
            .pipe(Effect.ignore);
        }
      }),
    );
  }

  // -- the surface the Code screen reads ------------------------------------

  async refs(): Promise<readonly Ref[]> {
    return await this.#run(
      Effect.gen(function* () {
        const repository = yield* Repository;
        const pairs = yield* repository.refs;
        return pairs
          .filter(([name]) => name.startsWith("refs/heads/") || name.startsWith("refs/tags/"))
          .map(([name, oid]) => ({ name, oid }));
      }),
    );
  }

  async files(ref: string): Promise<readonly FileEntry[]> {
    return await this.#run(
      Effect.gen(function* () {
        const repository = yield* Repository;
        const start = yield* repository.resolve(qualify(ref));
        if (start === null) return [];
        const tree = yield* treeAt(repository, start);

        const out: Array<FileEntry> = [];
        const walk: Array<{ readonly oid: Oid; readonly prefix: string }> = [
          { oid: tree, prefix: "" },
        ];
        while (walk.length > 0) {
          const at = walk.pop();
          if (at === undefined) break;
          for (const entry of yield* repository.readTree(at.oid)) {
            if (isGitlink(entry.mode)) continue;
            const path = `${at.prefix}${entry.name}`;
            if (isTree(entry.mode)) walk.push({ oid: entry.oid, prefix: `${path}/` });
            else out.push({ path, mode: entry.mode, oid: entry.oid });
          }
        }
        return out.sort((a, b) => (a.path < b.path ? -1 : 1));
      }),
    );
  }

  async file(ref: string, path: string): Promise<string> {
    return await this.#run(
      Effect.gen(function* () {
        const repository = yield* Repository;
        const start = yield* repository.resolve(qualify(ref));
        if (start === null) {
          return yield* Effect.fail({ _tag: "Invalid", reason: `unknown ref '${ref}'` } as const);
        }
        let tree = yield* treeAt(repository, start);
        const parts = path.split("/");
        const leaf = parts.pop() ?? "";
        for (const part of parts) {
          const next = (yield* repository.readTree(tree)).find(
            (entry) => entry.name === part && isTree(entry.mode),
          );
          if (next === undefined) {
            return yield* Effect.fail({
              _tag: "ObjectNotFound",
              reason: `no '${path}' at ${ref}`,
            } as const);
          }
          tree = next.oid;
        }
        const entry = (yield* repository.readTree(tree)).find((item) => item.name === leaf);
        if (entry === undefined || isTree(entry.mode)) {
          return yield* Effect.fail({
            _tag: "ObjectNotFound",
            reason: `no '${path}' at ${ref}`,
          } as const);
        }
        return decoder.decode(yield* repository.readBlob(entry.oid));
      }),
    );
  }

  async commitFiles(options: Readonly<CommitFilesRequest>): Promise<CommitCreated> {
    const author = this.author;
    return await this.#locked(async () => await this.#commitFiles(options, author));
  }

  async #commitFiles(
    options: Readonly<CommitFilesRequest>,
    author: { name: string; email: string },
  ): Promise<CommitCreated> {
    return await this.#run(
      Effect.gen(function* () {
        const repository = yield* Repository;
        const branch = options.branch.startsWith("refs/")
          ? options.branch
          : `${HEADS}${options.branch}`;
        const base = yield* repository.resolve(branch);
        const baseTree = base === null ? undefined : (yield* repository.readCommit(base)).tree;

        const changes = options.files.map((file) => ({
          path: file.path,
          content: file.content === null ? null : bytesOf(file.content, file.encoding),
        }));
        const tree =
          baseTree === undefined
            ? yield* repository.writeFiles({ changes })
            : yield* repository.writeFiles({ base: baseTree, changes });

        const at: Signature = {
          ...author,
          at: new Date(),
          offset: -new Date().getTimezoneOffset(),
        };
        const oid = yield* repository.commit({
          branch,
          tree,
          message: options.message,
          author: at,
          // The editor pins the tip it opened at; absent that, the tip the
          // tree was layered onto — the same compare-and-swap the server
          // applies, because a lost race is the same lie locally.
          //
          // SAFETY: `expected` came from this same store's ref answers, which
          // only ever hand out oids; the store re-checks the swap either way.
          expected: (options.expected as Oid | undefined) ?? base,
        });
        return { oid, tree };
      }),
    );
  }

  async branchCreate(name: string, base: string): Promise<Ref> {
    return await this.#locked(
      async () =>
        await this.#run(
          Effect.gen(function* () {
            const repository = yield* Repository;
            const oid = yield* repository.branch({ name, base: qualify(base) });
            return { name: `${HEADS}${name}`, oid };
          }),
        ),
    );
  }

  async commitDetail(oid: string): Promise<CommitDetail> {
    return await this.#run(
      Effect.gen(function* () {
        const repository = yield* Repository;
        // SAFETY: the oid was handed out by this store's own refs/log; a bad
        // one fails inside as `ObjectNotFound`, which is the honest answer.
        const commit = yield* repository.readCommit(oid as Oid);
        return {
          oid,
          subject: (commit.message.split("\n", 1)[0] ?? "").trim(),
          author: commit.author.name,
          email: commit.author.email,
          at: commit.author.at,
          parents: commit.parents,
        };
      }),
    );
  }

  async recentCommits(oid: string, limit = 30): Promise<readonly CommitDetail[]> {
    return await this.#run(
      Effect.gen(function* () {
        const repository = yield* Repository;
        // SAFETY: as in `commitDetail` — store-issued, and checked by the read.
        const commits = yield* Stream.runCollect(repository.log(oid as Oid, { limit }));
        return commits.map((commit) => ({
          oid: commit.oid,
          subject: (commit.message.split("\n", 1)[0] ?? "").trim(),
          author: commit.author.name,
          email: commit.author.email,
          at: commit.author.at,
          parents: commit.parents,
        }));
      }),
    );
  }

  async history(oid: string, path: string, limit = "20"): Promise<readonly CommitSummary[]> {
    const bound = Number.parseInt(limit, 10);
    return await this.#run(
      Effect.gen(function* () {
        // SAFETY: as in `commitDetail` — store-issued, and checked by the read.
        const commits = yield* Stream.runCollect(
          pathHistory(oid as Oid, path, { limit: Number.isInteger(bound) ? bound : 20 }),
        );
        return commits.map((commit) => ({ oid: commit.oid, message: commit.message }));
      }),
    );
  }

  // -- sync -----------------------------------------------------------------

  /**
   * Where `branch` stands against origin, counted by walking the local
   * graph. Bounded: a branch more than 250 commits apart reads as 250 — the
   * badge's job is "there is something to push", not exact arithmetic.
   */
  async sync(branch: string): Promise<SyncState> {
    return await this.#run(
      Effect.gen(function* () {
        const repository = yield* Repository;
        const local = yield* repository.resolve(`${HEADS}${branch}`);
        const remote = yield* repository.resolve(`${REMOTE}${branch}`);
        if (local === null) return { branch, ahead: 0, behind: 0, remote };

        /** The recent history of `from`, as a membership question. */
        const reachable = (from: Oid | null) =>
          from === null
            ? Effect.succeed(new Set<Oid>())
            : Stream.runCollect(repository.log(from, { limit: 250 })).pipe(
                Effect.map((commits) => new Set(commits.map((commit) => commit.oid))),
              );

        /**
         * Commits reachable from `from` before *any* commit of the other
         * side appears. Counting until the other side's exact tip — the
         * previous rule — miscounted both directions the moment either side
         * moved: a branch one commit ahead read its origin as one commit
         * "behind", because origin's tip is an ancestor, not the local tip.
         */
        const countUntil = (from: Oid, seen: ReadonlySet<Oid>) =>
          Stream.runCollect(repository.log(from, { limit: 250 })).pipe(
            Effect.map((commits) => {
              let count = 0;
              for (const commit of commits) {
                if (seen.has(commit.oid)) return count;
                count += 1;
              }
              return count;
            }),
          );

        const localSeen = yield* reachable(local);
        const remoteSeen = yield* reachable(remote);
        return {
          branch,
          ahead: yield* countUntil(local, remoteSeen),
          behind: remote === null ? 0 : yield* countUntil(remote, localSeen),
          remote,
        };
      }),
    );
  }

  /** Push one branch to origin — the manual sync the commit bar offers. */
  async push(branch: string): Promise<readonly PushResult[]> {
    return await this.#locked(async () => await this.#push(branch));
  }

  async #push(branch: string): Promise<readonly PushResult[]> {
    const url = this.cloneUrl;
    const results = await this.#run(
      pushBranches({
        url,
        refs: [{ local: `${HEADS}${branch}`, remote: `${HEADS}${branch}` }],
        authorize: authorizeSmartHttp,
      }),
    );
    if (results.every((result) => result.ok)) {
      await this.#run(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const tip = yield* repository.resolve(`${HEADS}${branch}`);
          if (tip !== null) {
            yield* repository.setRef({ name: `${REMOTE}${branch}`, to: tip }).pipe(Effect.ignore);
          }
        }),
      );
    }
    return results;
  }

  /**
   * Bring origin's movement in: fast-forward the local branches that can be,
   * and record origin's tips either way so `sync` reports divergence
   * honestly rather than not at all.
   */
  async fetchOrigin(): Promise<{ readonly updated: number; readonly rejected: number }> {
    return await this.#locked(async () => await this.#fetchOrigin());
  }

  async #fetchOrigin(): Promise<{ readonly updated: number; readonly rejected: number }> {
    const url = this.cloneUrl;
    // One advertisement, one negotiation, one pack — carrying two refspecs.
    // The tracking half is forced because it *is* origin's state, observed;
    // the local half fast-forwards what it can and rejects the rest, so a
    // divergent local branch keeps its commits while its tracking ref
    // advances and `sync` reports the divergence truthfully.
    const fetched = await this.#run(
      Effect.gen(function* () {
        const target = { objects: yield* ObjectStore, refs: yield* RefStore };
        return yield* fetchRepository({
          url,
          stores: target,
          authorize: authorizeSmartHttp,
          refspecs: [
            { force: false, source: "refs/heads/*", destination: "refs/heads/*" },
            { force: true, source: "refs/heads/*", destination: `${REMOTE}*` },
          ],
        });
      }),
    );
    return {
      updated: fetched.refs.filter((ref) => ref.name.startsWith(HEADS)).length,
      rejected: fetched.rejected.filter((ref) => ref.name.startsWith(HEADS)).length,
    };
  }

  // -- the local halves of the remaining read surface -----------------------

  /**
   * Search blob contents at a ref — literal and case-insensitive, exactly
   * the promise the server's `/grep` makes to the same search box.
   */
  async grep(pattern: string, ref: string, maxMatches = 50): Promise<GrepResponse> {
    const needle = pattern.toLowerCase();
    return await this.#run(
      Effect.gen(function* () {
        const repository = yield* Repository;
        const start = yield* repository.resolve(qualify(ref));
        if (start === null) return { matches: [], truncated: false, skipped: [] };
        const tree = yield* treeAt(repository, start);

        const matches: Array<{ path: string; line: number; text: string }> = [];
        const skipped: Array<string> = [];
        let truncated = false;

        const walk: Array<{ readonly oid: Oid; readonly prefix: string }> = [
          { oid: tree, prefix: "" },
        ];
        while (walk.length > 0 && !truncated) {
          const at = walk.pop();
          if (at === undefined) break;
          for (const entry of yield* repository.readTree(at.oid)) {
            if (truncated) break;
            if (isGitlink(entry.mode)) continue;
            const path = `${at.prefix}${entry.name}`;
            if (isTree(entry.mode)) {
              walk.push({ oid: entry.oid, prefix: `${path}/` });
              continue;
            }
            const bytes = yield* repository.readBlob(entry.oid);
            // The same bound the server applies, for the same reason: a
            // 200 MB log decoded to a string is the tab's memory, not an
            // answer.
            if (bytes.length > 4 * 1024 * 1024) {
              skipped.push(path);
              continue;
            }
            if (isBinary(bytes)) continue;
            const lines = decoder.decode(bytes).split("\n");
            for (let index = 0; index < lines.length; index += 1) {
              const text = lines[index] ?? "";
              if (!text.toLowerCase().includes(needle)) continue;
              matches.push({ path, line: index + 1, text });
              if (matches.length >= maxMatches) {
                truncated = true;
                break;
              }
            }
          }
        }
        return { matches, truncated, skipped };
      }),
    );
  }

  /**
   * The patch set between two revisions, computed here: both trees are in
   * OPFS, and `git/Diff.ts` is the same unified-patch writer the server
   * answers with.
   */
  async diff(from: string, to: string): Promise<readonly DiffFile[]> {
    const filesAt = (ref: string) => this.files(ref);
    const readText = (ref: string, path: string) => this.file(ref, path);
    const before = new Map((await filesAt(from)).map((entry) => [entry.path, entry]));
    const after = new Map((await filesAt(to)).map((entry) => [entry.path, entry]));

    const out: Array<DiffFile> = [];
    const paths = new Set([...before.keys(), ...after.keys()]);
    for (const path of [...paths].sort()) {
      const was = before.get(path);
      const is = after.get(path);
      if (was !== undefined && is !== undefined && was.oid === is.oid) continue;
      const status = was === undefined ? "added" : is === undefined ? "removed" : "modified";
      const oldText = was === undefined ? "" : await readText(from, path);
      const newText = is === undefined ? "" : await readText(to, path);
      out.push({ path, status, binary: false, patch: unified(oldText, newText) });
    }
    return out;
  }

  /** Replay one commit onto a branch, locally, moving the branch. */
  async cherryPick(commit: string, onto: string): Promise<ReplayResult> {
    return await this.#locked(
      async () =>
        await this.#run(
          replayCommit({ commit: qualify(commit), onto: qualify(onto), into: qualify(onto) }),
        ),
    );
  }

  /** The next revision to test between marks — the same walk `/bisect` runs. */
  async bisect(good: readonly string[], bad: string): Promise<BisectAnswer> {
    // SAFETY: the marks came from this store's own commit listings.
    return await this.#run(bisectNext({ bad: bad as Oid, good: good as readonly Oid[] }));
  }

  /** Replay a branch onto another, locally, moving the branch. */
  async rebase(branch: string, onto: string): Promise<ReplayResult> {
    return await this.#locked(
      async () =>
        await this.#run(
          replayBranch({ branch: qualify(branch), onto: qualify(onto), into: qualify(branch) }),
        ),
    );
  }
}
