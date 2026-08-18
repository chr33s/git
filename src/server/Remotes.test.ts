/**
 * The remote registry, and the endpoints that turn this server into a client
 * of another one.
 *
 * The port half is checked on all three backends — the credential is the
 * thing to watch, and the assertion that carries the design is that no read
 * path the API can reach ever produces it.
 *
 * The rest is end to end and deliberately so: two repositories served by
 * `host/Node.ts`, a push from one into the other through `POST /:repo/push`
 * over real HTTP, and a fetch back. `git fsck --strict` and `git log` on the
 * receiving directory are the independent reader — "the objects arrived and
 * are still git objects" is a claim about git's data model, not about ours.
 *
 * The pull cases are the point of the file: a fast-forward moves the branch,
 * and a divergence reports and leaves it alone.
 */
import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { DatabaseSync } from "node:sqlite";
import { promisify } from "node:util";
import { afterAll, beforeAll, describe, it } from "@effect/vitest";

import { Effect } from "effect";

import { remote } from "../client/Client.ts";
import type { Sql } from "../git/Sql.ts";
import { serve, type Server } from "../host/Node.ts";
import { enableHubUnder } from "../testing/Hub.ts";
import { hasGit } from "../testing/Git.ts";
import { file as remotesFile } from "./Remotes.node.ts";
import * as Remotes from "./Remotes.ts";

const execFileAsync = promisify(execFile);

/**
 * Async on purpose: the servers under test run on this process's event loop,
 * so a synchronous `git` invocation would deadlock — the binary waiting on a
 * response the blocked loop can never produce.
 */
const git = async (cwd: string, ...args: string[]): Promise<string> => {
  const result = await execFileAsync(
    "git",
    ["-c", "user.name=Test", "-c", "user.email=test@example.com", ...args],
    { cwd, encoding: "utf8", env: { ...process.env, GIT_TERMINAL_PROMPT: "0" } },
  );
  return result.stdout;
};

const author = {
  name: "Alice",
  email: "alice@example.com",
  at: new Date(1_700_000_000_000).toISOString(),
  offset: 0,
};

/**
 * The credential the guarded server accepts.
 *
 * Assigned once the repository behind that server has a genesis and a member:
 * there is no server-side secret to invent one from any more, so the
 * credential has to come from a key that repository actually trusts.
 */
let TOKEN = "";

let root: string;
let server: Server;
/** A second server, behind `Auth.guard`, for the credential to matter to. */
let guarded: Server;

beforeAll(async () => {
  root = await fs.mkdtemp(path.join(os.tmpdir(), "server-remotes-"));
  server = await serve({ root: path.join(root, "open"), allowAnonymousWrites: true });
  guarded = await serve({ root: path.join(root, "guarded"), allowAnonymousWrites: true });
  const member = await enableHubUnder(path.join(root, "guarded"), "received", [
    "repo.read",
    "source.push",
  ]);
  TOKEN = member.credential;
});

afterAll(async () => {
  await server.close();
  await guarded.close();
  await fs.rm(root, { recursive: true, force: true });
});

/**
 * A repository directory git will look at. Nothing in these flows writes
 * `HEAD` — a bare repository this server hosts is only ever read over the
 * protocol, which carries the default branch in the advertisement — and
 * without it git says "not a git repository" before it says anything useful.
 */
const gitDir = async (repo: string): Promise<string> => {
  const directory = path.join(root, "open", repo);
  await fs.writeFile(path.join(directory, "HEAD"), "ref: refs/heads/main\n");
  return directory;
};

const commit = (repo: string, file: string, content: string, message: string) =>
  Effect.gen(function* () {
    const api = yield* remote(server.url);
    const created = yield* api.repo.create({
      params: { repo },
      payload: { message, author, files: [{ path: file, content }] },
    });
    return created.oid;
  }).pipe(Effect.scoped);

describe("Remotes", () => {
  it.effect("keeps a credential and never hands it back", () =>
    Effect.gen(function* () {
      const registry = yield* Remotes.Remotes;
      const added = yield* registry.add({
        name: "origin",
        url: "https://example.com/repo.git",
        credential: "s3cret",
      });

      // The port holds it, because the fetch path is what needs it.
      assert.equal(added.credential, "s3cret");
      assert.equal((yield* registry.get("origin"))?.credential, "s3cret");

      // What the API is allowed to say, and what it cannot.
      assert.deepEqual(Remotes.redact(added), {
        name: "origin",
        url: "https://example.com/repo.git",
        has_credential: true,
        // Nothing was asked for, so nothing happens to this remote on its own.
        sync: null,
        created_at: added.createdAt.toISOString(),
      });
      const rows = yield* registry.list;
      assert.equal(JSON.stringify(rows.map(Remotes.redact)).includes("s3cret"), false);

      assert.equal(yield* registry.remove("origin"), true);
      assert.equal(yield* registry.remove("origin"), false);
      assert.deepEqual(yield* registry.list, []);
    }).pipe(Effect.provide(Remotes.memory)),
  );

  it("reads a hand-edited standing instruction it cannot make sense of as none", async () => {
    // The file is a repository's, and repositories get edited and get carried
    // between versions. Trusted as written, a `refs` that is not a list is a
    // shape the forwarder walks straight into — on `post-receive`, where
    // nothing is watching. The SQL registry already decoded it with a schema;
    // the file registry now decodes it with the same one.
    const directory = await fs.mkdtemp(path.join(os.tmpdir(), "remotes-file-"));
    try {
      const location = path.join(directory, "remotes.json");
      await fs.writeFile(
        location,
        JSON.stringify([
          {
            name: "bent",
            url: "https://example.com/repo.git",
            credential: null,
            sync: { mode: "push", refs: "refs/heads/main" },
            createdAt: new Date(0).toISOString(),
          },
        ]),
      );

      const held = await Effect.runPromise(
        Effect.flatMap(Remotes.Remotes, (registry) => registry.list).pipe(
          Effect.provide(remotesFile(location)),
        ),
      );

      assert.equal(
        held[0]?.sync,
        null,
        "unreadable reads as manual, not as a shape nothing checked",
      );
      assert.equal(Remotes.sends(held[0]!), false);
    } finally {
      await fs.rm(directory, { recursive: true, force: true });
    }
  });

  it.effect("reads a remote with no standing instruction as having none", () =>
    Effect.gen(function* () {
      // `undefined` is a legal way to write "no opinion", and a spread carries
      // it straight over a default — so `sync` arrived as `undefined` where
      // every reader had been promised `Sync | null`, and the one that decides
      // whether to forward reads `.mode` off it.
      const registry = yield* Remotes.Remotes;
      const [held] = yield* registry.list;
      assert.equal(held?.sync, null);
      assert.equal(Remotes.sends(held!), false);
    }).pipe(
      Effect.provide(
        Remotes.of([{ name: "quiet", url: "https://example.com/repo.git", sync: undefined }]),
      ),
    ),
  );

  it.effect("refuses a standing instruction nothing here can carry out", () =>
    Effect.gen(function* () {
      // Nothing drives a scheduled fetch, so storing one would leave a remote
      // configured to do something that never happens — configuration that
      // reads as working and is not. `push` is the half that has a trigger.
      const registry = yield* Remotes.Remotes;
      const refused = yield* registry
        .add({
          name: "scheduled",
          url: "https://example.com/repo.git",
          sync: { mode: "fetch", refs: [] },
        })
        .pipe(
          Effect.as(null),
          Effect.catchTag("Invalid", (error) => Effect.succeed(error.reason)),
        );
      const taken = yield* registry.add({
        name: "forwarded",
        url: "https://example.com/other.git",
        sync: { mode: "push", refs: [] },
      });

      assert.match(refused ?? "", /not implemented/);
      assert.equal(taken.sync?.mode, "push", "the half that does have a trigger is stored");
    }).pipe(Effect.provide(Remotes.memory)),
  );

  it.effect("refuses a second remote under the same name", () =>
    Effect.gen(function* () {
      const registry = yield* Remotes.Remotes;
      yield* registry.add({ name: "origin", url: "https://example.com/a.git" });
      const failure = yield* registry
        .add({ name: "origin", url: "https://example.com/b.git" })
        .pipe(Effect.flip);
      if (failure._tag !== "Invalid") assert.fail(`expected Invalid, got ${failure._tag}`);
      assert.match(failure.reason, /already exists/);
      // The first one is untouched: a refused registration changes nothing.
      assert.equal((yield* registry.get("origin"))?.url, "https://example.com/a.git");
    }).pipe(Effect.provide(Remotes.memory)),
  );

  it.effect("refuses names and URLs a fetch could not survive", () =>
    Effect.gen(function* () {
      const registry = yield* Remotes.Remotes;
      const refused = (input: Remotes.NewRemote) =>
        registry.add(input).pipe(
          Effect.flip,
          Effect.map((failure) => (failure._tag === "Invalid" ? failure.field : failure._tag)),
        );

      // The name becomes `refs/remotes/<name>/main`.
      assert.equal(yield* refused({ name: "", url: "https://example.com/r.git" }), "name");
      assert.equal(yield* refused({ name: "a b", url: "https://example.com/r.git" }), "name");
      assert.equal(yield* refused({ name: "../evil", url: "https://example.com/r.git" }), "name");
      assert.equal(yield* refused({ name: "a/b", url: "https://example.com/r.git" }), "name");

      assert.equal(yield* refused({ name: "origin", url: "not a url" }), "url");
      assert.equal(yield* refused({ name: "origin", url: "http://example.com/r.git" }), "url");
      assert.equal(yield* refused({ name: "origin", url: "file:///etc/passwd" }), "url");
      // Userinfo would come straight back out of `list`, which is the whole
      // reason a credential is stored separately.
      assert.equal(
        yield* refused({ name: "origin", url: "https://user:pw@example.com/r.git" }),
        "url",
      );

      // Loopback http is how a server reaches the one next to it.
      const local = yield* registry.add({ name: "next-door", url: "http://127.0.0.1:8080/r" });
      assert.equal(local.credential, null);
    }).pipe(Effect.provide(Remotes.memory)),
  );

  it.effect("has a registry that stores nothing, and one that is stated inline", () =>
    Effect.gen(function* () {
      yield* Effect.provide(
        Effect.gen(function* () {
          const empty = yield* Remotes.Remotes;
          assert.deepEqual(yield* empty.list, []);
          assert.equal(yield* empty.get("origin"), null);
          assert.equal(yield* empty.remove("origin"), false);
          // Read-only, and it says so as a storage failure rather than
          // pretending the registration worked.
          const failure = yield* empty
            .add({ name: "o", url: "https://example.com/r.git" })
            .pipe(Effect.flip);
          assert.equal(failure._tag, "StorageFailure");
        }),
        Remotes.none,
      );

      yield* Effect.provide(
        Effect.gen(function* () {
          const fixed = yield* Remotes.Remotes;
          assert.equal((yield* fixed.get("origin"))?.url, "https://example.com/r.git");
          assert.equal((yield* fixed.get("origin"))?.credential, null);
          assert.equal((yield* fixed.list).length, 1);
        }),
        Remotes.of([{ name: "origin", url: "https://example.com/r.git" }]),
      );
    }),
  );

  it.effect("survives the process in a file, one repository at a time in SQL", () =>
    Effect.gen(function* () {
      const location = path.join(root, "registry", "remotes.json");

      const written = yield* Effect.provide(
        Effect.gen(function* () {
          const registry = yield* Remotes.Remotes;
          return yield* registry.add({
            name: "origin",
            url: "https://example.com/r.git",
            credential: "s3cret",
          });
        }),
        remotesFile(location),
      );

      // A second layer over the same file is a second process, as far as the
      // registry is concerned.
      const reread = yield* Effect.provide(
        Effect.gen(function* () {
          const registry = yield* Remotes.Remotes;
          // The name is the key across processes too, not only within one.
          const failure = yield* registry
            .add({ name: "origin", url: "https://example.com/other.git" })
            .pipe(Effect.flip);
          assert.equal(failure._tag, "Invalid");
          return yield* registry.get("origin");
        }),
        remotesFile(location),
      );
      assert.deepEqual(reread, written);
      // Nothing partial is ever left behind by the temp-and-rename write.
      assert.deepEqual((yield* Effect.promise(() => fs.readdir(path.dirname(location)))).sort(), [
        "remotes.json",
      ]);

      const database = new DatabaseSync(":memory:");
      const sql: Sql = {
        exec: <Row extends Record<string, ArrayBuffer | string | number | null>>(
          query: string,
          ...bindings: ReadonlyArray<string | number | null>
        ) => {
          const kind = query.trimStart().slice(0, 6).toUpperCase();
          if (kind === "CREATE" || kind === "DROP") {
            database.exec(query);
            return { toArray: (): Row[] => [] };
          }
          const statement = database.prepare(query);
          if (kind !== "SELECT") {
            statement.run(...bindings);
            return { toArray: (): Row[] => [] };
          }
          // SAFETY: the `Sql` port's caller names the row type its own query
          // produces; `node:sqlite` returns rows it cannot type any closer.
          return { toArray: () => statement.all(...bindings) as Row[] };
        },
      };

      const stored = yield* Effect.provide(
        Effect.gen(function* () {
          const registry = yield* Remotes.Remotes;
          yield* registry.add({ name: "origin", url: "https://example.com/r.git" });
          return yield* registry.list;
        }),
        Remotes.sql(sql, "one"),
      );
      assert.deepEqual(
        stored.map((row) => row.name),
        ["origin"],
      );
      // The table is keyed by repository as well as name: another repository
      // on the same database sees none of it.
      const other = yield* Effect.provide(
        Effect.gen(function* () {
          return yield* (yield* Remotes.Remotes).list;
        }),
        Remotes.sql(sql, "two"),
      );
      assert.deepEqual(other, []);
      database.close();
    }),
  );
});

describe("Remotes, over HTTP", () => {
  it.live("registers a remote, and never returns its credential", () =>
    Effect.gen(function* () {
      const api = yield* remote(server.url);
      const added = yield* api.remotes.remoteAdd({
        params: { repo: "registered" },
        payload: { name: "origin", url: `${server.url}/other`, credential: TOKEN },
      });
      assert.equal(added.has_credential, true);
      assert.equal(JSON.stringify(added).includes(TOKEN), false);

      const listed = yield* api.remotes.remoteList({ params: { repo: "registered" } });
      assert.equal(JSON.stringify(listed).includes(TOKEN), false);
      assert.deepEqual(
        listed.remotes.map((row) => [row.name, row.url, row.has_credential]),
        [["origin", `${server.url}/other`, true]],
      );

      assert.deepEqual(
        yield* api.remotes.remoteRemove({ params: { repo: "registered", name: "origin" } }),
        { deleted: true },
      );
      assert.deepEqual(
        yield* api.remotes.remoteRemove({ params: { repo: "registered", name: "origin" } }),
        { deleted: false },
      );
    }).pipe(Effect.scoped),
  );

  it.live("refuses an operation that names no remote, or two", () =>
    Effect.gen(function* () {
      const api = yield* remote(server.url);
      const refused = (payload: { name?: string; url?: string }) =>
        api.remotes.fetch({ params: { repo: "ambiguous" }, payload }).pipe(
          Effect.flip,
          Effect.map((failure) => failure._tag),
        );

      assert.equal(yield* refused({}), "Invalid");
      assert.equal(yield* refused({ name: "origin", url: `${server.url}/x` }), "Invalid");
      // Named but not registered, which is a different mistake and says so.
      assert.equal(yield* refused({ name: "origin" }), "Invalid");
    }).pipe(Effect.scoped),
  );

  it.live("sends the stored credential, and only has the one to send", () =>
    Effect.gen(function* () {
      const api = yield* remote(server.url);
      yield* commit("sender", "s.txt", "sent\n", "first");

      yield* api.remotes.remoteAdd({
        params: { repo: "sender" },
        payload: { name: "open", url: `${guarded.url}/received` },
      });
      yield* api.remotes.remoteAdd({
        params: { repo: "sender" },
        payload: { name: "authorized", url: `${guarded.url}/received`, credential: TOKEN },
      });

      // Without it the remote never gets as far as the ref advertisement.
      const denied = yield* api.remotes
        .push({
          params: { repo: "sender" },
          payload: { name: "open", refs: [{ local: "refs/heads/main" }] },
        })
        .pipe(Effect.flip);
      assert.equal(denied._tag, "Invalid");

      const pushed = yield* api.remotes.push({
        params: { repo: "sender" },
        payload: { name: "authorized", refs: [{ local: "refs/heads/main" }] },
      });
      assert.deepEqual(pushed.refs, [{ ref: "refs/heads/main", ok: true, reason: null }]);

      const behind = yield* remote(guarded.url, { token: TOKEN });
      const refs = yield* behind.repo.refs({ params: { repo: "received" } });
      // The guarded repository has trust refs of its own, so the branch that
      // arrived is named rather than counted.
      assert.ok(
        refs.refs.some((ref) => ref.name === "refs/heads/main"),
        `expected the pushed branch among ${refs.refs.map((ref) => ref.name).join(", ")}`,
      );
    }).pipe(Effect.scoped),
  );

  it.live("fetches a branch into a tracking ref, and leaves a tag alone", () =>
    Effect.gen(function* () {
      const api = yield* remote(server.url);
      const head = yield* commit("tagged", "t.txt", "tagged\n", "first");
      yield* api.repo.tagCreate({
        params: { repo: "tagged" },
        payload: { name: "v1", target: head },
      });

      // A tag of the same name, on a different commit, already here.
      const mine = yield* commit("tagger", "m.txt", "mine\n", "mine");
      yield* api.repo.tagCreate({
        params: { repo: "tagger" },
        payload: { name: "v1", target: mine },
      });

      yield* api.remotes.remoteAdd({
        params: { repo: "tagger" },
        payload: { name: "up", url: `${server.url}/tagged` },
      });
      const fetched = yield* api.remotes.fetch({
        params: { repo: "tagger" },
        payload: { name: "up" },
      });

      // The branch lands beside the local one, never on top of it; the tag is
      // a name that already means something here and is not re-pointed.
      assert.deepEqual(
        fetched.refs.map((ref) => ref.name),
        ["refs/remotes/up/main"],
      );
      const refs = yield* api.repo.refs({ params: { repo: "tagger" } });
      assert.equal(refs.refs.find((ref) => ref.name === "refs/tags/v1")?.oid, mine);
      assert.equal(refs.refs.find((ref) => ref.name === "refs/heads/main")?.oid, mine);
      assert.equal(refs.refs.find((ref) => ref.name === "refs/remotes/up/main")?.oid, head);
    }).pipe(Effect.scoped),
  );

  it.live("reports a divergence instead of merging it", () =>
    Effect.gen(function* () {
      const api = yield* remote(server.url);
      const pull = (payload: { name: string; branch: string }) =>
        api.remotes.pull({ params: { repo: "down" }, payload });
      const branchOf = (repo: string, name: string) =>
        api.repo
          .refs({ params: { repo } })
          .pipe(Effect.map(({ refs }) => refs.find((ref) => ref.name === name)?.oid ?? null));

      const first = yield* commit("up", "f.txt", "one\n", "one");
      yield* api.remotes.remoteAdd({
        params: { repo: "down" },
        payload: { name: "up", url: `${server.url}/up` },
      });

      const created = yield* pull({ name: "up", branch: "main" });
      assert.deepEqual(created, {
        kind: "created",
        branch: "refs/heads/main",
        tracking: "refs/remotes/up/main",
        from: null,
        to: first,
        // The commit, its tree and its blob.
        objects: 3,
      });

      // Nothing moved on either side: no pack is even asked for.
      const repeated = yield* pull({ name: "up", branch: "main" });
      assert.equal(repeated.kind, "up-to-date");
      assert.equal(repeated.objects, 0);

      const second = yield* commit("up", "g.txt", "two\n", "two");
      const forwarded = yield* pull({ name: "up", branch: "refs/heads/main" });
      assert.equal(forwarded.kind, "fast-forward");
      assert.equal(forwarded.from, first);
      assert.equal(forwarded.to, second);
      assert.equal(yield* branchOf("down", "refs/heads/main"), second);

      // Now both sides commit on top of the same tip.
      const ours = yield* commit("down", "ours.txt", "ours\n", "ours");
      const theirs = yield* commit("up", "theirs.txt", "theirs\n", "theirs");

      const diverged = yield* pull({ name: "up", branch: "main" });
      assert.equal(diverged.kind, "non-fast-forward");
      assert.equal(diverged.from, ours);
      assert.equal(diverged.to, theirs);

      // The branch is where it was, and no merge commit was written: the
      // tracking ref moved, so a caller can still merge or rebase from it.
      assert.equal(yield* branchOf("down", "refs/heads/main"), ours);
      assert.equal(yield* branchOf("down", "refs/remotes/up/main"), theirs);
      const tip = yield* api.repo.read({ params: { repo: "down", oid: ours } });
      assert.deepEqual(tip.parents, [second]);
      assert.equal(tip.message, "ours");
    }).pipe(Effect.scoped),
  );

  it.live("refuses a branch that is not one, rather than nesting it under heads", () =>
    Effect.gen(function* () {
      // The gate judged what `refNameOf` made of the name and the write used
      // what `Sync.pull` made of it, and the two disagreed: `refs/tags/x` was
      // judged as `refs/tags/x` and written as `refs/heads/refs/tags/x`, so a
      // `source.push` holder could create a ref inside a fully protected
      // `refs/heads/*` namespace without the branch rules ever seeing it.
      const api = yield* remote(server.url);
      // `HEAD` too: the gate leaves it alone — it is already a full ref name —
      // while the write qualified it to `refs/heads/HEAD`, so a ref landed
      // inside a namespace the branch rules were judging under another name.
      const head = yield* api.remotes
        .pull({ params: { repo: "down" }, payload: { name: "up", branch: "HEAD" } })
        .pipe(Effect.flip);
      assert.equal(head._tag, "Invalid");
      assert.match(head.reason, /is not a branch/);

      const failed = yield* api.remotes
        .pull({ params: { repo: "down" }, payload: { name: "up", branch: "refs/tags/x" } })
        .pipe(Effect.flip);
      // Refused for being the wrong *shape*, before anything is fetched — not
      // for the remote happening not to have `refs/heads/refs/tags/x`, which
      // is what the nesting bug produced and is indistinguishable from success
      // on a remote that does.
      assert.equal(failed._tag, "Invalid");
      assert.match(failed.reason, /is not a branch/);

      const { refs } = yield* api.repo.refs({ params: { repo: "down" } });
      assert.equal(
        refs.find((ref) => ref.name.startsWith("refs/heads/refs/")),
        undefined,
        `nothing nested under heads: ${refs.map((ref) => ref.name).join(", ")}`,
      );
    }).pipe(Effect.scoped),
  );
});

describe.skipIf(!hasGit)("Remotes, read back by the git binary", () => {
  it.live("pushes to another repository and fetches back what it grew", () =>
    Effect.gen(function* () {
      const api = yield* remote(server.url);
      const first = yield* commit("source", "hello.txt", "pushed by hand\n", "first");

      yield* api.remotes.remoteAdd({
        params: { repo: "source" },
        payload: { name: "target", url: `${server.url}/target` },
      });
      const pushed = yield* api.remotes.push({
        params: { repo: "source" },
        payload: { name: "target", refs: [{ local: "refs/heads/main" }] },
      });
      assert.deepEqual(pushed.refs, [{ ref: "refs/heads/main", ok: true, reason: null }]);

      // git's own reading of what arrived, from a clone it made itself.
      const work = path.join(root, "work-target");
      yield* Effect.promise(() => git(root, "clone", "--quiet", `${server.url}/target`, work));
      yield* Effect.promise(() => git(work, "fsck", "--strict"));
      assert.equal((yield* Effect.promise(() => git(work, "rev-parse", "HEAD"))).trim(), first);
      assert.equal(
        yield* Effect.promise(() => fs.readFile(path.join(work, "hello.txt"), "utf8")),
        "pushed by hand\n",
      );

      // The other direction: the target grows a commit, and the source takes
      // it — sending its own tip as a `have`, so only the new objects travel.
      const second = yield* commit("target", "there.txt", "from over there\n", "second");
      const fetched = yield* api.remotes.fetch({
        params: { repo: "source" },
        payload: { name: "target" },
      });
      assert.equal(fetched.remote, "target");
      assert.deepEqual(fetched.refs, [
        { name: "refs/remotes/target/main", oid: second, from: null },
      ]);
      assert.equal(fetched.objects, 3);

      const directory = yield* Effect.promise(() => gitDir("source"));
      const inSource = (...args: string[]) =>
        Effect.promise(() => git(root, `--git-dir=${directory}`, ...args));
      yield* inSource("fsck", "--strict", "--no-dangling");
      assert.equal((yield* inSource("rev-parse", "refs/remotes/target/main")).trim(), second);
      // Readable by git, which means the objects behind the ref came too.
      assert.deepEqual(
        (yield* inSource("log", "--format=%s", "refs/remotes/target/main")).trim().split("\n"),
        ["second", "first"],
      );
    }).pipe(Effect.scoped),
  );

  it.live("fetches only what a ref filter names", () =>
    Effect.gen(function* () {
      const api = yield* remote(server.url);
      const main = yield* commit("many", "m.txt", "main\n", "main");
      yield* api.repo.branch({
        params: { repo: "many" },
        payload: { name: "topic", base: "refs/heads/main" },
      });

      yield* api.remotes.remoteAdd({
        params: { repo: "picky" },
        payload: { name: "up", url: `${server.url}/many` },
      });
      const fetched = yield* api.remotes.fetch({
        params: { repo: "picky" },
        payload: { name: "up", refs: ["topic"] },
      });
      assert.deepEqual(fetched.refs, [{ name: "refs/remotes/up/topic", oid: main, from: null }]);

      const directory = yield* Effect.promise(() => gitDir("picky"));
      yield* Effect.promise(() =>
        git(root, `--git-dir=${directory}`, "fsck", "--strict", "--no-dangling"),
      );
      assert.equal(
        (yield* Effect.promise(() =>
          git(root, `--git-dir=${directory}`, "for-each-ref", "--format=%(refname)"),
        )).trim(),
        "refs/remotes/up/topic",
      );
    }).pipe(Effect.scoped),
  );
});
