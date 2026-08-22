/**
 * The JSON API, driven through its own derived client.
 *
 * `HttpApiTest` builds a client from the same `HttpApi` value the server
 * implements and runs it against the handlers in-process — no socket, no
 * spawned server. What this buys is the no-drift property: a payload or
 * error-shape change that broke the client would fail to compile here, and a
 * `RefConflict` comes back as a typed value in the failure channel, not as a
 * status code to interpret.
 */
import assert from "node:assert/strict";
import { describe, it } from "@effect/vitest";

import { Effect, FileSystem, Layer, Path } from "effect";
import { Etag, HttpPlatform, HttpRouter } from "effect/unstable/http";
import { HttpApiTest } from "effect/unstable/httpapi";

import { fingerprint, formatPublicKey, generate, NAMESPACE, sign } from "../crypto/SshSignature.ts";
import * as HubEvent from "../hub/Event.ts";
import * as PullRequest from "../hub/PullRequest.ts";
import * as HubTask from "../hub/Task.ts";
import * as HubSession from "../hub/Session.ts";
import { EMPTY_TREE_OID } from "../git/Format.ts";
import { stores } from "../git/Memory.ts";
import * as GitRepository from "../git/Repository.ts";
import * as Certificate from "../trust/Certificate.ts";
import { create, signGenesis, writeGenesis } from "../trust/Genesis.ts";
import * as Log from "../trust/Log.ts";
import { project } from "../trust/Projection.ts";
import * as Api from "./Api.ts";
import * as Auth from "./Auth.ts";
import * as Policy from "./Policy.ts";
import * as Subscribers from "./Subscribers.ts";

const repository = GitRepository.layer.pipe(
  Layer.provide(GitRepository.hooksNoop),
  Layer.provide(stores),
);

const live = Layer.mergeAll(
  Api.handlers,
  Api.hubHandlers,
  HttpPlatform.layer.pipe(Layer.provide(FileSystem.layerNoop({}))),
  Etag.layerWeak,
  FileSystem.layerNoop({}),
  Path.layer,
).pipe(
  Layer.provideMerge(repository),
  Layer.provideMerge(Subscribers.memory),
  // These repositories have no genesis, so the policy boundary refuses writes
  // to them unless the host says otherwise — `serve --open`'s choice.
  Layer.provideMerge(Policy.anonymousWrites(true)),
);

/**
 * The handlers reach `Repository` and `Subscribers` through the request
 * context: a host discharges those `Request<"Requires", …>` markers by
 * handing its layer to `HttpRouter.toWebHandler`, which resolves them from
 * the layer's outputs. `HttpApiTest` dispatches in-process instead and
 * resolves the same services from the ambient context — where `live` merges
 * them — so the markers are satisfied at dispatch, just not anywhere the
 * type system can watch it happen.
 *
 * SAFETY: `live` merges `Repository` and `Subscribers` into the test context,
 * which is exactly where the in-process dispatch resolves these request-scoped
 * markers; the cast erases what every dispatched request already receives.
 */
const dispatched = <E>(
  effect: Effect.Effect<
    void,
    E,
    | HttpRouter.Request<"Requires", GitRepository.Repository>
    | HttpRouter.Request<"Requires", Subscribers.Subscribers>
  >,
): Effect.Effect<void, E> => {
  // SAFETY: `live` already merged Repository and Subscribers into the
  // dispatch context; HttpApiTest cannot name that in the type.
  // @effect-diagnostics-next-line unsafeEffectTypeAssertion:off
  return effect as Effect.Effect<void, E>;
};

const alice = {
  name: "Alice",
  email: "alice@example.com",
  at: new Date(1_700_000_000_000).toISOString(),
  offset: 0,
};

/**
 * `it.live`, not `it.effect`: the latter installs a `TestClock` whose
 * time never advances on its own, and `Repository.commit` retries a
 * `RefConflict` behind a 10ms schedule — the conflict assertion below
 * would wait forever. The win is the same either way: the test body *is*
 * an Effect, so there is no `runPromise` at the edge and a failure is
 * reported as a `Cause` with its fiber trace.
 */
describe("Api", () => {
  it.effect("swaps against the value the rewrite charge was judged on", () =>
    Effect.promise(async () => {
      // The charge is decided from a snapshot of `into`, and the write happens
      // after a merge or a replay that takes as long as the history is deep.
      // Read again at write time, the swap compared the ref's value against
      // itself and could not fail — so a push landing in that window was
      // silently overwritten, and a write judged a fast-forward became one that
      // drops commits, which is `source.force-push`'s to allow.
      const outcome = await Effect.runPromise(
        Effect.gen(function* () {
          const git = yield* GitRepository.Repository;
          const first = yield* git.commit({
            branch: "refs/heads/main",
            tree: EMPTY_TREE_OID,
            message: "first",
            author: { ...alice, at: new Date(1_700_000_000_000) },
          });
          yield* git.setRef({ name: "refs/heads/topic", to: first });
          const ahead = yield* git.commit({
            branch: "refs/heads/topic",
            tree: EMPTY_TREE_OID,
            message: "ahead",
            author: { ...alice, at: new Date(1_700_000_001_000) },
          });

          // Judged here: `main` is at `first`, and merging `topic` into it drops
          // nothing.
          const judged = yield* Api.discards("refs/heads/main", [
            "refs/heads/main",
            "refs/heads/topic",
          ]);

          // And now somebody else's push lands on `main`.
          const landed = yield* git.commit({
            branch: "refs/heads/main",
            tree: EMPTY_TREE_OID,
            message: "landed",
            author: { ...alice, at: new Date(1_700_000_002_000) },
          });

          const merged = yield* git
            .merge({
              ours: "refs/heads/main",
              theirs: "refs/heads/topic",
              author: { ...alice, at: new Date(1_700_000_003_000) },
              into: "refs/heads/main",
              expected: judged.swap,
              noFastForward: true,
            })
            .pipe(
              Effect.as(null),
              Effect.catchTag("RefConflict", (error) => Effect.succeed(error._tag)),
            );

          return {
            rewrites: judged.rewrites,
            merged,
            main: yield* git.resolve("refs/heads/main"),
            landed,
            ahead,
          };
        }).pipe(Effect.provide(repository)),
      );

      assert.equal(outcome.rewrites, false, "the merge was judged to drop nothing");
      assert.equal(outcome.merged, "RefConflict", "so the write that would has to fail the swap");
      assert.equal(outcome.main, outcome.landed, "and the push that landed first stands");
    }),
  );

  it.effect("reports the swap as what the store compares, not as what the ref resolves to", () =>
    Effect.promise(async () => {
      // Two readings of "what `into` is now", and they differ. Reachability
      // wants the commit the destination resolves to; the compare-and-swap
      // wants exactly what the store will compare against — the ref's own
      // value, and nothing at all when the destination was spelled as an oid.
      // Handed the resolved oid instead, a write to a symbolic destination
      // names a value nobody wrote and fails as a conflict for good, and a
      // write to an oid destination swaps against a ref that does not exist.
      // `Policy.evaluate` splits the same two readings for the same reason.
      const outcome = await Effect.runPromise(
        Effect.gen(function* () {
          const git = yield* GitRepository.Repository;
          const first = yield* git.commit({
            branch: "refs/heads/main",
            tree: EMPTY_TREE_OID,
            message: "first",
            author: { ...alice, at: new Date(1_700_000_000_000) },
          });
          return {
            first,
            named: (yield* Api.discards("refs/heads/main", ["refs/heads/main"])).swap,
            spelled: (yield* Api.discards(first, [first])).swap,
            absent: (yield* Api.discards("refs/heads/nowhere", ["refs/heads/main"])).swap,
            // A base this repository cannot resolve is not evidence of a
            // rewrite: the verb is about to fail on that revision anyway, and
            // claiming one turns "unknown revision" into a `source.force-push`
            // refusal — an answer that is wrong, and given only to callers who
            // lack that capability, so one request reports two different
            // problems depending on who asks it.
            unknown: (yield* Api.discards("refs/heads/main", ["refs/heads/nowhere"])).rewrites,
          };
        }).pipe(Effect.provide(repository)),
      );

      assert.equal(outcome.named, outcome.first, "a ref swaps against its own value");
      assert.equal(outcome.spelled, undefined, "an oid destination is no ref to swap against");
      assert.equal(outcome.absent, null, "and a ref that does not exist swaps against nothing");
      assert.equal(outcome.unknown, false, "an unresolvable base is not a rewrite to charge for");
    }),
  );

  it.effect("charges a rewrite only when the destination would lose commits", () =>
    Effect.promise(async () => {
      // The charge was "does `into` exist", so `{onto: "main", into: "main"}` —
      // an ordinary fast-forward — was refused to a member holding only
      // `source.push`. And for a merge it compared tips by oid, which misses the
      // destination a side already reaches without being it. The question is
      // whether the write *contains* what the destination holds, and the write
      // is not made yet — so it is asked of the bases: a replay lands on top of
      // `onto`, and a merge commit holds both of its sides.
      const outcome = await Effect.runPromise(
        Effect.gen(function* () {
          const git = yield* GitRepository.Repository;
          const first = yield* git.commit({
            branch: "refs/heads/main",
            tree: EMPTY_TREE_OID,
            message: "first",
            author: { ...alice, at: new Date(1_700_000_000_000) },
          });
          const second = yield* git.commit({
            branch: "refs/heads/main",
            tree: EMPTY_TREE_OID,
            message: "second",
            author: { ...alice, at: new Date(1_700_000_001_000) },
          });
          yield* git.setRef({ name: "refs/heads/behind", to: first });
          // A branch that shares no history with `main`.
          yield* git.commit({
            branch: "refs/heads/apart",
            tree: EMPTY_TREE_OID,
            message: "apart",
            author: { ...alice, at: new Date(1_700_000_002_000) },
          });
          void second;

          return {
            onto: (yield* Api.discards("refs/heads/main", ["refs/heads/main"])).rewrites,
            behind: (yield* Api.discards("refs/heads/behind", ["refs/heads/main"])).rewrites,
            fresh: (yield* Api.discards("refs/heads/absent", ["refs/heads/main"])).rewrites,
            side: (yield* Api.discards("refs/heads/behind", [
              "refs/heads/apart",
              "refs/heads/main",
            ])).rewrites,
            apart: (yield* Api.discards("refs/heads/apart", ["refs/heads/main"])).rewrites,
          };
        }).pipe(Effect.provide(repository)),
      );

      assert.equal(outcome.onto, false, "landing where you started discards nothing");
      assert.equal(outcome.behind, false, "nor does a fast-forward");
      assert.equal(outcome.fresh, false, "nor does creating the destination");
      assert.equal(outcome.side, false, "nor does a merge whose other side reaches it");
      assert.equal(outcome.apart, true, "a destination neither base reaches is a rewrite");
    }),
  );

  it.live("tells an anonymous request what it may do, rather than refusing the question", () =>
    dispatched(
      Effect.gen(function* () {
        const client = yield* HttpApiTest.groups(Api.api, ["repo"]);
        const answer = yield* client.repo.whoami({ params: { repo: "r" } });

        // Answered, not refused: a caller may always be told what it may do,
        // and being told "nothing" is the useful form of that answer.
        assert.equal(answer.member, false);
        assert.equal(answer.repo, null);
        assert.equal(answer.subject, null);
        assert.match(answer.why ?? "", /no genesis/);
        assert.equal(answer.branches["(any other ref)"]?.push, "refused");
      }).pipe(Effect.scoped, Effect.provide(live)),
    ),
  );

  it.live("answers over the wire for what the credential holds, not what the member does", () =>
    dispatched(
      Effect.gen(function* () {
        const git = yield* GitRepository.Repository;

        // A repository with an identity, one member, and a protected branch.
        const root = yield* generate("root@example.com");
        const member = yield* generate("member@example.com");
        const genesis = yield* create([formatPublicKey(root.publicKey)], 1);
        yield* writeGenesis(genesis, [yield* signGenesis(genesis, root)]);
        yield* Log.issue(
          yield* Certificate.grant({
            repo: genesis.repoId,
            publicKey: formatPublicKey(member.publicKey),
            capabilities: ["repo.read", "source.push", "hub.create-pr"],
            id: Log.newId(),
          }),
          [root],
        );

        const blob = yield* git.writeBlob(
          Policy.encodeRules({
            ...Policy.OPEN,
            protected: ["refs/heads/main"],
            requiredApprovals: 1,
            requiredChecks: ["test"],
            requirePullRequest: true,
          }),
        );
        const tree = yield* git.writeTree([{ mode: "100644", name: "policy.json", oid: blob }]);
        const commit = yield* git.commitTree({
          tree,
          parents: [],
          message: "policy\n",
          author: { ...alice, at: new Date(1_700_000_000_000) },
        });
        yield* git.setRef({ name: Policy.RULES_REF, to: commit });

        const projection = yield* project(genesis);
        const signer = yield* fingerprint(member.publicKey);
        const client = yield* HttpApiTest.groups(Api.api, ["repo"]);

        const answer = yield* client.repo.whoami({ params: { repo: "r" } }).pipe(
          Effect.provideService(Auth.Requester, {
            principal: projection.members.get(signer) ?? null,
            signer,
            // The credential was minted narrower than the grant: what this
            // *request* may do is the intersection, and that is the answer
            // its holder needs — reported as the member's full grant, an
            // agent would plan a push its own credential cannot make.
            capabilities: ["repo.read"],
            projection,
            envelope: null,
          }),
        );

        assert.equal(answer.member, true);
        assert.equal(answer.repo, genesis.repoId);
        assert.equal(answer.subject, signer);
        assert.deepEqual(answer.capabilities, ["repo.read"]);
        assert.equal(
          answer.branches["(any other ref)"]?.push,
          "refused",
          "a credential without source.push may not write, whatever its member holds",
        );

        // The nearest obstacle, not every obstacle: this credential cannot
        // write at all, so saying what `main` additionally requires would be
        // advice about a push it could never make.
        const main = answer.branches["refs/heads/main"];
        assert.equal(main?.push, "refused");
        assert.deepEqual(main?.why, ["source.push is not granted to this key"]);

        // Widen the credential to what the member actually holds, and the
        // branch's own requirements are what is left to answer.
        const wider = yield* client.repo.whoami({ params: { repo: "r" } }).pipe(
          Effect.provideService(Auth.Requester, {
            principal: projection.members.get(signer) ?? null,
            signer,
            capabilities: ["repo.read", "source.push"],
            projection,
            envelope: null,
          }),
        );
        assert.equal(wider.branches["(any other ref)"]?.push, "allowed");
        assert.deepEqual(wider.branches["refs/heads/main"]?.why, [
          "requirePullRequest",
          "requiredApprovals: 1",
          "requiredChecks: [test]",
          "a direct push meets none of these; open a pull request",
        ]);
      }).pipe(Effect.scoped, Effect.provide(live)),
    ),
  );

  it.live("reports what a key holds when the request was served as a public read", () =>
    dispatched(
      Effect.gen(function* () {
        const git = yield* GitRepository.Repository;
        const root = yield* generate("root@example.com");
        const member = yield* generate("member@example.com");
        const genesis = yield* create([formatPublicKey(root.publicKey)], 1);
        yield* writeGenesis(genesis, [yield* signGenesis(genesis, root)]);
        yield* Log.issue(
          yield* Certificate.grant({
            repo: genesis.repoId,
            publicKey: formatPublicKey(member.publicKey),
            capabilities: ["source.push"],
            id: Log.newId(),
          }),
          [root],
        );
        yield* git.commit({
          branch: "refs/heads/main",
          tree: EMPTY_TREE_OID,
          message: "first",
          author: { ...alice, at: new Date(1_700_000_000_000) },
        });

        const projection = yield* project(genesis);
        const signer = yield* fingerprint(member.publicKey);
        const client = yield* HttpApiTest.groups(Api.api, ["repo"]);

        // What the guard hands a member who holds no `repo.read` on a
        // repository anonymous readers may clone: a real signer, no principal,
        // and the literal capability the *read* was served under. Read as the
        // credential's scope, this told a member holding `source.push` that
        // every push would be refused — an answer the boundary contradicts.
        const answer = yield* client.repo.whoami({ params: { repo: "r" } }).pipe(
          Effect.provideService(Auth.Requester, {
            principal: null,
            signer,
            capabilities: ["repo.read"],
            projection,
            envelope: null,
          }),
        );

        assert.equal(answer.member, true);
        assert.deepEqual(answer.capabilities, ["source.push"]);
        assert.equal(answer.branches["(any other ref)"]?.push, "allowed");
      }).pipe(Effect.scoped, Effect.provide(live)),
    ),
  );

  it.live("says nothing about trust freshness for a repository with no identity", () =>
    dispatched(
      Effect.gen(function* () {
        const git = yield* GitRepository.Repository;
        const blob = yield* git.writeBlob(
          Policy.encodeRules({ ...Policy.OPEN, maxTrustAgeSeconds: 60 }),
        );
        const tree = yield* git.writeTree([{ mode: "100644", name: "policy.json", oid: blob }]);
        const commit = yield* git.commitTree({
          tree,
          parents: [],
          message: "policy\n",
          author: { ...alice, at: new Date(1_700_000_000_000) },
        });
        yield* git.setRef({ name: Policy.RULES_REF, to: commit });

        const client = yield* HttpApiTest.groups(Api.api, ["repo"]);

        // `Auth.anonymous` is what the guard hands a request to a repository
        // with no genesis, and it carries the *empty* projection rather than
        // none at all — which is the whole point of this case.
        const answer = yield* client.repo
          .whoami({ params: { repo: "r" } })
          .pipe(Effect.provideService(Auth.Requester, Auth.anonymous));

        // That empty projection is not a membership view, and judging its
        // staleness reported a bound the CLI — handed no projection at all —
        // says nothing about.
        assert.equal(answer.repo, null);
        assert.equal(answer.trust, null);
      }).pipe(Effect.scoped, Effect.provide(live)),
    ),
  );

  it.live("drives the derived client end to end, typed errors included", () =>
    dispatched(
      Effect.gen(function* () {
        const client = yield* HttpApiTest.groups(Api.api, ["repo"]);

        const created = yield* client.repo.create({
          params: { repo: "r" },
          payload: { message: "first", author: alice },
        });
        assert.match(created.oid, /^[0-9a-f]{40}$/);

        const read = yield* client.repo.read({ params: { repo: "r", oid: created.oid } });
        assert.equal(read.message, "first");
        assert.deepEqual(read.parents, []);
        // The enriched view: one read carries everything a UI header needs.
        assert.equal(read.subject, "first");
        assert.deepEqual(read.author, { name: "Alice", email: "alice@example.com" });
        assert.equal(read.at, alice.at);
        assert.match(read.tree, /^[0-9a-f]{40}$/);

        const second = yield* client.repo.create({
          params: { repo: "r" },
          payload: { message: "second", author: alice },
        });
        const log = yield* client.repo.log({ params: { repo: "r", oid: second.oid } });
        assert.deepEqual(
          log.commits.map((commit) => commit.message),
          ["second", "first"],
        );
        assert.equal(log.commits[0]?.subject, "second");
        assert.equal(log.commits[0]?.author.name, "Alice");
        assert.equal(log.commits[0]?.at, alice.at);

        const refs = yield* client.repo.refs({ params: { repo: "r" } });
        assert.deepEqual(refs.refs, [{ name: "refs/heads/main", oid: second.oid }]);

        // Paged endpoints: the cursor walks, `has_more` closes.
        const firstPage = yield* client.repo.commits({
          params: { repo: "r", oid: second.oid },
          query: { limit: "1" },
        });
        assert.deepEqual(
          firstPage.items.map((commit) => commit.message),
          ["second"],
        );
        assert.deepEqual(firstPage.items[0]?.parents, [created.oid]);
        assert.equal(firstPage.has_more, true);
        const nextPage = yield* client.repo.commits({
          params: { repo: "r", oid: second.oid },
          query: { limit: "1", cursor: firstPage.next_cursor! },
        });
        assert.deepEqual(
          nextPage.items.map((commit) => commit.message),
          ["first"],
        );
        assert.equal(nextPage.has_more, false);

        // `limit=0` asks for a page that advances the cursor by nothing. Left
        // unclamped it answered with no items, `has_more`, and the very cursor
        // it was handed — a client following `next_cursor` never terminates.
        const zero = yield* client.repo.commits({
          params: { repo: "r", oid: second.oid },
          query: { limit: "0" },
        });
        assert.equal(zero.items.length, 1);
        assert.notEqual(zero.next_cursor, "0");

        // Branch creation, and the paged branch list that follows it.
        const created2 = yield* client.repo.branch({
          params: { repo: "r" },
          payload: { name: "feature", base: "refs/heads/main" },
        });
        assert.equal(created2.name, "refs/heads/feature");
        assert.equal(created2.oid, second.oid);

        const branches = yield* client.repo.branches({ params: { repo: "r" }, query: {} });
        assert.deepEqual(
          branches.items.map((ref) => ref.name),
          ["refs/heads/feature", "refs/heads/main"],
        );
        assert.equal(branches.has_more, false);

        // Creating it twice is a conflict, typed.
        const conflict2 = yield* client.repo
          .branch({
            params: { repo: "r" },
            payload: { name: "feature", base: "refs/heads/main" },
          })
          .pipe(Effect.flip);
        assert.equal(conflict2._tag, "RefConflict");

        // The failure channel carries the domain error, decoded.
        const conflict = yield* client.repo
          .create({
            params: { repo: "r" },
            payload: { message: "third", author: alice, expected: null },
          })
          .pipe(Effect.flip);
        assert.equal(conflict._tag, "RefConflict");
      }).pipe(Effect.scoped, Effect.provide(live)),
    ),
  );

  it.live(
    "pages enriched commits — author, date, parents, subject and message in one response",
    () =>
      dispatched(
        Effect.gen(function* () {
          const client = yield* HttpApiTest.groups(Api.api, ["repo"]);

          const seed = yield* client.repo.create({
            params: { repo: "r" },
            payload: { message: "seed the tree\n\nwith a body", author: alice },
          });
          yield* client.repo.branch({
            params: { repo: "r" },
            payload: { name: "side", base: "refs/heads/main" },
          });
          yield* client.repo.create({
            params: { repo: "r" },
            payload: {
              branch: "side",
              message: "side work",
              author: {
                name: "Bob",
                email: "bob@example.com",
                at: new Date(1_700_000_001_000).toISOString(),
                offset: 0,
              },
              files: [{ path: "side.txt", content: "side\n" }],
            },
          });
          yield* client.repo.create({
            params: { repo: "r" },
            payload: { branch: "main", message: "main moves on", author: alice },
          });
          const merged = yield* client.repo.merge({
            params: { repo: "r" },
            payload: {
              ours: "refs/heads/main",
              theirs: "refs/heads/side",
              author: alice,
              into: "main",
            },
          });
          assert.equal(merged.kind, "merged");

          // No author at all: the server's defaults, carried on the wire like
          // any other commit's.
          const anonymous = yield* client.repo.create({
            params: { repo: "r" },
            payload: { message: "unsigned" },
          });

          const page = yield* client.repo.commits({
            params: { repo: "r", oid: anonymous.oid },
            query: { limit: "10" },
          });
          assert.equal(page.items.length, 5);
          assert.equal(page.has_more, false);
          for (const item of page.items) {
            assert.match(item.oid, /^[0-9a-f]{40}$/);
            assert.equal(item.subject, item.message.split("\n", 1)[0]?.trim());
            assert.ok(item.author.name.length > 0, item.oid);
            assert.match(item.author.email, /@/);
            assert.ok(!Number.isNaN(Date.parse(item.at)), item.at);
            assert.ok(Array.isArray(item.parents));
          }

          const rows = new Map(page.items.map((item) => [item.subject, item]));
          assert.deepEqual(rows.get("unsigned")?.author, {
            name: "Anonymous",
            email: "anonymous@example.com",
          });
          assert.deepEqual(rows.get("unsigned")?.parents, [merged.commit]);
          assert.equal(rows.get("side work")?.author.name, "Bob");
          assert.equal(rows.get("side work")?.at, new Date(1_700_000_001_000).toISOString());
          assert.equal(rows.get("seed the tree")?.message, "seed the tree\n\nwith a body");
          assert.deepEqual(rows.get("seed the tree")?.parents, []);

          const mergeRow = page.items.find((item) => item.oid === merged.commit);
          assert.equal(mergeRow?.parents.length, 2, "the merge names both parents");
          assert.equal(mergeRow?.at, alice.at);

          // And the single read agrees with the row, field for field.
          const read = yield* client.repo.read({ params: { repo: "r", oid: seed.oid } });
          assert.equal(read.subject, "seed the tree");
          assert.deepEqual(read.author, { name: "Alice", email: "alice@example.com" });
          assert.equal(read.at, alice.at);
          assert.match(read.tree, /^[0-9a-f]{40}$/);

          // Paging keeps the same shape.
          const firstPage = yield* client.repo.commits({
            params: { repo: "r", oid: anonymous.oid },
            query: { limit: "2" },
          });
          assert.equal(firstPage.items.length, 2);
          assert.equal(firstPage.has_more, true);
          assert.equal(firstPage.items[0]?.subject, "unsigned");
        }).pipe(Effect.scoped, Effect.provide(live)),
      ),
  );

  it.live("commits real content, and reads it back", () =>
    dispatched(
      Effect.gen(function* () {
        const client = yield* HttpApiTest.groups(Api.api, ["repo"]);

        // The path the API exists for: files in, a commit whose tree holds
        // them out. Nested, because a flat tree would not exercise the
        // bottom-up write.
        const first = yield* client.repo.create({
          params: { repo: "r" },
          payload: {
            message: "add sources",
            author: alice,
            files: [
              { path: "readme.md", content: "hello\n" },
              { path: "src/index.ts", content: "export const answer = 42;\n" },
              { path: "src/lib/util.ts", content: "export const noop = () => {};\n" },
            ],
          },
        });

        const root = yield* client.repo.readTree({ params: { repo: "r", oid: first.tree } });
        assert.deepEqual(
          root.entries.map((entry) => entry.name),
          ["readme.md", "src"],
        );
        const src = root.entries.find((entry) => entry.name === "src")!;
        assert.equal(src.mode, "40000");

        const inner = yield* client.repo.readTree({ params: { repo: "r", oid: src.oid } });
        assert.deepEqual(
          inner.entries.map((entry) => entry.name),
          ["index.ts", "lib"],
        );

        const blob = inner.entries.find((entry) => entry.name === "index.ts")!;
        const content = yield* client.repo.readBlob({ params: { repo: "r", oid: blob.oid } });
        assert.equal(atob(content.content), "export const answer = 42;\n");
        assert.equal(content.size, 26);

        // A second commit carries the first's tree forward: touching one path
        // must not drop the others.
        const second = yield* client.repo.create({
          params: { repo: "r" },
          payload: {
            message: "edit one file",
            author: alice,
            files: [{ path: "src/index.ts", content: "export const answer = 43;\n" }],
          },
        });
        const nextRoot = yield* client.repo.readTree({ params: { repo: "r", oid: second.tree } });
        assert.deepEqual(
          nextRoot.entries.map((entry) => entry.name),
          ["readme.md", "src"],
        );
        assert.equal(
          nextRoot.entries.find((entry) => entry.name === "readme.md")!.oid,
          root.entries.find((entry) => entry.name === "readme.md")!.oid,
        );

        // Removing the last file in a directory removes the directory: git
        // has no empty trees.
        const third = yield* client.repo.create({
          params: { repo: "r" },
          payload: {
            message: "drop lib",
            author: alice,
            files: [{ path: "src/lib/util.ts", content: null }],
          },
        });
        const afterSrc = yield* client.repo.readTree({
          params: {
            repo: "r",
            oid: (yield* client.repo.readTree({
              params: { repo: "r", oid: third.tree },
            })).entries.find((entry) => entry.name === "src")!.oid,
          },
        });
        assert.deepEqual(
          afterSrc.entries.map((entry) => entry.name),
          ["index.ts"],
        );

        // Binary survives the round trip, which is what base64 is for.
        const bytes = new Uint8Array([0, 1, 2, 250, 251, 252]);
        let binary = "";
        for (const byte of bytes) binary += String.fromCharCode(byte);
        const written = yield* client.repo.blob({
          params: { repo: "r" },
          payload: { content: btoa(binary), encoding: "base64" },
        });
        const readBack = yield* client.repo.readBlob({
          params: { repo: "r", oid: written.oid },
        });
        assert.equal(readBack.content, btoa(binary));

        // A tree can also be stated outright, and a commit can name it.
        const tree = yield* client.repo.tree({
          params: { repo: "r" },
          payload: { files: [{ path: "only.txt", content: "one\n" }] },
        });
        const explicit = yield* client.repo.create({
          params: { repo: "r" },
          payload: { message: "explicit tree", author: alice, tree: tree.oid },
        });
        assert.equal(explicit.tree, tree.oid);

        // A path that escapes the root is refused rather than normalised.
        const escaped = yield* client.repo
          .create({
            params: { repo: "r" },
            payload: {
              message: "nope",
              author: alice,
              files: [{ path: "../outside", content: "x" }],
            },
          })
          .pipe(Effect.flip);
        assert.equal(escaped._tag, "Invalid");
      }).pipe(Effect.scoped, Effect.provide(live)),
    ),
  );

  it.live("tags, annotated and lightweight, and checks its own integrity", () =>
    dispatched(
      Effect.gen(function* () {
        const client = yield* HttpApiTest.groups(Api.api, ["repo"]);

        const commit = yield* client.repo.create({
          params: { repo: "r" },
          payload: { message: "release", author: alice, files: [{ path: "a", content: "a\n" }] },
        });

        // Annotated: a tag object of its own, so the ref points at the tag
        // rather than at the commit.
        const annotated = yield* client.repo.tagCreate({
          params: { repo: "r" },
          payload: {
            name: "v1.0.0",
            target: "refs/heads/main",
            message: "first release\n",
            tagger: alice,
          },
        });
        assert.equal(annotated.ref, "refs/tags/v1.0.0");
        assert.equal(annotated.target, commit.oid);
        assert.notEqual(annotated.oid, commit.oid);

        const read = yield* client.repo.tagRead({ params: { repo: "r", oid: annotated.oid } });
        assert.equal(read.tag, "v1.0.0");
        assert.equal(read.type, "commit");
        assert.equal(read.object, commit.oid);
        assert.equal(read.message, "first release\n");

        // Lightweight: no message, so the ref points straight at the commit.
        const light = yield* client.repo.tagCreate({
          params: { repo: "r" },
          payload: { name: "latest", target: "refs/heads/main" },
        });
        assert.equal(light.oid, commit.oid);

        const tags = yield* client.repo.tags({ params: { repo: "r" }, query: {} });
        assert.deepEqual(
          tags.items.map((tag) => tag.name),
          ["refs/tags/latest", "refs/tags/v1.0.0"],
        );

        // A tag is meant to be stable: replacing one is opt-in.
        const clash = yield* client.repo
          .tagCreate({
            params: { repo: "r" },
            payload: { name: "latest", target: "refs/heads/main" },
          })
          .pipe(Effect.flip);
        assert.equal(clash._tag, "RefConflict");

        const forced = yield* client.repo.tagCreate({
          params: { repo: "r" },
          payload: { name: "latest", target: "refs/heads/main", force: true },
        });
        assert.equal(forced.oid, commit.oid);

        // Everything written so far hashes to its own name.
        const report = yield* client.repo.fsck({ params: { repo: "r" } });
        assert.equal(report.ok, true);
        assert.deepEqual(report.problems, []);
        assert.deepEqual(report.dangling_refs, []);
        assert.equal(report.checked > 0, true);

        const removed = yield* client.repo.tagRemove({ params: { repo: "r", name: "latest" } });
        assert.equal(removed.deleted, true);

        // Deleting through `receive` rather than `deleteTag` is what carries
        // the compare-and-swap the policy boundary judged; "there was nothing
        // there" still has to read as `false` rather than as a refusal.
        const again = yield* client.repo.tagRemove({ params: { repo: "r", name: "latest" } });
        assert.equal(again.deleted, false);

        const absent = yield* client.repo.branchRemove({ params: { repo: "r", name: "nope" } });
        assert.equal(absent.deleted, false);
      }).pipe(Effect.scoped, Effect.provide(live)),
    ),
  );

  it.live("merges: fast-forward, clean three-way, and a reported conflict", () =>
    dispatched(
      Effect.gen(function* () {
        const client = yield* HttpApiTest.groups(Api.api, ["repo"]);

        const base = yield* client.repo.create({
          params: { repo: "r" },
          payload: {
            message: "base",
            author: alice,
            files: [
              { path: "shared.txt", content: "one\ntwo\nthree\n" },
              { path: "untouched.txt", content: "stable\n" },
            ],
          },
        });

        yield* client.repo.branch({
          params: { repo: "r" },
          payload: { name: "feature", base: "refs/heads/main" },
        });

        // Only the branch moves: main can fast-forward onto it.
        yield* client.repo.create({
          params: { repo: "r" },
          payload: {
            branch: "feature",
            message: "add a file",
            author: alice,
            files: [{ path: "new.txt", content: "added\n" }],
          },
        });

        const forward = yield* client.repo.merge({
          params: { repo: "r" },
          payload: {
            ours: "refs/heads/main",
            theirs: "refs/heads/feature",
            into: "refs/heads/main",
          },
        });
        assert.equal(forward.kind, "fast-forward");
        assert.equal(forward.base, base.oid);

        const afterForward = yield* client.repo.files({ params: { repo: "r" }, query: {} });
        assert.deepEqual(
          afterForward.files.map((file) => file.path),
          ["new.txt", "shared.txt", "untouched.txt"],
        );

        // Merging again has nothing to do.
        const idempotent = yield* client.repo.merge({
          params: { repo: "r" },
          payload: { ours: "refs/heads/main", theirs: "refs/heads/feature" },
        });
        assert.equal(idempotent.kind, "up-to-date");

        // Now diverge on different files: a clean three-way merge.
        yield* client.repo.branch({
          params: { repo: "r" },
          payload: { name: "side", base: "refs/heads/main" },
        });
        yield* client.repo.create({
          params: { repo: "r" },
          payload: {
            branch: "side",
            message: "theirs edits shared",
            author: alice,
            files: [{ path: "shared.txt", content: "one\nTWO\nthree\n" }],
          },
        });
        yield* client.repo.create({
          params: { repo: "r" },
          payload: {
            branch: "main",
            message: "ours adds another",
            author: alice,
            files: [{ path: "ours.txt", content: "mine\n" }],
          },
        });

        // Spelled short on purpose. The gate qualified the name and the write
        // used the raw one, so `into: "main"` was judged as `refs/heads/main`
        // and written to a top-level ref called `main`: the branch never moved
        // and the response said it had.
        // `HEAD` is already a full ref name. Qualified, it became a literal
        // branch called `refs/heads/HEAD`: the merge reported success while
        // the checked-out branch never moved.
        const atHead = yield* client.repo
          .merge({
            params: { repo: "r" },
            payload: {
              ours: "HEAD",
              theirs: "refs/heads/side",
              author: alice,
              into: "HEAD",
            },
          })
          .pipe(Effect.flip);
        assert.equal(atHead._tag, "Invalid");
        const { refs: afterHead } = yield* client.repo.refs({ params: { repo: "r" } });
        assert.equal(
          afterHead.find((ref) => ref.name === "refs/heads/HEAD"),
          undefined,
          `no branch called HEAD: ${afterHead.map((ref) => ref.name).join(", ")}`,
        );

        // An object id is not a destination: nothing moves it. Qualified along
        // with every other short name, `into: "<40 hex>"` created a branch
        // *named after the object id* — silently, and reported as success.
        const atOid = yield* client.repo
          .merge({
            params: { repo: "r" },
            payload: {
              ours: "refs/heads/main",
              theirs: "refs/heads/side",
              author: alice,
              into: EMPTY_TREE_OID,
            },
          })
          .pipe(Effect.flip);
        assert.equal(atOid._tag, "Invalid");
        const { refs: afterOid } = yield* client.repo.refs({ params: { repo: "r" } });
        assert.equal(
          afterOid.find((ref) => ref.name === `refs/heads/${EMPTY_TREE_OID}`),
          undefined,
          `no branch named after an oid: ${afterOid.map((ref) => ref.name).join(", ")}`,
        );

        const merged = yield* client.repo.merge({
          params: { repo: "r" },
          payload: {
            ours: "refs/heads/main",
            theirs: "refs/heads/side",
            author: alice,
            into: "main",
          },
        });
        assert.equal(merged.kind, "merged");
        assert.deepEqual(merged.conflicts, []);

        const { refs } = yield* client.repo.refs({ params: { repo: "r" } });
        assert.equal(
          refs.find((ref) => ref.name === "refs/heads/main")?.oid,
          merged.commit,
          `the branch named must be the branch moved: ${refs.map((ref) => ref.name).join(", ")}`,
        );
        assert.equal(
          refs.find((ref) => ref.name === "main"),
          undefined,
          "and no stray top-level ref",
        );

        // The merge commit has both parents, which is what makes it a merge.
        const commit = yield* client.repo.read({
          params: { repo: "r", oid: merged.commit! },
        });
        assert.equal(commit.parents.length, 2);
        assert.equal(commit.author.name, "Alice");
        assert.equal(commit.at, alice.at);

        // Their edit to shared.txt survived, and our file is still there.
        const shared = yield* client.repo.file({
          params: { repo: "r" },
          query: { ref: "refs/heads/main", path: "shared.txt" },
        });
        assert.equal(atob(shared.content), "one\nTWO\nthree\n");
        yield* client.repo.file({
          params: { repo: "r" },
          query: { ref: "refs/heads/main", path: "ours.txt" },
        });

        // Both sides edit the same line: a conflict, reported not thrown.
        yield* client.repo.branch({
          params: { repo: "r" },
          payload: { name: "clash", base: "refs/heads/main" },
        });
        yield* client.repo.create({
          params: { repo: "r" },
          payload: {
            branch: "clash",
            message: "theirs",
            author: alice,
            files: [{ path: "shared.txt", content: "one\nTHEIRS\nthree\n" }],
          },
        });
        yield* client.repo.create({
          params: { repo: "r" },
          payload: {
            branch: "main",
            message: "ours",
            author: alice,
            files: [{ path: "shared.txt", content: "one\nOURS\nthree\n" }],
          },
        });

        const conflicted = yield* client.repo.merge({
          params: { repo: "r" },
          payload: { ours: "refs/heads/main", theirs: "refs/heads/clash", author: alice },
        });
        assert.equal(conflicted.kind, "conflicted");
        assert.deepEqual(
          conflicted.conflicts.map((conflict) => [conflict.path, conflict.reason]),
          [["shared.txt", "content"]],
        );
        assert.equal(conflicted.commit, null);

        // A conflicted merge still writes a tree, with the markers in it —
        // otherwise there is nothing for a caller to resolve against.
        const markers = yield* client.repo.file({
          params: { repo: "r" },
          query: { ref: conflicted.tree!, path: "shared.txt" },
        });
        assert.match(atob(markers.content), /<<<<<<</);
        assert.match(atob(markers.content), />>>>>>>/);

        // Choosing a side resolves it without markers.
        const theirs = yield* client.repo.merge({
          params: { repo: "r" },
          payload: {
            ours: "refs/heads/main",
            theirs: "refs/heads/clash",
            author: alice,
            strategy: "theirs",
          },
        });
        assert.equal(theirs.kind, "merged");
        const resolved = yield* client.repo.file({
          params: { repo: "r" },
          query: { ref: theirs.tree!, path: "shared.txt" },
        });
        assert.equal(atob(resolved.content), "one\nTHEIRS\nthree\n");
      }).pipe(Effect.scoped, Effect.provide(live)),
    ),
  );

  it.live("diffs two revisions as unified patches", () =>
    dispatched(
      Effect.gen(function* () {
        const client = yield* HttpApiTest.groups(Api.api, ["repo"]);

        const first = yield* client.repo.create({
          params: { repo: "r" },
          payload: {
            message: "first",
            author: alice,
            files: [
              { path: "kept.txt", content: "same\n" },
              { path: "edited.txt", content: "one\ntwo\nthree\n" },
              { path: "removed.txt", content: "bye\n" },
            ],
          },
        });

        const second = yield* client.repo.create({
          params: { repo: "r" },
          payload: {
            message: "second",
            author: alice,
            files: [
              { path: "edited.txt", content: "one\nTWO\nthree\n" },
              { path: "removed.txt", content: null },
              { path: "added.txt", content: "new\n" },
            ],
          },
        });

        const diff = yield* client.repo.diff({
          params: { repo: "r" },
          payload: { from: first.oid, to: second.oid },
        });

        // Unchanged files are absent; that is what makes a diff a diff.
        assert.deepEqual(
          diff.files.map((file) => [file.path, file.status]),
          [
            ["added.txt", "added"],
            ["edited.txt", "modified"],
            ["removed.txt", "removed"],
          ],
        );

        const edited = diff.files.find((file) => file.path === "edited.txt")!;
        assert.match(edited.patch, /^--- a\/edited\.txt$/m);
        assert.match(edited.patch, /^\+\+\+ b\/edited\.txt$/m);
        assert.match(edited.patch, /^-two$/m);
        assert.match(edited.patch, /^\+TWO$/m);
        assert.match(edited.patch, /^@@ -1,3 \+1,3 @@$/m);
      }).pipe(Effect.scoped, Effect.provide(live)),
    ),
  );

  it.live("reads the tree by path: files, one file, raw objects, reflog and grep", () =>
    dispatched(
      Effect.gen(function* () {
        const client = yield* HttpApiTest.groups(Api.api, ["repo"]);

        const commit = yield* client.repo.create({
          params: { repo: "r" },
          payload: {
            message: "seed",
            author: alice,
            files: [
              { path: "readme.md", content: "# Title\nhello world\n" },
              { path: "src/a.ts", content: "export const a = 1;\nconst hello = 2;\n" },
              { path: "src/deep/b.ts", content: "export const b = 3;\n" },
            ],
          },
        });

        // Paths, recursively, from a ref rather than a tree oid — which is
        // how a caller who has a branch name and no oids asks.
        const all = yield* client.repo.files({ params: { repo: "r" }, query: {} });
        assert.deepEqual(
          all.files.map((file) => file.path),
          ["readme.md", "src/a.ts", "src/deep/b.ts"],
        );

        const scoped = yield* client.repo.files({
          params: { repo: "r" },
          query: { path: "src" },
        });
        assert.deepEqual(
          scoped.files.map((file) => file.path),
          ["src/a.ts", "src/deep/b.ts"],
        );

        const file = yield* client.repo.file({
          params: { repo: "r" },
          query: { path: "src/a.ts" },
        });
        assert.equal(atob(file.content), "export const a = 1;\nconst hello = 2;\n");
        assert.equal(file.mode, "100644");

        const missing = yield* client.repo
          .file({ params: { repo: "r" }, query: { path: "nope.txt" } })
          .pipe(Effect.flip);
        assert.equal(missing._tag, "ObjectNotFound");

        // The raw object, whatever its type — the escape hatch for a caller
        // that knows git's model.
        const raw = yield* client.repo.object({ params: { repo: "r", oid: commit.oid } });
        assert.equal(raw.type, "commit");
        assert.match(atob(raw.content), /^tree [0-9a-f]{40}/);

        const log = yield* client.repo.reflog({
          params: { repo: "r" },
          query: { ref: "refs/heads/main" },
        });
        assert.equal(log.entries.length, 1);
        assert.equal(log.entries[0]!.to, commit.oid);
        assert.equal(log.entries[0]!.from, null);

        const found = yield* client.repo.grep({
          params: { repo: "r" },
          payload: { pattern: "hello" },
        });
        assert.deepEqual(
          found.matches.map((match) => [match.path, match.line]),
          [
            ["readme.md", 2],
            ["src/a.ts", 2],
          ],
        );
        assert.equal(found.truncated, false);

        const cased = yield* client.repo.grep({
          params: { repo: "r" },
          payload: { pattern: "HELLO", ignore_case: true },
        });
        assert.equal(cased.matches.length, 2);

        const scopedGrep = yield* client.repo.grep({
          params: { repo: "r" },
          payload: { pattern: "hello", path: "src/" },
        });
        assert.deepEqual(
          scopedGrep.matches.map((match) => match.path),
          ["src/a.ts"],
        );

        // A prefix stops at a path boundary: `src` is a directory, not the
        // first three characters of one, so `src-generated/` is not under it.
        yield* client.repo.create({
          params: { repo: "r" },
          payload: {
            message: "a sibling directory",
            author: alice,
            files: [{ path: "src-generated/g.ts", content: "const hello = 9;\n" }],
          },
        });
        const anchored = yield* client.repo.grep({
          params: { repo: "r" },
          payload: { pattern: "hello", path: "src" },
        });
        assert.deepEqual(
          anchored.matches.map((match) => match.path),
          ["src/a.ts"],
        );

        // A cap, and an honest flag when it bites.
        const capped = yield* client.repo.grep({
          params: { repo: "r" },
          payload: { pattern: "e", max_matches: 1 },
        });
        assert.equal(capped.matches.length, 1);
        assert.equal(capped.truncated, true);

        // A bad regex is the caller's mistake, not a 500.
        const bad = yield* client.repo
          .grep({ params: { repo: "r" }, payload: { pattern: "([unclosed" } })
          .pipe(Effect.flip);
        assert.equal(bad._tag, "Invalid");

        // …and the same string as a fixed pattern is fine.
        const literal = yield* client.repo.grep({
          params: { repo: "r" },
          payload: { pattern: "([unclosed", fixed: true },
        });
        assert.deepEqual(literal.matches, []);

        // A file too large to hold three times over — the bytes, the decoded
        // string and the array of lines — is named as skipped rather than
        // read, decoded and split inside a worker with 128 MiB.
        yield* client.repo.create({
          params: { repo: "r" },
          payload: {
            message: "a big one",
            author: alice,
            files: [{ path: "big.log", content: "hello padding\n".repeat(400_000) }],
          },
        });
        const big = yield* client.repo.grep({
          params: { repo: "r" },
          payload: { pattern: "hello" },
        });
        assert.deepEqual(big.skipped, ["big.log"]);
        assert.equal(
          big.matches.some((match) => match.path === "big.log"),
          false,
        );
      }).pipe(Effect.scoped, Effect.provide(live)),
    ),
  );

  it.live("collects what no ref can reach, and nothing else", () =>
    dispatched(
      Effect.gen(function* () {
        const client = yield* HttpApiTest.groups(Api.api, ["repo"]);

        const kept = yield* client.repo.create({
          params: { repo: "r" },
          payload: {
            message: "keep",
            author: alice,
            files: [{ path: "kept.txt", content: "k\n" }],
          },
        });

        // A blob written but never committed is exactly what gc is for.
        const orphan = yield* client.repo.blob({
          params: { repo: "r" },
          payload: { content: "nobody references this\n" },
        });

        const dry = yield* client.repo.gc({ params: { repo: "r" }, payload: { dry_run: true } });
        assert.deepEqual(dry.removed, [orphan.oid]);
        // A dry run reports and keeps.
        yield* client.repo.readBlob({ params: { repo: "r", oid: orphan.oid } });

        const swept = yield* client.repo.gc({ params: { repo: "r" }, payload: {} });
        assert.deepEqual(swept.removed, [orphan.oid]);
        // A repack that was not asked for is not a refusal, and says so — the
        // field exists so a caller can tell "nothing to pack" from "would not".
        assert.equal(swept.repack_skipped, null);

        const gone = yield* client.repo
          .readBlob({ params: { repo: "r", oid: orphan.oid } })
          .pipe(Effect.flip);
        assert.equal(gone._tag, "ObjectNotFound");

        // Everything reachable survived, and the repository still checks out.
        const commit = yield* client.repo.read({ params: { repo: "r", oid: kept.oid } });
        assert.equal(commit.message, "keep");
        const report = yield* client.repo.fsck({ params: { repo: "r" } });
        assert.equal(report.ok, true);

        // A second pass has nothing left to do.
        const again = yield* client.repo.gc({ params: { repo: "r" }, payload: {} });
        assert.deepEqual(again.removed, []);
      }).pipe(Effect.scoped, Effect.provide(live)),
    ),
  );

  it.live("registers, lists and removes webhooks without ever echoing the secret", () =>
    dispatched(
      Effect.gen(function* () {
        const client = yield* HttpApiTest.groups(Api.api, ["repo"]);

        const added = yield* client.repo.webhookAdd({
          params: { repo: "r" },
          payload: { url: "https://example.test/hook", secret: "a-long-enough-secret" },
        });
        assert.equal(added.url, "https://example.test/hook");
        // The response shape has no secret field at all — it cannot leak.
        assert.equal("secret" in added, false);

        const listed = yield* client.repo.webhookList({ params: { repo: "r" } });
        assert.deepEqual(
          listed.webhooks.map((hook) => hook.url),
          ["https://example.test/hook"],
        );
        assert.equal("secret" in listed.webhooks[0]!, false);

        // A receiver that cannot be reached securely is refused at
        // registration, not discovered at delivery.
        const insecure = yield* client.repo
          .webhookAdd({
            params: { repo: "r" },
            payload: { url: "http://example.test/hook", secret: "a-long-enough-secret" },
          })
          .pipe(Effect.flip);
        assert.equal(insecure._tag, "Invalid");

        const weak = yield* client.repo
          .webhookAdd({
            params: { repo: "r" },
            payload: { url: "https://example.test/hook", secret: "short" },
          })
          .pipe(Effect.flip);
        assert.equal(weak._tag, "Invalid");

        const removed = yield* client.repo.webhookRemove({
          params: { repo: "r", id: added.id },
        });
        assert.equal(removed.deleted, true);

        const again = yield* client.repo.webhookRemove({
          params: { repo: "r", id: added.id },
        });
        assert.equal(again.deleted, false);

        const empty = yield* client.repo.webhookList({ params: { repo: "r" } });
        assert.deepEqual(empty.webhooks, []);
      }).pipe(Effect.scoped, Effect.provide(live)),
    ),
  );
});

describe("Api hub", () => {
  it.live("lists tasks without needing a genesis", () =>
    dispatched(
      Effect.gen(function* () {
        const key = yield* generate("worker@example.com");
        const opened = yield* HubTask.open({
          repo: "r",
          title: "wire the hub over HTTP",
          description: "so a browser can read what the fleet is doing",
          refs: ["refs/heads/main"],
          key,
        });

        const client = yield* HttpApiTest.groups(Api.api, ["hub"]);
        const answer = yield* client.hub.tasks({ params: { repo: "r" }, query: {} });

        assert.equal(answer.items.length, 1);
        const task = answer.items[0]!;
        assert.equal(task.task, opened.task);
        assert.equal(task.title, "wire the hub over HTTP");
        assert.equal(task.available, true);
        assert.equal(task.claim, null);
        assert.equal(task.closed, null);
      }).pipe(Effect.scoped, Effect.provide(live)),
    ),
  );

  it.live("groups tasks under the parent its children name", () =>
    dispatched(
      Effect.gen(function* () {
        const key = yield* generate("worker@example.com");
        const milestone = yield* HubTask.open({ repo: "r", title: "v0.4 — Identity", key });
        const first = yield* HubTask.open({
          repo: "r",
          title: "sign events with the browser key",
          parent: milestone.task,
          key,
        });
        // Filed after the fact, which is the case a field on `task.opened`
        // could never answer: work moves between releases.
        const second = yield* HubTask.open({ repo: "r", title: "verify signatures", key });
        yield* HubTask.reparent({
          repo: "r",
          task: second.task,
          parent: milestone.task,
          key,
        });

        const client = yield* HttpApiTest.groups(Api.api, ["hub"]);
        const answer = yield* client.hub.tasks({ params: { repo: "r" }, query: {} });
        const byId = new Map(answer.items.map((task) => [task.task, task]));

        assert.deepEqual(
          [...(byId.get(milestone.task)?.children ?? [])].sort(),
          [first.task, second.task].sort(),
        );
        assert.equal(byId.get(milestone.task)?.parent, null);
        assert.equal(byId.get(first.task)?.parent, milestone.task);
        assert.equal(byId.get(second.task)?.parent, milestone.task);
        // The edge lives on the children; the parent's own ref never hears of
        // it, so it carries no children of its own to report.
        assert.deepEqual(byId.get(first.task)?.children, []);
      }).pipe(Effect.scoped, Effect.provide(live)),
    ),
  );

  it.live("lets a member re-file work somebody else opened, but not close it", () =>
    dispatched(
      Effect.gen(function* () {
        const opener = yield* generate("opener@example.com");
        const other = yield* generate("other@example.com");
        const milestone = yield* HubTask.open({ repo: "r", title: "v0.4 — Identity", key: opener });
        const work = yield* HubTask.open({ repo: "r", title: "sign events", key: opener });

        // Triage, and undone by another `task.reparented`: the boundary has
        // already asked for a `hub.*` capability to append here at all.
        yield* HubTask.reparent({
          repo: "r",
          task: work.task,
          parent: milestone.task,
          key: other,
        });
        // Closing is not triage and does not come back, so it stays the
        // opener's — the two rules differ on purpose.
        yield* HubTask.close({ repo: "r", task: work.task, key: other });

        const client = yield* HttpApiTest.groups(Api.api, ["hub"]);
        const answer = yield* client.hub.tasks({ params: { repo: "r" }, query: {} });
        const byId = new Map(answer.items.map((task) => [task.task, task]));
        assert.equal(byId.get(work.task)?.parent, milestone.task);
        assert.deepEqual(byId.get(milestone.task)?.children, [work.task]);
        assert.equal(byId.get(work.task)?.closed, null);
      }).pipe(Effect.scoped, Effect.provide(live)),
    ),
  );

  it.live("severs a loop the refs closed behind its back", () =>
    dispatched(
      Effect.gen(function* () {
        const key = yield* generate("worker@example.com");
        const a = yield* HubTask.open({ repo: "r", title: "A", key });
        const b = yield* HubTask.open({ repo: "r", title: "B", parent: a.task, key });

        // The edge `hub/Task.ts` refuses, written the way `POST /hub/events`
        // writes: signed bytes appended straight to the ref, never asking the
        // module that would have said no. Two members racing on two refs get
        // here honestly; this is the same end state, reached on purpose.
        const base = yield* HubTask.context("r", a.task);
        const payload = { ...base, type: "task.reparented" as const, parent: b.task };
        const bytes = HubTask.encode(payload);
        yield* HubEvent.appendTo({
          ref: HubTask.refOf(a.task),
          message: `task.reparented ${payload.id}\n`,
          payload: bytes,
          signatures: [yield* sign(key, bytes, NAMESPACE)],
        });

        const client = yield* HttpApiTest.groups(Api.api, ["hub"]);
        const answer = yield* client.hub.tasks({ params: { repo: "r" }, query: {} });
        const byId = new Map(answer.items.map((task) => [task.task, task]));
        // Both ends of the loop report no parent, so every chain a reader can
        // walk is finite — which is what lets the walkers not guard.
        assert.equal(byId.get(a.task)?.parent, null);
        assert.equal(byId.get(b.task)?.parent, null);
        assert.deepEqual(byId.get(a.task)?.children, []);
        assert.deepEqual(byId.get(b.task)?.children, []);
      }).pipe(Effect.scoped, Effect.provide(live)),
    ),
  );

  it.live("answers an empty, disabled hub for a repository with no genesis", () =>
    dispatched(
      Effect.gen(function* () {
        const client = yield* HttpApiTest.groups(Api.api, ["hub"]);
        const answer = yield* client.hub.pulls({ params: { repo: "r" }, query: {} });
        assert.equal(answer.enabled, false);
        assert.match(answer.reason ?? "", /no genesis/);
        assert.deepEqual(answer.items, []);
      }).pipe(Effect.scoped, Effect.provide(live)),
    ),
  );

  it.live("projects a member's pull request into the listing and the detail", () =>
    dispatched(
      Effect.gen(function* () {
        const git = yield* GitRepository.Repository;

        const root = yield* generate("root@example.com");
        const member = yield* generate("member@example.com");
        const genesis = yield* create([formatPublicKey(root.publicKey)], 1);
        yield* writeGenesis(genesis, [yield* signGenesis(genesis, root)]);
        yield* Log.issue(
          yield* Certificate.grant({
            repo: genesis.repoId,
            publicKey: formatPublicKey(member.publicKey),
            capabilities: ["repo.read", "source.push", "hub.create-pr", "hub.comment"],
            id: Log.newId(),
          }),
          [root],
        );

        const head = yield* git.commit({
          branch: "refs/heads/topic",
          tree: EMPTY_TREE_OID,
          message: "proposed",
          author: { ...alice, at: new Date(1_700_000_000_000) },
        });

        const opened = yield* PullRequest.open({
          repo: genesis.repoId,
          title: "teach the hub HTTP",
          description: "reads for every screen",
          base: "refs/heads/main",
          head,
          key: member,
        });

        const client = yield* HttpApiTest.groups(Api.api, ["hub"]);

        const listing = yield* client.hub.pulls({ params: { repo: "r" }, query: {} });
        assert.equal(listing.enabled, true);
        assert.equal(listing.items.length, 1);
        const summary = listing.items[0]!;
        assert.equal(summary.id, opened.pr);
        assert.equal(summary.title, "teach the hub HTTP");
        assert.equal(summary.state, "open");
        assert.equal(summary.head, head);
        assert.equal(summary.approvals, 0);

        const detail = yield* client.hub.pull({ params: { repo: "r", id: opened.pr } });
        assert.equal(detail.description, "reads for every screen");
        assert.equal(detail.author, yield* fingerprint(member.publicKey));
        assert.deepEqual(detail.reviews, []);
        assert.deepEqual(detail.checkList, []);

        const missing = yield* client.hub
          .pull({ params: { repo: "r", id: "nope" } })
          .pipe(Effect.flip);
        assert.equal(missing._tag, "Invalid");
      }).pipe(Effect.scoped, Effect.provide(live)),
    ),
  );

  it.live("appends a pre-signed event and refuses bytes nobody signed", () =>
    dispatched(
      Effect.gen(function* () {
        const git = yield* GitRepository.Repository;

        const root = yield* generate("root@example.com");
        const member = yield* generate("member@example.com");
        const genesis = yield* create([formatPublicKey(root.publicKey)], 1);
        yield* writeGenesis(genesis, [yield* signGenesis(genesis, root)]);
        yield* Log.issue(
          yield* Certificate.grant({
            repo: genesis.repoId,
            publicKey: formatPublicKey(member.publicKey),
            capabilities: ["repo.read", "source.push", "hub.create-pr"],
            id: Log.newId(),
          }),
          [root],
        );

        const head = yield* git.commit({
          branch: "refs/heads/topic",
          tree: EMPTY_TREE_OID,
          message: "proposed",
          author: { ...alice, at: new Date(1_700_000_000_000) },
        });

        // The exact bytes `PullRequest.open` would sign, signed elsewhere —
        // which is the whole point of the endpoint: the key never travels.
        const pr = HubEvent.newId();
        const payload = {
          version: 1,
          type: "pr.opened",
          repo: genesis.repoId,
          pr,
          id: HubEvent.newId(),
          issuedAt: new Date(1_700_000_001_000).toISOString(),
          trustHead: yield* git.resolve(Log.LOG_REF),
          title: "opened over JSON",
          description: "",
          base: "refs/heads/main",
          head: HubEvent.qualify(head),
        } as const;
        const bytes = HubEvent.encode(payload);
        const armored = yield* sign(member, bytes, NAMESPACE);
        const base64 = btoa(String.fromCharCode(...bytes));

        const projection = yield* project(genesis);
        const signer = yield* fingerprint(member.publicKey);
        const asMember = Effect.provideService(Auth.Requester, {
          principal: projection.members.get(signer) ?? null,
          signer,
          capabilities: ["repo.read", "source.push", "hub.create-pr"],
          projection,
          envelope: null,
        });

        const client = yield* HttpApiTest.groups(Api.api, ["hub"]);
        const appended = yield* client.hub
          .append({
            params: { repo: "r" },
            payload: { payload: base64, signatures: [armored] },
          })
          .pipe(asMember);
        assert.equal(appended.ref, HubEvent.refOf(pr));

        const listing = yield* client.hub.pulls({ params: { repo: "r" }, query: {} });
        assert.equal(
          listing.items.some((entry) => entry.id === pr),
          true,
        );

        // Tampered bytes: the signature no longer covers them, so nothing is
        // appended — whatever the projection would later have said.
        const tampered = btoa(String.fromCharCode(...HubEvent.encode({ ...payload, title: "x" })));
        const refused = yield* client.hub
          .append({ params: { repo: "r" }, payload: { payload: tampered, signatures: [armored] } })
          .pipe(asMember, Effect.flip);
        assert.equal(refused._tag, "Invalid");
      }).pipe(Effect.scoped, Effect.provide(live)),
    ),
  );
});

describe("Api hub extensions", () => {
  it.live("pages the task listing like every other listing", () =>
    dispatched(
      Effect.gen(function* () {
        const key = yield* generate("worker@example.com");
        for (let index = 0; index < 3; index += 1) {
          yield* HubTask.open({ repo: "r", title: `task ${String(index)}`, key });
        }

        const client = yield* HttpApiTest.groups(Api.api, ["hub"]);
        const first = yield* client.hub.tasks({ params: { repo: "r" }, query: { limit: "2" } });
        assert.equal(first.items.length, 2);
        assert.equal(first.has_more, true);
        assert.notEqual(first.next_cursor, null);
        const rest = yield* client.hub.tasks({
          params: { repo: "r" },
          query: { limit: "2", cursor: first.next_cursor ?? "0" },
        });
        assert.equal(rest.items.length, 1);
        assert.equal(rest.has_more, false);
      }).pipe(Effect.scoped, Effect.provide(live)),
    ),
  );

  it.live("projects sessions: the listing's summaries and the whole account", () =>
    dispatched(
      Effect.gen(function* () {
        const key = yield* generate("agent@example.com");
        const opened = yield* HubSession.open({
          repo: "r",
          agent: { kind: "coding", model: "m", harness: "h" },
          prompt: "wire the sessions over HTTP",
          key,
        });
        yield* HubSession.ask({
          repo: "r",
          session: opened.session,
          question: "merge strategy?",
          options: ["merge", "rebase"],
          refs: [],
          key,
        });

        const client = yield* HttpApiTest.groups(Api.api, ["hub"]);
        const listing = yield* client.hub.sessions({ params: { repo: "r" }, query: {} });
        assert.equal(listing.items.length, 1);
        const summary = listing.items[0]!;
        assert.equal(summary.session, opened.session);
        assert.equal(summary.agent?.model, "m");
        assert.equal(summary.decisions.open, 1);

        const detail = yield* client.hub.session({
          params: { repo: "r", id: opened.session },
        });
        assert.equal(detail.prompts[0]?.prompt, "wire the sessions over HTTP");
        assert.equal(detail.decisions[0]?.question, "merge strategy?");
        assert.equal(detail.decisions[0]?.chose, null);

        const missing = yield* client.hub
          .session({ params: { repo: "r", id: "nope" } })
          .pipe(Effect.flip);
        assert.equal(missing._tag, "Invalid");
      }).pipe(Effect.scoped, Effect.provide(live)),
    ),
  );

  it.live("appends a pre-signed session event beside the task and pull kinds", () =>
    dispatched(
      Effect.gen(function* () {
        const key = yield* generate("agent@example.com");
        const session = HubSession.newId();
        const payload = {
          version: 1,
          type: "session.opened",
          repo: "r",
          session,
          id: HubSession.newId(),
          issuedAt: new Date(1_700_000_000_000).toISOString(),
          trustHead: null,
          agent: { kind: "coding", model: "m", harness: "h" },
          prompt: "opened over JSON",
          role: "user",
          context: null,
        } as const;
        const bytes = HubSession.encode(payload);
        const armored = yield* sign(key, bytes, NAMESPACE);
        const base64 = btoa(String.fromCharCode(...bytes));

        const client = yield* HttpApiTest.groups(Api.api, ["hub"]);
        const appended = yield* client.hub.append({
          params: { repo: "r" },
          payload: { payload: base64, signatures: [armored] },
        });
        assert.equal(appended.ref, HubSession.refOf(session));

        const detail = yield* client.hub.session({ params: { repo: "r", id: session } });
        assert.equal(detail.prompts[0]?.prompt, "opened over JSON");
      }).pipe(Effect.scoped, Effect.provide(live)),
    ),
  );

  it.live("charges task events hub.task and this repository's identity at the door", () =>
    dispatched(
      Effect.gen(function* () {
        const git = yield* GitRepository.Repository;

        const root = yield* generate("root@example.com");
        const worker = yield* generate("worker@example.com");
        const commenter = yield* generate("commenter@example.com");
        const stranger = yield* generate("stranger@example.com");
        const genesis = yield* create([formatPublicKey(root.publicKey)], 1);
        yield* writeGenesis(genesis, [yield* signGenesis(genesis, root)]);
        yield* Log.issue(
          yield* Certificate.grant({
            repo: genesis.repoId,
            publicKey: formatPublicKey(worker.publicKey),
            capabilities: ["repo.read", "source.push", "hub.task"],
            id: Log.newId(),
          }),
          [root],
        );
        yield* Log.issue(
          yield* Certificate.grant({
            repo: genesis.repoId,
            publicKey: formatPublicKey(commenter.publicKey),
            capabilities: ["repo.read", "source.push", "hub.comment"],
            id: Log.newId(),
          }),
          [root],
        );

        const projection = yield* project(genesis);
        const signer = yield* fingerprint(worker.publicKey);
        const asWorker = Effect.provideService(Auth.Requester, {
          principal: projection.members.get(signer) ?? null,
          signer,
          capabilities: ["repo.read", "source.push", "hub.task"],
          projection,
          envelope: null,
        });

        const client = yield* HttpApiTest.groups(Api.api, ["hub"]);

        const opened = (repo: string) => {
          const task = HubTask.newId();
          const payload = {
            version: 1,
            type: "task.opened",
            repo,
            task,
            id: HubEvent.newId(),
            issuedAt: new Date(1_700_000_002_000).toISOString(),
            trustHead: null,
            title: "judge my signer",
            description: "",
            refs: [],
            pulls: [],
          } as const;
          return { task, bytes: HubTask.encode(payload) };
        };
        const submit = (bytes: Uint8Array, armored: string) =>
          client.hub
            .append({
              params: { repo: "r" },
              payload: {
                payload: btoa(String.fromCharCode(...bytes)),
                signatures: [armored],
              },
            })
            .pipe(asWorker);

        // A member holding another hub capability — but not `hub.task` — is
        // refused: capabilities keep their boundaries even inside the family.
        const mine = opened(genesis.repoId);
        const underCapability = yield* submit(
          mine.bytes,
          yield* sign(commenter, mine.bytes, NAMESPACE),
        ).pipe(Effect.flip);
        assert.equal(underCapability._tag, "Invalid");
        if (underCapability._tag === "Invalid") {
          assert.match(underCapability.reason, /hub\.task/);
        }

        // A signature from a key this repository never trusted decides
        // nothing, whoever carried it here.
        const untrusted = yield* submit(
          mine.bytes,
          yield* sign(stranger, mine.bytes, NAMESPACE),
        ).pipe(Effect.flip);
        assert.equal(untrusted._tag, "Invalid");

        // An event minted for another repository is refused before a ref moves.
        const foreign = opened("some-other-repository");
        const wrongRepo = yield* submit(
          foreign.bytes,
          yield* sign(worker, foreign.bytes, NAMESPACE),
        ).pipe(Effect.flip);
        assert.equal(wrongRepo._tag, "Invalid");
        assert.equal(yield* git.resolve(HubTask.refOf(foreign.task)), null);

        // The minimally different permitted case: the same bytes, signed by
        // the member who holds exactly the capability the event charges.
        const appended = yield* submit(mine.bytes, yield* sign(worker, mine.bytes, NAMESPACE));
        assert.equal(appended.ref, HubTask.refOf(mine.task));
        const listing = yield* client.hub.tasks({ params: { repo: "r" }, query: {} });
        assert.equal(
          listing.items.some((entry) => entry.task === mine.task),
          true,
        );
      }).pipe(Effect.scoped, Effect.provide(live)),
    ),
  );

  it.live("settles a pull request as one transition: base and pr.merged move together", () =>
    dispatched(
      Effect.gen(function* () {
        const git = yield* GitRepository.Repository;

        const root = yield* generate("root@example.com");
        const merger = yield* generate("merger@example.com");
        const commenter = yield* generate("commenter@example.com");
        const genesis = yield* create([formatPublicKey(root.publicKey)], 1);
        yield* writeGenesis(genesis, [yield* signGenesis(genesis, root)]);
        yield* Log.issue(
          yield* Certificate.grant({
            repo: genesis.repoId,
            publicKey: formatPublicKey(merger.publicKey),
            capabilities: ["repo.read", "source.push", "hub.create-pr", "hub.merge"],
            id: Log.newId(),
          }),
          [root],
        );
        yield* Log.issue(
          yield* Certificate.grant({
            repo: genesis.repoId,
            publicKey: formatPublicKey(commenter.publicKey),
            capabilities: ["repo.read", "source.push", "hub.comment"],
            id: Log.newId(),
          }),
          [root],
        );

        // A base with one commit and a topic exactly one ahead of it — the
        // fast-forward shape the endpoint promises to preserve.
        const base = yield* git.commit({
          branch: "refs/heads/main",
          tree: EMPTY_TREE_OID,
          message: "first",
          author: { ...alice, at: new Date(1_700_000_000_000) },
        });
        const head = yield* git.commitTree({
          tree: EMPTY_TREE_OID,
          parents: [base],
          message: "proposed",
          author: { ...alice, at: new Date(1_700_000_001_000) },
        });

        const opened = yield* PullRequest.open({
          repo: genesis.repoId,
          title: "settle me atomically",
          description: "",
          base: "refs/heads/main",
          head,
          key: merger,
        });

        const merged = (by: typeof merger) =>
          Effect.gen(function* () {
            const payload = {
              version: 1,
              type: "pr.merged",
              repo: genesis.repoId,
              pr: opened.pr,
              id: HubEvent.newId(),
              issuedAt: new Date(1_700_000_002_000).toISOString(),
              trustHead: yield* git.resolve(Log.LOG_REF),
              head: HubEvent.qualify(head),
              mergeCommit: HubEvent.qualify(head),
            } as const;
            const bytes = HubEvent.encode(payload);
            return {
              payload: btoa(String.fromCharCode(...bytes)),
              signatures: [yield* sign(by, bytes, NAMESPACE)],
            };
          });

        const projection = yield* project(genesis);
        const signer = yield* fingerprint(merger.publicKey);
        const asMerger = Effect.provideService(Auth.Requester, {
          principal: projection.members.get(signer) ?? null,
          signer,
          capabilities: ["repo.read", "source.push", "hub.create-pr", "hub.merge"],
          projection,
          envelope: null,
        });

        const client = yield* HttpApiTest.groups(Api.api, ["hub"]);

        // A signer holding hub.comment cannot settle anything.
        const signed = yield* merged(merger);
        const underCapability = yield* client.hub
          .merge({
            params: { repo: "r", id: opened.pr },
            payload: { head, expected: base, ...(yield* merged(commenter)) },
          })
          .pipe(asMerger, Effect.flip);
        assert.equal(underCapability._tag, "Invalid");

        // A stale compare-and-swap is a conflict, and nothing moves.
        const stale = yield* client.hub
          .merge({
            params: { repo: "r", id: opened.pr },
            payload: { head, expected: head, ...signed },
          })
          .pipe(asMerger, Effect.flip);
        assert.equal(stale._tag, "RefConflict");
        assert.equal(yield* git.resolve("refs/heads/main"), base);

        // The permitted case: base advances to exactly the approved head and
        // the projection — read fresh — says merged.
        const settled = yield* client.hub
          .merge({
            params: { repo: "r", id: opened.pr },
            payload: { head, expected: base, ...signed },
          })
          .pipe(asMerger);
        assert.equal(settled.commit, head);
        assert.equal(yield* git.resolve("refs/heads/main"), head);
        const detail = yield* client.hub.pull({ params: { repo: "r", id: opened.pr } });
        assert.equal(detail.state, "merged");
        assert.equal(detail.mergeCommit, head);

        // Asking again for the transition that already committed agrees.
        const again = yield* client.hub
          .merge({
            params: { repo: "r", id: opened.pr },
            payload: { head, expected: base, ...signed },
          })
          .pipe(asMerger);
        assert.equal(again.commit, head);
      }).pipe(Effect.scoped, Effect.provide(live)),
    ),
  );

  it.live("answers the trust roster, and an empty one without a genesis", () =>
    dispatched(
      Effect.gen(function* () {
        const client = yield* HttpApiTest.groups(Api.api, ["hub"]);
        const empty = yield* client.hub.members({ params: { repo: "r" } });
        assert.equal(empty.enabled, false);
        assert.deepEqual(empty.members, []);

        const root = yield* generate("root@example.com");
        const member = yield* generate("member@example.com");
        const genesis = yield* create([formatPublicKey(root.publicKey)], 1);
        yield* writeGenesis(genesis, [yield* signGenesis(genesis, root)]);
        yield* Log.issue(
          yield* Certificate.grant({
            repo: genesis.repoId,
            publicKey: formatPublicKey(member.publicKey),
            capabilities: ["repo.read", "source.push"],
            id: Log.newId(),
          }),
          [root],
        );

        const roster = yield* client.hub.members({ params: { repo: "r" } });
        assert.equal(roster.enabled, true);
        const signer = yield* fingerprint(member.publicKey);
        const found = roster.members.find((entry) => entry.fingerprint === signer);
        assert.notEqual(found, undefined);
        assert.deepEqual(found?.capabilities, ["repo.read", "source.push"]);
      }).pipe(Effect.scoped, Effect.provide(live)),
    ),
  );
});

describe("Api policy and archive", () => {
  it.live("answers the defaults, accepts new rules, and reads them back", () =>
    dispatched(
      Effect.gen(function* () {
        const client = yield* HttpApiTest.groups(Api.api, ["repo"]);

        const defaults = yield* client.repo.policy({ params: { repo: "r" } });
        assert.equal(defaults.ref, null);
        assert.equal(defaults.rules.requiredApprovals, 0);

        const written = yield* client.repo.policyWrite({
          params: { repo: "r" },
          payload: {
            ...defaults.rules,
            protected: ["refs/heads/main"],
            requiredApprovals: 2,
            requiredChecks: ["test"],
          },
        });
        assert.equal(written.rules.requiredApprovals, 2);

        const after = yield* client.repo.policy({ params: { repo: "r" } });
        assert.equal(after.ref, written.commit);
        assert.deepEqual(after.rules.protected, ["refs/heads/main"]);
        assert.equal(after.rules.requiredChecks[0], "test");

        // Turn on something a client built before it existed knows nothing
        // about, then write again as such a client would: without the field.
        yield* client.repo.policyWrite({
          params: { repo: "r" },
          payload: { ...after.rules, queueCandidates: true, queueDepth: 3 },
        });
        const { queueCandidates: _on, queueDepth: _deep, ...older } = after.rules;
        const blind = yield* client.repo.policyWrite({
          params: { repo: "r" },
          payload: { ...older, requiredApprovals: 1 },
        });
        assert.equal(
          blind.rules.queueCandidates,
          true,
          "a field the client did not send keeps what the repository had",
        );
        assert.equal(blind.rules.queueDepth, 3);

        // And what it is told is what will be enforced, clamp included.
        const clamped = yield* client.repo.policyWrite({
          params: { repo: "r" },
          payload: { ...after.rules, queueDepth: 1_000_000 },
        });
        assert.equal(clamped.rules.queueDepth, Policy.MAX_QUEUE_DEPTH);
        const stored = yield* client.repo.policy({ params: { repo: "r" } });
        assert.equal(stored.rules.queueDepth, clamped.rules.queueDepth);
      }).pipe(Effect.scoped, Effect.provide(live)),
    ),
  );

  it.live("streams a revision as an archive a reader can take away", () =>
    dispatched(
      Effect.gen(function* () {
        const git = yield* GitRepository.Repository;
        const blob = yield* git.writeBlob(new TextEncoder().encode("archived\n"));
        const tree = yield* git.writeTree([{ mode: "100644", name: "a.txt", oid: blob }]);
        yield* git.commit({
          branch: "refs/heads/main",
          tree,
          message: "first",
          author: { ...alice, at: new Date(1_700_000_000_000) },
        });

        const client = yield* HttpApiTest.groups(Api.api, ["repo"]);
        const bytes = yield* client.repo.archive({
          params: { repo: "r" },
          query: { ref: "refs/heads/main", format: "tar" },
        });
        // A tar of one small file: header block plus content plus the
        // end-of-archive blocks, and the path visible in the header.
        assert.equal(bytes.length % 512, 0);
        const text = new TextDecoder().decode(bytes);
        assert.match(text, /a\.txt/);
        assert.match(text, /archived/);
      }).pipe(Effect.scoped, Effect.provide(live)),
    ),
  );
});
