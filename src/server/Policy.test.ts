import assert from "node:assert/strict";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { describe, it } from "@effect/vitest";

import { Effect, Layer } from "effect";

import { fingerprint, formatPublicKey, generate, type PrivateKey } from "../crypto/SshSignature.ts";
import { EMPTY_TREE_OID, type Signature } from "../git/Format.ts";
import { stores } from "../git/Memory.ts";
import { stores as nodeStores } from "../git/Node.ts";
import * as GitRepository from "../git/Repository.ts";
import { Repository } from "../git/Repository.ts";
import { ObjectStore, type Oid, type RefUpdate, storageOf } from "../git/Store.ts";
import * as Event from "../hub/Event.ts";
import * as PullRequest from "../hub/PullRequest.ts";
import * as Session from "../hub/Session.ts";
import * as Task from "../hub/Task.ts";
import * as Inbox from "../social/Inbox.ts";
import * as SocialLog from "../social/Log.ts";
import * as Statement from "../social/Statement.ts";
import * as Certificate from "../trust/Certificate.ts";
import { create, type Genesis, GENESIS_REF, signGenesis, writeGenesis } from "../trust/Genesis.ts";
import * as Log from "../trust/Log.ts";
import { principalId } from "../trust/Principal.ts";
import { type Member, project as projectTrust } from "../trust/Projection.ts";
import * as Auth from "./Auth.ts";
import * as Policy from "./Policy.ts";
import { evaluate, OPEN, type Principal, type Rules } from "./Policy.ts";

/** A projection standing in for one nothing in these checks reads. */
const EMPTY_PROJECTION = {
  // SAFETY: `gateWrite` reads only the requester's member and capabilities;
  // this value is never compared against a real repository identity.
  repoId: "" as never,
  head: null,
  members: new Map(),
  former: new Map(),
  revoked: new Map(),
  principals: new Map(),
  formerPrincipals: new Map(),
  revokedPrincipals: new Map(),
  roots: [],
  threshold: 0,
  checkpoint: null,
  checkpoints: [],
  rejected: [],
};

const scenario = <A, E>(effect: Effect.Effect<A, E, Repository | ObjectStore>) =>
  Effect.runPromise(
    effect.pipe(
      Effect.provide(
        GitRepository.layer.pipe(
          Layer.provide(GitRepository.hooksNoop),
          Layer.provideMerge(stores),
        ),
      ),
    ),
  );

const author: Signature = {
  name: "Alice",
  email: "alice@example.com",
  at: new Date(1_700_000_000_000),
  offset: 0,
};

interface World {
  readonly genesis: Genesis;
  readonly root: PrivateKey;
  readonly dev: PrivateKey;
  /** A second member, so an approval can come from somebody else. */
  readonly reviewer: PrivateKey;
  readonly principal: Principal;
}

const world = Effect.fn("test.world")(function* (capabilities: ReadonlyArray<string>) {
  const root = yield* generate("root@example.com");
  const dev = yield* generate("dev@example.com");
  const reviewer = yield* generate("reviewer@example.com");

  const genesis = yield* create([formatPublicKey(root.publicKey)], 1);
  yield* writeGenesis(genesis, [yield* signGenesis(genesis, root)]);
  const grant = (key: PrivateKey, held: ReadonlyArray<string>) =>
    Effect.flatMap(
      Certificate.grant({
        repo: genesis.repoId,
        publicKey: formatPublicKey(key.publicKey),
        capabilities: held,
        id: Log.newId(),
      }),
      (payload) => Log.issue(payload, [root]),
    );
  yield* grant(dev, capabilities);
  // A pull request's own author cannot approve it, so anything that needs an
  // approval needs a second member to give one.
  yield* grant(reviewer, ["hub.review", "hub.approve"]);

  const trust = yield* projectTrust(genesis);
  const print = yield* fingerprint(dev.publicKey);
  // SAFETY: `dev` was granted above, so the fold holds them.
  const member = trust.members.get(print) as Member;
  return {
    genesis,
    root,
    dev,
    reviewer,
    principal: { member, capabilities },
  } satisfies World;
});

const trustOf = (where: World) => projectTrust(where.genesis);

/** Two commits on a branch, so there is something to fast-forward and to drop. */
const history = Effect.fn("test.history")(function* (branch: string) {
  const repository = yield* Repository;
  const first = yield* repository.commit({
    branch,
    tree: EMPTY_TREE_OID,
    message: "first",
    author,
  });
  const second = yield* repository.commit({
    branch,
    tree: EMPTY_TREE_OID,
    message: "second",
    author,
  });
  return { first, second };
});

/** The rules a repository has published, written the way it publishes them. */
const publish = Effect.fn("test.publish")(function* (rules: Rules) {
  const repository = yield* Repository;
  const blob = yield* repository.writeBlob(Policy.encodeRules(rules));
  const tree = yield* repository.writeTree([{ mode: "100644", name: "policy.json", oid: blob }]);
  const commit = yield* repository.commitTree({
    tree,
    parents: [],
    message: "policy\n",
    author,
  });
  yield* repository.setRef({ name: Policy.RULES_REF, to: commit });
});

/**
 * The receive-pack path, as a request from one principal.
 *
 * `gate` reads the requester, the genesis and the rules from the repository
 * itself, so the only thing a test supplies is who is asking. `null` is the
 * anonymous caller — a plain git repository's only kind.
 */
const gateAs = (where: World | null, updates: ReadonlyArray<RefUpdate>, atomic = true) =>
  Policy.gate(updates, atomic).pipe(
    Effect.provide(
      Auth.requester({
        principal: where?.principal.member ?? null,
        signer: null,
        capabilities: where?.principal.capabilities ?? [],
        projection: EMPTY_PROJECTION,
        envelope: null,
      }),
    ),
  );

/** The JSON verbs' pre-check, as a request from one principal. */
const gateWriteAs = (where: World | null, ref: string, rewrites = false) =>
  Policy.gateWrite(ref, rewrites).pipe(
    Effect.provide(
      Auth.requester({
        principal: where?.principal.member ?? null,
        signer: null,
        capabilities: where?.principal.capabilities ?? [],
        projection: EMPTY_PROJECTION,
        envelope: null,
      }),
    ),
  );

const judge = (where: World, update: { name: string; value: Oid | null }, rules: Rules = OPEN) =>
  Effect.flatMap(trustOf(where), (trust) =>
    evaluate({ update, principal: where.principal, genesis: where.genesis, trust, rules }),
  );

describe("Policy", () => {
  describe("capabilities", () => {
    it("allows a fast-forward from a member who may push", async () => {
      const decision = await scenario(
        Effect.gen(function* () {
          const where = yield* world(["source.push"]);
          const { second } = yield* history("refs/heads/topic");
          return yield* judge(where, { name: "refs/heads/topic", value: second });
        }),
      );
      assert.equal(decision.ok, true);
    });

    it("refuses a push from a member who may not", async () => {
      const decision = await scenario(
        Effect.gen(function* () {
          const where = yield* world(["repo.read"]);
          const { second } = yield* history("refs/heads/topic");
          return yield* judge(where, { name: "refs/heads/topic", value: second });
        }),
      );
      assert.equal(decision.ok, false);
      assert.match(decision.ok === false ? decision.reason : "", /source\.push/);
    });

    it("refuses a push that drops commits without source.force-push", async () => {
      const decision = await scenario(
        Effect.gen(function* () {
          const where = yield* world(["source.push"]);
          const { first } = yield* history("refs/heads/topic");
          // Back to the first commit: the second stops being reachable.
          return yield* judge(where, { name: "refs/heads/topic", value: first });
        }),
      );
      assert.equal(decision.ok, false);
      assert.match(decision.ok === false ? decision.reason : "", /drops commits/);
    });

    it("allows it from a member who holds source.force-push", async () => {
      const decision = await scenario(
        Effect.gen(function* () {
          const where = yield* world(["source.push", "source.force-push"]);
          const { first } = yield* history("refs/heads/topic");
          return yield* judge(where, { name: "refs/heads/topic", value: first });
        }),
      );
      assert.equal(decision.ok, true);
    });

    it("refuses a deletion without source.delete", async () => {
      const decision = await scenario(
        Effect.gen(function* () {
          const where = yield* world(["source.push"]);
          yield* history("refs/heads/topic");
          return yield* judge(where, { name: "refs/heads/topic", value: null });
        }),
      );
      assert.equal(decision.ok, false);
      assert.match(decision.ok === false ? decision.reason : "", /source\.delete/);
    });

    it("refuses an anonymous write outright", async () => {
      const decision = await scenario(
        Effect.gen(function* () {
          const where = yield* world(["source.push"]);
          const { second } = yield* history("refs/heads/topic");
          const trust = yield* trustOf(where);
          return yield* evaluate({
            update: { name: "refs/heads/topic", value: second },
            principal: { member: null, capabilities: [] },
            genesis: where.genesis,
            trust,
            rules: OPEN,
          });
        }),
      );
      assert.equal(decision.ok, false);
      assert.match(decision.ok === false ? decision.reason : "", /authentication required/);
    });
  });

  describe("namespaces", () => {
    it("admits only the social log and charges social.write to append it", async () => {
      const outcome = await scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const allowed = yield* world(["source.push", "social.write"]);
          const allowedTrust = yield* trustOf(allowed);
          const allowedPrincipal = principalId(allowed.genesis.repoId);
          const allowedHead = yield* SocialLog.issue(
            Statement.follow({
              author: allowedPrincipal,
              subject: allowedPrincipal,
              id: SocialLog.newId(),
              socialHead: null,
              trustHead: allowedTrust.head,
              petname: "self",
            }),
            allowed.dev,
          );
          yield* repository.deleteRef(SocialLog.LOG_REF);

          const accepted = yield* judge(allowed, {
            name: SocialLog.LOG_REF,
            value: allowedHead,
          });
          const stray = yield* judge(allowed, {
            name: "refs/social/notes",
            value: allowedHead,
          });

          const missingCapability = yield* judge(
            {
              ...allowed,
              principal: { ...allowed.principal, capabilities: ["source.push"] },
            },
            {
              name: SocialLog.LOG_REF,
              value: allowedHead,
            },
          );

          return { accepted, stray, missingCapability };
        }),
      );

      assert.equal(outcome.accepted.ok, true);
      assert.equal(outcome.stray.ok, false);
      assert.match(
        outcome.stray.ok === false ? outcome.stray.reason : "",
        /not part of the social namespace/,
      );
      assert.equal(outcome.missingCapability.ok, false);
      assert.match(
        outcome.missingCapability.ok === false ? outcome.missingCapability.reason : "",
        /social\.write/,
      );
    });

    it("refuses a newly pushed social statement signed by a revoked device", async () => {
      const decision = await scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const where = yield* world(["source.push", "social.write"]);
          const beforeRevocation = yield* trustOf(where);
          yield* Log.issue(
            Certificate.revoke({
              repo: where.genesis.repoId,
              subject: yield* fingerprint(where.dev.publicKey),
              reason: "rotated",
              id: Log.newId(),
            }),
            [where.root],
          );

          const principal = principalId(where.genesis.repoId);
          const head = yield* SocialLog.issue(
            Statement.follow({
              author: principal,
              subject: principal,
              id: SocialLog.newId(),
              socialHead: null,
              trustHead: beforeRevocation.head,
              petname: "backdated",
            }),
            where.dev,
          );
          yield* repository.deleteRef(SocialLog.LOG_REF);
          return yield* judge(where, { name: SocialLog.LOG_REF, value: head });
        }),
      );

      assert.equal(decision.ok, false);
      assert.match(decision.ok === false ? decision.reason : "", /revoked.*social\.follow/);
    });

    it("keeps the inbox quarantine writable only through its dedicated operation", async () => {
      const outcome = await scenario(
        Effect.gen(function* () {
          const where = yield* world(["source.push"]);
          const { second } = yield* history("refs/heads/proposal");
          const trust = yield* trustOf(where);
          const id = "018bcfe5-6800-7000-8000-000000000001";
          return {
            push: yield* judge(where, {
              name: "refs/quarantine/inbox/proposal",
              value: second,
            }),
            api: yield* gateWriteAs(where, "refs/quarantine/inbox/proposal"),
            inbox: yield* evaluate({
              update: { name: `${Inbox.PENDING_PREFIX}${id}`, value: second },
              principal: { member: null, capabilities: [Auth.INBOX_SUBMIT] },
              genesis: where.genesis,
              trust,
              rules: OPEN,
            }),
            malformed: yield* evaluate({
              update: { name: "refs/quarantine/inbox/proposal", value: second },
              principal: { member: null, capabilities: [Auth.INBOX_SUBMIT] },
              genesis: where.genesis,
              trust,
              rules: OPEN,
            }),
          };
        }),
      );

      assert.equal(outcome.push.ok, false);
      assert.match(outcome.push.ok ? "" : outcome.push.reason, /quarantine operation/);
      assert.match(outcome.api ?? "", /quarantine operation/);
      assert.equal(outcome.inbox.ok, true);
      assert.equal(outcome.malformed.ok, false);
      assert.match(outcome.malformed.ok ? "" : outcome.malformed.reason, /UUIDv7/);
    });

    it("refuses to move the genesis", async () => {
      const decision = await scenario(
        Effect.gen(function* () {
          const where = yield* world(["repo.admin"]);
          const { second } = yield* history("refs/heads/topic");
          return yield* judge(where, { name: "refs/meta/trust/genesis", value: second });
        }),
      );
      assert.equal(decision.ok, false);
      assert.match(decision.ok === false ? decision.reason : "", /written once/);
    });

    it("refuses to delete a hub ref", async () => {
      const decision = await scenario(
        Effect.gen(function* () {
          const where = yield* world(["repo.admin"]);
          const { pr } = yield* PullRequest.open({
            repo: where.genesis.repoId,
            title: "t",
            base: "refs/heads/main",
            head: EMPTY_TREE_OID,
            key: where.dev,
          });
          return yield* judge(where, { name: `refs/hub/pr/${pr}`, value: null });
        }),
      );
      assert.equal(decision.ok, false);
      assert.match(decision.ok === false ? decision.reason : "", /append-only/);
    });

    it("refuses a hub update that does not contain what it replaces", async () => {
      const decision = await scenario(
        Effect.gen(function* () {
          const where = yield* world(["repo.admin"]);
          const { pr } = yield* PullRequest.open({
            repo: where.genesis.repoId,
            title: "t",
            base: "refs/heads/main",
            head: EMPTY_TREE_OID,
            key: where.dev,
          });
          // An unrelated commit: rewriting the history rather than adding to it.
          const { second } = yield* history("refs/heads/elsewhere");
          return yield* judge(where, { name: `refs/hub/pr/${pr}`, value: second });
        }),
      );
      assert.equal(decision.ok, false);
      assert.match(decision.ok === false ? decision.reason : "", /must contain/);
    });

    it("holds a hub ref to the fold ceiling when the push creates it", async () => {
      // Asked only once the ref existed, the ceiling missed the push that
      // matters most: the *first* one. A create could bring a history of any
      // size onto a namespace nothing can ever delete, so every later fold,
      // protected-branch push and collection paid for it forever.
      const outcome = await scenario(
        Effect.gen(function* () {
          const where = yield* world(["repo.admin"]);
          const repository = yield* Repository;
          const { pr } = yield* PullRequest.open({
            repo: where.genesis.repoId,
            title: "t",
            base: "refs/heads/main",
            head: EMPTY_TREE_OID,
            key: where.dev,
          });
          yield* PullRequest.comment({
            repo: where.genesis.repoId,
            pr,
            body: "more",
            key: where.dev,
          });
          const head = yield* repository.resolve(`refs/hub/pr/${pr}`);

          // Judged as a create of a ref this repository does not have, which is
          // what a first push of somebody else's oversized history looks like.
          return yield* judge(where, { name: "refs/hub/pr/elsewhere", value: head }).pipe(
            Effect.provide(Event.ceiling(1)),
          );
        }),
      );
      assert.equal(outcome.ok, false);
      assert.match(outcome.ok === false ? outcome.reason : "", /a fold will walk/);
    });

    it("refuses a hub event signed by a key this repository has revoked", async () => {
      // A hub event's trust head is written by its own signer, and the only
      // thing the fold holds it to is the floor its ancestors raise it to — so
      // a pull request that has just been opened has no floor at all. Name a
      // pre-revocation head on the `pr.opened` and it becomes the floor for an
      // approval signed by the revoked key against the same head: the
      // revocation is unreachable from there, `former` supplies the
      // capabilities it had, and the approval satisfies a protected branch.
      // Revocation had no effect on new pull requests.
      //
      // The boundary answers what the events cannot. Not "does this head
      // predate a revocation" — that catches the honest straggler too, and on
      // an append-only ref that is a pull request they can never push again —
      // but "is this signature from a key this repository has already
      // revoked", which no honest client produces.
      const outcome = await scenario(
        Effect.gen(function* () {
          const where = yield* world(["repo.admin"]);
          const repository = yield* Repository;
          const { pr } = yield* PullRequest.open({
            repo: where.genesis.repoId,
            title: "t",
            base: "refs/heads/main",
            head: EMPTY_TREE_OID,
            key: where.dev,
          });
          const before = yield* repository.resolve(`refs/hub/pr/${pr}`);
          yield* PullRequest.review({
            repo: where.genesis.repoId,
            pr,
            head: EMPTY_TREE_OID,
            decision: "approve",
            key: where.reviewer,
          });
          const after = yield* repository.resolve(`refs/hub/pr/${pr}`);
          // Rewound, so the approval reads as new to the boundary.
          yield* repository.setRef({ name: `refs/hub/pr/${pr}`, to: before! });

          yield* Log.issue(
            Certificate.revoke({
              repo: where.genesis.repoId,
              subject: yield* fingerprint(where.reviewer.publicKey),
              reason: "compromised",
              id: Log.newId(),
            }),
            [where.root],
          );
          const trust = yield* projectTrust(where.genesis);

          const judged = (value: Oid) =>
            evaluate({
              update: { name: `refs/hub/pr/${pr}`, value },
              principal: where.principal,
              genesis: where.genesis,
              trust,
              rules: OPEN,
            });
          return { settled: yield* judged(before!), added: yield* judged(after!) };
        }),
      );

      assert.equal(outcome.settled.ok, true, "what the ref already held is left alone");
      assert.equal(outcome.added.ok, false);
      assert.match(outcome.added.ok === false ? outcome.added.reason : "", /has been revoked/);
    });

    it("does not re-judge what the ref already holds when a join reconciles it", async () => {
      // The walk's boundary is one oid, and it cuts only the chain that runs
      // through it — a join has a second parent, so an ordinary reconciling
      // push walked back to the root and re-read every event already on the
      // ref. One old comment from a member revoked since then made that pull
      // request refuse its own joins for good, on a namespace that cannot be
      // rewound: the pull request, and any protected branch behind it, stuck.
      const outcome = await scenario(
        Effect.gen(function* () {
          const where = yield* world(["repo.admin"]);
          const repository = yield* Repository;
          const { pr } = yield* PullRequest.open({
            repo: where.genesis.repoId,
            title: "t",
            base: "refs/heads/main",
            head: EMPTY_TREE_OID,
            key: where.dev,
          });
          // An approval from the member who is about to be revoked, already on
          // the ref and already judged when it arrived.
          yield* PullRequest.review({
            repo: where.genesis.repoId,
            pr,
            head: EMPTY_TREE_OID,
            decision: "approve",
            key: where.reviewer,
          });
          const settled = yield* repository.resolve(`refs/hub/pr/${pr}`);

          yield* Log.issue(
            Certificate.revoke({
              repo: where.genesis.repoId,
              subject: yield* fingerprint(where.reviewer.publicKey),
              reason: "compromised",
              id: Log.newId(),
            }),
            [where.root],
          );

          // Two ordinary appends by a live member, reconciled by a join: the
          // everyday shape of two people commenting at once.
          yield* PullRequest.comment({
            repo: where.genesis.repoId,
            pr,
            body: "mine",
            key: where.dev,
          });
          const mine = yield* repository.resolve(`refs/hub/pr/${pr}`);
          const yours = yield* repository.commitTree({
            tree: EMPTY_TREE_OID,
            parents: [settled!],
            message: "join\n",
            author,
          });
          const joined = yield* repository.commitTree({
            tree: EMPTY_TREE_OID,
            parents: [mine!, yours],
            message: "join\n",
            author,
          });

          return yield* evaluate({
            update: { name: `refs/hub/pr/${pr}`, value: joined },
            principal: where.principal,
            genesis: where.genesis,
            trust: yield* projectTrust(where.genesis),
            rules: OPEN,
          });
        }),
      );

      assert.equal(
        outcome.ok === false ? outcome.reason : "allowed",
        "allowed",
        "the revoked member's approval was judged when it arrived, not again now",
      );
    });

    it("refuses every event a since-revoked key adds, whatever it says", async () => {
      // Three attempts at an exemption list — "grants authority", then "moves
      // authority", then a list of families — each sprang a leak, because
      // almost everything here feeds a branch rule by some path. `checks` is
      // keyed by name and head, so a `check.started` *replaces* a completed
      // success and flips the branch's checks to failing; a `comment.created`
      // opens an unresolved thread and fails `requireResolvedThreads`. What is
      // already on the ref was judged when it arrived and is not re-judged.
      const outcome = await scenario(
        Effect.gen(function* () {
          const where = yield* world(["repo.admin"]);
          const repository = yield* Repository;
          const { pr } = yield* PullRequest.open({
            repo: where.genesis.repoId,
            title: "t",
            base: "refs/heads/main",
            head: EMPTY_TREE_OID,
            key: where.dev,
          });
          const opened = yield* repository.resolve(`refs/hub/pr/${pr}`);
          yield* PullRequest.comment({
            repo: where.genesis.repoId,
            pr,
            body: "just talking",
            key: where.reviewer,
          });
          const said = yield* repository.resolve(`refs/hub/pr/${pr}`);
          yield* PullRequest.checkCompleted({
            repo: where.genesis.repoId,
            pr,
            head: EMPTY_TREE_OID,
            name: "build",
            provider: "ci",
            status: "success",
            key: where.reviewer,
          });
          const checked = yield* repository.resolve(`refs/hub/pr/${pr}`);
          // Rewound to the opening, so both read as new to the boundary.
          yield* repository.setRef({ name: `refs/hub/pr/${pr}`, to: opened! });

          yield* Log.issue(
            Certificate.revoke({
              repo: where.genesis.repoId,
              subject: yield* fingerprint(where.reviewer.publicKey),
              reason: "compromised",
              id: Log.newId(),
            }),
            [where.root],
          );
          const trust = yield* projectTrust(where.genesis);
          const judged = (value: Oid) =>
            evaluate({
              update: { name: `refs/hub/pr/${pr}`, value },
              principal: where.principal,
              genesis: where.genesis,
              trust,
              rules: OPEN,
            });
          return {
            settled: yield* judged(opened!),
            said: yield* judged(said!),
            checked: yield* judged(checked!),
          };
        }),
      );

      assert.equal(outcome.settled.ok, true, "what the ref already held is left alone");
      assert.equal(outcome.said.ok, false, "a comment opens a thread a branch rule reads");
      assert.equal(outcome.checked.ok, false, "and a check result is read by another");
      assert.match(outcome.checked.ok === false ? outcome.checked.reason : "", /has been revoked/);
    });

    it("refuses a since-revoked member's close, which takes authority away", async () => {
      // Granting is not the only way to move authority. `protectedBranch`
      // skips a pull request that is not open, so a relayed `pr.closed` from a
      // revoked key freezes the branch behind an approved change until a
      // `hub.merge` holder reopens it — and with the revocation post-dating
      // the pull request's last event there is no floor to catch the
      // back-declared head, `former` supplies the capabilities it had, and
      // `signer === author` waives the charge that would otherwise apply.
      const outcome = await scenario(
        Effect.gen(function* () {
          const where = yield* world(["repo.admin"]);
          const repository = yield* Repository;
          const { pr } = yield* PullRequest.open({
            repo: where.genesis.repoId,
            title: "t",
            base: "refs/heads/main",
            head: EMPTY_TREE_OID,
            key: where.reviewer,
          });
          const opened = yield* repository.resolve(`refs/hub/pr/${pr}`);
          yield* PullRequest.close({ repo: where.genesis.repoId, pr, key: where.reviewer });
          const closed = yield* repository.resolve(`refs/hub/pr/${pr}`);
          yield* repository.setRef({ name: `refs/hub/pr/${pr}`, to: opened! });

          yield* Log.issue(
            Certificate.revoke({
              repo: where.genesis.repoId,
              subject: yield* fingerprint(where.reviewer.publicKey),
              reason: "compromised",
              id: Log.newId(),
            }),
            [where.root],
          );

          return yield* evaluate({
            update: { name: `refs/hub/pr/${pr}`, value: closed },
            principal: where.principal,
            genesis: where.genesis,
            trust: yield* projectTrust(where.genesis),
            rules: OPEN,
          });
        }),
      );

      assert.equal(outcome.ok, false);
      assert.match(outcome.ok === false ? outcome.reason : "", /has been revoked/);
    });

    it("refuses a pushed tombstone from a member whose grant has expired", async () => {
      // A permanent verdict does not consult expiry — it cannot, or the answer
      // would move on a wall clock and the host that acted on it would fold a
      // history no replica agrees with. So the only place an expired redactor
      // is turned away is the door, and the door was not looking: a relayed
      // tombstone from a membership that lapsed was honoured, and `gc`
      // destroyed the payload it named.
      const outcome = await scenario(
        Effect.gen(function* () {
          const where = yield* world(["repo.admin"]);
          const repository = yield* Repository;
          yield* Log.issue(
            yield* Certificate.grant({
              repo: where.genesis.repoId,
              publicKey: formatPublicKey(where.dev.publicKey),
              capabilities: ["hub.create-pr", "hub.comment", "hub.redact"],
              id: Log.newId(),
            }),
            [where.root],
          );

          const { pr } = yield* PullRequest.open({
            repo: where.genesis.repoId,
            title: "t",
            base: "refs/heads/main",
            head: EMPTY_TREE_OID,
            key: where.dev,
          });
          const said = yield* PullRequest.comment({
            repo: where.genesis.repoId,
            pr,
            body: "the deploy key is hunter2",
            key: where.dev,
          });
          const before = yield* repository.resolve(`refs/hub/pr/${pr}`);
          const { events } = yield* Event.entries(pr);
          const target = events.find((entry) => entry.commit === said)?.payload?.id ?? "";
          yield* PullRequest.redact({
            repo: where.genesis.repoId,
            pr,
            target,
            reason: "sensitive-content",
            key: where.dev,
          });
          const after = yield* repository.resolve(`refs/hub/pr/${pr}`);
          yield* repository.setRef({ name: `refs/hub/pr/${pr}`, to: before! });

          // Re-granted with an expiry already behind us: they still hold
          // `hub.redact`, and the grant carrying it has lapsed.
          yield* Log.issue(
            yield* Certificate.grant({
              repo: where.genesis.repoId,
              publicKey: formatPublicKey(where.dev.publicKey),
              capabilities: ["hub.create-pr", "hub.comment", "hub.redact"],
              expiresAt: new Date(1_700_000_000_000),
              id: Log.newId(),
            }),
            [where.root],
          );

          return yield* evaluate({
            update: { name: `refs/hub/pr/${pr}`, value: after },
            principal: where.principal,
            genesis: where.genesis,
            trust: yield* projectTrust(where.genesis),
            rules: OPEN,
          });
        }),
      );

      assert.equal(outcome.ok, false);
      assert.match(outcome.ok === false ? outcome.reason : "", /unexpired hub\.redact/);
    });

    it("refuses a pushed tombstone from a member whose hub.redact was narrowed away", async () => {
      // The fold's first pass asks only whether a tombstone's signer *ever*
      // held `hub.redact`, because that set has to be monotone: an answer that
      // shrinks leaves the host which already deleted a payload folding a
      // history no replica agrees with. Generous and monotone is right there
      // and wrong here — a member whose grant was narrowed, keeping
      // `source.push`, could push decoy tombstones naming the ancestors of a
      // real one, drop them out of the first pass and with them the trust
      // floor, and have a tombstone signed against a stale head accepted,
      // sending somebody else's payload to `gc`. "Now" is knowable at the
      // boundary, so it is refused there.
      const outcome = await scenario(
        Effect.gen(function* () {
          const where = yield* world(["repo.admin"]);
          const repository = yield* Repository;
          yield* Log.issue(
            yield* Certificate.grant({
              repo: where.genesis.repoId,
              publicKey: formatPublicKey(where.dev.publicKey),
              capabilities: ["hub.create-pr", "hub.comment", "hub.redact"],
              id: Log.newId(),
            }),
            [where.root],
          );

          const { pr } = yield* PullRequest.open({
            repo: where.genesis.repoId,
            title: "t",
            base: "refs/heads/main",
            head: EMPTY_TREE_OID,
            key: where.dev,
          });
          const comment = yield* PullRequest.comment({
            repo: where.genesis.repoId,
            pr,
            body: "the deploy key is hunter2",
            key: where.dev,
          });
          const before = yield* repository.resolve(`refs/hub/pr/${pr}`);
          const { events } = yield* Event.entries(pr);
          const target = events.find((entry) => entry.commit === comment)?.payload?.id ?? "";
          yield* PullRequest.redact({
            repo: where.genesis.repoId,
            pr,
            target,
            reason: "sensitive-content",
            key: where.dev,
          });
          const after = yield* repository.resolve(`refs/hub/pr/${pr}`);
          // Rewound, so the tombstone reads as new to the boundary.
          yield* repository.setRef({ name: `refs/hub/pr/${pr}`, to: before! });

          // Narrowed, not revoked: they may still push, and they still *ever*
          // held the capability.
          yield* Log.issue(
            yield* Certificate.grant({
              repo: where.genesis.repoId,
              publicKey: formatPublicKey(where.dev.publicKey),
              capabilities: ["hub.create-pr", "hub.comment"],
              id: Log.newId(),
            }),
            [where.root],
          );

          return yield* evaluate({
            update: { name: `refs/hub/pr/${pr}`, value: after },
            principal: where.principal,
            genesis: where.genesis,
            trust: yield* projectTrust(where.genesis),
            rules: OPEN,
          });
        }),
      );

      assert.equal(outcome.ok, false);
      assert.match(outcome.ok === false ? outcome.reason : "", /needs an unexpired hub\.redact/);
    });

    it("holds the trust log to a ceiling of its own", async () => {
      // The log is append-only and needs only `source.push` to grow, and every
      // duplicate statement in it is ranked by a reach walk per copy. Bounded
      // for `refs/hub/` alone, the one ref that is read on every membership
      // check, every push and every collection was the one with no bound.
      const outcome = await scenario(
        Effect.gen(function* () {
          const where = yield* world(["repo.admin"]);
          const repository = yield* Repository;
          const head = yield* repository.resolve(Log.LOG_REF);
          // The membership fold is read first and at the ordinary ceiling: the
          // question here is what the *boundary* does with a value it would
          // not be able to fold, not what a host with no readable log does.
          const trust = yield* trustOf(where);
          return yield* evaluate({
            update: { name: Log.LOG_REF, value: head },
            principal: where.principal,
            genesis: where.genesis,
            trust,
            rules: OPEN,
          }).pipe(Effect.provide(Log.ceiling(0)));
        }),
      );
      assert.equal(outcome.ok, false);
      assert.match(outcome.ok === false ? outcome.reason : "", /a fold will walk/);
    });

    it("still reconciles a pull request whose tree object never arrived", async () => {
      // What the ref already reaches was walked with the namespace predicate,
      // which deliberately steps over a commit whose *tree* is absent — refs
      // are applied without a connectivity check, so a replica can hold one.
      // The walk stopped there, everything behind it dropped out of the set,
      // and the next ordinary reconciling push met an unaccounted parent and
      // was refused for good on a ref that cannot be rewound.
      const outcome = await scenario(
        Effect.gen(function* () {
          const where = yield* world(["repo.admin"]);
          const repository = yield* Repository;
          const objects = yield* ObjectStore;
          const { pr } = yield* PullRequest.open({
            repo: where.genesis.repoId,
            title: "t",
            base: "refs/heads/main",
            head: EMPTY_TREE_OID,
            key: where.dev,
          });
          const opened = yield* repository.resolve(`refs/hub/pr/${pr}`);
          yield* PullRequest.comment({
            repo: where.genesis.repoId,
            pr,
            body: "more",
            key: where.dev,
          });
          const head = yield* repository.resolve(`refs/hub/pr/${pr}`);

          // The middle of the history loses its tree.
          const info = yield* repository.readCommit(head!);
          yield* objects.delete(info.tree);

          // An ordinary concurrent append, reconciled by a join.
          const sibling = yield* repository.commitTree({
            tree: EMPTY_TREE_OID,
            parents: [opened!],
            message: "concurrent\n",
            author,
          });
          const joined = yield* repository.commitTree({
            tree: EMPTY_TREE_OID,
            parents: [head!, sibling],
            message: "join\n",
            author,
          });
          return yield* judge(where, { name: `refs/hub/pr/${pr}`, value: joined });
        }),
      );

      assert.equal(
        outcome.ok === false ? outcome.reason : "allowed",
        "allowed",
        "one missing object must not make the pull request unpushable",
      );
    });

    it("judges edges and signatures alike when a parent commit never arrived", async () => {
      // What the ref already reaches was answered "nothing I can name" when
      // the walk met a commit object that never arrived — refs are applied
      // without a connectivity check, so a replica can hold one. The two rules
      // that read that answer then went opposite ways, and both were wrong:
      // the graft rule waved a source edge through onto a ref that can never
      // be deleted, and the revoked-signer rule re-judged events the ref
      // already held and refused an ordinary join for good.
      const outcome = await scenario(
        Effect.gen(function* () {
          const where = yield* world(["repo.admin"]);
          const repository = yield* Repository;
          const objects = yield* ObjectStore;
          const { pr } = yield* PullRequest.open({
            repo: where.genesis.repoId,
            title: "t",
            base: "refs/heads/main",
            head: EMPTY_TREE_OID,
            key: where.dev,
          });
          const opened = yield* repository.resolve(`refs/hub/pr/${pr}`);
          yield* PullRequest.comment({
            repo: where.genesis.repoId,
            pr,
            body: "more",
            key: where.dev,
          });
          const head = yield* repository.resolve(`refs/hub/pr/${pr}`);

          // The commit object in the middle of the history, gone — not its
          // tree, the commit itself, which is the case that slipped through.
          yield* objects.delete(opened!);

          // A source commit, joined in behind the missing one.
          const blob = yield* repository.writeBlob(new TextEncoder().encode("source\n"));
          const tree = yield* repository.writeTree([
            { mode: "100644", name: "file.txt", oid: blob },
          ]);
          const source = yield* repository.commitTree({
            tree,
            parents: [],
            message: "source\n",
            author,
          });
          const grafted = yield* repository.commitTree({
            tree: EMPTY_TREE_OID,
            parents: [head!, source],
            message: "join\n",
            author,
          });

          // And an ordinary reconciling join, which brings no new edge at all.
          const sibling = yield* repository.commitTree({
            tree: EMPTY_TREE_OID,
            parents: [head!],
            message: "concurrent\n",
            author,
          });
          const joined = yield* repository.commitTree({
            tree: EMPTY_TREE_OID,
            parents: [head!, sibling],
            message: "join\n",
            author,
          });

          return {
            grafted: yield* judge(where, { name: `refs/hub/pr/${pr}`, value: grafted }),
            joined: yield* judge(where, { name: `refs/hub/pr/${pr}`, value: joined }),
          };
        }),
      );

      assert.equal(outcome.grafted.ok, false, "the source edge is still refused");
      assert.match(
        outcome.grafted.ok === false ? outcome.grafted.reason : "",
        /is not part of this history/,
      );
      assert.equal(
        outcome.joined.ok === false ? outcome.joined.reason : "allowed",
        "allowed",
        "and the ordinary join still lands",
      );
    });

    it("refuses a hub ref pointed at a commit from outside the namespace", async () => {
      // Nothing else asks. The ceiling walk steps over a foreign head and
      // reports an empty history; the graft walk steps over it too. So a
      // `source.push` holder could create `refs/hub/pr/<uuid>` at any source
      // commit at all — on a name that can never be deleted, pinning
      // everything it reaches out of reach of `gc` for good.
      const outcome = await scenario(
        Effect.gen(function* () {
          const where = yield* world(["repo.admin"]);
          const repository = yield* Repository;
          const blob = yield* repository.writeBlob(new TextEncoder().encode("source\n"));
          const tree = yield* repository.writeTree([
            { mode: "100644", name: "file.txt", oid: blob },
          ]);
          const source = yield* repository.commitTree({
            tree,
            parents: [],
            message: "source\n",
            author,
          });
          return yield* judge(where, { name: "refs/hub/pr/borrowed", value: source });
        }),
      );
      assert.equal(outcome.ok, false);
      assert.match(outcome.ok === false ? outcome.reason : "", /is not part of/);
    });

    it("refuses a first push that hangs a pull request off the source history", async () => {
      // The edge rule ran only once the ref existed, so the push that matters
      // most went unchecked. On a create only the tip was inspected, and the
      // ceiling walk is itself bounded to the namespace and steps straight
      // over a foreign parent — so a `source.push` holder with no hub
      // capability at all could hang one event commit off the commit holding
      // a secret and create `refs/hub/pr/<fresh-uuid>` at it. `refs/hub/*` can
      // never be deleted and `gc` treats every ref as a root, so the secret
      // stayed reachable and clonable for good.
      const outcome = await scenario(
        Effect.gen(function* () {
          const where = yield* world(["source.push", "hub.create-pr"]);
          const repository = yield* Repository;

          const secret = yield* repository.writeBlob(new TextEncoder().encode("hunter2\n"));
          const tree = yield* repository.writeTree([
            { mode: "100644", name: "secret.txt", oid: secret },
          ]);
          const source = yield* repository.commitTree({
            tree,
            parents: [],
            message: "source\n",
            author,
          });

          // A commit that reads as a hub event — an empty tree is a join —
          // hanging off the source commit rather than off a history of its
          // own.
          const grafted = yield* repository.commitTree({
            tree: EMPTY_TREE_OID,
            parents: [source],
            message: "join\n",
            author,
          });
          return yield* judge(where, { name: "refs/hub/pr/borrowed", value: grafted });
        }),
      );
      assert.equal(outcome.ok, false);
      assert.match(outcome.ok === false ? outcome.reason : "", /is not part of this history/);
    });

    it("refuses a first push bringing two competing openings at once", async () => {
      // The same rule, a step earlier: several parentless `pr.opened` commits
      // in the very push that makes the pull request. Refused on every later
      // push and, before this, allowed on the one that creates it.
      const outcome = await scenario(
        Effect.gen(function* () {
          const where = yield* world(["source.push", "hub.create-pr"]);
          const repository = yield* Repository;
          const one = yield* repository.commitTree({
            tree: EMPTY_TREE_OID,
            parents: [],
            message: "pr.opened one\n",
            author,
          });
          const two = yield* repository.commitTree({
            tree: EMPTY_TREE_OID,
            parents: [],
            message: "pr.opened two\n",
            author,
          });
          const joined = yield* repository.commitTree({
            tree: EMPTY_TREE_OID,
            parents: [one, two],
            message: "join\n",
            author,
          });
          return {
            two: yield* judge(where, { name: "refs/hub/pr/twinned", value: joined }),
            one: yield* judge(where, { name: "refs/hub/pr/single", value: one }),
          };
        }),
      );
      assert.equal(outcome.two.ok, false);
      assert.match(outcome.two.ok === false ? outcome.two.reason : "", /second history/);
      assert.equal(outcome.one.ok, true, "one beginning is what a create is");
    });

    it("refuses an append from somebody who holds no capability of that kind", async () => {
      // Both namespaces read an empty tree as a join, so a chain of commits
      // onto either need carry no statement at all. Charged `source.push`
      // alone, an ordinary contributor could run a pull request or the trust
      // log to the ceiling a fold will walk and — on a namespace with no way
      // back — leave it refusing every later push: the revocation of the
      // padder, the checkpoint that lifts a staleness bound, the approval a
      // protected branch was waiting on.
      const outcome = await scenario(
        Effect.gen(function* () {
          const where = yield* world(["source.push"]);
          const repository = yield* Repository;
          const log = yield* repository.resolve(Log.LOG_REF);
          const padding = yield* repository.commitTree({
            tree: EMPTY_TREE_OID,
            parents: [log!],
            message: "join\n",
            author,
          });
          return {
            trust: yield* judge(where, { name: Log.LOG_REF, value: padding }),
            hub: yield* judge(where, { name: "refs/hub/pr/padded", value: padding }),
          };
        }),
      );

      assert.equal(outcome.trust.ok, false);
      assert.match(outcome.trust.ok === false ? outcome.trust.reason : "", /member\.\* capability/);
      assert.equal(outcome.hub.ok, false);
      assert.match(outcome.hub.ok === false ? outcome.hub.reason : "", /hub\.\* capability/);
    });

    it("lets the holder of a real membership capability grow the trust log", async () => {
      // The charge above has to name a capability that exists. Spelled
      // `trust.*` — a prefix nothing in `CAPABILITIES` starts with — it read
      // as a tighter rule and was in fact a lockout: `repo.admin` and nobody
      // else could append to the log, so the `member.revoke` holder the trust
      // model exists to empower could sign a revocation and never publish it.
      const outcome = await scenario(
        Effect.gen(function* () {
          const where = yield* world(["source.push", "member.revoke"]);
          const repository = yield* Repository;
          const log = yield* repository.resolve(Log.LOG_REF);
          const grown = yield* repository.commitTree({
            tree: EMPTY_TREE_OID,
            parents: [log!],
            message: "join\n",
            author,
          });
          return yield* judge(where, { name: Log.LOG_REF, value: grown });
        }),
      );

      assert.equal(outcome.ok, true, outcome.ok === false ? outcome.reason : "");
    });

    it("refuses a hub ref that does not name a pull request", async () => {
      // `refs/hub/` as a whole is undeletable, and only `refs/hub/pr/<id>` is
      // ever counted, folded or listed as a pull request. A name outside that
      // shape is a permanent entry the population bound does not see, in every
      // ref listing, advertisement, collection root and memo key for the life
      // of the repository.
      const outcome = await scenario(
        Effect.gen(function* () {
          const where = yield* world(["repo.admin"]);
          const repository = yield* Repository;
          const { pr } = yield* PullRequest.open({
            repo: where.genesis.repoId,
            title: "t",
            base: "refs/heads/main",
            head: EMPTY_TREE_OID,
            key: where.dev,
          });
          const head = yield* repository.resolve(`refs/hub/pr/${pr}`);
          return {
            nested: yield* judge(where, { name: "refs/hub/pr/team/42", value: head }),
            beside: yield* judge(where, { name: "refs/hub/index", value: head }),
            proper: yield* judge(where, { name: `refs/hub/pr/${pr}`, value: head }),
          };
        }),
      );
      assert.equal(outcome.nested.ok, false);
      assert.match(outcome.nested.ok === false ? outcome.nested.reason : "", /pull request/);
      assert.equal(outcome.beside.ok, false);
      assert.equal(outcome.proper.ok, true, "a pull request still moves");
    });

    it("requires a commit to name the session that produced it, in the same push", async () => {
      // The flow the rule is for: a branch and the record of what produced it
      // travel in one receive-pack. Judged against the session as it stands on
      // disk, the push that did everything right would be the one refused —
      // its session ref has not been applied yet either.
      const outcome = await scenario(
        Effect.gen(function* () {
          const where = yield* world(["repo.admin"]);
          const repository = yield* Repository;

          const opened = yield* Session.open({
            repo: where.genesis.repoId,
            agent: { kind: "claude-code", model: "m", harness: "h" },
            prompt: "do the thing",
            key: where.dev,
          });
          const before = yield* repository.resolve(Session.refOf(opened.session));

          const provenanced = yield* repository.commitTree({
            tree: EMPTY_TREE_OID,
            parents: [],
            message: `did the thing\n\nSession: ${opened.session}\n`,
            author,
          });
          const bare = yield* repository.commitTree({
            tree: EMPTY_TREE_OID,
            parents: [],
            message: "did the thing quietly\n",
            author,
          });

          yield* Session.produced({
            repo: where.genesis.repoId,
            session: opened.session,
            key: where.dev,
            commits: [provenanced],
          });
          const head = yield* repository.resolve(Session.refOf(opened.session));

          // Rewound, so the repository holds what it held before the push and
          // the batch carries the rest — which is what a receive-pack is.
          yield* repository.setRef({ name: Session.refOf(opened.session), to: before! });

          yield* publish({ ...OPEN, requireProvenance: true });

          return {
            together: yield* gateAs(where, [
              { name: Session.refOf(opened.session), value: head },
              { name: "refs/heads/main", value: provenanced },
            ]),
            alone: yield* gateAs(where, [{ name: "refs/heads/topic", value: provenanced }]),
            bare: yield* gateAs(where, [{ name: "refs/heads/other", value: bare }]),
          };
        }),
      );

      assert.deepEqual(
        outcome.together.refused,
        [],
        "a branch and its provenance in one push are the case this is for",
      );
      assert.equal(outcome.alone.refused.length, 1, "and the record has to be there at all");
      assert.equal(outcome.bare.refused.length, 1);
      assert.match(outcome.bare.refused[0]?.reason ?? "", /Session: trailer/);
    });

    it("judges a push by the membership standing now, not the one the guard saw", async () => {
      // The guard folds the log to decide who is asking; `gate` re-folds when
      // the log has moved since. It re-folded for the namespace rules and kept
      // the guard's capabilities — so a revocation that landed between the two
      // was applied to every rule that reads the trust graph and to none of the
      // capability checks, and the revoked member's push, already in flight,
      // was judged by the grant they no longer had.
      const outcome = await scenario(
        Effect.gen(function* () {
          const where = yield* world(["source.push"]);
          const repository = yield* Repository;
          const commit = yield* repository.commitTree({
            tree: EMPTY_TREE_OID,
            parents: [],
            message: "in flight\n",
            author,
          });

          // What the guard reached, a moment before the revocation.
          const seen = yield* projectTrust(where.genesis);
          const signer = yield* fingerprint(where.dev.publicKey);

          yield* Log.issue(
            Certificate.revoke({
              repo: where.genesis.repoId,
              subject: signer,
              reason: "compromised",
              id: Log.newId(),
            }),
            [where.root],
          );

          return yield* Policy.gate([{ name: "refs/heads/main", value: commit }], true).pipe(
            Effect.provide(
              Auth.requester({
                principal: where.principal.member,
                signer,
                capabilities: where.principal.capabilities,
                projection: seen,
                envelope: null,
              }),
            ),
          );
        }),
      );

      assert.equal(outcome.updates.length, 0, "a revoked member's push does not land");
      assert.equal(outcome.refused.length, 1);
      // A member who holds nothing any more is nobody, and that is what the
      // refusal says: the push is turned away for the same reason a stranger's
      // is, rather than on the strength of a capability list nobody still has.
      assert.match(outcome.refused[0]?.reason ?? "", /authentication required/);
    });

    it("charges hub.redact for a tombstone on a session, not just on a pull request", async () => {
      // The gate below read the record with `Event.decode`, which is the pull
      // request's union — a session's tombstone decoded as nothing there and
      // fell through the `event.redacted` check entirely. So the namespace
      // whose records are prompts, the one this matters most for, let any
      // holder of `hub.session` push a removal of somebody else's prompt.
      const outcome = await scenario(
        Effect.gen(function* () {
          const where = yield* world(["hub.session", "source.push"]);
          const repository = yield* Repository;

          const opened = yield* Session.open({
            repo: where.genesis.repoId,
            agent: { kind: "claude-code", model: "m", harness: "h" },
            prompt: "do the thing",
            key: where.dev,
          });
          const { events } = yield* Session.entries(opened.session);
          const first = events[0]!;
          const before = yield* repository.resolve(Session.refOf(opened.session));

          const base = yield* Session.context(where.genesis.repoId, opened.session);
          yield* Session.issue(
            {
              ...base,
              type: "event.redacted",
              target: first.payload.id,
              targetCommit: Event.qualify(first.commit),
              reason: "I would rather that were not there",
            },
            where.dev,
          );
          const head = yield* repository.resolve(Session.refOf(opened.session));
          yield* repository.setRef({ name: Session.refOf(opened.session), to: before! });

          return yield* judge(where, { name: Session.refOf(opened.session), value: head });
        }),
      );

      assert.equal(outcome.ok, false, "a removal is not something hub.session buys");
      assert.match(outcome.ok === false ? outcome.reason : "", /hub\.redact/);
    });

    it("charges hub.task for a task ref, not merely any hub capability", async () => {
      // The task fold deliberately never re-judges its signers, so this
      // boundary is the only place `hub.task` is ever asked for. Charged
      // "any hub.* capability", a `hub.comment` holder could open, claim and
      // close tasks — which is exactly the coarse door the docs say
      // `hub.task` exists to close.
      const write = (capabilities: ReadonlyArray<string>) =>
        scenario(
          Effect.gen(function* () {
            const where = yield* world(capabilities);
            const repository = yield* Repository;
            const opened = yield* Task.open({
              repo: where.genesis.repoId,
              title: "who may say so",
              key: where.dev,
            });
            const head = yield* repository.resolve(Task.refOf(opened.task));
            return yield* judge(where, { name: Task.refOf(opened.task), value: head });
          }),
        );

      const refused = await write(["hub.comment", "source.push"]);
      assert.equal(refused.ok, false, "hub.comment does not buy the task namespace");
      assert.match(refused.ok === false ? refused.reason : "", /hub\.task/);

      const allowed = await write(["hub.task", "source.push"]);
      assert.equal(allowed.ok, true, "hub.task is exactly what the namespace charges");
    });

    it("admits a session ref, which is the other shape this namespace holds", async () => {
      // Sessions live under `refs/hub/session/<id>` on the same machinery a
      // pull request uses, so the namespace rules have to know about both:
      // read as pull requests only, every session push was refused as a name
      // that does not belong here.
      const outcome = await scenario(
        Effect.gen(function* () {
          const where = yield* world(["repo.admin"]);
          const repository = yield* Repository;
          const opened = yield* Session.open({
            repo: where.genesis.repoId,
            agent: { kind: "claude-code", model: "m", harness: "h" },
            prompt: "do the thing",
            key: where.dev,
          });
          const head = yield* repository.resolve(Session.refOf(opened.session));
          return {
            proper: yield* judge(where, { name: Session.refOf(opened.session), value: head }),
            nested: yield* judge(where, {
              name: `refs/hub/session/${opened.session}/extra`,
              value: head,
            }),
          };
        }),
      );
      assert.equal(outcome.proper.ok, true, "a session ref moves");
      assert.equal(outcome.nested.ok, false, "a name outside either shape does not");
    });

    it("refuses a hub update that grafts a second beginning onto the history", async () => {
      // Every hub event is written onto the ref's current head, so the history
      // has exactly one parentless commit: the `pr.opened` that started it.
      // A join over a *second* root is not adding to that history, it is
      // adding another one beside it — and the fold then has to choose
      // between two openings by something, which on a pull request with no
      // activity yet can only be the oid, which whoever wrote the commit
      // ground. That is how a `hub.create-pr` holder took the authorship, the
      // title and the base of a pull request they had no part in opening.
      const decision = await scenario(
        Effect.gen(function* () {
          const where = yield* world(["repo.admin"]);
          const repository = yield* Repository;
          const { pr } = yield* PullRequest.open({
            repo: where.genesis.repoId,
            title: "t",
            base: "refs/heads/main",
            head: EMPTY_TREE_OID,
            key: where.dev,
          });
          const head = yield* repository.resolve(`refs/hub/pr/${pr}`);

          // A parentless commit of the same shape, joined in beside the
          // opening. The join keeps everything the ref held, so the
          // append-only rule above is satisfied.
          const forged = yield* repository.commitTree({
            tree: EMPTY_TREE_OID,
            parents: [],
            message: "pr.opened forged\n",
            author,
          });
          const joined = yield* repository.commitTree({
            tree: EMPTY_TREE_OID,
            parents: [head!, forged],
            message: "join\n",
            author,
          });
          return yield* judge(where, { name: `refs/hub/pr/${pr}`, value: joined });
        }),
      );
      assert.equal(decision.ok, false);
      assert.match(decision.ok === false ? decision.reason : "", /second history/);
    });

    it("refuses a hub commit that reaches into the source history", async () => {
      // Two problems, one shape. The graft walk had no namespace predicate, so
      // a hub commit naming a *source* commit as a second parent turned one
      // tiny push into a walk of the whole repository, synchronously, on the
      // receive-pack path. Bounding that walk fixed the cost and left the
      // permission: `gc` treats every ref as a root, so the source commit and
      // everything it reaches were pinned out of reach of collection through a
      // name that can never be deleted — a purged secret among them.
      const outcome = await scenario(
        Effect.gen(function* () {
          const where = yield* world(["repo.admin"]);
          const repository = yield* Repository;
          const { pr } = yield* PullRequest.open({
            repo: where.genesis.repoId,
            title: "t",
            base: "refs/heads/main",
            head: EMPTY_TREE_OID,
            key: where.dev,
          });
          const head = yield* repository.resolve(`refs/hub/pr/${pr}`);
          // An ordinary source commit — a real tree, so nothing about it looks
          // like a hub join — named as a parent of a hub commit.
          const blob = yield* repository.writeBlob(new TextEncoder().encode("source\n"));
          const tree = yield* repository.writeTree([
            { mode: "100644", name: "file.txt", oid: blob },
          ]);
          const source = yield* repository.commitTree({
            tree,
            parents: [],
            message: "source\n",
            author,
          });
          const joined = yield* repository.commitTree({
            tree: EMPTY_TREE_OID,
            parents: [head!, source],
            message: "join\n",
            author,
          });
          return yield* judge(where, { name: `refs/hub/pr/${pr}`, value: joined });
        }),
      );

      assert.equal(outcome.ok, false);
      assert.match(outcome.ok === false ? outcome.reason : "", /is not part of this history/);
    });

    it("counts a graft as a second beginning even when it names a parent", async () => {
      // "Root" here means a root of *this* history, which is not the same as a
      // commit with no parents at all. The walk is bounded to the namespace's
      // own commits, so a parent outside it — a fabricated oid, or a commit
      // from the source history — is not an edge this DAG has. Tested against
      // the raw parent list, a graft naming any junk oid read as attached and
      // slipped through: it then out-ranked the genuine opening on descent,
      // supplied the base and the author, and the real `pr.opened` was refused
      // as re-opening somebody else's pull request.
      const outcome = await scenario(
        Effect.gen(function* () {
          const where = yield* world(["repo.admin"]);
          const repository = yield* Repository;
          const { pr } = yield* PullRequest.open({
            repo: where.genesis.repoId,
            title: "t",
            base: "refs/heads/main",
            head: EMPTY_TREE_OID,
            key: where.dev,
          });
          const head = yield* repository.resolve(`refs/hub/pr/${pr}`);

          // A parentless commit, dressed as one that has a parent: the oid it
          // names is not a commit this repository holds at all.
          const forged = yield* repository.commitTree({
            tree: EMPTY_TREE_OID,
            // SAFETY: forty lowercase hex characters, which is what `Oid`
            // brands; it names nothing, which is the point.
            parents: ["f".repeat(40) as Oid],
            message: "pr.opened forged\n",
            author,
          });
          const joined = yield* repository.commitTree({
            tree: EMPTY_TREE_OID,
            parents: [head!, forged],
            message: "join\n",
            author,
          });
          return yield* judge(where, { name: `refs/hub/pr/${pr}`, value: joined });
        }),
      );
      assert.equal(outcome.ok, false);
      assert.match(outcome.ok === false ? outcome.reason : "", /is not part of this history/);
    });

    it("judges a push against the log as it stands, not as the guard last saw it", async () => {
      // The guard folds the log to decide who is asking, and the boundary
      // reuses that fold because it is an Ed25519 verification per signature
      // per record. What makes the reuse safe is the *head*: between the two
      // sits the whole pack upload, and on a host that queues per repository
      // the queue as well. A revocation landing in that window was invisible
      // — so an approval from a key compromised in the meantime still
      // satisfied a protected branch, against the premise this same boundary
      // states elsewhere, that it knows whose keys are revoked now.
      const outcome = await scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const where = yield* world(["source.push", "hub.create-pr"]);
          const { second } = yield* history("refs/heads/main");
          const { pr } = yield* PullRequest.open({
            repo: where.genesis.repoId,
            title: "please",
            base: "refs/heads/main",
            head: second,
            key: where.dev,
          });
          yield* PullRequest.review({
            repo: where.genesis.repoId,
            pr,
            head: second,
            decision: "approve",
            key: where.reviewer,
          });

          // What the guard reached, a moment ago: the approval counts.
          const asOfTheGuard = yield* trustOf(where);

          // And then the reviewer's key turns out to have been compromised
          // all along, which is the revocation that reaches backwards.
          yield* Log.issue(
            Certificate.revoke({
              repo: where.genesis.repoId,
              subject: yield* fingerprint(where.reviewer.publicKey),
              reason: "compromised",
              id: Log.newId(),
            }),
            [where.root],
          );

          // The rules are read from the repository, so they are published
          // here rather than passed.
          const blob = yield* repository.writeBlob(
            Policy.encodeRules({
              ...OPEN,
              protected: ["refs/heads/main"],
              requirePullRequest: true,
              requiredApprovals: 1,
            }),
          );
          const tree = yield* repository.writeTree([
            { mode: "100644", name: "policy.json", oid: blob },
          ]);
          yield* repository.setRef({
            name: Policy.RULES_REF,
            to: yield* repository.commitTree({ tree, parents: [], message: "policy\n", author }),
          });

          const asked = (projection: typeof asOfTheGuard) =>
            Policy.gate([{ name: "refs/heads/main", value: second }], false).pipe(
              Effect.provide(
                Auth.requester({
                  principal: where.principal.member,
                  signer: null,
                  capabilities: where.principal.capabilities,
                  projection,
                  envelope: null,
                }),
              ),
            );

          return { stale: yield* asked(asOfTheGuard), fresh: yield* asked(yield* trustOf(where)) };
        }),
      );

      assert.equal(outcome.stale.updates.length, 0, "the revocation reaches the push it raced");
      assert.equal(outcome.fresh.updates.length, 0, "as it does when the fold is taken fresh");
    });

    it("refuses a name under refs/meta/trust that is not the log or the genesis", async () => {
      // The namespace holds two things, and the rules know both. Anything else
      // under it is not append-only, so nothing bounds it — and the JSON verbs
      // refuse the whole namespace, so receive-pack was the only door left
      // open. Such a ref is hidden from the advertisement, copied to every
      // mirror by the hub refspec, which only adds refs, and roots collection:
      // a permanent, invisible pin on the object graph of every replica, put
      // there by anybody holding `source.push`.
      const outcome = await scenario(
        Effect.gen(function* () {
          const where = yield* world(["repo.admin"]);
          const repository = yield* Repository;
          const log = yield* repository.resolve(Log.LOG_REF);
          return {
            stray: yield* judge(where, { name: "refs/meta/trust/notes", value: log! }),
            // The log is one ref, not a namespace. Spelled as one, a name
            // under it passed for append-only and slipped through this very
            // check — while no fold ever opens it, nothing bounds how many
            // there are, and every replica roots collection at each.
            under: yield* judge(where, { name: "refs/meta/trust/log/shard", value: log! }),
            // The two the namespace is actually for still work.
            log: yield* judge(where, { name: Log.LOG_REF, value: log! }),
          };
        }),
      );

      assert.equal(outcome.stray.ok, false);
      assert.match(
        outcome.stray.ok === false ? outcome.stray.reason : "",
        /not part of the trust namespace/,
      );
      assert.equal(outcome.under.ok, false, "and a name spelled underneath the log is one too");
      assert.match(
        outcome.under.ok === false ? outcome.under.reason : "",
        /not part of the trust namespace/,
      );
      assert.equal(outcome.log.ok, true, "the log itself is still appendable");
    });

    it("refuses to open more pull requests than a push will walk", async () => {
      // The per-pull-request ceiling bounds one fold; nothing bounded how many
      // folds a protected-branch push, a collection and a deepening fetch each
      // have to make. `refs/hub/pr/*` is append-only, so a closed pull request
      // costs the same as an open one and the list only ever grows — which
      // makes opening them the cheapest way for anybody holding
      // `hub.create-pr` to make every later push slower for good.
      const outcome = await scenario(
        Effect.gen(function* () {
          const where = yield* world(["repo.admin"]);
          const repository = yield* Repository;
          const { pr } = yield* PullRequest.open({
            repo: where.genesis.repoId,
            title: "t",
            base: "refs/heads/main",
            head: EMPTY_TREE_OID,
            key: where.dev,
          });
          const head = yield* repository.resolve(`refs/hub/pr/${pr}`);

          const judged = (name: string) =>
            judge(where, { name, value: head }).pipe(Effect.provide(Event.population(1)));
          return {
            // The one that already exists is an append, not a create.
            existing: yield* judged(`refs/hub/pr/${pr}`),
            fresh: yield* judged("refs/hub/pr/another"),
          };
        }),
      );

      assert.equal(outcome.existing.ok, true, "an existing pull request keeps working");
      assert.equal(outcome.fresh.ok, false);
      assert.match(outcome.fresh.ok === false ? outcome.fresh.reason : "", /already holds 1/);
    });

    it("counts what one push is opening, not only what the repository holds", async () => {
      // The bound is read from the store, and a push is judged in full before
      // any of it is applied — so every create in one receive-pack saw the same
      // pre-push count and every one of them passed. The bound said 65 536 and
      // one push could open as many pull requests as it liked, on a namespace
      // nothing can delete, which makes the cost to every later
      // protected-branch push, collection and deepening fetch permanent.
      const outcome = await scenario(
        Effect.gen(function* () {
          const where = yield* world(["repo.admin"]);
          const repository = yield* Repository;
          const { pr } = yield* PullRequest.open({
            repo: where.genesis.repoId,
            title: "t",
            base: "refs/heads/main",
            head: EMPTY_TREE_OID,
            key: where.dev,
          });
          const head = yield* repository.resolve(`refs/hub/pr/${pr}`);

          // One held, a ceiling of two: room for exactly one more, whether it
          // arrives on its own or with company.
          return yield* gateAs(
            where,
            ["a", "b", "c"].map((id) => ({ name: `refs/hub/pr/${id}`, value: head! })),
            false,
          ).pipe(Effect.provide(Event.population(2)));
        }),
      );

      assert.equal(outcome.updates.length, 1, "one create fits under the ceiling");
      assert.equal(outcome.refused.length, 2, "and the rest of the batch does not");
      assert.match(outcome.refused[0]?.reason ?? "", /already holds 2/);
    });

    it("gives back the population slot of a create it goes on to refuse", async () => {
      // The namespace rules record a create as soon as they have passed it,
      // and they are not the last word — the rules after them refuse too. A
      // create the batch does not actually get was still spending a slot the
      // create beside it was entitled to.
      const outcome = await scenario(
        Effect.gen(function* () {
          const where = yield* world(["repo.admin"]);
          const repository = yield* Repository;
          const { pr } = yield* PullRequest.open({
            repo: where.genesis.repoId,
            title: "t",
            base: "refs/heads/main",
            head: EMPTY_TREE_OID,
            key: where.dev,
          });
          const head = yield* repository.resolve(`refs/hub/pr/${pr}`);

          // `refs/hub/pr/blocked` is protected, so it passes the namespace
          // rules and is refused after them.
          const blob = yield* repository.writeBlob(
            Policy.encodeRules({
              ...OPEN,
              protected: ["refs/hub/pr/blocked"],
              requirePullRequest: true,
              requiredApprovals: 1,
            }),
          );
          const tree = yield* repository.writeTree([
            { mode: "100644", name: "policy.json", oid: blob },
          ]);
          yield* repository.setRef({
            name: Policy.RULES_REF,
            to: yield* repository.commitTree({ tree, parents: [], message: "policy\n", author }),
          });

          // One held, a ceiling of two: room for exactly one more create.
          return yield* gateAs(
            where,
            ["blocked", "wanted"].map((id) => ({ name: `refs/hub/pr/${id}`, value: head! })),
            false,
          ).pipe(Effect.provide(Event.population(2)));
        }),
      );

      assert.equal(outcome.updates.length, 1);
      assert.equal(outcome.updates[0]?.name, "refs/hub/pr/wanted", "the one that was not refused");
      assert.equal(outcome.refused.length, 1);
      assert.doesNotMatch(outcome.refused[0]?.reason ?? "", /already holds/);
    });

    it("allows a join that keeps both heads it already had", async () => {
      // The rule is about *new* roots, not about joins: two members appending
      // concurrently is the ordinary case, and the join that reconciles them
      // brings no beginning the ref did not already have.
      const decision = await scenario(
        Effect.gen(function* () {
          const where = yield* world(["repo.admin"]);
          const repository = yield* Repository;
          const { pr } = yield* PullRequest.open({
            repo: where.genesis.repoId,
            title: "t",
            base: "refs/heads/main",
            head: EMPTY_TREE_OID,
            key: where.dev,
          });
          const opened = yield* repository.resolve(`refs/hub/pr/${pr}`);
          yield* PullRequest.comment({
            repo: where.genesis.repoId,
            pr,
            body: "more",
            key: where.dev,
          });
          const head = yield* repository.resolve(`refs/hub/pr/${pr}`);

          // A second branch off the opening, and a join over both — divergence
          // and its resolution, with no root but the one the ref started at.
          const sibling = yield* repository.commitTree({
            tree: EMPTY_TREE_OID,
            parents: [opened!],
            message: "concurrent\n",
            author,
          });
          const joined = yield* repository.commitTree({
            tree: EMPTY_TREE_OID,
            parents: [head!, sibling],
            message: "join\n",
            author,
          });
          return yield* judge(where, { name: `refs/hub/pr/${pr}`, value: joined });
        }),
      );
      assert.equal(decision.ok, true);
    });

    it("allows a hub update that grows the history", async () => {
      const decision = await scenario(
        Effect.gen(function* () {
          const where = yield* world(["repo.admin"]);
          const repository = yield* Repository;
          const { pr } = yield* PullRequest.open({
            repo: where.genesis.repoId,
            title: "t",
            base: "refs/heads/main",
            head: EMPTY_TREE_OID,
            key: where.dev,
          });
          yield* PullRequest.comment({
            repo: where.genesis.repoId,
            pr,
            body: "more",
            key: where.dev,
          });
          const head = yield* repository.resolve(`refs/hub/pr/${pr}`);
          return yield* judge(where, { name: `refs/hub/pr/${pr}`, value: head });
        }),
      );
      assert.equal(decision.ok, true);
    });
  });

  describe("protected branches", () => {
    const guarded: Rules = {
      ...OPEN,
      protected: ["refs/heads/main"],
      requirePullRequest: true,
      requiredApprovals: 1,
    };

    it("refuses a direct push to a protected branch", async () => {
      const decision = await scenario(
        Effect.gen(function* () {
          const where = yield* world(["source.push"]);
          const { second } = yield* history("refs/heads/main");
          return yield* judge(where, { name: "refs/heads/main", value: second }, guarded);
        }),
      );
      assert.equal(decision.ok, false);
      assert.match(decision.ok === false ? decision.reason : "", /approved pull request/);
    });

    it("refuses a protected branch a pull request has not had approved", async () => {
      const decision = await scenario(
        Effect.gen(function* () {
          const where = yield* world(["source.push", "hub.create-pr"]);
          const { second } = yield* history("refs/heads/main");
          yield* PullRequest.open({
            repo: where.genesis.repoId,
            title: "please",
            base: "refs/heads/main",
            head: second,
            key: where.dev,
          });
          return yield* judge(where, { name: "refs/heads/main", value: second }, guarded);
        }),
      );
      assert.equal(decision.ok, false);
      assert.match(decision.ok === false ? decision.reason : "", /0 approvals/);
    });

    it("counts pull requests in the mention memo, not the revisions they propose", async () => {
      // The memo exists so a protected-branch push does not re-read every pull
      // request's event DAG, and its ceiling is written as a number of pull
      // requests — sized well above what a busy repository holds, because a
      // miss *is* the walk. Keyed on the ref's oid, every push to a pull
      // request left the answer for the head before it behind, so the ceiling
      // counted revisions: a repository far inside the population bound turned
      // the memo over on ordinary activity and paid for the walk again, on the
      // synchronous receive-pack path. Compared instead of keyed, an append
      // overwrites.
      const outcome = await scenario(
        Effect.gen(function* () {
          const where = yield* world(["source.push", "hub.create-pr"]);
          const repository = yield* Repository;
          const { first, second } = yield* history("refs/heads/main");
          const { pr } = yield* PullRequest.open({
            repo: where.genesis.repoId,
            title: "please",
            base: "refs/heads/main",
            head: first,
            key: where.dev,
          });

          const identity = yield* repository.resolve(GENESIS_REF);
          const storage = yield* storageOf();
          const before = Policy.mentionsHeld(storage, identity);
          // Each round moves the pull request's head and then asks the
          // boundary about the protected branch, which is what walks it.
          for (const head of [second, first, second, first, second]) {
            yield* PullRequest.update({ repo: where.genesis.repoId, pr, head, key: where.dev });
            yield* judge(where, { name: "refs/heads/main", value: head }, guarded);
          }
          // Sanity: the rounds really did move the ref each time.
          const at = yield* repository.resolve(Event.refOf(pr));
          return { grew: Policy.mentionsHeld(storage, identity) - before, at, opened: first };
        }),
      );

      assert.notEqual(outcome.at, null);
      assert.equal(outcome.grew, 1, "five revisions of one pull request are one entry");
    });

    it("bounds the mention memo by repositories, so one sweep always fits", async () => {
      // The memo exists so a protected-branch push does not re-walk every pull
      // request's event DAG, and one such push sweeps *all* of them. Bounded
      // flat — one ceiling shared by every repository the host serves, and set
      // below the population the boundary lets a repository reach — the LRU
      // was smaller than a single sweep: every push then missed on every key
      // and re-walked every DAG synchronously, which is the cost the memo
      // exists to remove, reached through the memo itself. One busy repository
      // also evicted every other one the host served. Bounded by repositories,
      // a sweep always fits and eviction happens at the granularity a host
      // idles at.
      const held = await scenario(
        Effect.gen(function* () {
          const where = yield* world(["source.push", "hub.create-pr"]);
          const { second } = yield* history("refs/heads/main");
          for (const title of ["one", "two", "three"]) {
            yield* PullRequest.open({
              repo: where.genesis.repoId,
              title,
              base: "refs/heads/main",
              head: second,
              key: where.dev,
            });
          }

          const repository = yield* Repository;
          const identity = yield* repository.resolve(GENESIS_REF);
          const storage = yield* storageOf();
          // One push, which walks every pull request the repository has — with
          // room in the memo for one repository, not for one pull request.
          yield* judge(where, { name: "refs/heads/main", value: second }, guarded).pipe(
            Effect.provide(Policy.memo(1)),
          );
          return Policy.mentionsHeld(storage, identity);
        }),
      );

      assert.equal(held, 3, "every pull request the sweep walked is still remembered");
    });

    it("keeps a mirror's answers apart from its origin's", async () => {
      // A memo keyed by what the repository *says it is* aliases an origin and
      // its mirror: the mirror is made by copying `refs/meta/trust/*`, so the
      // genesis bytes and the RepoID are the same, and right after a
      // replication the hub ref oids are too. What they can actually read need
      // not agree — refs are applied without a connectivity check — so
      // whichever folded first answered for both, and an answer computed on
      // the replica that is missing objects was served for the origin: an
      // approved pull request filtered out of a protected-branch push.
      const swept = Effect.fn("test.swept")(function* () {
        const repository = yield* Repository;
        const where = yield* world(["source.push", "hub.create-pr"]);
        const { second } = yield* history("refs/heads/main");
        yield* PullRequest.open({
          repo: where.genesis.repoId,
          title: "one",
          base: "refs/heads/main",
          head: second,
          key: where.dev,
        });
        const identity = yield* repository.resolve(GENESIS_REF);
        const storage = yield* storageOf();
        yield* judge(where, { name: "refs/heads/main", value: second }, guarded);
        return { storage, identity, held: Policy.mentionsHeld(storage, identity) };
      });

      // Two builds of the memory store: different repositories on one host,
      // which is what a mirror beside its origin is.
      const origin = await scenario(swept());
      const mirror = await scenario(swept());

      assert.notEqual(origin.storage, mirror.storage, "two repositories, not one twice");
      assert.equal(origin.held, 1);
      assert.equal(mirror.held, 1);
      assert.equal(
        Policy.mentionsHeld(origin.storage, origin.identity),
        1,
        "the origin's answers are still its own after the mirror has folded",
      );
    });

    it("caps what the mention memo retains in total, evicting whole repositories", async () => {
      // Bounding only the number of repositories left the entries within one
      // unbounded: a repository pushed to the population bound had every one
      // of its pull requests memoised by a single sweep, retained for as many
      // repositories as the outer bound allows. The total is capped too — and
      // what the cap evicts is a whole repository, never an entry belonging to
      // the sweep that is running, which is the flat cap's failure.
      //
      // Room for plenty of repositories and two entries, so only the total can
      // bite.
      const bounded = Policy.memo(4096, 2);

      const swept = Effect.fn("test.swept")(function* (titles: ReadonlyArray<string>) {
        const repository = yield* Repository;
        const where = yield* world(["source.push", "hub.create-pr"]);
        const { second } = yield* history("refs/heads/main");
        for (const title of titles) {
          yield* PullRequest.open({
            repo: where.genesis.repoId,
            title,
            base: "refs/heads/main",
            head: second,
            key: where.dev,
          });
        }
        const identity = yield* repository.resolve(GENESIS_REF);
        const storage = yield* storageOf();
        yield* judge(where, { name: "refs/heads/main", value: second }, guarded).pipe(
          Effect.provide(bounded),
        );
        return { storage, identity, held: Policy.mentionsHeld(storage, identity) };
      });

      // Two repositories, one after the other: separate stores, one memo.
      const first = await scenario(swept(["one", "two", "three"]));
      const second = await scenario(swept(["theirs"]));

      assert.notEqual(first.identity, second.identity, "two repositories, not one twice");
      assert.equal(first.held, 3, "the sweep that is running is kept whole");
      assert.equal(second.held, 1);
      assert.equal(
        Policy.mentionsHeld(first.storage, first.identity),
        0,
        "and the one that went idle is evicted whole, not entry by entry",
      );
    });

    it("passes over a pull request this replica cannot fold", async () => {
      // The fold's ceiling is enforced where a *push* crosses it, so a history
      // that arrived by replication may sit above it. Left uncaught in the
      // cheap pre-filter that runs in front of the fold, one such pull request
      // refused every push to every protected branch on that replica,
      // permanently — the denial the ceiling exists to prevent, reached
      // through the ceiling itself.
      const outcome = await scenario(
        Effect.gen(function* () {
          const where = yield* world(["source.push", "hub.create-pr", "hub.review", "hub.approve"]);
          const { second } = yield* history("refs/heads/main");
          const { pr } = yield* PullRequest.open({
            repo: where.genesis.repoId,
            title: "please",
            base: "refs/heads/main",
            head: second,
            key: where.dev,
          });
          yield* PullRequest.review({
            repo: where.genesis.repoId,
            pr,
            head: second,
            decision: "approve",
            key: where.reviewer,
          });
          // Two events, and a ceiling of one: the same shape as a replicated
          // pull request larger than this host will walk.
          return yield* judge(where, { name: "refs/heads/main", value: second }, guarded).pipe(
            Effect.provide(Event.ceiling(1)),
          );
        }),
      );

      assert.equal(outcome.ok, false, "it cannot approve anything this host cannot read");
      assert.match(
        outcome.ok === false ? outcome.reason : "",
        /approved pull request/,
        "and the refusal is the ordinary one, not a failure",
      );
    });

    it("allows it once the current revision is approved", async () => {
      const decision = await scenario(
        Effect.gen(function* () {
          const where = yield* world(["source.push", "hub.create-pr", "hub.review", "hub.approve"]);
          const { second } = yield* history("refs/heads/main");
          const { pr } = yield* PullRequest.open({
            repo: where.genesis.repoId,
            title: "please",
            base: "refs/heads/main",
            head: second,
            key: where.dev,
          });
          yield* PullRequest.review({
            repo: where.genesis.repoId,
            pr,
            head: second,
            decision: "approve",
            key: where.reviewer,
          });
          return yield* judge(where, { name: "refs/heads/main", value: second }, guarded);
        }),
      );
      assert.equal(decision.ok, true);
    });

    it("refuses when the approval was of a revision that has been replaced", async () => {
      const decision = await scenario(
        Effect.gen(function* () {
          const where = yield* world(["source.push", "hub.create-pr", "hub.review", "hub.approve"]);
          const { first, second } = yield* history("refs/heads/main");
          const { pr } = yield* PullRequest.open({
            repo: where.genesis.repoId,
            title: "please",
            base: "refs/heads/main",
            head: first,
            key: where.dev,
          });
          yield* PullRequest.review({
            repo: where.genesis.repoId,
            pr,
            head: first,
            decision: "approve",
            key: where.reviewer,
          });
          // The proposal moves on; the approval stays true about `first`.
          yield* PullRequest.update({
            repo: where.genesis.repoId,
            pr,
            head: second,
            key: where.dev,
          });
          return yield* judge(where, { name: "refs/heads/main", value: second }, guarded);
        }),
      );
      assert.equal(decision.ok, false);
      assert.match(decision.ok === false ? decision.reason : "", /0 approvals/);
    });

    it("does not let a merged pull request unlock the branch forever", async () => {
      const decision = await scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const where = yield* world([
            "source.push",
            "hub.create-pr",
            "hub.review",
            "hub.approve",
            "hub.merge",
          ]);
          const { second } = yield* history("refs/heads/main");
          const { pr } = yield* PullRequest.open({
            repo: where.genesis.repoId,
            title: "landed",
            base: "refs/heads/main",
            head: second,
            key: where.dev,
          });
          yield* PullRequest.review({
            repo: where.genesis.repoId,
            pr,
            head: second,
            decision: "approve",
            key: where.reviewer,
          });
          yield* PullRequest.merged({
            repo: where.genesis.repoId,
            pr,
            head: second,
            mergeCommit: second,
            key: where.dev,
          });

          // A later commit descends from the merged revision, and every commit
          // ever made after a merge does. If descent were enough, one merged
          // pull request would authorize direct pushes to the branch forever.
          const later = yield* repository.commit({
            branch: "refs/heads/main",
            tree: EMPTY_TREE_OID,
            message: "sneaked in",
            author,
          });
          return yield* judge(where, { name: "refs/heads/main", value: later }, guarded);
        }),
      );
      assert.equal(decision.ok, false);
      assert.match(decision.ok === false ? decision.reason : "", /approved pull request/);
    });

    it("refuses a merge commit that merely names the approved revision", async () => {
      // A merge's tree is unconstrained, so "has the approved head as a
      // parent" says nothing at all about what is being landed.
      const decision = await scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const where = yield* world(["source.push", "hub.create-pr", "hub.review", "hub.approve"]);
          const { first, second } = yield* history("refs/heads/main");
          const { pr } = yield* PullRequest.open({
            repo: where.genesis.repoId,
            title: "please",
            base: "refs/heads/main",
            head: second,
            key: where.dev,
          });
          yield* PullRequest.review({
            repo: where.genesis.repoId,
            pr,
            head: second,
            decision: "approve",
            key: where.reviewer,
          });

          // Any content at all, with the approved head hung off it as a parent.
          const blob = yield* repository.writeBlob(new TextEncoder().encode("anything\n"));
          const tree = yield* repository.writeTree([{ mode: "100644", name: "x.txt", oid: blob }]);
          const wrapper = yield* repository.commitTree({
            tree,
            parents: [first, second],
            message: "merge\n",
            author,
          });
          return yield* judge(where, { name: "refs/heads/main", value: wrapper }, guarded);
        }),
      );
      assert.equal(decision.ok, false);
    });

    it("is not blocked by an unapproved duplicate of the same proposal", async () => {
      const decision = await scenario(
        Effect.gen(function* () {
          const where = yield* world(["source.push", "hub.create-pr", "hub.review", "hub.approve"]);
          const { second } = yield* history("refs/heads/main");

          // Two pull requests for the same revision; only one is approved.
          const approved = yield* PullRequest.open({
            repo: where.genesis.repoId,
            title: "approved",
            base: "refs/heads/main",
            head: second,
            key: where.dev,
          });
          yield* PullRequest.open({
            repo: where.genesis.repoId,
            title: "duplicate",
            base: "refs/heads/main",
            head: second,
            key: where.dev,
          });
          yield* PullRequest.review({
            repo: where.genesis.repoId,
            pr: approved.pr,
            head: second,
            decision: "approve",
            key: where.reviewer,
          });

          return yield* judge(where, { name: "refs/heads/main", value: second }, guarded);
        }),
      );
      assert.equal(decision.ok, true, "an unapproved duplicate must not block the approved one");
    });

    it("refuses when a required check has not passed", async () => {
      const decision = await scenario(
        Effect.gen(function* () {
          const where = yield* world(["source.push", "hub.create-pr", "hub.review", "hub.approve"]);
          const { second } = yield* history("refs/heads/main");
          const { pr } = yield* PullRequest.open({
            repo: where.genesis.repoId,
            title: "please",
            base: "refs/heads/main",
            head: second,
            key: where.dev,
          });
          yield* PullRequest.review({
            repo: where.genesis.repoId,
            pr,
            head: second,
            decision: "approve",
            key: where.reviewer,
          });
          return yield* judge(
            where,
            { name: "refs/heads/main", value: second },
            { ...guarded, requiredChecks: ["test"] },
          );
        }),
      );
      assert.equal(decision.ok, false);
      assert.match(decision.ok === false ? decision.reason : "", /has not passed test/);
    });

    it("refuses while a review thread is unresolved", async () => {
      const decision = await scenario(
        Effect.gen(function* () {
          const where = yield* world([
            "source.push",
            "hub.create-pr",
            "hub.review",
            "hub.approve",
            "hub.comment",
          ]);
          const { second } = yield* history("refs/heads/main");
          const { pr } = yield* PullRequest.open({
            repo: where.genesis.repoId,
            title: "please",
            base: "refs/heads/main",
            head: second,
            key: where.dev,
          });
          yield* PullRequest.review({
            repo: where.genesis.repoId,
            pr,
            head: second,
            decision: "approve",
            key: where.reviewer,
          });
          yield* PullRequest.comment({
            repo: where.genesis.repoId,
            pr,
            body: "what about this",
            key: where.dev,
          });
          return yield* judge(
            where,
            { name: "refs/heads/main", value: second },
            { ...guarded, requireResolvedThreads: true },
          );
        }),
      );
      assert.equal(decision.ok, false);
      assert.match(decision.ok === false ? decision.reason : "", /unresolved/);
    });
  });

  describe("the rules themselves", () => {
    it("refuses to let a pusher rewrite the branch rules", async () => {
      const decision = await scenario(
        Effect.gen(function* () {
          const where = yield* world(["source.push"]);
          const { second } = yield* history("refs/heads/topic");
          // Without this, anybody who may push may also publish an `OPEN`
          // policy and then push wherever they like.
          return yield* judge(where, { name: "refs/meta/policy", value: second });
        }),
      );
      assert.equal(decision.ok, false);
      assert.match(decision.ok === false ? decision.reason : "", /policy\.write/);
    });

    it("leaves a repository with no identity a way back too", async () => {
      // `serve --open` has no members, so nobody holds `policy.write` — and a
      // door that asks for it leaves exactly that repository with no repair
      // route: the same anonymous client that may publish the rules can
      // publish an unparseable one and lock every write on the repository, its
      // own next attempt included. Whoever may publish them may repair them.
      const outcome = await scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const { first, second } = yield* history("refs/heads/main");
          const broken = yield* repository.writeBlob(new TextEncoder().encode("{ not json"));
          const tree = yield* repository.writeTree([
            { mode: "100644", name: "policy.json", oid: broken },
          ]);
          yield* repository.setRef({
            name: Policy.RULES_REF,
            to: yield* repository.commitTree({ tree, parents: [], message: "policy\n", author }),
          });

          const open = Policy.anonymousWrites(true);
          return {
            repair: yield* gateAs(null, [{ name: Policy.RULES_REF, value: first }], false).pipe(
              Effect.provide(open),
            ),
            other: yield* gateAs(null, [{ name: "refs/heads/topic", value: second }], false).pipe(
              Effect.provide(open),
            ),
          };
        }),
      );

      assert.equal(outcome.repair.updates.length, 1, "the rules can be republished");
      assert.equal(outcome.other.updates.length, 0, "and nothing else moves until they are");
    });

    it("still takes the push that repairs a rules file nothing can parse", async () => {
      // A policy file that exists and will not parse fails rather than reading
      // as `OPEN` — otherwise anybody could turn branch protection off by
      // corrupting it. But the rules are read before any per-ref decision, so
      // failing closed refused *every* write on the repository including the
      // corrective one: one bad push and the only way back was filesystem
      // access to the host. The rules have nothing to say about their own file
      // in any case; the staleness bound already exempts it for that reason.
      const outcome = await scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const where = yield* world(["source.push", "policy.write"]);
          const { first, second } = yield* history("refs/heads/main");

          const broken = yield* repository.writeBlob(new TextEncoder().encode("{ not json"));
          const tree = yield* repository.writeTree([
            { mode: "100644", name: "policy.json", oid: broken },
          ]);
          yield* repository.setRef({
            name: Policy.RULES_REF,
            to: yield* repository.commitTree({ tree, parents: [], message: "policy\n", author }),
          });

          const repair = yield* gateAs(where, [{ name: Policy.RULES_REF, value: first }], false);
          const other = yield* gateAs(where, [{ name: "refs/heads/topic", value: second }], false);
          // Applied, not merely allowed. The compare-and-swap the decision
          // carries is what the store checks, and `null` reads as *must not
          // exist* — which the ref being repaired always does, so an allowed
          // repair that carried `null` was reported as allowed and never
          // landed.
          const applied = yield* Effect.forEach(repair.updates, (update) =>
            repository
              .setRef({ name: update.name, to: update.value ?? first, expected: update.expected })
              .pipe(
                Effect.as(true),
                Effect.catchTag("RefConflict", () => Effect.succeed(false)),
              ),
          );
          return { repair, other, applied, at: yield* repository.resolve(Policy.RULES_REF) };
        }),
      );

      assert.equal(outcome.repair.updates.length, 1, "the push that fixes it is allowed");
      assert.deepEqual(outcome.applied, [true], "and actually lands");
      assert.equal(outcome.at, outcome.repair.updates[0]?.value, "on the ref it was aimed at");
      assert.equal(outcome.other.updates.length, 0, "and everything the rules govern is refused");
      assert.match(outcome.other.refused.at(0)?.reason ?? "", /policy could not be evaluated/);
    });

    it("allows the holder of policy.write", async () => {
      const decision = await scenario(
        Effect.gen(function* () {
          const where = yield* world(["source.push", "policy.write"]);
          const { second } = yield* history("refs/heads/topic");
          return yield* judge(where, { name: "refs/meta/policy", value: second });
        }),
      );
      assert.equal(decision.ok, true);
    });
  });

  describe("a repository with no identity", () => {
    it("refuses a write unless the host says otherwise", async () => {
      // §14 is unconditional that anonymous does not get `source.push`, and a
      // repository with no genesis has no membership to grant it. The previous
      // model required a server secret, so deploying this over an existing
      // installation would otherwise have made every repository accept
      // unauthenticated pushes, force-pushes and deletes included.
      const outcome = await scenario(
        Effect.gen(function* () {
          const { second } = yield* history("refs/heads/topic");
          const closed = yield* gateAs(null, [{ name: "refs/heads/other", value: second }]);
          const open = yield* gateAs(null, [{ name: "refs/heads/other", value: second }]).pipe(
            Effect.provide(Policy.anonymousWrites(true)),
          );
          return { closed, open };
        }),
      );

      assert.equal(outcome.closed.updates.length, 0);
      assert.match(outcome.closed.refused.at(0)?.reason ?? "", /hub init/);
      assert.equal(outcome.open.updates.length, 1, "--open is what serves them anyway");
    });

    it("refuses the JSON verbs too, not only receive-pack", async () => {
      // `gateWrite` is the pre-check for the verbs that compute a ref's new
      // value while doing the work. Allowing there while `gate` refused meant
      // `git push` was blocked and unauthenticated `POST /commit`, `/branch`,
      // `/tags`, a merge or rebase with `into`, a pull and commit-pack all
      // still wrote refs — every door but the one that was locked.
      const outcome = await scenario(
        Effect.gen(function* () {
          const closed = yield* gateWriteAs(null, "refs/heads/topic");
          const open = yield* gateWriteAs(null, "refs/heads/topic").pipe(
            Effect.provide(Policy.anonymousWrites(true)),
          );
          return { closed, open };
        }),
      );

      assert.match(outcome.closed ?? "", /hub init/);
      assert.equal(outcome.open, null, "--open is what serves them anyway");
    });

    it("never lets a push establish an identity, open or not", async () => {
      // Whoever got there first would own the repository, and its actual owner
      // would be locked out of it by a stranger.
      const refused = await scenario(
        Effect.gen(function* () {
          const { second } = yield* history("refs/heads/topic");
          return yield* gateAs(null, [{ name: "refs/meta/trust/genesis", value: second }]).pipe(
            Effect.provide(Policy.anonymousWrites(true)),
          );
        }),
      );

      assert.equal(refused.updates.length, 0);
      assert.match(refused.refused.at(0)?.reason ?? "", /not by a push/);
    });

    it("honours it at the JSON verbs too, not only at receive-pack", async () => {
      // The two doors disagreed about the same file on the same repository:
      // receive-pack, `reset` and a branch or tag delete honoured
      // `refs/meta/policy` while `commit`, `branch`, `tagCreate`, a merge,
      // rebase or cherry-pick with `into`, `fetch`, `pull` and commit-pack
      // ignored it — because the name-only gate returned as soon as it saw a
      // repository with no identity.
      const outcome = await scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const blob = yield* repository.writeBlob(
            Policy.encodeRules({ ...OPEN, protected: ["refs/heads/main"] }),
          );
          const tree = yield* repository.writeTree([
            { mode: "100644", name: "policy.json", oid: blob },
          ]);
          const commit = yield* repository.commitTree({
            tree,
            parents: [],
            message: "policy\n",
            author,
          });
          yield* repository.setRef({ name: Policy.RULES_REF, to: commit });

          const open = Policy.anonymousWrites(true);
          return {
            guarded: yield* gateWriteAs(null, "refs/heads/main").pipe(Effect.provide(open)),
            other: yield* gateWriteAs(null, "refs/heads/topic").pipe(Effect.provide(open)),
          };
        }),
      );

      assert.match(outcome.guarded ?? "", /protected/);
      assert.equal(outcome.other, null, "an unprotected branch still moves");
    });

    it("still honours the branch protection it publishes", async () => {
      // The genesis-less branch returned before `input.rules` was ever
      // consulted, so a published `refs/meta/policy` was inert on exactly the
      // repositories with no other protection at all: `--open` served the
      // delete and the force-push the file refused. The approval half of
      // protection genuinely cannot apply here — there is no membership for a
      // review to come from — but "may not be deleted" and "may not be
      // force-pushed" ask nothing of trust.
      const outcome = await scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const { first, second } = yield* history("refs/heads/main");

          const blob = yield* repository.writeBlob(
            Policy.encodeRules({ ...OPEN, protected: ["refs/heads/main"] }),
          );
          const tree = yield* repository.writeTree([
            { mode: "100644", name: "policy.json", oid: blob },
          ]);
          const commit = yield* repository.commitTree({
            tree,
            parents: [],
            message: "policy\n",
            author,
          });
          yield* repository.setRef({ name: Policy.RULES_REF, to: commit });

          // Built on the tip without moving it, so the fast-forward below is
          // a real one rather than a write of the value already there.
          const third = yield* repository.commitTree({
            tree: EMPTY_TREE_OID,
            parents: [second],
            message: "third\n",
            author,
          });

          const open = Policy.anonymousWrites(true);
          return {
            deleted: yield* gateAs(null, [{ name: "refs/heads/main", value: null }]).pipe(
              Effect.provide(open),
            ),
            forced: yield* gateAs(null, [{ name: "refs/heads/main", value: first }]).pipe(
              Effect.provide(open),
            ),
            ahead: yield* gateAs(null, [{ name: "refs/heads/main", value: third }]).pipe(
              Effect.provide(open),
            ),
          };
        }),
      );

      assert.equal(outcome.deleted.updates.length, 0);
      assert.match(outcome.deleted.refused.at(0)?.reason ?? "", /may not be deleted/);
      assert.equal(outcome.forced.updates.length, 0);
      assert.match(outcome.forced.refused.at(0)?.reason ?? "", /may not be force-pushed/);
      assert.equal(outcome.ahead.updates.length, 1, "an ordinary push still lands");
    });
  });

  describe("how stale a membership view may be", () => {
    /** Publish rules with a checkpoint age bound. */
    const withMaxAge = Effect.fn("test.withMaxAge")(function* (seconds: number) {
      const repository = yield* Repository;
      const blob = yield* repository.writeBlob(
        Policy.encodeRules({ ...OPEN, maxTrustAgeSeconds: seconds }),
      );
      const tree = yield* repository.writeTree([
        { mode: "100644", name: "policy.json", oid: blob },
      ]);
      const commit = yield* repository.commitTree({
        tree,
        parents: [],
        message: "policy\n",
        author,
      });
      yield* repository.setRef({ name: Policy.RULES_REF, to: commit });
    });

    it("refuses a write when the repository has never checkpointed", async () => {
      // A hash-linked log makes withholding visible but not impossible: a
      // replica can serve a consistent view that simply stops short of a
      // revocation. A checkpoint is the signed statement that bounds it, and
      // this rule is what makes the bound mean anything.
      const outcome = await scenario(
        Effect.gen(function* () {
          const where = yield* world(["source.push"]);
          const { second } = yield* history("refs/heads/topic");
          yield* withMaxAge(3600);
          return yield* gateAs(where, [{ name: "refs/heads/topic", value: second }]);
        }),
      );

      assert.equal(outcome.updates.length, 0);
      assert.match(outcome.refused.at(0)?.reason ?? "", /checkpoint/);
    });

    it("still lets the pushes that would lift the bound through", async () => {
      // A checkpoint is how a stale view stops being stale, and it lands on
      // the trust log; the bound itself lives in the rules file. Refusing
      // those alongside everything else made the flag a one-way door — the
      // repository became unwritable over the network and neither push that
      // would recover it could be made. Both are charged their own capability
      // elsewhere, so exempting them from *this* check opens nothing.
      const outcome = await scenario(
        Effect.gen(function* () {
          const where = yield* world(["source.push", "policy.write"]);
          const repository = yield* Repository;
          const { second } = yield* history("refs/heads/topic");
          yield* withMaxAge(3600);
          const log = yield* repository.resolve(Log.LOG_REF);
          const rules = yield* repository.resolve(Policy.RULES_REF);

          return {
            ordinary: yield* gateAs(where, [{ name: "refs/heads/topic", value: second }]),
            recovery: yield* gateAs(where, [
              { name: Log.LOG_REF, value: log },
              { name: Policy.RULES_REF, value: rules },
            ]),
          };
        }),
      );

      assert.equal(outcome.ordinary.updates.length, 0, "an ordinary push is still refused");
      assert.match(outcome.ordinary.refused.at(0)?.reason ?? "", /checkpoint/);
      // Whatever else these two are held to — a trust record needs its own
      // capability, and `refs/meta/policy` needs `policy.write` — the answer
      // is never "your view is stale", which is the door this closes.
      for (const entry of outcome.recovery.refused) {
        assert.doesNotMatch(entry.reason, /checkpoint/, entry.ref);
      }
    });

    it("allows it once a recent checkpoint exists", async () => {
      const outcome = await scenario(
        Effect.gen(function* () {
          const where = yield* world(["source.push"]);
          const { second } = yield* history("refs/heads/topic");
          yield* Log.issue(
            Certificate.checkpoint({
              repo: where.genesis.repoId,
              frontier: [],
              id: Log.newId(),
            }),
            [where.root],
          );
          yield* withMaxAge(3600);
          return yield* gateAs(where, [{ name: "refs/heads/topic", value: second }]);
        }),
      );

      assert.deepEqual(outcome.refused, []);
      assert.equal(outcome.updates.length, 1);
    });

    it("bounds the JSON verbs too, not only receive-pack", async () => {
      // Applied only in `gate`, the bound covered receive-pack and left
      // `commit`, `branch`, `tagCreate`, `merge`, `rebase`, `cherry-pick`,
      // `pull` and commit-pack accepting a membership view of any age — which
      // is most of the ways a ref moves.
      const refusal = await scenario(
        Effect.gen(function* () {
          const where = yield* world(["source.push"]);
          yield* withMaxAge(3600);
          return yield* gateWriteAs(where, "refs/heads/topic");
        }),
      );
      assert.match(refusal ?? "", /checkpoint/);
    });

    it("leaves a repository that set no bound alone", async () => {
      // The default, and it has to stay the default: a bound nobody configured
      // would refuse every push on every repository that has never checkpointed.
      const outcome = await scenario(
        Effect.gen(function* () {
          const where = yield* world(["source.push"]);
          const { second } = yield* history("refs/heads/topic");
          return yield* gateAs(where, [{ name: "refs/heads/topic", value: second }]);
        }),
      );
      assert.deepEqual(outcome.refused, []);
    });
  });

  describe("writing a ref whose value is not known yet", () => {
    /** The JSON verbs that compute the new value while doing the work. */
    it("lets a member write an ordinary branch", async () => {
      // The regression this guards: gating without providing the requester
      // refused *every* caller, admins included.
      const refusal = await scenario(
        Effect.gen(function* () {
          const where = yield* world(["source.push"]);
          return yield* gateWriteAs(where, "refs/heads/topic");
        }),
      );
      assert.equal(refusal, null);
    });

    it("refuses a protected branch, which only a pull request may move", async () => {
      const refusal = await scenario(
        Effect.gen(function* () {
          const where = yield* world(["source.push", "policy.write"]);
          const repository = yield* Repository;
          const blob = yield* repository.writeBlob(
            Policy.encodeRules({
              ...OPEN,
              protected: ["refs/heads/main"],
              requirePullRequest: true,
            }),
          );
          const tree = yield* repository.writeTree([
            { mode: "100644", name: "policy.json", oid: blob },
          ]);
          const commit = yield* repository.commitTree({
            tree,
            parents: [],
            message: "policy\n",
            author,
          });
          yield* repository.setRef({ name: Policy.RULES_REF, to: commit });

          return yield* gateWriteAs(where, "refs/heads/main");
        }),
      );
      // Refused by name, before the write: these verbs name a branch and not
      // the revision the protected-branch rules are about, so there is nothing
      // here to evaluate those rules against. A protected branch moves through
      // receive-pack, where the revision arrives with the request.
      assert.match(refusal ?? "", /pushing an approved revision/);
    });

    it("refuses to establish an identity over the API", async () => {
      // A repository with no genesis must not acquire one this way: whoever
      // asked first would own somebody else's repository.
      const refusal = await scenario(gateWriteAs(null, "refs/meta/trust/genesis"));
      assert.match(refusal ?? "", /hub init/);
    });

    it("refuses the tag namespace a fetch writes, when it is protected", async () => {
      // The `fetch` and `pull` verbs write two namespaces: tracking refs, and
      // tags — `Sync` rewrites `refs/heads/*` into `refs/remotes/<name>/*` and
      // leaves tag names exactly as the remote spelled them. Gating tracking
      // alone let every `refs/tags/*` write past the policy boundary.
      const refusal = await scenario(
        Effect.gen(function* () {
          const where = yield* world(["source.push", "policy.write"]);
          const repository = yield* Repository;
          const blob = yield* repository.writeBlob(
            Policy.encodeRules({ ...OPEN, protected: ["refs/tags/*"], requirePullRequest: true }),
          );
          const tree = yield* repository.writeTree([
            { mode: "100644", name: "policy.json", oid: blob },
          ]);
          const commit = yield* repository.commitTree({
            tree,
            parents: [],
            message: "policy\n",
            author,
          });
          yield* repository.setRef({ name: Policy.RULES_REF, to: commit });

          return yield* gateWriteAs(where, "refs/tags/*");
        }),
      );
      assert.match(refusal ?? "", /protected/);
    });

    it("refuses a namespace write against a rule narrower than the namespace", async () => {
      // `refs/tags/*` is what a fetch asks about: it writes every tag the
      // remote has and does not know their names until the negotiation is
      // over. Compared as a literal name it matched a rule protecting
      // `refs/tags/*` and missed one protecting `refs/tags/v*` — so the
      // narrower rule was the one with the hole in it.
      const refusal = await scenario(
        Effect.gen(function* () {
          const where = yield* world(["source.push", "policy.write"]);
          const repository = yield* Repository;
          const blob = yield* repository.writeBlob(
            Policy.encodeRules({ ...OPEN, protected: ["refs/tags/v*"], requirePullRequest: true }),
          );
          const tree = yield* repository.writeTree([
            { mode: "100644", name: "policy.json", oid: blob },
          ]);
          const commit = yield* repository.commitTree({
            tree,
            parents: [],
            message: "policy\n",
            author,
          });
          yield* repository.setRef({ name: Policy.RULES_REF, to: commit });

          return yield* gateWriteAs(where, "refs/tags/*");
        }),
      );
      assert.match(refusal ?? "", /protected/);
    });

    it("lets a namespace write through when no rule reaches into it", async () => {
      const refusal = await scenario(
        Effect.gen(function* () {
          const where = yield* world(["source.push", "policy.write"]);
          const repository = yield* Repository;
          const blob = yield* repository.writeBlob(
            Policy.encodeRules({ ...OPEN, protected: ["refs/heads/main"] }),
          );
          const tree = yield* repository.writeTree([
            { mode: "100644", name: "policy.json", oid: blob },
          ]);
          const commit = yield* repository.commitTree({
            tree,
            parents: [],
            message: "policy\n",
            author,
          });
          yield* repository.setRef({ name: Policy.RULES_REF, to: commit });

          return yield* gateWriteAs(where, "refs/tags/*");
        }),
      );
      assert.equal(refusal, null);
    });

    it("charges a force-push for the writes that behave like one", async () => {
      // Two JSON verbs move a ref to a value that need not contain what it
      // currently holds: `tagCreate --force`, which drops the create-only
      // compare-and-swap, and a merge whose `into` is a third branch, whose
      // result is a commit over `ours` and `theirs` and nothing else. Both
      // were gated as ordinary writes, so `source.push` alone did what
      // receive-pack charges `source.force-push` for.
      const outcome = await scenario(
        Effect.gen(function* () {
          const where = yield* world(["source.push"]);
          return {
            plain: yield* gateWriteAs(where, "refs/tags/v1"),
            forced: yield* gateWriteAs(where, "refs/tags/v1", true),
            branch: yield* gateWriteAs(where, "refs/heads/main"),
            rewrite: yield* gateWriteAs(where, "refs/heads/main", true),
          };
        }),
      );

      assert.equal(outcome.plain, null, "an ordinary tag needs only source.push");
      assert.match(outcome.forced ?? "", /source\.force-push/);
      assert.equal(outcome.branch, null);
      assert.match(outcome.rewrite ?? "", /source\.force-push/);
    });

    it("refuses a hub ref, which is appended to and never written", async () => {
      const refusal = await scenario(
        Effect.gen(function* () {
          const where = yield* world(["repo.admin"]);
          return yield* gateWriteAs(where, "refs/hub/pr/anything");
        }),
      );
      assert.match(refusal ?? "", /appended to/);
    });
  });

  describe("applying", () => {
    it("compares against the ref's own value, not what a symref resolves to", async () => {
      // `RefStore.apply` checks `expected` against the ref's stored value, and
      // a symbolic ref stores no oid. Taking the compare-and-swap from the
      // *resolved* value named a commit the store would never agree with, so
      // every gated write to a symbolic ref failed as a conflict against a
      // value nobody had written there.
      //
      // On disk rather than in memory, because that is where a symbolic ref
      // under `refs/` can exist at all: git writes `refs/remotes/origin/HEAD`
      // as one in every repository it clones, and nothing in this codebase has
      // a way to create one.
      const root = await fs.mkdtemp(path.join(os.tmpdir(), "policy-symref-"));
      try {
        const onDisk = GitRepository.layer.pipe(
          Layer.provide(GitRepository.hooksNoop),
          Layer.provideMerge(nodeStores(root)),
        );

        const outcome = await Effect.runPromise(
          Effect.gen(function* () {
            const repository = yield* Repository;
            const where = yield* world(["source.push"]);
            const { second } = yield* history("refs/heads/topic");

            yield* Effect.promise(async () => {
              await fs.mkdir(path.join(root, "refs/remotes/origin"), { recursive: true });
              await fs.writeFile(
                path.join(root, "refs/remotes/origin/HEAD"),
                "ref: refs/heads/topic\n",
              );
            });

            const gated = yield* gateAs(where, [
              { name: "refs/remotes/origin/HEAD", value: second },
            ]);
            const results = yield* repository.receive(gated.updates);
            return { gated, ok: results.at(0)?.ok, reason: results.at(0)?.reason };
          }).pipe(Effect.provide(onDisk)),
        );

        assert.deepEqual(outcome.gated.refused, []);
        assert.equal(
          outcome.ok,
          true,
          `the compare-and-swap must match what the store checks: ${outcome.reason ?? ""}`,
        );
      } finally {
        await fs.rm(root, { recursive: true, force: true });
      }
    });

    it("applies under the value it judged, so a moved ref loses", async () => {
      const outcome = await scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const where = yield* world(["source.push"]);
          const { first, second } = yield* history("refs/heads/topic");
          const trust = yield* trustOf(where);

          // Judged against `second`… and then somebody else moves the branch
          // before the update lands.
          const judged = yield* evaluate({
            update: { name: "refs/heads/topic", value: second },
            principal: where.principal,
            genesis: where.genesis,
            trust,
            rules: OPEN,
          });
          yield* repository.setRef({ name: "refs/heads/topic", to: first });

          const results = yield* repository.receive(
            judged.ok ? [{ ...judged.allowed.update, expected: judged.allowed.expected }] : [],
          );
          return results;
        }),
      );

      assert.equal(outcome.at(0)?.ok, false, "the compare-and-swap must refuse a moved ref");
    });

    it("applies nothing when an atomic batch has a refusal in it", async () => {
      const outcome = await scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const where = yield* world(["source.push"]);
          const { second } = yield* history("refs/heads/topic");

          const gated = yield* gateAs(where, [
            { name: "refs/heads/ok", value: second },
            { name: "refs/meta/trust/genesis", value: second },
          ]);
          yield* repository.receive(gated.updates);
          return { gated, landed: yield* repository.resolve("refs/heads/ok") };
        }),
      );

      assert.equal(outcome.gated.updates.length, 0);
      assert.equal(outcome.gated.refused.length, 1);
      assert.equal(outcome.landed, null, "an atomic batch applies all or nothing");
    });

    it("leaves a repository with no genesis alone", async () => {
      const outcome = await scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const { second } = yield* history("refs/heads/topic");
          const gated = yield* gateAs(null, [{ name: "refs/heads/other", value: second }]).pipe(
            Effect.provide(Policy.anonymousWrites(true)),
          );
          yield* repository.receive(gated.updates);
          return { gated, landed: yield* repository.resolve("refs/heads/other") };
        }),
      );

      assert.equal(outcome.gated.refused.length, 0);
      assert.equal(outcome.landed !== null, true, "a plain git repository stays servable");
    });
  });
});
