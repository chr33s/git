/**
 * What a trace tombstone actually removes.
 *
 * The whole reason the trace namespace has redaction is `context/render.bin`:
 * it holds the task string verbatim and the exact bytes of every exposed file,
 * so a credential typed into a prompt is *there* as well as in the session's
 * account of the work. A redaction that covered only `event.json` removed the
 * account of the exposure and left the exposure itself readable and clonable
 * forever — while the module's own docstring said otherwise and the audit
 * reported "has been collected or redacted" for a blob nothing had touched.
 *
 * So the assertion here is on the blob, not on what the audit says about it:
 * take the render's oid before the removal, and ask the object store for it
 * afterwards. Nothing else can tell the difference.
 */
import assert from "node:assert/strict";
import { describe, it } from "@effect/vitest";

import { Effect, Layer } from "effect";

import { formatPublicKey, generate, type PrivateKey } from "../crypto/SshSignature.ts";
import { EMPTY_TREE_OID, type Signature } from "../git/Format.ts";
import { stores } from "../git/Memory.ts";
import * as GitRepository from "../git/Repository.ts";
import { Repository } from "../git/Repository.ts";
import { ObjectStore } from "../git/Store.ts";
import * as Exposure from "../context/Exposure.ts";
import * as Pack from "../context/Pack.ts";
import * as Event from "../hub/Event.ts";
import * as PullRequest from "../hub/PullRequest.ts";
import * as Redaction from "../hub/Redaction.ts";
import * as Trace from "../hub/Trace.ts";
import * as Certificate from "../trust/Certificate.ts";
import { create, type Genesis, signGenesis, writeGenesis } from "../trust/Genesis.ts";
import * as Log from "../trust/Log.ts";
import { project as projectTrust } from "../trust/Projection.ts";
import { GENESIS_REF } from "../trust/Genesis.ts";
import * as Audit from "../cli/audit.ts";
import * as Invocation from "./Invocation.ts";
import * as Session from "../hub/Session.ts";
import * as Tombstone from "../hub/Tombstone.ts";
import * as Oid from "../git/Oid.ts";
import * as Records from "./Records.ts";

const SESSION = "0192f000-0000-7000-8000-000000000000";
const SECRET = "rotate the deploy token hunter2";

const author: Signature = {
  name: "Runner",
  email: "runner@example.com",
  at: new Date(1_700_000_000_000),
  offset: 0,
};

const world = GitRepository.layer.pipe(
  Layer.provide(GitRepository.hooksNoop),
  Layer.provideMerge(stores),
);

const scenario = <A, E>(effect: Effect.Effect<A, E, Repository | ObjectStore>): Promise<A> =>
  Effect.runPromise(effect.pipe(Effect.provide(world)));

/** A repository whose one member may both write traces and remove them. */
const enabled = Effect.fn("test.enabled")(function* () {
  const root = yield* generate("root@example.com");
  const agent = yield* generate("agent@example.com");
  const genesis = yield* create([formatPublicKey(root.publicKey)], 1);
  yield* writeGenesis(genesis, [yield* signGenesis(genesis, root)]);
  yield* Log.issue(
    yield* Certificate.grant({
      repo: genesis.repoId,
      publicKey: formatPublicKey(agent.publicKey),
      capabilities: ["repo.read", "hub.trace", "hub.redact", "hub.create-pr", "hub.comment"],
      id: Log.newId(),
    }),
    [root],
  );
  return { genesis, agent } as const;
});

/** An exposure over a dirty checkout, whose overlay carries a secret file. */
const leaked = Effect.fn("test.leaked")(function* (genesis: Genesis, key: PrivateKey) {
  const repository = yield* Repository;
  // A tracked file edited but never committed: the overlay tree the exposure
  // retains is the only thing in the repository that holds these bytes.
  const blob = yield* repository.writeBlob(new TextEncoder().encode("token = hunter2\n"));
  const tree = yield* repository.writePaths([{ path: "src/config.ts", oid: blob, mode: "100644" }]);
  const commit = yield* repository.commitTree({
    tree: yield* repository.writeTree([]),
    parents: [],
    message: "root\n",
    author,
  });

  const written = yield* Exposure.expose({
    repo: genesis.repoId,
    session: SESSION,
    key,
    pack: {
      version: 1,
      view: { base: Pack.qualify(commit), tree: Pack.qualify(tree) },
      items: [{ kind: "blob", path: "src/config.ts", blob: Pack.qualify(blob) }],
    },
    segments: [
      { placement: "developer", mediaType: "text/plain", body: new TextEncoder().encode("ctx") },
    ],
  });
  return { written, blob } as const;
});

/** An exposure whose render carries the secret, with the render retained. */
const exposed = Effect.fn("test.exposed")(function* (
  genesis: Genesis,
  key: PrivateKey,
  session: string = SESSION,
  body: string = SECRET,
) {
  const repository = yield* Repository;
  const commit = yield* repository.commitTree({
    tree: yield* repository.writeTree([]),
    parents: [],
    message: "root\n",
    author,
  });
  const written = yield* Exposure.expose({
    repo: genesis.repoId,
    session,
    key,
    pack: { version: 1, view: yield* Pack.committed(commit), items: [] },
    segments: [
      { placement: "user", mediaType: "text/plain", body: new TextEncoder().encode(body) },
    ],
  });

  const info = yield* repository.readCommit(written.commit);
  const render = yield* repository.findPath(info.tree, Exposure.RENDER);
  const pack = yield* repository.findPath(info.tree, Exposure.PACK);
  const record = yield* repository.findPath(info.tree, "event.json");
  return { written, render: render!.oid, pack: pack!.oid, record: record!.oid } as const;
});

describe("trace redaction", () => {
  it.effect("removes the render bytes, which is what the leak is", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const where = yield* enabled();
          const made = yield* exposed(where.genesis, where.agent);

          // The secret is in the object store, exactly as the render framed it.
          const before = yield* repository.readBlob(made.render);
          assert.match(new TextDecoder().decode(before), /hunter2/);

          yield* Records.redact({
            repo: where.genesis.repoId,
            session: SESSION,
            target: made.written.oid,
            reason: "the prompt carried a token",
            key: where.agent,
          });

          // The tombstone names the record; `gc` is what removes the bytes,
          // and only the exclusion set tells it which bytes a tombstone
          // accounts for.
          const exclude = yield* Redaction.excluded();
          assert.equal(exclude.has(made.render), true, "the render is covered");
          assert.equal(exclude.has(made.pack), true, "and so is the pack");

          yield* repository.gc({ exclude });

          const gone = yield* repository
            .readBlob(made.render)
            .pipe(Effect.catchTag("ObjectNotFound", () => Effect.succeed(null)));
          assert.equal(gone, null, "the verbatim prompt is no longer readable");
        }),
      ),
    ),
  );

  it.effect("leaves the record itself on the ref", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const where = yield* enabled();
          const made = yield* exposed(where.genesis, where.agent);

          yield* Records.redact({
            repo: where.genesis.repoId,
            session: SESSION,
            target: made.written.oid,
            reason: "leaked",
            key: where.agent,
          });
          yield* repository.gc({ exclude: yield* Redaction.excluded() });

          // A hash chain with a hole in it is not a hash chain: the commit
          // stays, and so does the tree entry naming the blob that is gone.
          const info = yield* repository.readCommit(made.written.commit);
          assert.notEqual(yield* repository.findPath(info.tree, Exposure.RENDER), null);
        }),
      ),
    ),
  );

  it.effect("keeps a blob a second, un-redacted exposure still names", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const where = yield* enabled();

          // Two exposures of the same view and the same segments. A pack and a
          // render are deterministic by design — identity *is* the blob oid of
          // the exact bytes — so both records name one `context/pack.json` and
          // one `context/render.bin`.
          const first = yield* exposed(where.genesis, where.agent);
          const second = yield* exposed(where.genesis, where.agent);
          assert.equal(first.render, second.render, "the render is one shared blob");
          assert.equal(first.pack, second.pack, "and so is the pack");
          assert.notEqual(first.written.commit, second.written.commit);

          yield* Records.redact({
            repo: where.genesis.repoId,
            session: SESSION,
            target: first.written.oid,
            reason: "leaked",
            key: where.agent,
          });

          const exclude = yield* Redaction.excluded();
          // Excluded without asking who else names them, these two blobs were
          // deleted out from under the surviving record — whose audit then
          // reported its own pack unavailable, forever, on a ref that cannot
          // be rewound.
          assert.equal(exclude.has(first.render), false);
          assert.equal(exclude.has(first.pack), false);

          yield* repository.gc({ exclude });
          const audited = yield* Exposure.audit({
            commit: second.written.commit,
            repo: where.genesis.repoId,
            session: SESSION,
          });
          assert.equal(audited.ok, true, "the exposure nobody redacted still verifies");
        }),
      ),
    ),
  );

  it.effect("holds back only the shared blobs when some ref cannot be walked", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const where = yield* enabled();
          const other = "0192f000-0000-7000-8000-0000000000bb";

          // One session with a single exposure whose blobs nothing else names,
          // redacted — so there is genuinely something to exclude.
          const made = yield* exposed(where.genesis, where.agent, SESSION, "only this one");
          yield* Records.redact({
            repo: where.genesis.repoId,
            session: SESSION,
            target: made.written.oid,
            reason: "leaked",
            key: where.agent,
          });

          // With every ref walkable, the exclusion is real.
          const whole = yield* Redaction.excluded();
          assert.equal(whole.has(made.render), true);

          // A second session long enough to sit over a lowered ceiling.
          for (let index = 0; index < 5; index++) {
            yield* exposed(where.genesis, where.agent, other, `ask ${index}`);
          }

          // Now one ref cannot be walked. The kept set is *subtracted* from
          // the exclusion, so a ref skipped for being unwalkable rescues
          // nothing — which makes skipping it the destructive direction, not
          // the cautious one for anything two records can share. A Pack and a
          // ContextRender are deterministic, so those wait for a walk that saw
          // every live record. A redaction that waits is recoverable; bytes
          // deleted because a ref could not be read are not.
          const partial = yield* Redaction.excluded().pipe(Effect.provide(Trace.ceiling(4)));
          assert.equal(partial.has(made.render), false);
          assert.equal(partial.has(made.pack), false);

          // The payload does not wait, and gating it on the same flag was the
          // whole blast radius: one unwalkable trace ref stopped every session
          // and task redaction in the repository from being honoured, silently
          // and on every collection after. Nothing but a byte-identical
          // `event.json` can be holding this object, and byte-identical means
          // the same signed statement — which is the one this tombstone names.
          assert.equal(partial.has(made.record), true);
        }),
      ),
    ),
  );

  it.effect("still explains an absence when a ref cannot be walked", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const where = yield* enabled();
          const other = "0192f000-0000-7000-8000-0000000000bb";
          const made = yield* exposed(where.genesis, where.agent, SESSION, "only this one");
          yield* Records.redact({
            repo: where.genesis.repoId,
            session: SESSION,
            target: made.written.oid,
            reason: "leaked",
            key: where.agent,
          });
          for (let index = 0; index < 5; index++) {
            yield* exposed(where.genesis, where.agent, other, `ask ${index}`);
          }

          // `covered` has the opposite polarity to `excluded`: it says which
          // absences are *explained*, so a smaller set is how a fetch starts
          // failing with no recovery. An unwalkable ref must not empty it —
          // doing so discarded every other ref's coverage too, and made every
          // clone of the repository fail permanently.
          const partial = yield* Redaction.covered().pipe(Effect.provide(Trace.ceiling(4)));
          assert.equal(partial.has(made.render), true);
        }),
      ),
    ),
  );

  it.effect("still explains an absence a live record also names", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const where = yield* enabled();
          const first = yield* exposed(where.genesis, where.agent);
          yield* exposed(where.genesis, where.agent);

          yield* Records.redact({
            repo: where.genesis.repoId,
            session: SESSION,
            target: first.written.oid,
            reason: "leaked",
            key: where.agent,
          });

          // `excluded` must leave a shared blob alone; `covered` must still
          // account for it. If another replica's collection already removed
          // it, an unexplained absence is a clone that cannot complete.
          assert.equal((yield* Redaction.excluded()).has(first.render), false);
          assert.equal((yield* Redaction.covered()).has(first.render), true);
        }),
      ),
    ),
  );

  it.effect("removes the retained view a dirty exposure is the only holder of", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const where = yield* enabled();
          const made = yield* leaked(where.genesis, where.agent);

          const before = yield* repository.readBlob(made.blob);
          assert.match(new TextDecoder().decode(before), /hunter2/);

          yield* Records.redact({
            repo: where.genesis.repoId,
            session: SESSION,
            target: made.written.oid,
            reason: "a tracked file carried a token",
            key: where.agent,
          });
          yield* repository.gc({ exclude: yield* Redaction.excluded() });

          // The file bytes are only in the overlay `context/view` holds — no
          // commit reaches them — so a redaction that stopped at the render
          // left the thing it was asked to remove readable via
          // `<record>^{tree}:context/view/src/config.ts`, forever.
          const gone = yield* repository
            .readBlob(made.blob)
            .pipe(Effect.catchTag("ObjectNotFound", () => Effect.succeed(null)));
          assert.equal(gone, null);
        }),
      ),
    ),
  );

  it.effect("leaves a view blob some branch also holds", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const where = yield* enabled();
          const made = yield* leaked(where.genesis, where.agent);

          // The same bytes, now committed on a branch. git dedupes by content,
          // so this is the very object the overlay names.
          const tree = yield* repository.writePaths([
            { path: "src/config.ts", oid: made.blob, mode: "100644" },
          ]);
          yield* repository.commit({
            branch: "refs/heads/main",
            tree,
            message: "commit it\n",
            author,
          });

          yield* Records.redact({
            repo: where.genesis.repoId,
            session: SESSION,
            target: made.written.oid,
            reason: "leaked",
            key: where.agent,
          });
          yield* repository.gc({ exclude: yield* Redaction.excluded() });

          // An exclusion says "the hub must not keep this alive", not "delete
          // this". A branch still reaches it, so it survives — otherwise a
          // redaction would be a licence to corrupt history.
          assert.notEqual(yield* repository.readBlob(made.blob), null);
        }),
      ),
    ),
  );

  it.effect("does not honour a tombstone nobody who could remove anything signed", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const where = yield* enabled();
          const made = yield* exposed(where.genesis, where.agent);
          const stranger = yield* generate("stranger@example.com");

          // Written by hand onto the ref, the way a fetched-in commit arrives:
          // replication is deliberately not policy-gated, so the boundary that
          // charges `hub.redact` never saw this.
          const base = yield* Records.context(where.genesis.repoId, SESSION);
          const payload = Records.encode({
            ...base,
            type: Records.REDACTED,
            target: "whatever",
            targetCommit: made.written.oid,
            reason: "not mine to remove",
          });
          yield* Trace.append({
            session: SESSION,
            type: Records.REDACTED,
            id: base.id,
            payload,
            key: stranger,
          });

          // The exclusion is authorization's to decide, and it says no.
          assert.equal((yield* Redaction.excluded()).has(made.render), false);

          // And the projection does not report it as a signed removal.
          const projected = yield* Invocation.project({
            session: SESSION,
            repo: where.genesis.repoId,
            trust: yield* projectTrust(where.genesis),
          });
          assert.deepEqual(projected.redacted, []);
          assert.notEqual(yield* repository.readBlob(made.render), null);
        }),
      ),
    ),
  );

  it.effect("rescues nothing when a ref's head cannot be classified", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const where = yield* enabled();
          const other = "0192f000-0000-7000-8000-0000000000cc";

          // Two sessions whose exposures share a deterministic pack, render
          // and view.
          const first = yield* exposed(where.genesis, where.agent);
          const second = yield* exposed(where.genesis, where.agent, other);
          assert.equal(first.render, second.render);

          // The other session's head commit is here; its tree is not — the
          // partial-replication state `isHubCommit` is written for, since refs
          // are applied without a connectivity check.
          const store = yield* ObjectStore;
          const head = yield* repository.resolve(Trace.refOf(other));
          yield* store.delete((yield* repository.readCommit(head!)).tree);

          yield* Records.redact({
            repo: where.genesis.repoId,
            session: SESSION,
            target: first.written.oid,
            reason: "leaked",
            key: where.agent,
          });

          // `Dag.reachable` does not fail here — `isHubCommit` answers `false`
          // and the walk comes back empty — so a walk read as complete found
          // no live records on that ref and handed the blobs it still needs to
          // `gc`, silently. The shared half is exactly what that would have
          // destroyed: the other session's exposure names the same objects and
          // no tombstone anywhere covers it.
          const partial = yield* Redaction.excluded();
          assert.equal(partial.has(first.render), false);
          assert.equal(partial.has(first.pack), false);

          // Its own payload still goes. No other record can be holding those
          // bytes without being the same signed statement, so an unwalkable
          // ref tells us nothing about them — and holding them back is how one
          // half-replicated trace ref stopped every redaction in the
          // repository from being honoured.
          assert.deepEqual([...partial], [first.record]);
        }),
      ),
    ),
  );

  it.effect("keeps honouring a pull request's tombstone when a trace ref will not walk", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const where = yield* enabled();

          // A pull request with a redacted comment: a tombstone in a namespace
          // that has nothing to do with traces.
          const { pr } = yield* PullRequest.open({
            repo: where.genesis.repoId,
            title: "Add a thing",
            base: "refs/heads/main",
            head: EMPTY_TREE_OID,
            key: where.agent,
          });
          const commit = yield* PullRequest.comment({
            repo: where.genesis.repoId,
            pr,
            body: "the deploy key is elsewhere",
            key: where.agent,
          });
          const info = yield* repository.readCommit(commit);
          const payload = yield* repository.findPath(info.tree, "event.json");
          const { events } = yield* Event.entries(pr);
          yield* PullRequest.redact({
            repo: where.genesis.repoId,
            pr,
            target: events.find((entry) => entry.commit === commit)?.payload?.id ?? "",
            reason: "sensitive-content",
            key: where.agent,
          });

          // And a trace ref this host will not walk, which has nothing to do
          // with that pull request.
          for (let index = 0; index < 5; index++) {
            yield* exposed(where.genesis, where.agent, SESSION, `ask ${index}`);
          }

          // Clearing the *whole* set took the pull request's tombstone down
          // with it, so one over-ceiling trace ref silently stopped every
          // redaction in the repository from being honoured — while
          // `tombstoned`/`blobs` were written to treat one unwalkable ref as
          // one ref rather than a broken repository.
          const narrowed = yield* Redaction.excluded().pipe(Effect.provide(Trace.ceiling(4)));
          assert.equal(narrowed.has(payload!.oid), true);
        }),
      ),
    ),
  );

  it.effect("does not move a foreign invocation's workspace change onto its neighbour", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const where = yield* enabled();
          const made = yield* exposed(where.genesis, where.agent, SESSION, "shared view");
          const packed = yield* Exposure.packOf(made.written.commit);
          const pack = yield* Pack.decode(packed.bytes);

          const mine = yield* Records.record(
            {
              ...(yield* Records.context(where.genesis.repoId, SESSION)),
              type: Records.INVOCATION,
              exposure: made.written.oid,
              capture: null,
              operation: { name: "mine" },
            },
            where.agent,
          );

          // An invocation a peer landed here that names another session.
          // `Records.entries` diverts it, so it reaches neither `runtimes` nor
          // `records` — and left out of the boundary set, `ownerOf` walked
          // straight through it and the transition *it* produced was reported
          // under the genuine invocation before it: a repository change that
          // invocation did not make.
          const written = yield* Records.record(
            {
              ...(yield* Records.context(
                where.genesis.repoId,
                "0192f000-0000-7000-8000-0000000000f9",
              )),
              type: Records.INVOCATION,
              exposure: null,
              capture: null,
              operation: { name: "stranger" },
            },
            where.agent,
          );
          const info = yield* repository.readCommit(written.commit);
          const stranger = yield* repository.commitTree({
            tree: info.tree,
            parents: [(yield* repository.resolve(Trace.refOf(SESSION)))!],
            message: info.message,
            author: info.author,
          });
          yield* repository.setRef({ name: Trace.refOf(SESSION), to: stranger });

          yield* Records.record(
            {
              ...(yield* Records.context(where.genesis.repoId, SESSION)),
              type: Records.WORKSPACE,
              beforeTree: pack.view.tree,
              afterTree: pack.view.tree,
              operation: null,
            },
            where.agent,
          );

          const projected = yield* Invocation.project({
            repo: where.genesis.repoId,
            session: SESSION,
            trust: yield* projectTrust(where.genesis),
          });
          assert.equal(projected.foreign.includes(stranger), true);
          const row = projected.invocations.find((entry) => entry.runtime?.record === mine.oid);
          assert.equal(row?.workspace ?? null, null);
        }),
      ),
    ),
  );

  it.effect("does not move a removed invocation's workspace change onto its neighbour", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const where = yield* enabled();
          const made = yield* exposed(where.genesis, where.agent, SESSION, "shared view");
          const packed = yield* Exposure.packOf(made.written.commit);
          const pack = yield* Pack.decode(packed.bytes);

          const first = yield* Records.record(
            {
              ...(yield* Records.context(where.genesis.repoId, SESSION)),
              type: Records.INVOCATION,
              exposure: made.written.oid,
              capture: null,
              operation: { name: "first" },
            },
            where.agent,
          );
          const second = yield* Records.record(
            {
              ...(yield* Records.context(where.genesis.repoId, SESSION)),
              type: Records.INVOCATION,
              exposure: made.written.oid,
              capture: null,
              operation: { name: "second" },
            },
            where.agent,
          );
          yield* Records.record(
            {
              ...(yield* Records.context(where.genesis.repoId, SESSION)),
              type: Records.WORKSPACE,
              beforeTree: pack.view.tree,
              afterTree: pack.view.tree,
              operation: null,
            },
            where.agent,
          );

          // The invocation that made the change is removed. Dropped from the
          // set the windows and the ownership are built from, it stopped being
          // a boundary — so `ownerOf` walked past it to the one before, whose
          // window no longer closed, and that row reported a change made under
          // the record an operator had just removed.
          yield* Records.redact({
            repo: where.genesis.repoId,
            session: SESSION,
            target: second.oid,
            reason: "leaked",
            key: where.agent,
          });

          const projected = yield* Invocation.project({
            repo: where.genesis.repoId,
            session: SESSION,
            trust: yield* projectTrust(where.genesis),
          });
          const row = projected.invocations.find((entry) => entry.runtime?.record === first.oid);
          assert.equal(row?.workspace ?? null, null);
        }),
      ),
    ),
  );

  it.effect("stops showing a removed record whatever kind it is", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const where = yield* enabled();
          const tool = yield* Records.record(
            {
              ...(yield* Records.context(where.genesis.repoId, SESSION)),
              type: Records.TOOL,
              invocation: null,
              capture: null,
              tool: { name: "Read", description: "read a file" },
            },
            where.agent,
          );
          const trust = yield* projectTrust(where.genesis);
          const before = yield* Invocation.project({
            repo: where.genesis.repoId,
            session: SESSION,
            trust,
          });
          assert.equal(before.tools.length, 1);

          yield* Records.redact({
            repo: where.genesis.repoId,
            session: SESSION,
            target: tool.oid,
            reason: "leaked",
            key: where.agent,
          });

          // Applied to context exposures and to nothing else, `trace redact`
          // reported success and the audit went on printing the removed
          // record in full — here and on every replica, until some `gc`
          // collected the blob, and forever on a replica that never collects.
          // The statement outranks the bytes for all six kinds or none.
          const after = yield* Invocation.project({
            repo: where.genesis.repoId,
            session: SESSION,
            trust,
          });
          assert.deepEqual(after.tools, []);
          assert.deepEqual(after.redacted, [tool.oid]);
        }),
      ),
    ),
  );

  it.effect("does not pin an empty exclusion taken from a ref it could not walk", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const where = yield* enabled();
          const other = "0192f000-0000-7000-8000-0000000000ce";
          const made = yield* exposed(where.genesis, where.agent, SESSION, "only this one");
          yield* Records.redact({
            repo: where.genesis.repoId,
            session: SESSION,
            target: made.written.oid,
            reason: "leaked",
            key: where.agent,
          });

          // And now this host cannot walk the very ref the tombstone is on, so
          // `marks()` comes back empty — which is exactly the shape "no
          // tombstones anywhere" takes. The short branch of `excluded`
          // hardcoded `complete: true` for that case, so the empty answer was
          // pinned against heads that do not move when the objects arrive, and
          // the removal was never honoured for the life of the process.
          void other;
          const head = yield* repository.resolve(Trace.refOf(SESSION));
          const info = yield* repository.readCommit(head!);
          const tree = yield* repository.readTree(info.tree);
          yield* (yield* ObjectStore).delete(info.tree);
          assert.equal((yield* Redaction.excluded()).has(made.render), false);

          yield* repository.writeTree(tree);
          assert.equal((yield* Redaction.excluded()).has(made.render), true);
        }),
      ),
    ),
  );

  it.effect("notices a walk that stopped part way through a ref", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const where = yield* enabled();
          const other = "0192f000-0000-7000-8000-0000000000f0";
          const made = yield* exposed(where.genesis, where.agent, SESSION, "only this one");
          yield* Records.redact({
            repo: where.genesis.repoId,
            session: SESSION,
            target: made.written.oid,
            reason: "leaked",
            key: where.agent,
          });
          assert.equal((yield* Redaction.excluded()).has(made.render), true);

          // Three records on another ref, with the *middle* one's tree gone.
          // `Dag.reachable` stops at any commit `isHubCommit` rejects, and it
          // rejects a commit whose tree is missing — so this ref's head is
          // intact, its walk comes back short, and it used to call itself
          // whole: the short answer pinned against a head that does not move,
          // and the live records behind the gap invisible to `stillNamed`,
          // whose subtraction is what stops `gc` taking a shared blob.
          yield* exposed(where.genesis, where.agent, other, "one");
          const middle = yield* exposed(where.genesis, where.agent, other, "two");
          yield* exposed(where.genesis, where.agent, other, "three");
          const buried = yield* repository.readCommit(middle.written.commit);
          const tree = yield* repository.readTree(buried.tree);
          yield* (yield* ObjectStore).delete(buried.tree);

          assert.equal((yield* Redaction.excluded()).has(made.render), false);

          yield* repository.writeTree(tree);
          assert.equal((yield* Redaction.excluded()).has(made.render), true);
        }),
      ),
    ),
  );

  it.effect("still refuses to re-retain a removed render when a ref is short", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const where = yield* enabled();
          const other = "0192f000-0000-7000-8000-0000000000ef";
          const made = yield* exposed(where.genesis, where.agent, SESSION, "only this one");
          yield* Records.redact({
            repo: where.genesis.repoId,
            session: SESSION,
            target: made.written.oid,
            reason: "leaked",
            key: where.agent,
          });

          // A second ref this host cannot walk, which makes `excluded` withhold
          // its shared half — the safe direction for `gc` and the wrong one for
          // retention. Read from there, `context for` saw the render as
          // never-removed and wrote those exact bytes back into the store,
          // where the redacted record's surviving tree entry resolves them
          // again: the resurrection `withheld` exists to close, re-opened by an
          // unrelated ref's replication state.
          const twin = yield* exposed(where.genesis, where.agent, other, "and another");
          void twin;
          const head = yield* repository.resolve(Trace.refOf(other));
          const info = yield* repository.readCommit(head!);
          yield* (yield* ObjectStore).delete(info.tree);

          assert.equal((yield* Redaction.excluded()).has(made.render), false);
          assert.equal((yield* Redaction.removed()).has(made.render), true);
        }),
      ),
    ),
  );

  it.effect("does not pin a withheld exclusion while objects were still missing", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const where = yield* enabled();
          const other = "0192f000-0000-7000-8000-0000000000cd";
          const made = yield* exposed(where.genesis, where.agent, SESSION, "only this one");
          yield* Records.redact({
            repo: where.genesis.repoId,
            session: SESSION,
            target: made.written.oid,
            reason: "leaked",
            key: where.agent,
          });

          // A second trace ref whose head tree has not arrived: the
          // partial-replication state `tombstonesOn` reports as
          // `complete: false`, which makes `excluded` withhold the shared
          // `context/` blobs. That is the safe direction — and it has to
          // survive the short branch too, where "no tombstones found" is
          // exactly the shape an unwalkable ref takes.
          const twin = yield* exposed(where.genesis, where.agent, other, "and another");
          const head = yield* repository.resolve(Trace.refOf(other));
          const tree = yield* repository.readTree((yield* repository.readCommit(head!)).tree);
          const store = yield* ObjectStore;
          yield* store.delete((yield* repository.readCommit(head!)).tree);
          assert.equal((yield* Redaction.excluded()).has(made.render), false);
          void twin;

          // And now the objects arrive, without any ref moving. Stored
          // unconditionally, the withholding was permanent — none of the heads
          // in that memo's key move when a deepening fetch delivers them — so
          // every later `gc` in the process kept the verbatim task string and
          // every exposed file byte clonable off the ref.
          yield* repository.writeTree(tree);
          assert.equal((yield* Redaction.excluded()).has(made.render), true);
        }),
      ),
    ),
  );

  it.effect("does not pin an answer taken while objects were still missing", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const where = yield* enabled();
          const made = yield* exposed(where.genesis, where.agent, SESSION, "only this one");
          yield* Records.redact({
            repo: where.genesis.repoId,
            session: SESSION,
            target: made.written.oid,
            reason: "leaked",
            key: where.agent,
          });

          // The partial-replication state: the tombstone is here, the record
          // it names is not readable yet. `covered` comes back short.
          const info = yield* repository.readCommit(made.written.commit);
          const payload = yield* repository.findPath(info.tree, "event.json");
          const tree = yield* repository.readTree(info.tree);
          const store = yield* ObjectStore;
          yield* store.delete(info.tree);
          const short = yield* Redaction.covered();
          assert.equal(short.has(payload!.oid), false);

          // And now the object arrives, without any ref moving — which is what
          // a deepening fetch looks like. Remembered against a head that did
          // not move, the short answer was pinned for the life of the process,
          // and this set has the opposite polarity to `excluded`'s: a subset
          // is a clone failing on an absence nothing can explain.
          yield* repository.writeTree(tree);
          const whole = yield* Redaction.covered();
          assert.equal(whole.has(payload!.oid), true);
        }),
      ),
    ),
  );

  it.effect("does not collect on a session tombstone bound elsewhere", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const where = yield* enabled();
          yield* Session.open({
            repo: where.genesis.repoId,
            session: SESSION,
            agent: { kind: "assistant", model: "opus", harness: "cli" },
            prompt: "start",
            key: where.agent,
          });
          const asked = yield* Session.ask({
            repo: where.genesis.repoId,
            session: SESSION,
            question: "the deploy key is hunter2",
            key: where.agent,
          });
          const head = yield* repository.resolve(Session.refOf(SESSION));
          const payload = yield* repository.findPath(
            (yield* repository.readCommit(head!)).tree,
            "event.json",
          );

          // A session tombstone naming this record, validly signed, whose
          // payload names another repository. Bound only in the trace branch,
          // this was folded in and honoured — so an origin and a mirror
          // sharing a trust graph could have a tombstone written on one
          // destroy a session payload on the other, on a ref nothing can
          // rewind.
          // SAFETY: deliberately not a `SessionPayload` this repository would
          // write — the point is that one naming another repository arrives by
          // replication, which is not policy-gated.
          yield* Session.issue(
            {
              ...(yield* Session.context(where.genesis.repoId, SESSION)),
              repo: "SHA256:somewhere-else",
              type: Tombstone.TAG,
              target: asked,
              targetCommit: Oid.qualify(head!),
              reason: "leaked",
            } as never,
            where.agent,
          );

          assert.equal((yield* Redaction.excluded()).has(payload!.oid), false);
        }),
      ),
    ),
  );

  it.effect("survives a genesis ref this host cannot read, and recovers", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const where = yield* enabled();
          const made = yield* exposed(where.genesis, where.agent, SESSION, "only this one");
          yield* Records.redact({
            repo: where.genesis.repoId,
            session: SESSION,
            target: made.written.oid,
            reason: "leaked",
            key: where.agent,
          });

          // The state a replica reaches when refs arrive without an ordering
          // check: the trace ref is here, the genesis is not. Every tombstone
          // on the ref then binds against `""` and reads as unbound.
          const genesis = yield* repository.resolve(GENESIS_REF);
          yield* repository.setRef({ name: GENESIS_REF, to: made.written.commit });
          assert.equal((yield* Redaction.excluded()).has(made.render), false);

          // And now it reads again. What this pins is the tolerance: failing
          // on a genesis ref that names the wrong object took `gc` and every
          // deepening fetch down over a state the rest of the module handles.
          //
          // The other half of the same finding — that the per-ref memo keyed
          // on the ref's head alone would hand back the unbound marks after
          // the genesis arrived — is not reachable from here: `storageOf()`
          // differs per call under the memory layer, so that memo never hits
          // in a test. The genesis is in its key; this does not prove it.
          yield* repository.setRef({ name: GENESIS_REF, to: genesis! });
          assert.equal((yield* Redaction.excluded()).has(made.render), true);
        }),
      ),
    ),
  );

  it.effect("takes a kept answer only under the key the walk was made from", () =>
    Effect.promise(async () => {
      // A host that keeps the answer, standing in for a persistent layer. What
      // matters is the contract, not the storage: `read` answers `null` for
      // anything it is not certain of, and the key carries everything the
      // answer depends on.
      const held = new Map<string, ReadonlyArray<Redaction.Mark>>();
      const keeping = Layer.succeed(
        Redaction.Answers,
        Redaction.Answers.of({
          read: (key) => Effect.succeed(held.get(key) ?? null),
          write: (key, found) =>
            Effect.sync(() => {
              held.set(key, found);
            }),
        }),
      );

      await Effect.runPromise(
        Effect.gen(function* () {
          const where = yield* enabled();
          const made = yield* exposed(where.genesis, where.agent, SESSION, "only this one");

          // Nothing removed yet, and that empty answer is worth keeping: it
          // is what most repositories have, and the walk that learns it is the
          // cost this port exists to remove.
          assert.equal((yield* Redaction.removed()).size, 0);
          const first = held.size;
          assert.equal(first > 0, true);

          // A removal moves the ref, so this ref's key changes and its kept
          // answer is not reachable — while every other ref's still is, which
          // is the point of keeping this per ref rather than per repository.
          yield* Records.redact({
            repo: where.genesis.repoId,
            session: SESSION,
            target: made.written.oid,
            reason: "leaked",
            key: where.agent,
          });
          assert.equal((yield* Redaction.removed()).has(made.render), true);
          assert.equal(held.size > first, true);

          // And a host that has lost the answer recomputes rather than
          // guessing. Answering `null` is always safe; answering "nothing was
          // removed" is the resurrection this lookup exists to prevent.
          held.clear();
          assert.equal((yield* Redaction.removed()).has(made.render), true);
        }).pipe(Effect.provide(Layer.provideMerge(keeping, world))),
      );
    }),
  );

  it.effect("does not collect on a tombstone bound elsewhere", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const where = yield* enabled();
          const made = yield* exposed(where.genesis, where.agent, SESSION, "only this one");

          // A tombstone naming this exposure, validly signed by a holder of
          // `hub.redact`, whose payload names another repository — which
          // reaches this ref because replication is not policy-gated.
          yield* Records.record(
            {
              ...(yield* Records.context(where.genesis.repoId, SESSION)),
              repo: "SHA256:somewhere-else",
              type: Records.REDACTED,
              target: "0192f000-0000-7000-8000-00000000aaaa",
              targetCommit: made.written.oid,
              reason: "leaked",
            },
            where.agent,
            true,
          );

          // Honoured, `gc` collected the payload, the pack, the render and the
          // view — while both readers bind, so the audit called the result a
          // replica gap rather than a removal and `context audit` failed on it
          // forever.
          assert.equal((yield* Redaction.excluded()).size, 0);

          // And `withheld` agrees about which tombstones count, because it
          // judges the same two things. A second exposure sharing the same
          // deterministic pack and render, redacted properly: the first record
          // is still live, so it is what holds the objects back.
          const other = "0192f000-0000-7000-8000-0000000000ff";
          const twin = yield* exposed(where.genesis, where.agent, other, "only this one");
          const written = yield* Records.redact({
            repo: where.genesis.repoId,
            session: other,
            target: twin.written.oid,
            reason: "leaked",
            key: where.agent,
          });

          // Judged on `counts` alone, the unbound tombstone above took its
          // target out of the holder search while `excluded` went on treating
          // that record as live and protecting its blobs — objects held with
          // nobody to blame, which the caller reports as a ref this host
          // cannot walk: the wrong cause.
          const said = yield* Redaction.withheld(written.targetCommit);
          assert.equal(said.blobs.length > 0, true);
          assert.deepEqual([...said.holders], [made.written.commit]);

          // `covered` has the opposite polarity: it says which absences are
          // *explained*, where a smaller set is how a fetch starts failing
          // with nothing to recover from. It still names them.
          assert.equal((yield* Redaction.covered()).size > 0, true);
        }),
      ),
    ),
  );

  it.effect("keeps coverage unknown after a removed health record is collected", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const where = yield* enabled();
          const base = yield* Records.context(where.genesis.repoId, SESSION);
          const clean = {
            ...base,
            type: Records.HEALTH,
            source: "otel",
            sampling: "none",
            transformed: false,
            dropped: 0,
          } as const;
          const lossy = yield* Records.record(
            { ...clean, stage: "local-collector", transformed: true, dropped: 12 },
            where.agent,
          );
          yield* Records.record(
            {
              ...clean,
              ...(yield* Records.context(where.genesis.repoId, SESSION)),
              type: Records.HEALTH,
              source: "otel",
              sampling: "none",
              transformed: false,
              dropped: 0,
              stage: "sdk-export",
            },
            where.agent,
          );

          yield* Records.redact({
            repo: where.genesis.repoId,
            session: SESSION,
            target: lossy.oid,
            reason: "leaked",
            key: where.agent,
          });

          const trust = yield* projectTrust(where.genesis);
          const before = yield* Invocation.project({
            repo: where.genesis.repoId,
            session: SESSION,
            trust,
          });
          assert.equal(before.coverage, "unknown");

          // What `gc` does to a redacted record: the payload blob goes and the
          // tree entry stays, so the record moves from `records` to
          // `unreadable`. Read only off the decoded side, the flag was true in
          // the window between the removal and the collection and false
          // forever after — so the collection silently re-enabled the exact
          // claim the flag exists to prevent.
          const info = yield* repository.readCommit(lossy.commit);
          const payload = yield* repository.findPath(info.tree, "event.json");
          yield* (yield* ObjectStore).delete(payload!.oid);

          const after = yield* Invocation.project({
            repo: where.genesis.repoId,
            session: SESSION,
            trust,
          });
          assert.equal(after.coverage, "unknown");
        }),
      ),
    ),
  );

  it.effect("does not honour a tombstone bound to another repository", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const where = yield* enabled();
          const made = yield* exposed(where.genesis, where.agent, SESSION, "only this one");

          // A tombstone naming this exposure, signed by the same key, bound to
          // a different repository — which reaches here because traces are
          // transferable by explicit refspec and replication is not gated on
          // payload contents.
          yield* Records.record(
            {
              ...(yield* Records.context(where.genesis.repoId, SESSION)),
              repo: "SHA256:somewhere-else",
              type: Records.REDACTED,
              target: "0192f000-0000-7000-8000-00000000aaaa",
              targetCommit: made.written.oid,
              reason: "leaked",
            },
            where.agent,
            true,
          );

          // `Records.entries` binds on session and leaves the repository half
          // to its caller, so every caller has to do it. `cli/context.ts`'s
          // `removalsOn` did not, and the exposure this names landed in
          // `redacted` there — excluded from `audits`, from `unreadable`, and
          // so from the non-zero exit, which meant `context audit <session> &&
          // deploy` deployed having never checked that exposure's signature,
          // trust, binding or evidence. This pins the filter on the reader
          // that can be reached from here; the CLI's is the same line.
          const projected = yield* Invocation.project({
            repo: where.genesis.repoId,
            session: SESSION,
            trust: yield* projectTrust(where.genesis),
          });
          assert.deepEqual(projected.redacted, []);
          void repository;
        }),
      ),
    ),
  );

  it.effect("records a provider's opaque correlation id without calling it a secret", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const where = yield* enabled();
          const base = yield* Records.context(where.genesis.repoId, SESSION);

          // A base62 conversation id of the length providers actually mint.
          // `conversation.externalId` is documented as correlation and nothing
          // else, and an opaque id is indistinguishable from a token by
          // entropy alone — so scanned as prose it refused the whole record,
          // with no override and nothing written, and the only way through was
          // to drop the correlation id §7.4 exists to record. UUID- and
          // hex-shaped ids stay under the threshold, so it failed for some
          // providers and not others.
          const opaque = "conv_9pQzR4tWx7YbN2mLkJ3hVcD8sFgA1eUo";
          yield* Records.record(
            {
              ...base,
              type: Records.INVOCATION,
              exposure: null,
              capture: null,
              operation: { name: "chat" },
              conversation: { externalId: opaque },
            },
            where.agent,
          );

          // The pattern rules still apply to it: a token is a token wherever
          // it is put.
          const refused = yield* Records.record(
            {
              ...(yield* Records.context(where.genesis.repoId, SESSION)),
              type: Records.INVOCATION,
              exposure: null,
              capture: null,
              operation: { name: "chat" },
              conversation: { externalId: `ghp_${"A".repeat(36)}` },
            },
            where.agent,
          ).pipe(Effect.flip);
          assert.equal(refused._tag, "Invalid");
        }),
      ),
    ),
  );

  it.effect("honours a tombstone whose type is written with a JSON escape", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const where = yield* enabled();
          const made = yield* exposed(where.genesis, where.agent, SESSION, "only this one");

          // The same payload a proper `trace redact` writes, with one
          // character of the type escaped. JSON parses it to the tag; a byte
          // search for the literal does not find it. This codebase's own
          // writer never emits that — `encode` uses `JSON.stringify` — but
          // another implementation replicating in is the threat model.
          const base = yield* Records.context(where.genesis.repoId, SESSION);
          const escaped = new TextEncoder().encode(
            JSON.stringify({
              ...base,
              type: "PLACEHOLDER",
              target: "0192f000-0000-7000-8000-00000000aaaa",
              targetCommit: made.written.oid,
              reason: "leaked",
            }).replace('"PLACEHOLDER"', '"event.redact\\u0065d"'),
          );
          assert.equal(new TextDecoder().decode(escaped).includes('"event.redacted"'), false);
          yield* Trace.append({
            session: SESSION,
            type: "event.redacted",
            id: base.id,
            payload: escaped,
            key: where.agent,
          });
          void repository;

          // Byte-searched, the removal was skipped: `gc` never collecting the
          // payload or the render while the audit reported the removal done.
          assert.equal((yield* Redaction.excluded()).has(made.render), true);
        }),
      ),
    ),
  );

  it.effect("says a session was emptied rather than never opened", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const where = yield* enabled();
          yield* Session.open({
            repo: where.genesis.repoId,
            session: SESSION,
            agent: { kind: "assistant", model: "opus", harness: "cli" },
            prompt: "start",
            key: where.agent,
          });
          const asked = yield* Session.ask({
            repo: where.genesis.repoId,
            session: SESSION,
            question: "the deploy key is hunter2",
            key: where.agent,
          });
          void asked;

          // Every payload gone, which is what a partially replicated session
          // looks like — and what a collection leaves behind once a removal
          // has been honoured. A redaction alone cannot produce it: the
          // tombstone is itself a readable event, so `exists` stays true.
          const repository = yield* Repository;
          const store = yield* ObjectStore;
          const walk = yield* Session.entries(SESSION);
          for (const entry of walk.events) {
            const info = yield* repository.readCommit(entry.commit);
            const payload = yield* repository.findPath(info.tree, "event.json");
            if (payload !== null) yield* store.delete(payload.oid);
          }

          // `exists` is "this walk read at least one event", so a session whose
          // payloads this replica cannot read says the same thing as one that
          // was never opened — asserting no session record was ever written,
          // on precisely the run where an operator is checking what happened
          // to the ones that were.
          const projected = yield* Session.project(SESSION);
          assert.equal(projected.exists, false);
          assert.equal(projected.unreadable.length > 0, true);
          const rendered = Audit.renderAll(
            yield* Invocation.project({
              repo: where.genesis.repoId,
              session: SESSION,
              trust: yield* projectTrust(where.genesis),
            }),
            projected,
          ).join("\n");
          assert.doesNotMatch(rendered, /no session record/);
        }),
      ),
    ),
  );

  it.effect("claims an unreadable record that names no kind anywhere", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const where = yield* enabled();
          const mute = yield* Trace.append({
            session: SESSION,
            type: "",
            id: "",
            payload: new TextEncoder().encode(JSON.stringify({ foo: 1 })),
            key: where.agent,
          });

          // And its payload gone, which is what a collection leaves behind.
          // `Trace.walk` gives `type: null` for a blank first line, and both
          // readers seeded their `unreadable` from the message — so a record
          // commit landed by replication with an empty message and a payload
          // this replica cannot read belonged to no namespace, and
          // `context audit` exited 0 over it while `session show --audit` saw
          // it, because the projection unions the walk directly.
          const info = yield* repository.readCommit(mute);
          const gone = yield* repository.findPath(info.tree, "event.json");
          yield* (yield* ObjectStore).delete(gone!.oid);

          const read = yield* Records.entries(SESSION);
          assert.equal(read.unreadable.includes(mute), true);
        }),
      ),
    ),
  );

  it.effect("says when a record on this ref names another session", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const where = yield* enabled();
          const other = "0192f000-0000-7000-8000-0000000000de";
          const theirs = yield* Records.record(
            {
              ...(yield* Records.context(where.genesis.repoId, other)),
              type: Records.TOOL,
              invocation: null,
              capture: null,
              tool: { name: "Read" },
            },
            where.agent,
          );

          // Grafted onto this session's ref, which replication does not gate.
          // Dropped silently it appeared in no section of the audit at all —
          // while `Exposure.entries` reports the mirrored case, on the ground
          // that an audit surface which discards records in silence is the
          // other half of the problem it is trying to solve.
          const info = yield* repository.readCommit(theirs.commit);
          const grafted = yield* repository.commitTree({
            tree: info.tree,
            parents: [],
            message: info.message,
            author: info.author,
          });
          yield* repository.setRef({ name: Trace.refOf(SESSION), to: grafted });

          const projected = yield* Invocation.project({
            repo: where.genesis.repoId,
            session: SESSION,
            trust: yield* projectTrust(where.genesis),
          });
          assert.deepEqual([...projected.foreign], [grafted]);
          assert.match(Audit.renderAll(projected).join("\n"), /name another session/);

          // And the repository half. A record naming *this* session and
          // another repository — which a member holding `hub.trace` in two of
          // them can land, since the boundary does not read payload contents.
          // `Invocation.project` was filtering these with a bare filter whose
          // rejects went nowhere, so it was in no section of the audit at all
          // while the identical mismatch on the session was named.
          const replayed = yield* Records.record(
            {
              ...(yield* Records.context(where.genesis.repoId, SESSION)),
              repo: "SHA256:somewhere-else",
              type: Records.TOOL,
              invocation: null,
              capture: null,
              tool: { name: "Write" },
            },
            where.agent,
          );

          const again = yield* Invocation.project({
            repo: where.genesis.repoId,
            session: SESSION,
            trust: yield* projectTrust(where.genesis),
          });
          assert.equal(again.foreign.includes(replayed.commit), true);
          assert.deepEqual(again.tools, []);
        }),
      ),
    ),
  );

  it.effect("does not take a record another session's ref carried", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const where = yield* enabled();
          const other = "0192f000-0000-7000-8000-0000000000dd";

          // A clean health record, validly signed, for a different session.
          yield* Records.record(
            {
              ...(yield* Records.context(where.genesis.repoId, other)),
              type: Records.HEALTH,
              source: "otel",
              stage: "sdk-export",
              sampling: "none",
              transformed: false,
              dropped: 0,
            },
            where.agent,
          );

          // Grafted onto this session's ref, which replication does not gate.
          // Nothing on the read path checked the binding: `check` looks at
          // timestamps, counts and oid shapes, `entries` returns whatever
          // decodes off the ref, and `verified` asks only for a good signature
          // from a `hub.trace` holder. So this session's coverage flipped from
          // `unknown` to `complete` on a claim nobody made for it.
          const head = yield* repository.resolve(Trace.refOf(other));
          const info = yield* repository.readCommit(head!);
          const grafted = yield* repository.commitTree({
            tree: info.tree,
            parents: [],
            message: info.message,
            author: info.author,
          });
          yield* repository.setRef({ name: Trace.refOf(SESSION), to: grafted });

          const projected = yield* Invocation.project({
            repo: where.genesis.repoId,
            session: SESSION,
            trust: yield* projectTrust(where.genesis),
          });
          assert.equal(projected.coverage, "unknown");
        }),
      ),
    ),
  );

  it.effect("does not read complete over a health record it could not judge", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const where = yield* enabled();
          const clean = {
            type: Records.HEALTH,
            source: "otel",
            stage: "sdk-export",
            sampling: "none",
            transformed: false,
            dropped: 0,
          } as const;
          yield* Records.record(
            { ...(yield* Records.context(where.genesis.repoId, SESSION)), ...clean },
            where.agent,
          );

          const trust = yield* projectTrust(where.genesis);
          const alone = yield* Invocation.project({
            repo: where.genesis.repoId,
            session: SESSION,
            trust,
          });
          assert.equal(alone.coverage, "complete");

          // And now one this membership cannot vouch for. Dropped outright, it
          // left the trusted record carrying the claim alone — so a session
          // whose own ref holds a signed statement from another stage that
          // records were lost read as `complete`, which §12 forbids. It is
          // remembered instead: kept out of the strictest-wins fold, and
          // enough to stop the answer being `complete`.
          const stranger = yield* generate("stranger@example.com");
          yield* Records.record(
            {
              ...(yield* Records.context(where.genesis.repoId, SESSION)),
              ...clean,
              stage: "local-collector",
              sampling: "parentbased_traceidratio",
              transformed: true,
              dropped: 12,
            },
            stranger,
          );

          const doubted = yield* Invocation.project({
            repo: where.genesis.repoId,
            session: SESSION,
            trust,
          });
          assert.equal(doubted.coverage, "unknown");
          assert.equal(doubted.unjudged.length, 1);
          assert.equal(doubted.health.length, 1);

          // And it is *said*. Kept out of the fold but printed by nothing, a
          // health record signed by a key that never held `hub.trace` was
          // invisible — a session with one read exactly like a session whose
          // capture path reported nothing, which is the distinction the field
          // was added to make.
          assert.match(
            Audit.renderAll(doubted).join("\n"),
            /capture health claimed by an unverified signer/,
          );
        }),
      ),
    ),
  );

  it.effect("scans every free-text field an invocation carries", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const where = yield* enabled();
          const base = {
            ...(yield* Records.context(where.genesis.repoId, SESSION)),
            type: Records.INVOCATION,
            exposure: null,
            capture: null,
            operation: { name: "chat" },
          } as const;
          const token = `ghp_${"A".repeat(36)}`;

          // `attempts[].errorType` is the same field of the same class one
          // level down from `outcome.errorType`, and `usage.estimator` is a
          // bare writer-supplied string `check` *requires* when usage is
          // estimated. `trace record --event` feeds caller JSON straight
          // through, so a token in either was signed onto an append-only,
          // replicating ref while the same bytes in `outcome.errorType` were
          // refused.
          const attempt = yield* Records.record(
            { ...base, attempts: [{ index: 1, status: "error", errorType: token }] },
            where.agent,
          ).pipe(Effect.flip);
          assert.equal(attempt._tag, "Invalid");
          assert.match(JSON.stringify(attempt), /looks like it carries/);

          const estimator = yield* Records.record(
            { ...base, usage: { source: "estimated", estimator: token } },
            where.agent,
          ).pipe(Effect.flip);
          assert.equal(estimator._tag, "Invalid");
          assert.match(JSON.stringify(estimator), /looks like it carries/);

          // And the capture's transport, which is the same class of field as
          // `TraceHealth`'s `source` and `sampling` — unbounded,
          // writer-supplied, no vocabulary, and fed straight through by
          // `trace record --event`, since `checkCapture` validates only
          // `stage`.
          const transport = yield* Records.record(
            { ...base, capture: { transport: `otlp ${token}` } },
            where.agent,
          ).pipe(Effect.flip);
          assert.equal(transport._tag, "Invalid");
          assert.match(JSON.stringify(transport), /looks like it carries/);

          // And every other writer-supplied string, because the list of them
          // was wrong four times over. `tool.name` is the likely one: a
          // harness recording the invoked command line as the tool name puts
          // the whole command, headers included, on a ref this version cannot
          // rewind. `response.finishReasons` is the clearest inconsistency —
          // unbounded writer strings, structurally identical to
          // `TraceHealth.reasons`, which was already scanned.
          for (const carrier of [
            { operation: { name: token } },
            { agent: { name: token } },
            { conversation: { externalId: token } },
            { model: { provider: token } },
            { response: { finishReasons: ["stop", token] } },
          ]) {
            const refused = yield* Records.record({ ...base, ...carrier }, where.agent).pipe(
              Effect.flip,
            );
            assert.equal(refused._tag, "Invalid", JSON.stringify(carrier));
          }

          // And the nested envelope names, which the filter was stripping at
          // every depth: `agent.id` and `agent.version` are writer-supplied
          // strings named exactly like two envelope fields, so a token in
          // `agent.name` was refused and the same token in `agent.id` signed.
          for (const carrier of [{ agent: { id: token } }, { agent: { version: token } }]) {
            const refused = yield* Records.record({ ...base, ...carrier }, where.agent).pipe(
              Effect.flip,
            );
            assert.equal(refused._tag, "Invalid", JSON.stringify(carrier));
          }

          const named = yield* Records.record(
            {
              ...(yield* Records.context(where.genesis.repoId, SESSION)),
              type: Records.TOOL,
              invocation: null,
              capture: null,
              tool: { name: `curl -H "authorization: ${token}"` },
            },
            where.agent,
          ).pipe(Effect.flip);
          assert.equal(named._tag, "Invalid");
        }),
      ),
    ),
  );

  it.effect("counts one removal once however many tombstones name it", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const where = yield* enabled();
          const made = yield* exposed(where.genesis, where.agent, SESSION, "only this one");
          const removal = {
            repo: where.genesis.repoId,
            session: SESSION,
            target: made.written.oid,
            reason: "leaked",
            key: where.agent,
          };
          yield* Records.redact(removal);

          // `redact` permits a second tombstone for a record already
          // unreadable, on purpose: a replica whose payload another host
          // collected still needs to write the local one that explains the
          // absence. So two tombstones for one removal are ordinary, and
          // `N record(s) removed by a signed redaction` counted them as two
          // removals. Every other consumer already wraps this in a `Set`.
          yield* Records.redact({ ...removal, reason: "again" });

          const projected = yield* Invocation.project({
            repo: where.genesis.repoId,
            session: SESSION,
            trust: yield* projectTrust(where.genesis),
          });
          assert.equal(projected.redacted.length, 1);
        }),
      ),
    ),
  );

  it.effect("keeps honouring a session's tombstone when a trace ref will not walk", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const where = yield* enabled();
          const session = "0192f000-0000-7000-8000-0000000000dd";

          // A session record carrying something the operator wants gone, in a
          // namespace with no `context/` half at all.
          yield* Session.open({
            repo: where.genesis.repoId,
            session,
            agent: { kind: "assistant", model: "opus", harness: "cli" },
            prompt: "start",
            key: where.agent,
          });
          const asked = yield* Session.ask({
            repo: where.genesis.repoId,
            session,
            question: "the deploy key is hunter2",
            key: where.agent,
          });
          const head = yield* repository.resolve(Session.refOf(session));
          const payload = yield* repository.findPath(
            (yield* repository.readCommit(head!)).tree,
            "event.json",
          );
          yield* Session.redact({
            repo: where.genesis.repoId,
            session,
            target: asked,
            reason: "sensitive-content",
            key: where.agent,
          });

          // And a trace ref this host cannot walk, which has nothing to do
          // with that session. Gating the payloads on the same flag as the
          // shared `context/` blobs made this ref stop every session and task
          // redaction in the repository from being honoured — silently, on
          // every collection after, while `trace redact` and `session redact`
          // went on reporting success.
          const other = "0192f000-0000-7000-8000-0000000000ee";
          const made = yield* exposed(where.genesis, where.agent, other);
          const store = yield* ObjectStore;
          yield* store.delete((yield* repository.readCommit(made.written.commit)).tree);

          const narrowed = yield* Redaction.excluded();
          assert.equal(narrowed.has(payload!.oid), true);
        }),
      ),
    ),
  );

  it.effect("says a trace ref cannot be read rather than that it is empty", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const where = yield* enabled();
          const made = yield* exposed(where.genesis, where.agent);

          // The head is here; its tree is not — the partial-replication state
          // `isHubCommit` is written for. `Dag.reachable` drops a head its
          // predicate rejects, so the walk came back empty with no error and
          // the session read as having no records at all: `session show
          // --audit` said "No invocations recorded" for a session that has
          // them, and `trace redact` could not name the record to remove.
          const store = yield* ObjectStore;
          yield* store.delete((yield* repository.readCommit(made.written.commit)).tree);

          const walked = yield* Effect.result(Trace.walk(SESSION));
          assert.equal(walked._tag, "Failure");

          const refused = yield* Effect.result(
            Records.redact({
              repo: where.genesis.repoId,
              session: SESSION,
              target: made.written.oid,
              reason: "leaked",
              key: where.agent,
            }),
          );
          assert.equal(refused._tag, "Failure");
        }),
      ),
    ),
  );

  it.effect("will not let a key without hub.redact write a tombstone by hand", () =>
    Effect.promise(() =>
      scenario(
        Effect.gen(function* () {
          const where = yield* enabled();
          const writer = yield* generate("writer@example.com");
          const made = yield* exposed(where.genesis, where.agent);

          // The shape `trace record --event` would feed straight through. The
          // capability is charged at `record`, the door every writer comes
          // through, rather than at `redact` alone — otherwise this wrote a
          // tombstone and left the ref unpushable for good, since the boundary
          // charges `hub.redact` for any record whose bytes claim the tag.
          const refused = yield* Effect.result(
            Records.record(
              {
                ...(yield* Records.context(where.genesis.repoId, SESSION)),
                type: Records.REDACTED,
                target: "whatever",
                targetCommit: made.written.oid,
                reason: "by hand",
              },
              writer,
            ),
          );
          assert.equal(refused._tag, "Failure");
        }),
      ),
    ),
  );
});
