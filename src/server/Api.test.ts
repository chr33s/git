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
import { Etag, HttpPlatform } from "effect/unstable/http";
import { HttpApiTest } from "effect/unstable/httpapi";

import { stores } from "../git/Memory.ts";
import * as GitRepository from "../git/Repository.ts";
import * as Api from "./Api.ts";
import * as Subscribers from "./Subscribers.ts";

const repository = GitRepository.layer.pipe(
  Layer.provide(GitRepository.hooksNoop),
  Layer.provide(stores),
);

const live = Layer.mergeAll(
  Api.handlers,
  HttpPlatform.layer.pipe(Layer.provide(FileSystem.layerNoop({}))),
  Etag.layerWeak,
  FileSystem.layerNoop({}),
  Path.layer,
).pipe(Layer.provideMerge(repository), Layer.provideMerge(Subscribers.memory));

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
  it.live(
    "drives the derived client end to end, typed errors included",
    () =>
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

        const second = yield* client.repo.create({
          params: { repo: "r" },
          payload: { message: "second", author: alice },
        });
        const log = yield* client.repo.log({ params: { repo: "r", oid: second.oid } });
        assert.deepEqual(
          log.commits.map((commit) => commit.message),
          ["second", "first"],
        );

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
        // `HttpApiTest`'s client carries the router's request-scoped
        // requirement, which the handlers layer satisfies at dispatch time but
        // the type cannot see discharged here.
      }).pipe(Effect.scoped, Effect.provide(live)) as Effect.Effect<void> as Effect.Effect<void>,
  );

  it.live(
    "commits real content, and reads it back",
    () =>
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
      }).pipe(Effect.scoped, Effect.provide(live)) as Effect.Effect<void> as Effect.Effect<void>,
  );

  it.live(
    "tags, annotated and lightweight, and checks its own integrity",
    () =>
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
      }).pipe(Effect.scoped, Effect.provide(live)) as Effect.Effect<void> as Effect.Effect<void>,
  );

  it.live(
    "registers, lists and removes webhooks without ever echoing the secret",
    () =>
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
      }).pipe(Effect.scoped, Effect.provide(live)) as Effect.Effect<void> as Effect.Effect<void>,
  );
});
