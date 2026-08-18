/**
 * The wake pass itself, where its bounds can be set.
 *
 * The CLI suite drives the whole thing as an operator does; this one exists
 * for the states an operator cannot easily reach from a terminal — a ref whose
 * history is larger than the ceiling the fold is held to, which is what a walk
 * failing actually looks like.
 */
import assert from "node:assert/strict";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { describe, it } from "@effect/vitest";

import { Effect, Layer } from "effect";

import { EMPTY_TREE_OID } from "../git/Format.ts";
import { stores } from "../git/Node.ts";
import * as GitRepository from "../git/Repository.ts";
import * as Event from "../hub/Event.ts";
import * as PullRequest from "../hub/PullRequest.ts";
import { enableHub } from "../testing/Hub.ts";
import * as Wake from "./Wake.node.ts";

const author = {
  name: "Dev",
  email: "dev@example.com",
  at: new Date(1_700_000_000_000),
  offset: 0,
};

describe("Wake", () => {
  it("keeps the bookmarks a pass earned when another ref cannot be walked", async () => {
    const root = await fs.mkdtemp(path.join(os.tmpdir(), "wake-node-"));
    const project = path.join(root, "project");
    await fs.mkdir(project, { recursive: true });

    try {
      const fixture = await enableHub(project, ["hub.create-pr", "hub.comment"]);
      const layer = GitRepository.layer.pipe(
        Layer.provide(GitRepository.hooksNoop),
        Layer.provide(stores(project)),
      );

      const built = await Effect.runPromise(
        Effect.gen(function* () {
          const repository = yield* GitRepository.Repository;
          const head = yield* repository.commit({
            branch: "refs/heads/main",
            tree: EMPTY_TREE_OID,
            message: "first",
            author,
          });

          // One pull request the ceiling below admits …
          const small = yield* PullRequest.open({
            repo: fixture.repoId,
            title: "Small",
            base: "refs/heads/main",
            head,
            key: fixture.member,
          });

          // … and one it does not.
          const large = yield* PullRequest.open({
            repo: fixture.repoId,
            title: "Large",
            base: "refs/heads/main",
            head,
            key: fixture.member,
          });
          for (const body of ["one", "two", "three"]) {
            yield* PullRequest.comment({
              repo: fixture.repoId,
              pr: large.pr,
              body,
              key: fixture.member,
            });
          }
          return { small: small.pr, large: large.pr };
        }).pipe(Effect.provide(layer)),
      );

      await fs.writeFile(
        path.join(project, "wake.json"),
        JSON.stringify({
          rules: [{ ref: "refs/hub/pr/*", on: ["*"], run: [process.execPath, "-e", ""] }],
        }),
      );

      const summary = await Effect.runPromise(
        Wake.dispatch({ directory: project, repo: "project" }).pipe(
          // Two events is enough for the small pull request and short of the
          // large one, so one ref walks and one cannot.
          Effect.provide(Layer.merge(Event.ceiling(2), layer)),
        ),
      );

      const bookmarks: Record<string, string> = JSON.parse(
        await fs.readFile(path.join(project, "wake.cursor.json"), "utf8"),
      );

      // The point: the file is written once at the end of a pass, so a ref
      // that could not be walked used to take every advance the healthy refs
      // had already earned down with it — and those refs then re-fired their
      // rules on every wake from then on.
      assert.ok(
        bookmarks[`refs/hub/pr/${built.small}`] !== undefined,
        `the ref that walked must be bookmarked: ${JSON.stringify(bookmarks)}`,
      );
      assert.equal(
        bookmarks[`refs/hub/pr/${built.large}`],
        undefined,
        "and the one that did not must not be",
      );
      assert.ok(summary.failed > 0, "the failure is still reported");
    } finally {
      await fs.rm(root, { recursive: true, force: true });
    }
  });
});
