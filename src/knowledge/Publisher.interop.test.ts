import assert from "node:assert/strict";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { describe, it } from "@effect/vitest";
import { Effect, Layer } from "effect";
import * as Node from "../git/Node.ts";
import * as GitRepository from "../git/Repository.ts";
import { Repository } from "../git/Repository.ts";
import { knowledgeFixture, draftPath } from "../testing/Knowledge.ts";
import { gitIn, hasGit } from "../testing/Git.ts";
import * as Proposal from "./Proposal.ts";
import * as Publisher from "./Publisher.ts";

describe.skipIf(!hasGit)("generated knowledge history with stock Git", () => {
  for (const scenario of ["fetch", "destination-race", "source-movement"] as const) {
    it.live(scenario, () =>
      Effect.promise(async () => {
        const root = await fs.mkdtemp(path.join(os.tmpdir(), "knowledge-interop-"));
        const directory = path.join(root, "source");
        try {
          const repository = await Effect.runPromise(
            Repository.pipe(
              Effect.provide(
                GitRepository.layer.pipe(
                  Layer.provide(GitRepository.hooksNoop),
                  Layer.provide(Node.stores(directory)),
                ),
              ),
            ),
          );
          const execute = <A, E>(effect: Effect.Effect<A, E, Repository>) =>
            Effect.runPromise(effect.pipe(Effect.provideService(Repository, repository)));
          const fixture = await execute(knowledgeFixture(true));
          await execute(repository.setHead("refs/heads/main"));
          const state = await execute(
            Publisher.prepare({
              evidence: fixture.evidence,
              output: fixture.output,
              runner: { id: "fixture", version: "1" },
              roots: [Proposal.ROOT],
              maximumBytes: 1048576,
            }),
          );
          assert.ok(state);
          let raced = false;
          const service = Repository.of({
            ...repository,
            setRef: (input) =>
              Effect.gen(function* () {
                if (scenario === "destination-race" && input.name === state.ref && !raced) {
                  raced = true;
                  yield* repository.setRef({ name: state.ref, to: fixture.source, expected: null });
                }
                return yield* repository.setRef(input);
              }),
          });
          const result = await Effect.runPromise(
            Publisher.publish({
              state,
              key: fixture.root,
              actors: fixture.evidence.actors,
              roots: [Proposal.ROOT],
              maximumBytes: 1048576,
              checkpoint: (next) =>
                Effect.gen(function* () {
                  if (
                    scenario === "source-movement" &&
                    next.phase === "report-attested" &&
                    !raced
                  ) {
                    raced = true;
                    yield* repository.setRef({
                      name: "refs/heads/main",
                      to:
                        (yield* repository.readCommit(fixture.source)).parents[0] ?? fixture.source,
                      expected: fixture.source,
                    });
                  }
                }).pipe(Effect.orDie),
            }).pipe(Effect.provideService(Repository, service)),
          );
          if (scenario === "destination-race") {
            assert.equal(result.state, "conflict");
            assert.equal(await execute(repository.readRef(state.ref)), fixture.source);
            return;
          }
          if (scenario === "source-movement") {
            assert.equal(result.state, "superseded-input");
            assert.equal(await execute(repository.readRef(state.ref)), null);
            return;
          }
          assert.equal(result.state, "written");
          assert.ok("commit" in result);
          const git = gitIn(root);
          git(`--git-dir=${directory}`, "fsck", "--strict", "--no-dangling");
          assert.match(
            git(`--git-dir=${directory}`, "show", `${result.commit}:${draftPath}`),
            /status: draft/u,
          );
          const clone = path.join(root, "fetched.git");
          git("init", "--bare", clone);
          git(`--git-dir=${clone}`, "fetch", directory, "+refs/*:refs/*");
          git(`--git-dir=${clone}`, "fsck", "--strict", "--no-dangling");
          assert.equal(git(`--git-dir=${clone}`, "rev-parse", state.ref).trim(), result.commit);
          assert.equal(
            git(
              `--git-dir=${clone}`,
              "rev-list",
              "--count",
              `${fixture.source}..${result.commit}`,
            ).trim(),
            "2",
          );
          assert.equal(
            git(`--git-dir=${clone}`, "show", `${result.commit}:${draftPath}`),
            git(`--git-dir=${directory}`, "show", `${result.commit}:${draftPath}`),
          );
        } finally {
          await fs.rm(root, { recursive: true, force: true });
        }
      }),
    );
  }
});
