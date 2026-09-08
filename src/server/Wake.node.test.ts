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
import * as net from "node:net";
import { describe, it } from "@effect/vitest";

import { Effect, Fiber, Layer, Predicate } from "effect";

import { EMPTY_TREE_OID } from "../git/Format.ts";
import { stores } from "../git/Node.ts";
import * as GitRepository from "../git/Repository.ts";
import * as Event from "../hub/Event.ts";
import * as PullRequest from "../hub/PullRequest.ts";
import * as Queue from "../hub/Queue.ts";
import * as Task from "../hub/Task.ts";
import { enableHub } from "../testing/Hub.ts";
import * as Wake from "./Wake.node.ts";

const author = {
  name: "Dev",
  email: "dev@example.com",
  at: new Date(1_700_000_000_000),
  offset: 0,
};

describe("Wake", () => {
  it.live("does not replay cursor ancestors reached through a newly merged branch", () =>
    Effect.promise(async () => {
      const root = await fs.mkdtemp(path.join(os.tmpdir(), "wake-join-"));
      try {
        const fixture = await enableHub(root, ["hub.create-pr", "hub.comment"]);
        const layer = GitRepository.layer.pipe(
          Layer.provide(GitRepository.hooksNoop),
          Layer.provide(stores(root)),
        );
        await fs.writeFile(
          path.join(root, "wake.json"),
          JSON.stringify({
            rules: [
              {
                ref: "refs/hub/pr/*",
                on: ["*"],
                run: [process.execPath, "-e", ""],
              },
            ],
          }),
        );
        await Effect.runPromise(
          Effect.gen(function* () {
            const repository = yield* GitRepository.Repository;
            const head = yield* repository.commit({
              branch: "main",
              tree: EMPTY_TREE_OID,
              message: "base",
              author,
            });
            const opened = yield* PullRequest.open({
              repo: fixture.repoId,
              title: "wake",
              base: "refs/heads/main",
              head,
              key: fixture.member,
            });
            const ref = Event.refOf(opened.pr);
            const initial = yield* repository.resolve(ref);
            assert.ok(initial !== null);
            const comment = (body: string) =>
              PullRequest.comment({
                repo: fixture.repoId,
                pr: opened.pr,
                body,
                key: fixture.member,
              });
            yield* comment("first side");
            const left = yield* repository.resolve(ref);
            assert.ok(left !== null);
            // Build the second replica's side from the same opening, then
            // restore the local side before its initial wake pass.
            yield* repository.setRef({ name: ref, to: initial });
            yield* comment("second side");
            const right = yield* repository.resolve(ref);
            assert.ok(right !== null);
            yield* repository.setRef({ name: ref, to: left });
            assert.equal((yield* Wake.dispatch({ directory: root, repo: "fixture" })).fired, 2);
            yield* Event.join(opened.pr, [left, right]);
            const joined = yield* Wake.dispatch({ directory: root, repo: "fixture" });
            assert.deepEqual(
              joined,
              { fired: 1, failed: 0 },
              "only the newly arrived comment wakes",
            );
            assert.equal((yield* Wake.dispatch({ directory: root, repo: "fixture" })).fired, 0);
          }).pipe(Effect.provide(layer)),
        );
      } finally {
        await fs.rm(root, { recursive: true, force: true });
      }
    }),
  );

  for (const wrapped of [false, true]) {
    it.live(
      `stops ${wrapped ? "a rule's subprocess" : "a running rule"} when dispatch is interrupted`,
      () =>
        Effect.promise(async () => {
          const root = await fs.mkdtemp(path.join(os.tmpdir(), "wake-interrupt-"));
          const started = Promise.withResolvers<ReadonlyArray<number>>();
          const sockets = new Set<net.Socket>();
          const disconnected = Promise.withResolvers<void>();
          const server = net.createServer((socket) => {
            sockets.add(socket);
            socket.once("data", (bytes) =>
              started.resolve(bytes.toString().split(",").map(Number)),
            );
            socket.once("close", () => disconnected.resolve());
            socket.on("close", () => sockets.delete(socket));
          });
          await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
          const address = server.address();
          assert.ok(address !== null && !Predicate.isString(address));
          let pids: ReadonlyArray<number> = [];
          try {
            const fixture = await enableHub(root, ["hub.task"]);
            const layer = GitRepository.layer.pipe(
              Layer.provide(GitRepository.hooksNoop),
              Layer.provide(stores(root)),
            );
            await Effect.runPromise(
              Task.open({ repo: fixture.repoId, title: "wake", key: fixture.member }).pipe(
                Effect.provide(layer),
              ),
            );
            const worker =
              "const socket = require('node:net').connect({ host: '127.0.0.1', port: Number(process.argv[1]) }, () => socket.write([process.pid, process.ppid].join(','))); setInterval(() => {}, 1000);";
            const command = wrapped
              ? `require('node:child_process').spawn(process.execPath, ['-e', ${JSON.stringify(worker)}, process.argv[1]], { stdio: 'inherit' }); setInterval(() => {}, 1000);`
              : worker;
            await fs.writeFile(
              path.join(root, "wake.json"),
              JSON.stringify({
                rules: [
                  {
                    ref: "refs/hub/task/*",
                    on: ["task.opened"],
                    run: [process.execPath, "-e", command, String(address.port)],
                  },
                ],
              }),
            );
            const dispatch = Effect.runFork(
              Wake.dispatch({ directory: root, repo: "fixture" }).pipe(Effect.provide(layer)),
            );
            const [workerPid, parentPid] = await started.promise;
            assert.ok(workerPid !== undefined && workerPid > 0);
            assert.ok(parentPid !== undefined && parentPid > 0);
            pids = wrapped ? [workerPid, parentPid] : [workerPid];
            await Effect.runPromise(Fiber.interrupt(dispatch));
            await assert.rejects(fs.stat(path.join(root, "wake.cursor.json.lock")), {
              code: "ENOENT",
            });
            assert.throws(
              () => process.kill(wrapped ? parentPid : workerPid, 0),
              { code: "ESRCH" },
              "the rule has exited before interruption finishes",
            );
            // The socket belongs to the worker, so a wrapper exiting cannot make
            // this pass while its child continues doing work. Orphaned children
            // may briefly remain zombies on Linux, making kill(pid, 0) misleading.
            await Effect.runPromise(
              Effect.promise(() => disconnected.promise).pipe(Effect.timeout("2 seconds")),
            );
            assert.equal(
              await fs.stat(path.join(root, "wake.cursor.json")).then(
                () => true,
                () => false,
              ),
              false,
            );
          } finally {
            for (const pid of pids) {
              try {
                process.kill(pid, "SIGKILL");
              } catch {
                /* already stopped */
              }
            }
            for (const socket of sockets) socket.destroy();
            await new Promise<void>((resolve) => server.close(() => resolve()));
            await fs.rm(root, { recursive: true, force: true });
          }
        }),
    );
  }

  it.effect("keeps the bookmarks a pass earned when another ref cannot be walked", () =>
    Effect.promise(async () => {
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
    }),
  );

  it.effect("wakes for every namespace under refs/hub, not only pull requests", () =>
    Effect.promise(async () => {
      // agents.md §20's working rhythm is `task.opened → hooks wake the fleet`,
      // and docs/queue.md's is `queue.entered → the runner builds`. Both were
      // silently impossible: the walk decoded every record as a pull-request
      // payload, so a task or queue event read as one this version "cannot
      // read" and its rule never fired. What a rule matches on is the type, and
      // every hub envelope spells that the same way.
      const root = await fs.mkdtemp(path.join(os.tmpdir(), "wake-namespaces-"));
      const project = path.join(root, "project");
      await fs.mkdir(project, { recursive: true });

      try {
        const fixture = await enableHub(project, ["hub.task", "hub.queue"]);
        const layer = GitRepository.layer.pipe(
          Layer.provide(GitRepository.hooksNoop),
          Layer.provide(stores(project)),
        );

        const built = await Effect.runPromise(
          Effect.gen(function* () {
            const opened = yield* Task.open({
              repo: fixture.repoId,
              title: "something to do",
              key: fixture.member,
            });
            const queue = yield* Queue.open({
              repo: fixture.repoId,
              target: "refs/heads/main",
              key: fixture.member,
            });
            return { task: opened.task, queue: queue.queue };
          }).pipe(Effect.provide(layer)),
        );

        await fs.writeFile(
          path.join(project, "wake.json"),
          JSON.stringify({
            rules: [
              { ref: "refs/hub/task/*", on: ["task.opened"], run: [process.execPath, "-e", ""] },
              { ref: "refs/hub/queue/*", on: ["queue.opened"], run: [process.execPath, "-e", ""] },
            ],
          }),
        );

        const summary = await Effect.runPromise(
          Wake.dispatch({ directory: project, repo: "project" }).pipe(Effect.provide(layer)),
        );

        assert.equal(summary.failed, 0);
        assert.equal(summary.fired, 2, "one rule per namespace, both matched by type");

        const bookmarks: Record<string, string> = JSON.parse(
          await fs.readFile(path.join(project, "wake.cursor.json"), "utf8"),
        );
        assert.ok(bookmarks[`refs/hub/task/${built.task}`] !== undefined);
        assert.ok(bookmarks[`refs/hub/queue/${built.queue}`] !== undefined);
      } finally {
        await fs.rm(root, { recursive: true, force: true });
      }
    }),
  );
});
