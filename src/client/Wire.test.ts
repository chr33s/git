/**
 * `pushObjects`, proved against the server we serve and the git binary.
 *
 * The objects are built with the `Format` encoders alone — no `Repository`,
 * which is the whole point of the wire path — and `git clone` afterwards is
 * the independent reader that says what arrived is what was meant.
 */
import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { promisify } from "node:util";
import { afterAll, beforeAll, describe, it } from "@effect/vitest";

import { Effect, Layer, Schema } from "effect";

import { Invalid } from "../git/Error.ts";
import { encodeCommit, encodeTree, hashObject } from "../git/Format.ts";
import { stores } from "../git/Node.ts";
import * as GitRepository from "../git/Repository.ts";
import { Repository } from "../git/Repository.ts";
import type { Oid, RawObject } from "../git/Store.ts";
import { serve, type Server } from "../host/Node.ts";
import { hasGit } from "../testing/Git.ts";
import { pushObjects } from "./Wire.ts";

const execFileAsync = promisify(execFile);
const encoder = new TextEncoder();

const signature = {
  name: "Bot",
  email: "bot@example.com",
  at: new Date(1_700_000_000_000),
  offset: 0,
};

let root: string;
let server: Server;

beforeAll(async () => {
  root = await fs.mkdtemp(path.join(os.tmpdir(), "client-wire-"));
  server = await serve({ root, allowAnonymousWrites: true });
});

afterAll(async () => {
  await server.close();
  await fs.rm(root, { recursive: true, force: true });
});

/** One commit holding one file, as the three objects a pack needs. */
const snapshot = Effect.fn(function* (content: string, parents: ReadonlyArray<Oid> = []) {
  const blob: RawObject = { type: "blob", data: encoder.encode(content) };
  const blobOid = yield* hashObject(blob);
  const tree: RawObject = {
    type: "tree",
    data: encodeTree([{ mode: "100644", name: "file.txt", oid: blobOid }]),
  };
  const treeOid = yield* hashObject(tree);
  const commit: RawObject = {
    type: "commit",
    data: encodeCommit({
      tree: treeOid,
      parents,
      author: signature,
      committer: signature,
      message: `${content}\n`,
    }),
  };
  return { oid: yield* hashObject(commit), objects: [blob, tree, commit] };
});

const serverRef = (repo: string, name: string) =>
  Effect.gen(function* () {
    return yield* (yield* Repository).resolve(name);
  }).pipe(
    Effect.provide(
      GitRepository.layer.pipe(
        Layer.provide(GitRepository.hooksNoop),
        Layer.provide(stores(path.join(root, repo))),
      ),
    ),
  );

describe("Wire.pushObjects", () => {
  it.effect("creates a ref from objects alone, then fast-forwards it", () =>
    Effect.gen(function* () {
      const url = `${server.url}/wire`;
      const first = yield* snapshot("one");
      const created = yield* pushObjects({ url, ref: "refs/heads/main", ...first, expected: null });
      assert.deepEqual(created, { ref: "refs/heads/main", ok: true });
      assert.equal(yield* serverRef("wire", "refs/heads/main"), first.oid);

      const second = yield* snapshot("two", [first.oid]);
      const moved = yield* pushObjects({
        url,
        ref: "refs/heads/main",
        ...second,
        expected: first.oid,
      });
      assert.deepEqual(moved, { ref: "refs/heads/main", ok: true });

      const again = yield* pushObjects({ url, ref: "refs/heads/main", ...second });
      assert.deepEqual(again, { ref: "refs/heads/main", ok: true, reason: "up to date" });

      if (!hasGit) return;
      const work = path.join(root, "work-wire");
      yield* Effect.promise(() =>
        execFileAsync("git", ["clone", "--quiet", url, work], {
          env: { ...process.env, GIT_TERMINAL_PROMPT: "0" },
        }),
      );
      assert.equal(
        yield* Effect.promise(() => fs.readFile(path.join(work, "file.txt"), "utf8")),
        "two",
      );
    }),
  );

  it.effect("refuses a stale lease before sending anything", () =>
    Effect.gen(function* () {
      const url = `${server.url}/lease`;
      const first = yield* snapshot("one");
      yield* pushObjects({ url, ref: "refs/heads/main", ...first });

      const other = yield* snapshot("other");
      const refused = yield* pushObjects({ url, ref: "refs/heads/main", ...other, expected: null });
      assert.deepEqual(refused, {
        ref: "refs/heads/main",
        ok: false,
        reason: "stale remote value",
      });
      assert.equal(yield* serverRef("lease", "refs/heads/main"), first.oid);
    }),
  );

  it.effect("refuses a short ref name rather than guessing its namespace", () =>
    Effect.gen(function* () {
      const { objects, oid } = yield* snapshot("short");
      const error = yield* pushObjects({
        url: `${server.url}/short`,
        ref: "releases/abc",
        oid,
        objects,
      }).pipe(Effect.flip);
      assert.ok(Schema.is(Invalid)(error));
      assert.equal(error.field, "ref");
    }),
  );

  it.effect("fails when the target object is not among the objects", () =>
    Effect.gen(function* () {
      const { objects, oid } = yield* snapshot("orphan");
      const error = yield* pushObjects({
        url: `${server.url}/missing`,
        ref: "refs/heads/main",
        oid,
        objects: objects.slice(0, 2),
      }).pipe(Effect.flip);
      assert.ok(Schema.is(Invalid)(error));
      assert.equal(error.field, "objects");
    }),
  );
});
