/**
 * Rename following, against the binary whose answer it claims to agree with.
 *
 * `NoteAudit` follows a vanished path to a new one at git's own default
 * similarity threshold, and the value of that claim is entirely in whether it
 * matches. So both halves are asked of `git` first — this pair it calls a
 * rename, that pair it does not — and the audit is held to the same two
 * answers on the same two repositories.
 */
import assert from "node:assert/strict";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { afterEach, beforeEach, describe, it } from "@effect/vitest";

import { Effect, Layer } from "effect";

import * as GitRepository from "../git/Repository.ts";
import { stores as nodeStores } from "../git/Node.ts";
import { gitIn, hasGit } from "../testing/Git.ts";
import { workspace } from "../git/Work.node.ts";
import { syntax as anchorSyntax } from "./Anchor.syntax.ts";
import { AnchorResolver } from "./Anchor.ts";
import * as Audit from "./NoteAudit.ts";
import type { Projection } from "./NoteProjection.ts";

const VERIFY = `export function verify(token: string): boolean {
  let same = 0
  for (const at of token) same += at.charCodeAt(0)
  return same > 0
}
`;

describe.skipIf(!hasGit)("following a rename, against git", () => {
  let root: string;
  const git = (...args: string[]) => gitIn(root)(...args);

  beforeEach(async () => {
    root = await fs.mkdtemp(path.join(os.tmpdir(), "note-rename-"));
    git("init", "--quiet", "--initial-branch=main");
  });

  afterEach(async () => {
    await fs.rm(root, { recursive: true, force: true });
  });

  const write = async (file: string, content: string) => {
    await fs.mkdir(path.join(root, path.dirname(file)), { recursive: true });
    await fs.writeFile(path.join(root, file), content);
  };

  /** `git diff -M --name-status`, which is where the threshold this agrees with lives. */
  const renamesGitSees = (): ReadonlyArray<string> =>
    git("diff", "-M", "--name-status", "HEAD~1", "HEAD")
      .split("\n")
      .filter((line) => line.startsWith("R"));

  const noteOn = (file: string): Projection => ({
    state: "ready",
    id: "01991f71-b8d7-7def-82d8-0c30c58ae122",
    path: file,
    anchor: "function verify",
    text: "comparison must remain constant-time",
    createdAt: "2026-01-01T00:00:00.000Z",
    createdBy: "SHA256:author",
    updatedAt: "2026-01-01T00:00:00.000Z",
    updatedBy: "SHA256:author",
    active: true,
    pinned: false,
    baseline: null,
  });

  /** The audit reading the same checkout git just wrote. */
  const audited = (file: string) =>
    Effect.runPromise(
      Effect.gen(function* () {
        return yield* Audit.audit(noteOn(file), yield* Audit.workTree());
      }).pipe(
        Effect.provide(
          Layer.mergeAll(
            GitRepository.layer.pipe(Layer.provide(GitRepository.hooksNoop)),
            workspace(root),
            anchorSyntax,
          ).pipe(Layer.provideMerge(nodeStores(path.join(root, ".git")))),
        ),
      ),
    );

  it("follows exactly the move git reports as a rename", async () => {
    await write("src/auth.ts", VERIFY);
    git("add", "-A");
    git("commit", "--quiet", "-m", "before");

    await fs.mkdir(path.join(root, "src/security"), { recursive: true });
    git("mv", "src/auth.ts", "src/security/auth.ts");
    git("commit", "--quiet", "-m", "move");

    assert.deepEqual(renamesGitSees(), ["R100\tsrc/auth.ts\tsrc/security/auth.ts"]);

    const result = await audited("src/auth.ts");
    assert.equal(result.pathMovedFrom, "src/auth.ts");
    assert.equal(result.path, "src/security/auth.ts");
    // Followed *and* still checked: the constraint is being asked about the
    // source at the new path, not merely reported as relocated.
    assert.equal(result.status, "fresh");
  });

  it("refuses to follow a replacement git does not call a rename", async () => {
    await write("src/auth.ts", VERIFY);
    git("add", "-A");
    git("commit", "--quiet", "-m", "before");

    await fs.rm(path.join(root, "src/auth.ts"));
    await write(
      "src/security/auth.ts",
      "export function verify(): void {\n  throw new Error('rewritten from nothing in common')\n}\n",
    );
    git("add", "-A");
    git("commit", "--quiet", "-m", "rewrite");

    assert.deepEqual(renamesGitSees(), []);
    assert.equal((await audited("src/auth.ts")).status, "source-missing");
  });

  it("resolves the anchors of a file git has just checked out", async () => {
    await write("src/auth.ts", VERIFY);
    git("add", "-A");
    git("commit", "--quiet", "-m", "before");

    const found = await Effect.runPromise(
      Effect.gen(function* () {
        const source = yield* (yield* Audit.workTree()).read("src/auth.ts");
        assert.ok(source !== null);
        return yield* (yield* AnchorResolver).anchors("src/auth.ts", source);
      }).pipe(
        Effect.provide(
          Layer.mergeAll(
            GitRepository.layer.pipe(Layer.provide(GitRepository.hooksNoop)),
            workspace(root),
            anchorSyntax,
          ).pipe(Layer.provideMerge(nodeStores(path.join(root, ".git")))),
        ),
      ),
    );
    assert.deepEqual(
      found.map((anchor) => anchor.value),
      ["@file", "function verify"],
    );
  });
});
