/**
 * The codecs against objects `git` itself wrote.
 *
 * A round-trip test that builds its own fixtures agrees with whatever the
 * codec happens to do — the interesting objects are the ones a real client
 * produces and this one never would: a commit whose message is Latin-1 under
 * an `encoding` header, a signed commit whose signature is a header spanning
 * forty lines, a tag whose signature sits in its message. Every one of those
 * came back from `parse` → `encode` as different bytes, which is a different
 * object id, which is a rebase quietly replacing what it was copying.
 */
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { afterEach, beforeEach, describe, it } from "@effect/vitest";

import { Effect, Layer, Result } from "effect";

import { gitIn, hasGit } from "../testing/Git.ts";
import { encodeCommit, encodeTag, parseCommit, parseTag } from "./Format.ts";
import { stores as nodeStores } from "./Node.ts";
import * as GitRepository from "./Repository.ts";
import { ObjectStore, type Oid } from "./Store.ts";

describe.skipIf(!hasGit)("the object codecs, against git", () => {
  let root: string;

  const git = (...args: string[]) => gitIn(root)(...args);

  /** The stored bytes of one object, read the way every caller reads them. */
  const bytesOf = (oid: Oid): Promise<Uint8Array> =>
    Effect.runPromise(
      Effect.gen(function* () {
        return (yield* (yield* ObjectStore).read(oid)).data;
      }).pipe(
        Effect.provide(
          GitRepository.layer.pipe(
            Layer.provide(GitRepository.hooksNoop),
            Layer.provideMerge(nodeStores(path.join(root, ".git"))),
          ),
        ),
      ),
    );

  /**
   * One commit, its message supplied as a *file*.
   *
   * `-F` rather than `-m`, because the message under test is bytes: passing
   * Latin-1 through a command line means passing it through this process's own
   * UTF-8 encoding, which is the very step being tested.
   */
  const commit = async (
    message: Uint8Array,
    /** `-c key=value` pairs, which git reads before the subcommand. */
    config: ReadonlyArray<string> = [],
    /** Flags for `commit` itself, which it reads after. */
    flags: ReadonlyArray<string> = [],
  ) => {
    execFileSync("sh", ["-c", "date > n.txt"], { cwd: root });
    git("add", "n.txt");
    const file = path.join(root, "message");
    await fs.writeFile(file, message);
    execFileSync(
      "git",
      [
        "-c",
        "user.name=T",
        "-c",
        "user.email=t@e.com",
        ...config,
        "commit",
        ...flags,
        "-q",
        "--cleanup=verbatim",
        "-F",
        file,
      ],
      {
        cwd: root,
        env: {
          ...process.env,
          GIT_AUTHOR_DATE: "1700000000 +0000",
          GIT_COMMITTER_DATE: "1700000000 +0000",
        },
      },
    );
    // SAFETY: `rev-parse HEAD` prints the forty hex characters of the commit
    // it just made, which is what the Oid brand names.
    return git("rev-parse", "HEAD").trim() as Oid;
  };

  beforeEach(async () => {
    root = await fs.mkdtemp(path.join(os.tmpdir(), "format-interop-"));
    git("init", "-q", "-b", "main");
  });

  afterEach(async () => {
    await fs.rm(root, { recursive: true, force: true });
  });

  it.effect("reproduces a commit whose message git stored as Latin-1", () =>
    Effect.promise(async () => {
      // `i18n.commitEncoding` is how a repository says its messages are not
      // UTF-8, and git records the fact in an `encoding` header and the message
      // in those bytes. Decoded and re-encoded, the message became U+FFFD and
      // the header vanished — so a replay wrote a commit that neither said what
      // the original said nor admitted which encoding it was in.
      // `café` with `é` as the single byte 0xe9, which is not a UTF-8 sequence.
      const oid = await commit(Uint8Array.from([0x63, 0x61, 0x66, 0xe9, 0x0a]), [
        "-c",
        "i18n.commitEncoding=ISO-8859-1",
      ]);
      const data = await bytesOf(oid);

      const parsed = parseCommit(data);
      assert.ok(Result.isSuccess(parsed), "git's own commit must parse");
      assert.deepEqual([...encodeCommit(parsed.success)], [...data]);

      assert.deepEqual(
        (parsed.success.headers ?? []).map((header) => header.name),
        ["encoding"],
        "the header git wrote has to survive the round trip",
      );
      // 0xe9 is `é` in Latin-1 and not a UTF-8 sequence at all, so this is the
      // case the decode cannot represent.
      assert.ok([...data].includes(0xe9), "the fixture must actually store the byte");
    }),
  );

  it.effect("reproduces a signed commit, signature header and all", () =>
    Effect.promise(async () => {
      // A signature is a header spanning as many lines as the armour needs,
      // each continuation beginning with a space. Split on newlines and
      // reassembled, it came back mangled; dropped, the commit came back
      // unsigned. Either way `git verify-commit` had nothing to verify.
      //
      // The signing program is a stub, and deliberately: what is under test is
      // git's *header folding*, not anybody's cryptography, and a test that
      // needed `ssh-keygen` on the machine would be a test that mostly skipped.
      // git assembles the header from whatever the program produces, exactly as
      // it does for a real key.
      const signer = path.join(root, "signer.sh");
      await fs.writeFile(
        signer,
        [
          "#!/bin/sh",
          // git calls `<program> -Y sign -n git -f <key> <payload>` and reads
          // `<payload>.sig` back.
          'for argument in "$@"; do last="$argument"; done',
          "printf -- '-----BEGIN SSH SIGNATURE-----\\nAAAAstub\\n\\nstub\\n-----END SSH SIGNATURE-----\\n' > \"$last.sig\"",
          "",
        ].join("\n"),
        { mode: 0o755 },
      );

      const oid = await commit(
        new TextEncoder().encode("signed\n"),
        [
          "-c",
          "gpg.format=ssh",
          "-c",
          `gpg.ssh.program=${signer}`,
          "-c",
          "user.signingkey=irrelevant",
        ],
        ["-S"],
      );
      const data = await bytesOf(oid);

      const parsed = parseCommit(data);
      assert.ok(Result.isSuccess(parsed), "a signed commit must parse");
      assert.deepEqual([...encodeCommit(parsed.success)], [...data]);
      assert.deepEqual(
        (parsed.success.headers ?? []).map((header) => header.name),
        ["gpgsig"],
      );
    }),
  );

  it.effect("reproduces an annotated tag, whose signature is in its message", () =>
    Effect.promise(async () => {
      const target = await commit(new TextEncoder().encode("tagged\n"));
      execFileSync(
        "git",
        ["-c", "user.name=T", "-c", "user.email=t@e.com", "tag", "-a", "v1.0", "-m", "release"],
        {
          cwd: root,
          env: {
            ...process.env,
            GIT_COMMITTER_DATE: "1700000000 +0000",
          },
        },
      );
      // SAFETY: `rev-parse` on an annotated tag prints the tag object's own id.
      const oid = git("rev-parse", "v1.0").trim() as Oid;
      assert.notEqual(oid, target, "the fixture must be an annotated tag, not a lightweight one");

      const data = await bytesOf(oid);
      const parsed = parseTag(data);
      assert.ok(Result.isSuccess(parsed), "git's own tag must parse");
      assert.deepEqual([...encodeTag(parsed.success)], [...data]);
    }),
  );
});
