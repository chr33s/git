/**
 * The streaming bulk-commit endpoint, driven the way a client would drive it.
 *
 * `handle` takes a web `Request` and answers a `Response`, so the tests speak
 * NDJSON straight at it — no socket, no host. The one thing worth arranging
 * deliberately is where the body is cut: a `ReadableStream` chunk has nothing
 * to do with a protocol line, and the test that proves it feeds slices that
 * land mid-line, mid-base64 and mid-UTF-8.
 */
import assert from "node:assert/strict";
import { describe, it } from "@effect/vitest";

import { Effect, Fiber, Layer, Predicate } from "effect";

import { hashObject } from "../git/Format.ts";
import { stores } from "../git/Memory.ts";
import * as GitRepository from "../git/Repository.ts";
import { Repository } from "../git/Repository.ts";
import { isOid, type Oid } from "../git/Store.ts";
import * as CommitPack from "./CommitPack.ts";
import * as Policy from "./Policy.ts";

// Scratch repositories with no genesis, which the policy boundary refuses to
// write to unless the host says otherwise — `serve --open`'s choice.
const live = GitRepository.layer.pipe(
  Layer.provide(GitRepository.hooksNoop),
  Layer.provide(stores),
  Layer.provideMerge(Policy.anonymousWrites(true)),
);

/** And one the host has *not* opened, where the boundary refuses every write. */
const closed = GitRepository.layer.pipe(
  Layer.provide(GitRepository.hooksNoop),
  Layer.provide(stores),
);

const encoder = new TextEncoder();
const decoder = new TextDecoder();

/** Fixed, so two runs of the same body produce the same commit oid. */
const alice = {
  name: "Alice",
  email: "alice@example.com",
  at: new Date(1_700_000_000_000).toISOString(),
  offset: 0,
};

const base64 = (bytes: Uint8Array): string => {
  let binary = "";
  for (const byte of bytes) binary += String.fromCharCode(byte);
  return btoa(binary);
};

const utf8 = (value: string): string => base64(encoder.encode(value));

/** A JSON value, which is all a protocol line or a response body may hold. */
type Json = string | number | boolean | null | ReadonlyArray<Json> | JsonRecord;

interface JsonRecord {
  readonly [field: string]: Json;
}

const isJsonRecord = (value: Json): value is JsonRecord => Predicate.isObject(value);

/** One line as a test spells it; `JSON.stringify` drops `undefined` fields. */
interface WireRecord {
  readonly [field: string]: Json | undefined;
}

const ndjson = (records: ReadonlyArray<WireRecord>): string =>
  `${records.map((record) => JSON.stringify(record)).join("\n")}\n`;

const post = (body: string | ReadableStream<Uint8Array>): Request =>
  // SAFETY: Node's fetch takes `duplex: "half"` — required for a streamed
  // request body — but the bundled RequestInit type does not know the field.
  new Request("http://git.test/r/commit-pack", {
    method: "POST",
    body,
    // Node refuses a streamed request body without it.
    duplex: "half",
  } as RequestInit);

/** The body cut every `size` bytes, wherever that happens to land. */
const sliced = (body: string, size: number): ReadableStream<Uint8Array> => {
  const bytes = encoder.encode(body);
  return new ReadableStream<Uint8Array>({
    start(controller) {
      for (let at = 0; at < bytes.length; at += size) {
        controller.enqueue(bytes.slice(at, at + size));
      }
      controller.close();
    },
  });
};

interface Answer {
  readonly status: number;
  readonly type: string | null;
  readonly payload: JsonRecord;
}

/** Payload fields that must be oids are checked, not trusted: a miss fails here. */
const oidOf = (value: Json | undefined): Oid => {
  if (Predicate.isString(value) && isOid(value)) return value;
  return assert.fail(`expected an oid, got ${JSON.stringify(value)}`);
};

const send = (request: Request): Effect.Effect<Answer, never, Repository> =>
  Effect.gen(function* () {
    const response = yield* CommitPack.handle(request);
    assert.ok(response !== null, "commit-pack should have claimed the request");
    const raw = yield* Effect.promise((): Promise<Json> => response.json());
    const payload = isJsonRecord(raw)
      ? raw
      : assert.fail("every commit-pack body is a JSON object");
    return { status: response.status, type: response.headers.get("content-type"), payload };
  });

/** Its own repository each time — which is what makes two runs comparable. */
const inFreshRepository = (request: Request): Effect.Effect<Answer> =>
  send(request).pipe(Effect.provide(live));

const run = (effect: Effect.Effect<unknown, unknown, Repository>): Effect.Effect<void> =>
  effect.pipe(Effect.provide(live), Effect.orDie, Effect.asVoid);

describe("CommitPack", () => {
  /**
   * `it.live`, not `it.effect`: `Repository.commit` retries a `RefConflict`
   * behind a 10ms schedule, and a `TestClock` whose time never advances would
   * leave the conflict assertions waiting forever.
   */
  it.live("commits several files, each written as its lines arrive", () =>
    run(
      Effect.gen(function* () {
        const answer = yield* send(
          post(
            ndjson([
              { type: "commit", branch: "main", message: "seed\n", author: alice },
              { type: "file", path: "readme.md" },
              { type: "chunk", data: utf8("hello\n") },
              { type: "end" },
              { type: "file", path: "src/run.sh", mode: "100755" },
              { type: "chunk", data: utf8("#!/bin/sh\n") },
              { type: "chunk", data: utf8("echo hi\n") },
              { type: "end" },
              { type: "done" },
            ]),
          ),
        );

        assert.equal(answer.status, 200);
        assert.equal(answer.type, "application/json");
        assert.equal(answer.payload.files, 2);
        assert.match(oidOf(answer.payload.oid), /^[0-9a-f]{40}$/);

        const repository = yield* Repository;
        const files = yield* repository.listFiles(oidOf(answer.payload.tree));
        assert.deepEqual(
          files.map((file) => [file.path, file.mode]),
          [
            ["readme.md", "100644"],
            ["src/run.sh", "100755"],
          ],
        );

        // The chunks of one file concatenate in the order they were sent.
        const script = files.find((file) => file.path === "src/run.sh")!;
        assert.equal(
          decoder.decode(yield* repository.readBlob(script.oid)),
          "#!/bin/sh\necho hi\n",
        );

        // …and the commit is on the branch, pointing at that tree.
        assert.equal(yield* repository.resolve("refs/heads/main"), answer.payload.oid);
        const commit = yield* repository.readCommit(oidOf(answer.payload.oid));
        assert.equal(commit.tree, answer.payload.tree);
        assert.equal(commit.message, "seed\n");
        assert.deepEqual(commit.parents, []);
      }),
    ),
  );

  it.live("reads the same commit out of a body cut at every awkward byte", () =>
    Effect.gen(function* () {
      const body = ndjson([
        // A message with multi-byte characters, so the cuts land inside a
        // UTF-8 sequence as well as inside a line.
        { type: "commit", message: "héllo — a wíde méssage\n", author: alice },
        { type: "file", path: "a/deeply/nested/file.txt" },
        { type: "chunk", data: utf8("one\ntwo\n") },
        { type: "chunk", data: utf8("three\n") },
        { type: "end" },
        { type: "file", path: "top.txt" },
        { type: "chunk", data: utf8("t\n") },
        { type: "end" },
        { type: "done" },
      ]);

      const whole = yield* inFreshRepository(post(body));
      assert.equal(whole.status, 200);

      // Sizes chosen to be coprime with nothing in particular: the reader's
      // chunks and the protocol's lines are unrelated, which is the point.
      for (const size of [1, 3, 7, 64]) {
        const streamed = yield* inFreshRepository(post(sliced(body, size)));
        assert.equal(streamed.status, 200, `cut every ${size} bytes`);
        assert.deepEqual(streamed.payload, whole.payload, `cut every ${size} bytes`);
      }
    }).pipe(Effect.orDie, Effect.asVoid),
  );

  it.live("reassembles a file delivered in many chunks, bytes that are not text included", () =>
    run(
      Effect.gen(function* () {
        // 0xff, 0xfe and 0x80 among them: decode the body as text and this
        // content would not survive, which is why only the lines are text.
        const content = new Uint8Array(1024);
        for (let at = 0; at < content.length; at++) content[at] = (at * 37 + (at % 7)) % 256;

        const chunks: WireRecord[] = [];
        for (let at = 0; at < content.length; at += 7) {
          chunks.push({ type: "chunk", data: base64(content.slice(at, at + 7)) });
        }
        // 7 does not divide 3, so most chunks are padded base64 of their own.
        assert.equal(chunks.length, 147);

        const answer = yield* send(
          post(
            ndjson([
              { type: "commit", message: "binary\n", author: alice },
              { type: "file", path: "blob.bin" },
              ...chunks,
              { type: "end" },
              { type: "done" },
            ]),
          ),
        );
        assert.equal(answer.status, 200);

        const repository = yield* Repository;
        const files = yield* repository.listFiles(oidOf(answer.payload.tree));
        const stored = yield* repository.readBlob(files[0]!.oid);
        assert.equal(stored.length, content.length);
        assert.deepEqual([...stored], [...content]);
      }),
    ),
  );

  it.live("removes a path the branch already had", () =>
    run(
      Effect.gen(function* () {
        const first = yield* send(
          post(
            ndjson([
              { type: "commit", message: "two files\n", author: alice },
              { type: "file", path: "gone.txt" },
              { type: "chunk", data: utf8("bye\n") },
              { type: "end" },
              { type: "file", path: "kept.txt" },
              { type: "chunk", data: utf8("still here\n") },
              { type: "end" },
              { type: "done" },
            ]),
          ),
        );
        assert.equal(first.status, 200);

        const second = yield* send(
          post(
            ndjson([
              { type: "commit", message: "drop one\n", author: alice },
              { type: "delete", path: "gone.txt" },
              { type: "done" },
            ]),
          ),
        );
        assert.equal(second.status, 200);
        // A delete writes no file, and says so.
        assert.equal(second.payload.files, 0);

        const repository = yield* Repository;
        const files = yield* repository.listFiles(oidOf(second.payload.tree));
        assert.deepEqual(
          files.map((file) => file.path),
          ["kept.txt"],
        );

        // It builds on the branch rather than replacing it.
        const commit = yield* repository.readCommit(oidOf(second.payload.oid));
        assert.deepEqual(commit.parents, [first.payload.oid]);
      }),
    ),
  );

  it.live("refuses a commit whose branch moved while its body was streaming", () =>
    run(
      Effect.gen(function* () {
        const repository = yield* Repository;
        yield* send(
          post(
            ndjson([
              { type: "commit", message: "first\n", author: alice },
              { type: "file", path: "a.txt" },
              { type: "chunk", data: utf8("a\n") },
              { type: "end" },
              { type: "done" },
            ]),
          ),
        );

        // The tree is snapshotted at the `commit` header, so the rest of the
        // body arrives *after* another commit lands. Parenting this tree on
        // that commit would revert its files with no conflict and a 200.
        let release: () => void = () => undefined;
        const raced = new Promise<void>((resolve) => {
          release = resolve;
        });
        const head = ndjson([
          { type: "commit", message: "slow\n", author: alice },
          { type: "file", path: "b.txt" },
        ]);
        const tail = ndjson([
          { type: "chunk", data: utf8("b\n") },
          { type: "end" },
          { type: "done" },
        ]);

        const body = new ReadableStream<Uint8Array>({
          async start(controller) {
            controller.enqueue(encoder.encode(head));
            await raced;
            controller.enqueue(encoder.encode(tail));
            controller.close();
          },
        });

        const inFlight = yield* Effect.forkChild(send(post(body)));

        const meanwhile = yield* repository.writeFiles({
          changes: [{ path: "c.txt", content: encoder.encode("c\n") }],
        });
        yield* repository.commit({
          branch: "main",
          tree: meanwhile,
          message: "raced",
          author: { ...alice, at: new Date(alice.at) },
        });
        release();

        const answer = yield* Fiber.join(inFlight);
        assert.equal(answer.status, 409, JSON.stringify(answer.payload));

        // The racing commit still stands, and its file is still there.
        const tip = (yield* repository.resolve("refs/heads/main"))!;
        const files = yield* repository.listFiles((yield* repository.readCommit(tip)).tree);
        assert.ok(files.some((file) => file.path === "c.txt"));
      }),
    ),
  );

  it.live("refuses a pack whose 'expected' does not match, and commits nothing", () =>
    run(
      Effect.gen(function* () {
        const first = yield* send(
          post(
            ndjson([
              { type: "commit", message: "first\n", author: alice },
              { type: "file", path: "a.txt" },
              { type: "chunk", data: utf8("a\n") },
              { type: "end" },
              { type: "done" },
            ]),
          ),
        );
        assert.equal(first.status, 200);

        const repository = yield* Repository;

        // `null` says the branch must not exist; it does.
        const fresh = yield* send(
          post(
            ndjson([
              { type: "commit", message: "clash\n", author: alice, expected: null },
              { type: "file", path: "b.txt" },
              { type: "chunk", data: utf8("b\n") },
              { type: "end" },
              { type: "done" },
            ]),
          ),
        );
        assert.equal(fresh.status, 409);
        assert.ok(Predicate.isString(fresh.payload.error), "a conflict names its reason");
        assert.equal(yield* repository.resolve("refs/heads/main"), first.payload.oid);

        // An oid that is real but not the tip loses the same way.
        const stale = yield* send(
          post(
            ndjson([
              {
                type: "commit",
                message: "stale\n",
                author: alice,
                expected: first.payload.tree,
              },
              { type: "file", path: "c.txt" },
              { type: "chunk", data: utf8("c\n") },
              { type: "end" },
              { type: "done" },
            ]),
          ),
        );
        assert.equal(stale.status, 409);
        assert.equal(yield* repository.resolve("refs/heads/main"), first.payload.oid);

        // Pinning the actual tip is how a client that read the branch wins.
        const ok = yield* send(
          post(
            ndjson([
              { type: "commit", message: "onwards\n", author: alice, expected: first.payload.oid },
              { type: "file", path: "d.txt" },
              { type: "chunk", data: utf8("d\n") },
              { type: "end" },
              { type: "done" },
            ]),
          ),
        );
        assert.equal(ok.status, 200);
        assert.equal(yield* repository.resolve("refs/heads/main"), ok.payload.oid);
      }),
    ),
  );

  it.live("rejects malformed packs without moving the branch", () =>
    run(
      Effect.gen(function* () {
        const repository = yield* Repository;
        const header = { type: "commit", message: "nope\n", author: alice };

        const malformed: ReadonlyArray<readonly [string, number, string]> = [
          [
            "a chunk before any file",
            400,
            ndjson([header, { type: "chunk", data: utf8("x") }, { type: "done" }]),
          ],
          ["a line that is not JSON", 400, `${JSON.stringify(header)}\nnot json at all\n`],
          [
            "no commit header",
            400,
            ndjson([
              { type: "file", path: "a.txt" },
              { type: "chunk", data: utf8("a\n") },
              { type: "end" },
              { type: "done" },
            ]),
          ],
          [
            "a body that stops before 'done'",
            400,
            ndjson([header, { type: "file", path: "a.txt" }, { type: "end" }]),
          ],
          [
            "a file left open",
            400,
            ndjson([header, { type: "file", path: "a.txt" }, { type: "done" }]),
          ],
          ["a record type nobody defined", 400, ndjson([header, { type: "teleport" }])],
          [
            "base64 that is not base64",
            400,
            ndjson([
              header,
              { type: "file", path: "a.txt" },
              { type: "chunk", data: "!!! not base64 !!!" },
              { type: "end" },
              { type: "done" },
            ]),
          ],
          [
            "a path that escapes the root",
            422,
            ndjson([
              header,
              { type: "file", path: "../outside" },
              { type: "chunk", data: utf8("x") },
              { type: "end" },
              { type: "done" },
            ]),
          ],
          ["an empty path", 422, ndjson([header, { type: "file", path: "" }, { type: "done" }])],
        ];

        for (const [name, status, body] of malformed) {
          const answer = yield* send(post(body));
          assert.equal(answer.status, status, name);
          assert.equal(answer.type, "application/json", name);
          assert.ok(Predicate.isString(answer.payload.error), name);
          // The ref is moved only after the body is drained, so every one of
          // these leaves the repository exactly as it found it.
          assert.equal(yield* repository.resolve("refs/heads/main"), null, name);
        }
      }),
    ),
  );

  it.live("refuses a write the boundary rejects before it writes any of the body", () =>
    Effect.gen(function* () {
      // The gate was asked at `done`, by which point every blob and tree in
      // the body had already been written on the way past — so a caller the
      // boundary refused had still put arbitrary content into the object
      // store. The branch is named in the first record; there is nothing to
      // wait for. This repository has no membership and the host has not
      // opened it, which is one of the several ways the boundary says no.
      const repository = yield* Repository;
      const content = "a secret the boundary said no to\n";
      const answer = yield* send(
        post(
          ndjson([
            { type: "commit", branch: "main", message: "nope\n", author: alice },
            { type: "file", path: "leak.txt" },
            { type: "chunk", data: utf8(content) },
            { type: "end" },
            { type: "done" },
          ]),
        ),
      );

      assert.equal(answer.status, 400, JSON.stringify(answer.payload));

      // And the bytes never landed. The oid is computed rather than written,
      // so asking for it cannot be what puts it there.
      const oid = yield* hashObject({ type: "blob", data: encoder.encode(content) });
      assert.equal(
        yield* repository.readBlob(oid).pipe(
          Effect.as(true),
          Effect.catchTag("ObjectNotFound", () => Effect.succeed(false)),
        ),
        false,
        "a refused pack must not leave its content in the object store",
      );
    }).pipe(Effect.provide(closed), Effect.orDie),
  );

  it.live("leaves requests that are not its own alone", () =>
    run(
      Effect.gen(function* () {
        assert.equal(yield* CommitPack.handle(new Request("http://git.test/r/info/refs")), null);

        const wrongMethod = yield* CommitPack.handle(new Request("http://git.test/r/commit-pack"));
        assert.equal(wrongMethod?.status, 405);
      }),
    ),
  );
});
