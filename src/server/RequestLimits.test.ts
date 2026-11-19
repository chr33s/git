import assert from "node:assert/strict";
import { gzipSync } from "node:zlib";
import { describe, it } from "@effect/vitest";
import { ConfigProvider, Effect, Layer, Stream } from "effect";
import { PackCorrupt, RequestLimitExceeded } from "../git/Error.ts";
import { stores } from "../git/Memory.ts";
import { DELIM, FLUSH, pkt } from "../git/Pkt.ts";
import { concatBytes } from "../git/Format.ts";
import * as Repository from "../git/Repository.ts";
import * as Lfs from "./Lfs.ts";
import * as Protocol from "./Protocol.ts";
import * as Auth from "./Auth.ts";
import * as Policy from "./Policy.ts";
import * as RequestBody from "./RequestBody.ts";
import { configuration, defaults, RequestLimits } from "./RequestLimits.ts";

const live = Repository.layer.pipe(Layer.provide(Repository.hooksNoop), Layer.provide(stores));

describe("aggregate request budgets", () => {
  it.live("rejects overflow even when sender cancellation never settles", () =>
    Effect.gen(function* () {
      let canceled = false;
      const request = new Request("http://host/r", {
        method: "POST",
        body: new ReadableStream({
          pull(controller) {
            controller.enqueue(new Uint8Array(16));
          },
          cancel() {
            canceled = true;
            return new Promise<void>(() => {});
          },
        }),
        duplex: "half",
      });
      const error = yield* RequestBody.text(request, 8).pipe(
        Effect.flip,
        Effect.timeout("1 second"),
      );
      assert.ok(error instanceof RequestLimitExceeded);
      assert.equal(canceled, true);
    }),
  );
  it.effect("an overflow during unpacking refuses every ref and cancels the upload", () =>
    Effect.gen(function* () {
      const repository = yield* Repository.Repository;
      const prefix = concatBytes([
        pkt(`${"0".repeat(40)} ${"a".repeat(40)} refs/heads/main\0report-status\n`),
        FLUSH,
      ]);
      let chunk = 0;
      let canceled = false;
      let unpacked = false;
      let applied = false;
      const request = new Request("http://host/r/git-receive-pack", {
        method: "POST",
        body: new ReadableStream(
          {
            pull(controller) {
              controller.enqueue(chunk++ === 0 ? prefix : new Uint8Array(64));
            },
            cancel() {
              canceled = true;
            },
          },
          { highWaterMark: 0 },
        ),
        duplex: "half",
      });
      const response = yield* Protocol.receivePack(request).pipe(
        Effect.provideService(
          Repository.Repository,
          Repository.Repository.of({
            ...repository,
            unpack: (source) => {
              unpacked = true;
              return Stream.runDrain(source).pipe(
                Effect.mapError((cause) =>
                  cause instanceof PackCorrupt
                    ? cause
                    : new PackCorrupt({ reason: "input stream failed" }),
                ),
                Effect.as([]),
              );
            },
            receive: () => {
              applied = true;
              return Effect.succeed([]);
            },
          }),
        ),
        Effect.provide(Policy.anonymousWrites(true)),
        Effect.provideService(RequestLimits, { ...defaults, receiveBytes: prefix.length + 4 }),
      );
      assert.equal(unpacked, true, "the failure must occur after parsing the command prefix");
      assert.equal(applied, false);
      assert.equal(canceled, true);
      assert.equal(chunk, 2, "stop at the first excess chunk");
      assert.match(
        yield* Effect.promise(() => response.text()),
        /unpack receive-pack bytes exceeds/,
      );
    }).pipe(Effect.provide(live)),
  );

  it.effect.each([false, true])("limits decoded negotiation bytes, gzip=%s", (gzip) =>
    Effect.gen(function* () {
      const bytes = new TextEncoder().encode("0000".repeat(100));
      const request = new Request("http://host/r/git-upload-pack", {
        method: "POST",
        body: gzip ? Uint8Array.from(gzipSync(bytes)) : bytes,
        headers: gzip ? { "content-encoding": "gzip" } : {},
      });
      const error = yield* Protocol.uploadPack(request).pipe(
        Effect.provide(live),
        Effect.provideService(RequestLimits, { ...defaults, negotiationBytes: 32 }),
        Effect.flip,
      );
      assert.ok(error instanceof RequestLimitExceeded);
      assert.equal(error.budget, "negotiation bytes");
    }),
  );

  it.effect.each([false, true])("bounds packet count before negotiating, v2=%s", (v2) =>
    Effect.gen(function* () {
      const request = new Request("http://host/r/git-upload-pack", {
        method: "POST",
        headers: v2 ? { "git-protocol": "version=2" } : {},
        body: v2
          ? concatBytes([pkt("command=fetch\n"), DELIM, pkt(`want ${"a".repeat(40)}\n`), FLUSH])
          : concatBytes([FLUSH, FLUSH, FLUSH]),
      });
      const error = yield* Protocol.uploadPack(request).pipe(
        Effect.provide(live),
        Effect.provideService(RequestLimits, { ...defaults, negotiationLines: 2 }),
        Effect.flip,
      );
      assert.ok(error instanceof RequestLimitExceeded);
      assert.equal(error.budget, "negotiation lines");
    }),
  );

  it.effect("cancels an oversized request without draining the sender", () =>
    Effect.gen(function* () {
      let pulls = 0;
      let canceled = false;
      const request = new Request("http://host/r/git-upload-pack", {
        method: "POST",
        body: new ReadableStream(
          {
            pull(controller) {
              pulls++;
              controller.enqueue(new TextEncoder().encode("00000000"));
            },
            cancel() {
              canceled = true;
            },
          },
          { highWaterMark: 0 },
        ),
        duplex: "half",
      });
      yield* Protocol.uploadPack(request).pipe(
        Effect.provide(live),
        Effect.provideService(RequestLimits, { ...defaults, negotiationBytes: 4 }),
        Effect.flip,
      );
      assert.equal(canceled, true);
      assert.equal(pulls, 1);
    }),
  );

  it.effect.each(["body", "commands", "prefix"])(
    "bounds receive-pack %s before applying refs",
    (kind) =>
      Effect.gen(function* () {
        const zero = "0".repeat(40);
        const request = new Request("http://host/r/git-receive-pack", {
          method: "POST",
          body: concatBytes([
            pkt(`${zero} ${zero} refs/heads/a\0report-status\n`),
            pkt(`${zero} ${zero} refs/heads/b\n`),
            FLUSH,
          ]),
        });
        const error = yield* Protocol.receivePack(request).pipe(
          Effect.provide(live),
          Effect.provideService(RequestLimits, {
            ...defaults,
            receiveBytes: kind === "body" ? 4 : defaults.receiveBytes,
            receiveCommands: kind === "commands" ? 1 : defaults.receiveCommands,
            receiveCommandBytes: kind === "prefix" ? 4 : defaults.receiveCommandBytes,
          }),
          Effect.flip,
        );
        assert.ok(error instanceof RequestLimitExceeded);
        assert.equal(
          error.budget,
          kind === "body"
            ? "receive-pack bytes"
            : kind === "commands"
              ? "receive commands"
              : "receive command bytes",
        );
      }),
  );

  it.effect.each(["bytes", "objects"])(
    "refuses an oversized LFS batch by %s before storage work",
    (kind) =>
      Effect.gen(function* () {
        const store = yield* Lfs.memory;
        let reads = 0;
        const response = yield* Lfs.handle(
          new Request("http://host/r/info/lfs/objects/batch", {
            method: "POST",
            body: JSON.stringify({
              operation: "download",
              objects: Array.from({ length: 3 }, () => ({ oid: "a".repeat(64), size: 1 })),
            }),
          }),
        ).pipe(
          Effect.provideService(Lfs.LfsStore, {
            ...store,
            head: (oid) => {
              reads++;
              return store.head(oid);
            },
          }),
          Effect.provideService(Auth.Requester, Auth.anonymous),
          Effect.provideService(RequestLimits, {
            ...defaults,
            lfsBatchBytes: kind === "bytes" ? 8 : defaults.lfsBatchBytes,
            lfsBatchObjects: 2,
          }),
        );
        assert.equal(response?.status, 413);
        assert.equal(reads, 0);
      }),
  );

  it.effect("validates operator budgets and supplies defaults", () =>
    Effect.gen(function* () {
      const configured = yield* configuration().pipe(
        Effect.provide(
          ConfigProvider.layer(
            ConfigProvider.fromUnknown({
              GIT_NEGOTIATION_BYTES: "512",
              GIT_FETCH_GRAPH_STEPS: "123",
            }),
          ),
        ),
      );
      assert.deepEqual(configured, { ...defaults, negotiationBytes: 512, fetchGraphSteps: 123 });
      for (const name of ["GIT_NEGOTIATION_BYTES", "GIT_FETCH_GRAPH_STEPS"]) {
        for (const invalid of ["0", "-1", "NaN", "1.5", "Infinity"]) {
          const result = yield* configuration().pipe(
            Effect.provide(ConfigProvider.layer(ConfigProvider.fromUnknown({ [name]: invalid }))),
            Effect.result,
          );
          assert.equal(result._tag, "Failure");
        }
      }
    }),
  );
});
