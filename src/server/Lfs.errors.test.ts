import assert from "node:assert/strict";
import { describe, it } from "@effect/vitest";
import { Effect, Logger, Stream } from "effect";
import { ObjectNotFound, StorageFailure } from "../git/Error.ts";
import * as Auth from "./Auth.ts";
import { handle, LfsStore } from "./Lfs.ts";
import { defaults, RequestLimits } from "./RequestLimits.ts";

const oid = "a".repeat(64);
const secret = "private-storage-path-and-credential";
const failed = () => new StorageFailure({ operation: "fixture", path: secret, cause: secret });
const request = (operation: "upload" | "download") =>
  new Request("https://example.com/r/info/lfs/objects/batch", {
    method: "POST",
    body: JSON.stringify({ operation, objects: [{ oid, size: 4 }] }),
  });

describe("LFS storage outage responses", () => {
  for (const overflow of [false, true]) {
    it.effect(
      `upload signals storage outages separately from budget refusals, overflow=${overflow}`,
      () =>
        Effect.gen(function* () {
          const logged: string[] = [];
          const logger = Logger.make<unknown, void>(({ message }) => {
            logged.push(JSON.stringify(message));
          });
          const store = LfsStore.of({
            head: () => Effect.die("unexpected lookup"),
            read: () => Effect.die("unexpected read"),
            write: (_, body) =>
              overflow
                ? Stream.runDrain(body).pipe(Effect.as({ oid, size: 4 }))
                : Effect.fail(failed()),
          });
          const response = yield* handle(
            new Request(`https://example.com/r/info/lfs/objects/${oid}`, {
              method: "PUT",
              body: "data",
            }),
          ).pipe(
            Effect.provideService(LfsStore, store),
            Effect.provideService(Auth.Requester, {
              ...Auth.anonymous,
              capabilities: ["source.push"],
            }),
            Effect.provideService(RequestLimits, { ...defaults, lfsUploadBytes: 2 }),
            Effect.provide(Logger.layer([logger])),
          );
          assert.ok(response);
          assert.equal(response.status, overflow ? 413 : 500);
          const text = yield* Effect.promise(() => response.text());
          assert.equal(logged.length, overflow ? 0 : 1);
          assert.equal((text + logged.join("")).includes(secret), false);
        }),
    );
  }

  for (const operation of ["upload", "download"] as const) {
    it.effect(`batch ${operation} reports failed lookups without inventing actions`, () =>
      Effect.gen(function* () {
        const logged: string[] = [];
        const logger = Logger.make<unknown, void>(({ message }) => {
          logged.push(JSON.stringify(message));
        });
        const store = LfsStore.of({
          head: () => Effect.fail(failed()),
          read: () => Effect.die("unexpected read"),
          write: () => Effect.die("unexpected write"),
        });
        const response = yield* handle(request(operation)).pipe(
          Effect.provideService(LfsStore, store),
          Effect.provideService(Auth.Requester, Auth.anonymous),
          Effect.provide(Logger.layer([logger])),
        );
        assert.ok(response);
        assert.equal(response.status, 200, "batch errors are reported per object");
        const text = yield* Effect.promise(() => response.text());
        assert.deepEqual(JSON.parse(text), {
          transfer: "basic",
          objects: [{ oid, size: 4, error: { code: 500, message: "object storage unavailable" } }],
        });
        assert.equal(logged.length, 1);
        assert.match(logged.join(""), /storage_failure/);
        assert.equal((text + logged.join("")).includes(secret), false);
      }),
    );
  }

  for (const failure of ["head", "read", "stream", "missing"] as const) {
    it.effect(`download distinguishes ${failure} from absence and redacts diagnostics`, () =>
      Effect.gen(function* () {
        const logged: string[] = [];
        const logger = Logger.make<unknown, void>(({ message }) => {
          logged.push(JSON.stringify(message));
        });
        const store = LfsStore.of({
          head: () =>
            failure === "head" ? Effect.fail(failed()) : Effect.succeed({ oid, size: 4 }),
          read: () => {
            if (failure === "read") return Effect.fail(failed());
            if (failure === "missing") return Effect.fail(new ObjectNotFound({ oid }));
            return Effect.succeed(Stream.fail(failed()));
          },
          write: () => Effect.die("unexpected write"),
        });
        const response = yield* handle(
          new Request(`https://example.com/r/info/lfs/objects/${oid}`),
        ).pipe(
          Effect.provideService(LfsStore, store),
          Effect.provideService(Auth.Requester, Auth.anonymous),
          Effect.provide(Logger.layer([logger])),
        );
        assert.ok(response);
        if (failure === "stream") {
          assert.equal(response.status, 200, "stream faults occur after response headers");
          yield* Effect.promise(() => assert.rejects(response.arrayBuffer()));
        } else {
          assert.equal(response.status, failure === "missing" ? 404 : 500);
          const text = yield* Effect.promise(() => response.text());
          assert.equal(text.includes(secret), false);
        }
        assert.equal(logged.length, failure === "missing" ? 0 : 1);
        assert.equal(logged.join("").includes(secret), false);
      }),
    );
  }
});
