/**
 * Git smart-HTTP endpoints.
 *
 * Today `Server` (`src/server.ts`) holds a hand-rolled `#routes` array matched
 * with `URLPattern`, plus a second table in `ServerApi` (`src/server.api.ts`,
 * 2.5k lines) with its own dispatch, its own JSON parsing and its own
 * try/catch-to-status mapping repeated per handler.
 *
 * Sketch: the raw protocol lives in an `HttpRouter` (it is byte-oriented — no
 * schema buys you anything on a packfile), and the JSON API moves to `HttpApi`
 * where the schema does earn its keep. Both mount into the same web handler.
 */
import { Effect, Layer, Stream } from "effect";
import {
  type HttpMiddleware,
  HttpRouter,
  HttpServerRequest,
  HttpServerResponse,
} from "effect/unstable/http";
import { PackCorrupt } from "../git/Error.sketch.ts";
import * as Pack from "../git/Pack.sketch.ts";
import { Repository } from "../git/Repository.sketch.ts";
import type { ObjectStore, RefStore } from "../git/Store.sketch.ts";

const advertise = (service: "git-upload-pack" | "git-receive-pack") =>
  Effect.gen(function* () {
    const repository = yield* Repository;
    const all = yield* repository.refs;
    const head = yield* repository.head;

    const lines = [
      Pack.pktLine.encode(new TextEncoder().encode(`# service=${service}\n`)),
      Pack.pktLine.flush,
      ...all.map(([name, oid]) =>
        Pack.pktLine.encode(new TextEncoder().encode(`${oid} ${name}\n`)),
      ),
      Pack.pktLine.flush,
    ];

    return HttpServerResponse.uint8Array(concat(lines), {
      headers: {
        "content-type": `application/x-${service}-advertisement`,
        "cache-control": "no-cache",
        "x-head": head,
      },
    });
  });

/**
 * receive-pack: the request body is a stream in and the ref-update report is a
 * stream out. Nothing between them holds the whole pack.
 */
const receivePack = Effect.gen(function* () {
  const request = yield* HttpServerRequest.HttpServerRequest;
  const repository = yield* Repository;

  const body = request.stream.pipe(
    Stream.mapError((cause) => new PackCorrupt({ reason: String(cause) })),
  );

  const [commands, pack] = yield* splitCommands(body);
  const results = yield* repository.receive(commands, { atomic: true, pack });

  return HttpServerResponse.uint8Array(
    concat(
      results.map((result) =>
        Pack.pktLine.encode(
          new TextEncoder().encode(
            result.ok ? `ok ${result.ref}\n` : `ng ${result.ref} ${result.reason ?? "rejected"}\n`,
          ),
        ),
      ),
    ),
    { headers: { "content-type": "application/x-git-receive-pack-result" } },
  );
});

/** upload-pack: response body is the lazily built pack. */
const uploadPack = Effect.gen(function* () {
  const request = yield* HttpServerRequest.HttpServerRequest;
  const repository = yield* Repository;
  const wants = yield* parseWants(
    request.stream.pipe(Stream.mapError((cause) => new PackCorrupt({ reason: String(cause) }))),
  );

  // A response body outlives the handler effect, so it must not carry
  // requirements. `repository.pack` closes over its stores and hands back a
  // plain stream — the walk runs while the bytes are flowing, and a client
  // hang-up interrupts it.
  return HttpServerResponse.stream(repository.pack(wants), {
    contentType: "application/x-git-upload-pack-result",
  });
});

export const routes = HttpRouter.use((router) =>
  Effect.gen(function* () {
    yield* router.add(
      "GET",
      "/:repo/info/refs",
      Effect.gen(function* () {
        // The router already parsed these; re-parsing the URL by hand is the
        // reflex to unlearn.
        const params = yield* HttpServerRequest.ParsedSearchParams;
        return yield* params.service === "git-receive-pack"
          ? advertise("git-receive-pack")
          : advertise("git-upload-pack");
      }),
    );
    yield* router.add("POST", "/:repo/git-receive-pack", receivePack);
    yield* router.add("POST", "/:repo/git-upload-pack", uploadPack);
  }),
);

/**
 * Error mapping lives in one middleware instead of the per-handler try/catch in
 * `server.api.ts` plus the duplicate mapping in `worker.ts`. Every member of the
 * `GitError` union carries its own `status`, so the mapping is total by
 * construction and a new error variant cannot be forgotten.
 */
export declare const errorMiddleware: HttpMiddleware.HttpMiddleware;

declare const splitCommands: (
  body: Stream.Stream<Uint8Array, PackCorrupt>,
) => Effect.Effect<
  readonly [
    ReadonlyArray<import("../git/Store.ts").RefUpdate>,
    Stream.Stream<Uint8Array, PackCorrupt>,
  ],
  PackCorrupt
>;
declare const parseWants: (
  body: Stream.Stream<Uint8Array, PackCorrupt>,
) => Effect.Effect<Stream.Stream<import("../git/Store.ts").Oid, PackCorrupt>, PackCorrupt>;
declare const concat: (chunks: ReadonlyArray<Uint8Array>) => Uint8Array;

export type ProtocolServices = Repository | RefStore | ObjectStore;
export type ProtocolLayer = Layer.Layer<never, never, ProtocolServices | HttpRouter.HttpRouter>;
