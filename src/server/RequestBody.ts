import { Effect, Schema } from "effect";
import { Invalid, RequestLimitExceeded } from "../git/Error.ts";

/** Own the reader so every early refusal cancels the remaining body. */
export const open = Effect.fn("RequestBody.open")(function* (
  request: Request,
  limit: number,
  budget: string,
  gzip = false,
) {
  const encoding = request.headers.get("content-encoding")?.trim().toLowerCase();
  if (encoding !== undefined && encoding !== "identity" && !(gzip && encoding === "gzip")) {
    yield* Effect.sync(() => {
      void request.body?.cancel().catch(() => undefined);
    });
    return yield* new Invalid({
      field: "content-encoding",
      reason: "unsupported request encoding",
    });
  }
  const stream =
    encoding === "gzip" ? request.body?.pipeThrough(new DecompressionStream("gzip")) : request.body;
  const reader = yield* Effect.acquireRelease(
    Effect.sync(() => stream?.getReader()),
    (reader) =>
      Effect.sync(() => {
        if (reader === undefined) return;
        // Cancellation closes our reader immediately; the sender's cleanup
        // promise is not trusted to settle and must not hold a rejection open.
        void reader.cancel().catch(() => undefined);
        reader.releaseLock();
      }),
  );
  return (async function* (): AsyncIterable<Uint8Array> {
    if (reader === undefined) return;
    let bytes = 0;
    for (;;) {
      const next = await reader.read();
      if (next.done) return;
      bytes += next.value.byteLength;
      if (bytes > limit) throw new RequestLimitExceeded({ budget, limit });
      yield next.value;
    }
  })();
});

/** JSON is buffered only after a streaming byte ceiling has been enforced. */
export const text = Effect.fn("RequestBody.text")(function* (request: Request, limit: number) {
  const chunks = yield* open(request, limit, "JSON body bytes");
  return yield* Effect.tryPromise({
    try: async () => {
      const decoder = new TextDecoder();
      let result = "";
      for await (const chunk of chunks) result += decoder.decode(chunk, { stream: true });
      return result + decoder.decode();
    },
    catch: (cause) =>
      Schema.is(RequestLimitExceeded)(cause)
        ? cause
        : new Invalid({ field: "body", reason: "cannot read request body" }),
  });
}, Effect.scoped);
