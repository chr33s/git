import { Invalid } from "../git/Error.ts";
import { Config, Effect, Schema } from "effect";

export const configuration = Effect.fn("RequestUpload.configuration")(function* () {
  return yield* Config.schema(
    Schema.Int.check(Schema.isGreaterThan(0), Schema.isLessThanOrEqualTo(2_147_483_647)),
    "GIT_CF_UPLOAD_IDLE_MS",
  ).pipe(Config.withDefault(30_000));
});

/** Bridge request cancellation to the body, including uninterruptible routes. */
export const open = (original: Request, signal: AbortSignal, idleMs = 30_000) => {
  const idle = Promise.withResolvers<void>();
  if (original.body === null)
    return {
      request: new Request(original, { signal }),
      close: () => undefined,
      idle: idle.promise,
    };
  const reader = original.body.getReader();
  let timer: ReturnType<typeof setTimeout> | undefined;
  let settled = false;
  let abort = () => {};
  const finish = () => {
    if (settled) return false;
    settled = true;
    clearTimeout(timer);
    signal.removeEventListener("abort", abort);
    return true;
  };
  const cancel = (reason: Invalid) => {
    // This is the incoming transport, not storage work. An uncooperative
    // sender's cancellation promise must not prevent the request being refused.
    void reader.cancel(reason).catch(() => undefined);
    reader.releaseLock();
  };
  const body = new ReadableStream<Uint8Array>(
    {
      start(controller) {
        abort = () => {
          if (!finish()) return;
          const error = new Invalid({ field: "body", reason: "request body cancelled" });
          controller.error(error);
          cancel(error);
        };
        signal.addEventListener("abort", abort, { once: true });
        if (signal.aborted) abort();
      },
      async pull(controller) {
        // Count only time waiting for sender bytes, not downstream storage or
        // authentication. The admission race selects 408 before cancelling us.
        timer = setTimeout(() => idle.resolve(), idleMs);
        try {
          const next = await reader.read();
          clearTimeout(timer);
          if (settled) return;
          if (next.done) {
            finish();
            controller.close();
            reader.releaseLock();
          } else controller.enqueue(next.value);
        } catch (error) {
          clearTimeout(timer);
          if (!finish()) return;
          controller.error(error);
          reader.releaseLock();
        }
      },
      cancel() {
        if (finish()) cancel(new Invalid({ field: "body", reason: "request body cancelled" }));
      },
    },
    { highWaterMark: 0 },
  );
  return {
    request: new Request(original, { body, signal, ...{ duplex: "half" } }),
    close: () => abort(),
    idle: idle.promise,
  };
};
