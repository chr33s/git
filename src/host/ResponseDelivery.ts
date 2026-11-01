import { ResponseTimeout } from "../git/Error.ts";

export interface Timeouts {
  readonly idleMs: number;
  readonly deadlineMs: number;
}

/** Observe transport settlement without buffering or hiding body failures. */
export const track = (response: Response, finished: () => void, timeouts?: Timeouts): Response => {
  if (response.body === null) {
    finished();
    return response;
  }
  let idle: ReturnType<typeof setTimeout> | undefined;
  let deadline: ReturnType<typeof setTimeout> | undefined;
  const clear = () => {
    clearTimeout(idle);
    clearTimeout(deadline);
  };
  const expire = (
    controller: TransformStreamDefaultController<Uint8Array>,
    kind: "idle" | "absolute",
  ) => {
    clear();
    // Error both sides of the transform. Aborting pipeTo alone can wait for a
    // backpressured write forever when the client never reads another chunk.
    controller.error(new ResponseTimeout({ kind }));
  };
  const resetIdle = (controller: TransformStreamDefaultController<Uint8Array>) => {
    if (timeouts === undefined) return;
    clearTimeout(idle);
    idle = setTimeout(() => expire(controller, "idle"), timeouts.idleMs);
  };
  const transport = new TransformStream<Uint8Array, Uint8Array>({
    start(controller) {
      if (timeouts === undefined) return;
      resetIdle(controller);
      deadline = setTimeout(() => expire(controller, "absolute"), timeouts.deadlineMs);
    },
    transform(chunk, controller) {
      resetIdle(controller);
      controller.enqueue(chunk);
    },
  });
  // Unlike transformer.flush, pipeTo settles on source failure as well as
  // completion and downstream cancellation. Observe both branches so a failed
  // response cannot leave maintenance waiting on a permanently pending lease.
  const settle = () => {
    clear();
    finished();
  };
  void response.body.pipeTo(transport.writable).then(settle, settle);
  return new Response(transport.readable, response);
};
