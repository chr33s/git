import assert from "node:assert/strict";
import type { Runner } from "../git/Store.contract.ts";
import { track } from "./ResponseDelivery.ts";

/** Native stream/timer semantics, shared with the real workerd harness. */
export const responseDeliveryContract = (runner: Runner): void => {
  runner.describe("response delivery deadlines", () => {
    for (const mode of ["idle-source", "idle-reader", "deadline"] as const) {
      runner.it(`cancels and releases ${mode}`, async () => {
        let finish: () => void = () => undefined;
        const settled = new Promise<void>((resolve) => {
          finish = resolve;
        });
        let cancelled = false;
        const source = new ReadableStream<Uint8Array>({
          start(controller) {
            if (mode !== "idle-source") controller.enqueue(new Uint8Array([42]));
          },
          cancel() {
            cancelled = true;
          },
        });
        const response = track(new Response(source), finish, {
          idleMs: mode === "deadline" ? 100 : 10,
          deadlineMs: mode === "deadline" ? 30 : 100,
        });
        assert.ok(response.body);
        const reader = response.body.getReader();
        if (mode === "deadline")
          assert.deepEqual((await reader.read()).value, new Uint8Array([42]));
        if (mode === "idle-reader") await settled;
        await assert.rejects(
          reader.read(),
          mode === "deadline"
            ? /response absolute deadline exceeded/
            : /response idle deadline exceeded/,
        );
        await settled;
        assert.equal(cancelled, true);
      });
    }
  });
};
