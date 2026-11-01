import assert from "node:assert/strict";
import { Effect } from "effect";
import type { Runner } from "../git/Store.contract.ts";
import { make } from "./CloudflareAdmission.ts";
import { open } from "./RequestUpload.ts";

/** The native body timer must cooperate with the admission race in workerd. */
export const requestUploadContract = (runner: Runner): void => {
  runner.describe("request upload inactivity", () => {
    runner.it("returns 408 after cancelling a stalled uninterruptible reader", async () => {
      const cancellation = new AbortController();
      let cancelled = false;
      const upload = open(
        new Request("http://host/repo/commit-pack", {
          method: "POST",
          body: new ReadableStream<Uint8Array>({
            cancel() {
              cancelled = true;
            },
          }),
          ...{ duplex: "half" },
        }),
        cancellation.signal,
        10,
      );
      const response = await Effect.runPromise(
        Effect.gen(function* () {
          const admit = yield* make();
          return yield* admit(
            Effect.promise(() => upload.request.text()).pipe(
              Effect.as(new Response(null, { status: 204 })),
              Effect.uninterruptible,
            ),
            () => cancellation.abort(),
            Effect.promise(() => upload.idle),
          );
        }),
      );
      assert.equal(response.status, 408);
      assert.equal(response.headers.get("cache-control"), "no-store");
      assert.equal(cancelled, true);
      upload.close();
    });
  });
};
