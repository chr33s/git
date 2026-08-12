/**
 * Negotiation over `uploadPack`, without a wire: requests are built by hand
 * and answers read back as pkt-lines. The interop suite proves stock git can
 * hold this conversation; this file pins the exact acknowledgments each round
 * shape earns, which a passing clone would hide.
 */
import assert from "node:assert/strict";
import { describe, it } from "@effect/vitest";

import { Effect, Layer } from "effect";

import { stores } from "../git/Memory.ts";
import { EMPTY_TREE_OID, type Signature } from "../git/Format.ts";
import { DELIM, FLUSH, pkt } from "../git/Pkt.ts";
import * as GitRepository from "../git/Repository.ts";
import { Repository } from "../git/Repository.ts";
import type { Oid } from "../git/Store.ts";
import { advertise, uploadPack } from "./Protocol.ts";

const decoder = new TextDecoder();

const alice: Signature = {
  name: "Alice",
  email: "alice@example.com",
  at: new Date(1_700_000_000_000),
  offset: 0,
};

/** Each test gets its own stores, so there is no shared state to reset. */
const scenario = <A, E>(effect: Effect.Effect<A, E, Repository>) =>
  Effect.runPromise(
    effect.pipe(
      Effect.provide(
        GitRepository.layer.pipe(
          Layer.provide(GitRepository.hooksNoop),
          Layer.provideMerge(stores),
        ),
      ),
    ) as Effect.Effect<A, E>,
  );

/** main: a <- b <- c, plus an unrelated root on `side`. */
const history = Effect.gen(function* () {
  const repository = yield* Repository;
  const commitOn = (branch: string, message: string) =>
    repository.commit({ branch, tree: EMPTY_TREE_OID, message, author: alice });
  const a = yield* commitOn("main", "a");
  const b = yield* commitOn("main", "b");
  const c = yield* commitOn("main", "c");
  const side = yield* commitOn("side", "elsewhere");
  return { a, b, c, side };
});

/** Request bodies framed by the same `pkt` the server and client use. */
const body = (parts: ReadonlyArray<Uint8Array>): Uint8Array<ArrayBuffer> => {
  const out = new Uint8Array(parts.reduce((sum, part) => sum + part.length, 0));
  let offset = 0;
  for (const part of parts) {
    out.set(part, offset);
    offset += part.length;
  }
  return out as Uint8Array<ArrayBuffer>;
};

const v0Request = (input: {
  readonly wants: ReadonlyArray<Oid>;
  readonly capabilities?: string;
  readonly haves?: ReadonlyArray<Oid>;
  readonly done?: boolean;
}): Request =>
  new Request("http://host/repo/git-upload-pack", {
    method: "POST",
    body: body([
      ...input.wants.map((oid, index) =>
        pkt(
          `want ${oid}${index === 0 && input.capabilities !== undefined ? ` ${input.capabilities}` : ""}\n`,
        ),
      ),
      FLUSH,
      ...(input.haves ?? []).map((oid) => pkt(`have ${oid}\n`)),
      input.done === true ? pkt("done\n") : FLUSH,
    ]),
  });

const v2Request = (args: ReadonlyArray<string>): Request =>
  new Request("http://host/repo/git-upload-pack", {
    method: "POST",
    headers: { "git-protocol": "version=2" },
    body: body([pkt("command=fetch"), DELIM, ...args.map((arg) => pkt(`${arg}\n`)), FLUSH]),
  });

/**
 * The response's pkt-lines up to where a packfile (or the body) ends.
 *
 * Hand-parsed for the same reason `client/Fetch.ts`'s prelude is:
 * `PktReader` treats a header it cannot parse as corruption, and by the
 * time it says so the four bytes are gone — here those four bytes are the
 * pack's own magic, which is exactly what the assertions need to see.
 */
const linesOf = (bytes: Uint8Array): { lines: string[]; sawPack: boolean } => {
  const lines: string[] = [];
  let at = 0;
  while (at + 4 <= bytes.length) {
    const header = decoder.decode(bytes.subarray(at, at + 4));
    if (header === "PACK") return { lines, sawPack: true };
    if (!/^[0-9a-f]{4}$/.test(header)) break;
    const length = Number.parseInt(header, 16);
    if (length < 4) {
      at += 4;
      continue;
    }
    lines.push(decoder.decode(bytes.subarray(at + 4, at + length)).replace(/\n$/, ""));
    at += length;
  }
  return { lines, sawPack: false };
};

const answer = (request: Request) =>
  Effect.gen(function* () {
    const response = yield* uploadPack(request);
    const bytes = new Uint8Array(yield* Effect.promise(() => response.arrayBuffer()));
    return linesOf(bytes);
  });

describe("Protocol negotiation", () => {
  it("advertises multi_ack_detailed on upload-pack", async () => {
    const text = await scenario(
      Effect.gen(function* () {
        yield* history;
        const response = yield* advertise("git-upload-pack");
        return decoder.decode(new Uint8Array(yield* Effect.promise(() => response.arrayBuffer())));
      }),
    );
    assert.match(text, /multi_ack_detailed/);
  });

  it("acknowledges every common have, says ready, and ends the round with NAK", async () => {
    const { a, b, lines } = await scenario(
      Effect.gen(function* () {
        const { a, b, c } = yield* history;
        const { lines } = yield* answer(
          v0Request({ wants: [c], capabilities: "multi_ack_detailed", haves: [a, b] }),
        );
        return { a, b, lines };
      }),
    );
    assert.deepEqual(lines, [`ACK ${a} common`, `ACK ${b} common`, `ACK ${b} ready`, "NAK"]);
  });

  it("withholds ready while a want cannot reach the common set", async () => {
    const { a, lines } = await scenario(
      Effect.gen(function* () {
        const { a, c, side } = yield* history;
        const { lines } = yield* answer(
          v0Request({ wants: [c, side], capabilities: "multi_ack_detailed", haves: [a] }),
        );
        return { a, lines };
      }),
    );
    assert.deepEqual(lines, [`ACK ${a} common`, "NAK"]);
  });

  it("answers a plain round with a single bare ACK when the capability is absent", async () => {
    const { a, lines } = await scenario(
      Effect.gen(function* () {
        const { a, b, c } = yield* history;
        const { lines } = yield* answer(v0Request({ wants: [c], haves: [a, b] }));
        return { a, lines };
      }),
    );
    // First common only: a single-ACK client stops offering at one ACK, so
    // acknowledging more would be lines nothing reads.
    assert.deepEqual(lines, [`ACK ${a}`]);
  });

  it("restates common acks before the final ACK on the done round", async () => {
    const { a, lines, sawPack } = await scenario(
      Effect.gen(function* () {
        const { a, c } = yield* history;
        const result = yield* answer(
          v0Request({ wants: [c], capabilities: "multi_ack_detailed", haves: [a], done: true }),
        );
        return { a, ...result };
      }),
    );
    assert.deepEqual(lines, [`ACK ${a} common`, `ACK ${a}`]);
    assert.equal(sawPack, true);
  });

  it("v2: acknowledges without ready while the common set cannot cover the wants", async () => {
    const { a, lines, sawPack } = await scenario(
      Effect.gen(function* () {
        const { a, c, side } = yield* history;
        const result = yield* answer(v2Request([`want ${c}`, `want ${side}`, `have ${a}`]));
        return { a, ...result };
      }),
    );
    assert.deepEqual(lines, ["acknowledgments", `ACK ${a}`]);
    assert.equal(sawPack, false);
  });

  it("v2: says ready and streams the pack once the wants are covered", async () => {
    const { a, lines } = await scenario(
      Effect.gen(function* () {
        const { a, c } = yield* history;
        const result = yield* answer(v2Request([`want ${c}`, `have ${a}`]));
        return { a, ...result };
      }),
    );
    assert.deepEqual(lines.slice(0, 4), ["acknowledgments", `ACK ${a}`, "ready", "packfile"]);
  });
});
