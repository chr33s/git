/** Private durable handoff state; temporary files publish atomically after fsync. */
import * as fs from "node:fs";
import * as path from "node:path";
import { randomUUID } from "node:crypto";
import { Effect, Schema } from "effect";
import { Invalid } from "../git/Error.ts";
import * as Capture from "../hub/SessionCapture.ts";
import * as Session from "../hub/Session.ts";
import * as LocalLease from "../host/LocalLease.node.ts";

const file = <A>(operation: () => A) =>
  Effect.try({
    try: operation,
    catch: (cause) => new Invalid({ field: "handoff", reason: String(cause) }),
  });

export const read = Effect.fn("cli.SessionHandoff.read")(function* (location: string) {
  if (!(yield* file(() => fs.existsSync(location)))) return null;
  const text = yield* file(() => {
    if (fs.statSync(location).size > Session.MAX_PAYLOAD + 16 * 1024)
      throw new Error("handoff exceeds its byte limit");
    return fs.readFileSync(location, "utf8");
  });
  return yield* Schema.decodeEffect(Schema.fromJsonString(Capture.Prepared))(text).pipe(
    Effect.mapError(
      (cause) =>
        new Invalid({
          field: "handoff",
          reason: `invalid or unsupported handoff: ${cause.message}`,
        }),
    ),
  );
});

const publish = Effect.fn("cli.SessionHandoff.publish")(function* (
  location: string,
  contents: string,
) {
  yield* file(() => {
    const temporary = `${location}.${randomUUID()}.tmp`;
    let published = false;
    try {
      const descriptor = fs.openSync(temporary, "wx", 0o600);
      try {
        fs.writeFileSync(descriptor, contents);
        fs.fsyncSync(descriptor);
      } finally {
        fs.closeSync(descriptor);
      }
      fs.renameSync(temporary, location);
      published = true;
      const directory = fs.openSync(path.dirname(location), "r");
      try {
        fs.fsyncSync(directory);
      } finally {
        fs.closeSync(directory);
      }
    } finally {
      if (!published) fs.rmSync(temporary, { force: true });
    }
  });
});

export const write = Effect.fn("cli.SessionHandoff.write")(function* (
  location: string,
  prepared: Capture.Prepared,
) {
  yield* publish(location, JSON.stringify(prepared));
});
export const writeState = Effect.fn("cli.SessionHandoff.writeState")(function* (
  location: string,
  session: string,
) {
  yield* publish(location, session);
});

/** Separate from wake/maintenance locks; contending harness deliveries retry. */
export const withLock = Effect.fn("cli.SessionHandoff.withLock")(function* <A, E, R>(
  location: string,
  effect: Effect.Effect<A, E, R>,
) {
  return yield* LocalLease.withLock(`${location}.lock`, effect);
});
