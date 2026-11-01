/** Private backup transport framing, not an authenticated or complete backup. */
import { parse, stringify } from "devalue";
import { Result, Schema } from "effect";

export const codec = "devalue@6.0.3";
export const frameBytes = 4 * 1024 * 1024;
const Envelope = Schema.Struct({
  schemaVersion: Schema.Literal(1),
  codec: Schema.Literal(codec),
  payload: Schema.String,
});
const encoder = new TextEncoder();
const bounded = (frame: string) => {
  if (frame.length > frameBytes || encoder.encode(frame).byteLength > frameBytes)
    throw new Error("Cloudflare backup frame exceeds byte budget");
};

export const encodeBackupFrame = <A>(value: A): string => {
  let payload: string;
  try {
    payload = stringify(value);
  } catch {
    throw new Error("Cloudflare backup value cannot be encoded");
  }
  const frame = JSON.stringify({ schemaVersion: 1, codec, payload });
  bounded(frame);
  return frame;
};

export const decodeBackupFrame = <A, I>(frame: string, schema: Schema.Codec<A, I>): A => {
  bounded(frame);
  const result = Schema.decodeResult(Schema.fromJsonString(Envelope))(frame);
  if (Result.isFailure(result))
    throw new Error("Invalid Cloudflare backup envelope or codec version");
  try {
    return Schema.decodeUnknownSync(schema)(parse(result.success.payload));
  } catch {
    throw new Error("Invalid Cloudflare backup payload");
  }
};
