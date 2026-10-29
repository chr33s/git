/** Source-anchor discovery and fingerprinting, kept behind a portable port. */
import { Context, Effect, Layer, Schema } from "effect";

import { AnchorResolutionFailure } from "../git/Error.ts";

export const FILE_ANCHOR = "@file";

/**
 * How many lines the text holds.
 *
 * `split("\n").length` counts the empty segment a trailing newline leaves, so
 * every normally-terminated file reported one line too many.
 */
export const countLines = (text: string): number =>
  Math.max(1, text.split("\n").length - (text.endsWith("\n") ? 1 : 0));

export const Anchor = Schema.Struct({
  value: Schema.String,
  startLine: Schema.Int,
  endLine: Schema.Int,
});
export interface Anchor extends Schema.Schema.Type<typeof Anchor> {}

export const Fingerprint = Schema.Struct({
  resolver: Schema.String,
  normalization: Schema.String,
  signatureHash: Schema.NullOr(Schema.String),
  contentHash: Schema.String,
  rawHash: Schema.String,
});
export interface Fingerprint extends Schema.Schema.Type<typeof Fingerprint> {}

export type AnchorResolution =
  | { readonly _tag: "Found"; readonly anchor: Anchor; readonly fingerprint: Fingerprint }
  | { readonly _tag: "Missing" }
  | { readonly _tag: "Unsupported" }
  | { readonly _tag: "Ambiguous"; readonly candidates: ReadonlyArray<Anchor> };

export class AnchorResolver extends Context.Service<
  AnchorResolver,
  {
    readonly name: string;
    readonly version: string;
    readonly anchors: (
      path: string,
      content: Uint8Array,
    ) => Effect.Effect<ReadonlyArray<Anchor>, AnchorResolutionFailure>;
    readonly resolve: (
      path: string,
      content: Uint8Array,
      anchor: string,
    ) => Effect.Effect<AnchorResolution, AnchorResolutionFailure>;
  }
>()("hub/AnchorResolver") {}

const hex = (bytes: Uint8Array): string =>
  [...bytes].map((byte) => byte.toString(16).padStart(2, "0")).join("");

/** SHA-256 over bytes already resident as one source region. */
export const hash = (bytes: Uint8Array): Effect.Effect<string> =>
  Effect.promise(async () =>
    hex(new Uint8Array(await crypto.subtle.digest("SHA-256", bytes.slice().buffer))),
  );

export const fingerprint = Effect.fn("hub.Anchor.fingerprint")(function* (input: {
  readonly resolver: string;
  readonly normalization: string;
  readonly raw: Uint8Array;
  readonly content: Uint8Array;
  readonly signature?: Uint8Array | null;
}) {
  return {
    resolver: input.resolver,
    normalization: input.normalization,
    signatureHash:
      input.signature === undefined || input.signature === null
        ? null
        : yield* hash(input.signature),
    contentHash: yield* hash(input.content),
    rawHash: yield* hash(input.raw),
  } satisfies Fingerprint;
});

/** Minimum resolver: every source type has a byte-exact whole-file anchor. */
export const file = Layer.sync(AnchorResolver, () => {
  const resolve = Effect.fn("hub.Anchor.file.resolve")(function* (
    path: string,
    content: Uint8Array,
    anchor: string,
  ) {
    if (anchor !== FILE_ANCHOR) return { _tag: "Unsupported" } as const;
    const lines = countLines(new TextDecoder().decode(content));
    return {
      _tag: "Found",
      anchor: { value: FILE_ANCHOR, startLine: 1, endLine: lines },
      fingerprint: yield* fingerprint({
        resolver: "file@1",
        normalization: "exact-v1",
        raw: content,
        content,
      }),
    } as const;
  });

  return AnchorResolver.of({
    name: "file",
    version: "1",
    anchors: (_path, content) =>
      Effect.succeed([
        {
          value: FILE_ANCHOR,
          startLine: 1,
          endLine: countLines(new TextDecoder().decode(content)),
        },
      ]),
    resolve,
  });
});
