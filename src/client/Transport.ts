import { Context, Effect, Option } from "effect";

/** Hosts supply network policy; browser and CLI library callers use native fetch. */
export class Transport extends Context.Service<
  Transport,
  { readonly fetch: typeof globalThis.fetch }
>()("client/Transport") {}

export const current = Effect.serviceOption(Transport).pipe(
  Effect.map((service) =>
    Option.match(service, {
      onNone: () => globalThis.fetch,
      onSome: (transport) => transport.fetch,
    }),
  ),
);
