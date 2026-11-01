import { Config, Effect } from "effect";
import { Invalid } from "../git/Error.ts";
import { commaList } from "../text.ts";

const denied = () => new Error("outbound network policy denied the destination");

/** These are trusted operator destinations, not a DNS or address-space filter. */
export const origins = (values: ReadonlyArray<string>): ReadonlySet<string> =>
  new Set(
    values.map((value) => {
      const url = new URL(value);
      if (
        url.protocol !== "https:" ||
        url.pathname !== "/" ||
        url.search !== "" ||
        url.hash !== "" ||
        url.username !== "" ||
        url.password !== "" ||
        url.hostname.includes("*") ||
        url.hostname.includes(":") ||
        /^[\d.]+$/.test(url.hostname) ||
        url.hostname === "localhost" ||
        url.hostname.endsWith(".localhost")
      )
        throw denied();
      return url.origin;
    }),
  );

export const configuration = Effect.fn("host.CloudflareEgress.configuration")(function* () {
  const raw = yield* Config.String("GIT_EGRESS_TRUSTED_ORIGINS").pipe(Config.withDefault(""));
  return yield* Effect.try({
    try: () => origins(commaList(raw)),
    catch: () =>
      new Invalid({
        field: "GIT_EGRESS_TRUSTED_ORIGINS",
        reason: "expected exact HTTPS hostname origins without credentials, paths, or wildcards",
      }),
  });
});

/**
 * Workers fetch cannot pin checked DNS answers to connections. Only destinations
 * whose DNS/routing the operator trusts may be enabled; none are enabled by default.
 */
export const make = (
  trustedOrigins: ReadonlySet<string>,
  transport: typeof globalThis.fetch = globalThis.fetch,
): typeof globalThis.fetch => {
  // Copy and validate even programmatic configuration; later mutations cannot
  // extend the policy of an already initialized host.
  const allowed = origins([...trustedOrigins]);
  return async (input, init) => {
    const request = new Request(input, init);
    let target = new URL(request.url);
    const origin = target.origin;
    if (!allowed.has(origin) || target.username !== "" || target.password !== "") throw denied();
    const read = request.method === "GET" || request.method === "HEAD";
    for (let redirects = 0; ; redirects++) {
      // Always disable native redirect following, including for signed retries
      // and streaming writes. Every subsequent destination is checked here.
      const response = await transport(new Request(target, request), { redirect: "manual" });
      if (![301, 302, 303, 307, 308].includes(response.status)) return response;
      if (request.redirect === "manual") return response;
      // Release the old response without waiting for an untrusted peer's cleanup.
      void response.body?.cancel().catch(() => undefined);
      if (!read || request.redirect === "error" || redirects >= 5) throw denied();
      const location = response.headers.get("location");
      if (location === null) throw denied();
      target = new URL(location, target);
      if (target.origin !== origin || target.username !== "" || target.password !== "")
        throw denied();
    }
  };
};
