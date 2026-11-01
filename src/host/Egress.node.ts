import { lookup } from "node:dns/promises";
import type { LookupAddress } from "node:dns";
import { isIP, type LookupFunction } from "node:net";
import { Agent, buildConnector, fetch as fetchWithDispatcher } from "undici";
import { Config, Effect } from "effect";
import { Invalid } from "../git/Error.ts";
import { publicAddress } from "../server/EgressAddress.ts";
import { commaList } from "../text.ts";

export type Resolve = (hostname: string) => Promise<ReadonlyArray<LookupAddress>>;
const resolve: Resolve = (hostname) => lookup(hostname, { all: true, verbatim: true });
const denied = () => new Error("outbound network policy denied the destination");
const loopback = (address: string) =>
  address === "::1" || (isIP(address) === 4 && address.startsWith("127."));
export const loopbackConfiguration = Config.Boolean("GIT_EGRESS_ALLOW_LOOPBACK").pipe(
  Config.withDefault(false),
);

const requestBytes = async function* (body: ReadableStream<Uint8Array>): AsyncIterable<Uint8Array> {
  const reader = body.getReader();
  try {
    for (;;) {
      const next = await reader.read();
      if (next.done) return;
      yield next.value;
    }
  } finally {
    void reader.cancel().catch(() => undefined);
    reader.releaseLock();
  }
};

/** Keep a native Response (and native clones) at the portable boundary. */
class CheckedResponse extends Response {
  readonly #target: string;
  readonly #followed: boolean;
  constructor(body: BodyInit | null, init: ResponseInit, target: string, followed: boolean) {
    super(body, init);
    this.#target = target;
    this.#followed = followed;
  }
  override get url() {
    return this.#target;
  }
  override get redirected() {
    return this.#followed;
  }
  override get type(): ResponseType {
    return "basic";
  }
  override clone(): Response {
    const copy = super.clone();
    return new CheckedResponse(
      copy.body,
      { status: copy.status, statusText: copy.statusText, headers: copy.headers },
      this.#target,
      this.#followed,
    );
  }
}

export const origins = (values: ReadonlyArray<string>): ReadonlySet<string> =>
  new Set(
    values.map((value) => {
      const url = new URL(value);
      if (
        (url.protocol !== "https:" && url.protocol !== "http:") ||
        url.pathname !== "/" ||
        url.search !== "" ||
        url.hash !== "" ||
        url.username !== "" ||
        url.password !== "" ||
        url.hostname.includes("*")
      )
        throw denied();
      return url.origin;
    }),
  );

export const configuration = Effect.fn("host.Egress.configuration")(function* () {
  const raw = yield* Config.String("GIT_EGRESS_PRIVATE_ORIGINS").pipe(Config.withDefault(""));
  return yield* Effect.try({
    try: () => origins(commaList(raw)),
    catch: () =>
      new Invalid({
        field: "GIT_EGRESS_PRIVATE_ORIGINS",
        reason:
          "expected comma-separated HTTP(S) origins without credentials, paths, queries, or fragments",
      }),
  });
});

/** DNS validation happens in the socket lookup callback, not at registration. */
export interface AddressPolicy {
  readonly allowPrivate: boolean;
  readonly allowLoopback?: boolean;
  readonly onlyLoopback?: boolean;
}

const permitsAddress = (policy: AddressPolicy, address: string): boolean => {
  if (policy.allowPrivate) return true;
  if (policy.onlyLoopback) return loopback(address);
  return publicAddress(address) || (policy.allowLoopback === true && loopback(address));
};

export const checkedLookup =
  (policy: AddressPolicy, resolver: Resolve = resolve): LookupFunction =>
  (hostname, options, callback) => {
    void resolver(hostname).then(
      (records) => {
        if (
          records.length === 0 ||
          records.some(
            (record) =>
              isIP(record.address) !== record.family || !permitsAddress(policy, record.address),
          )
        ) {
          callback(denied(), []);
          return;
        }
        const matching = records.filter(
          (record) => !options.family || options.family === record.family,
        );
        const first = matching[0];
        if (first === undefined) callback(denied(), []);
        else if (options.all) callback(null, [...matching]);
        else callback(null, first.address, first.family);
      },
      () => callback(denied(), []),
    );
  };

export const make = (
  privateOrigins: ReadonlySet<string>,
  settings?: { readonly resolve?: Resolve; readonly allowLoopback?: boolean },
) => {
  const resolver = settings?.resolve ?? resolve;
  const allowsHttp = (url: URL) =>
    privateOrigins.has(url.origin) ||
    (settings?.allowLoopback === true &&
      (url.hostname === "localhost" || loopback(url.hostname.replace(/^\[|\]$/g, ""))));
  const dispatcher = new Agent({
    connections: 4,
    maxOrigins: 128,
    connect: (options, callback) => {
      const hostname = options.hostname.replace(/^\[|\]$/g, "");
      const host = hostname.includes(":") ? `[${hostname}]` : hostname;
      const origin = new URL(
        `${options.protocol}//${host}:${options.port || (options.protocol === "https:" ? "443" : "80")}`,
      ).origin;
      const allowPrivate = privateOrigins.has(origin);
      const policy: AddressPolicy = {
        allowPrivate,
        allowLoopback: settings?.allowLoopback === true,
        onlyLoopback: options.protocol === "http:" && !allowPrivate,
      };
      if (isIP(hostname) !== 0 && !permitsAddress(policy, hostname)) {
        callback(denied(), null);
        return;
      }
      buildConnector({
        lookup: checkedLookup(policy, resolver),
        timeout: 10_000,
      })(options, callback);
    },
  });

  const transport: typeof globalThis.fetch = async (input, init) => {
    let request = new Request(input, init);
    const initial = new URL(request.url);
    for (let redirects = 0; redirects <= 5; redirects++) {
      const url = new URL(request.url);
      if (
        url.username !== "" ||
        url.password !== "" ||
        (url.protocol !== "https:" && !(url.protocol === "http:" && allowsHttp(url)))
      )
        throw denied();
      // Only GET/HEAD discovery redirects can be followed. Streaming writes
      // must never be implicitly replayed or retained in memory for a retry.
      const follows =
        request.redirect === "follow" && (request.method === "GET" || request.method === "HEAD");
      const response = await fetchWithDispatcher(request.url, {
        method: request.method,
        headers: Array.from(request.headers),
        body: request.body === null ? undefined : requestBytes(request.body),
        duplex: "half",
        signal: request.signal,
        dispatcher,
        redirect: follows ? "manual" : request.redirect === "follow" ? "error" : request.redirect,
        window: null,
      });
      // SAFETY: Undici's Node response body is a native Web byte stream; only
      // the Node/Workers stream declarations differ at this platform boundary.
      const body = response.body as ReadableStream<Uint8Array> | null;
      const webResponse = new CheckedResponse(
        body,
        {
          status: response.status,
          statusText: response.statusText,
          headers: Array.from(response.headers),
        },
        response.url,
        redirects > 0,
      );
      if (!follows || ![301, 302, 303, 307, 308].includes(response.status)) return webResponse;
      const location = response.headers.get("location");
      if (location === null) return webResponse;
      void response.body?.cancel().catch(() => undefined);
      const next = new URL(location, request.url);
      if (next.origin !== initial.origin || redirects === 5) throw denied();
      request = new Request(next, {
        method: request.method,
        headers: request.headers,
        signal: request.signal,
        redirect: "follow",
      });
    }
    throw denied();
  };
  return { fetch: transport, close: () => dispatcher.destroy() };
};
