/**
 * Small parsing helpers with no home of their own. Its own module because the
 * CLI and the host configuration both need them, and either importing the
 * other for a string helper would invert the layering.
 */

/**
 * A comma-separated flag or environment value as a list.
 *
 * One helper because several of these exist — capabilities, social scopes,
 * extra root keys, trusted hosts — and each had grown its own
 * `split(",").map(trim)`, already drifted over whether a blank entry survives.
 * It does not: `""` is no list rather than a list of one nothing, `"a,,b"` is
 * two, and a trailing comma is a typo rather than an empty member.
 *
 * Commas and nothing else, never whitespace: some of these lists name files,
 * and a path may contain a space.
 */
export const commaList = (raw: string): ReadonlyArray<string> =>
  raw
    .split(",")
    .map((value) => value.trim())
    .filter((value) => value !== "");

/**
 * Loopback hostnames, spelled the way `URL.hostname` returns them — the IPv6
 * literal keeps its brackets.
 */
export const LOOPBACK: ReadonlySet<string> = new Set(["localhost", "127.0.0.1", "[::1]"]);

/**
 * Why this server may not be pointed at `raw`, or `null`.
 *
 * One reading because two registries hand this server an address to call —
 * `server/Remotes.ts` and `server/Subscribers.ts` — with identical
 * requirements and two implementations that had already drifted apart in both
 * directions. The webhook side exempted the *whole scheme* when the hostname
 * was `localhost`, so `file://localhost/…` validated, while refusing the
 * `127.0.0.1` and `[::1]` spellings of the same machine that the remote side
 * accepts. And only the remote side refused userinfo, though both hand the URL
 * straight back out of a list endpoint — which is what storing a credential in
 * its own field is for.
 *
 * Loopback http is how a test, and a developer, reaches the server next to
 * them; everything else on the network is https or nothing.
 *
 * `secretField` names where a credential belongs for a caller that has such a
 * field, so the refusal can say where to put it rather than only where not to.
 */
export const outboundUrl = (raw: string, secretField?: string): string | null => {
  let parsed: URL;
  try {
    parsed = new URL(raw);
  } catch {
    return `not a URL: '${raw}'`;
  }
  if (
    parsed.protocol !== "https:" &&
    !(parsed.protocol === "http:" && LOOPBACK.has(parsed.hostname))
  ) {
    return "must be https, or http to a loopback address (localhost, 127.0.0.1, [::1])";
  }
  if (parsed.username !== "" || parsed.password !== "") {
    return secretField === undefined
      ? "a URL carrying credentials is handed back by every read path; keep the secret out of it"
      : `credentials belong in '${secretField}', not in the URL`;
  }
  return null;
};
