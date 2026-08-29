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
