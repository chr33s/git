/** A 40-character lowercase hexadecimal git object id. */
export type Oid = string & { readonly Oid: unique symbol };

/** The one boundary that earns the `Oid` brand. */
export const isOid = (value: string): value is Oid => /^[0-9a-f]{40}$/.test(value);

/**
 * The object format this repository writes, as a protocol record spells it.
 *
 * Every serialized record here qualifies the oids it names — `sha1:<hex>` —
 * even though only one format is implemented, because the point of qualifying
 * is that the payloads do not have to change when a second one arrives.
 */
export const ALGORITHM = "sha1";

/** An object id as a payload spells one: `sha1:<hex>`. */
export const qualify = (oid: Oid): string => `${ALGORITHM}:${oid}`;

/**
 * The oid a qualified spelling names, or `null`.
 *
 * `null` covers both a malformed value and one qualified with an algorithm
 * this repository does not use: neither names an object here, and a caller
 * that needs to tell them apart is asking about a repository it is not in.
 */
export const unqualify = (value: string): Oid | null => {
  const separator = value.indexOf(":");
  if (separator === -1) return null;
  const algorithm = value.slice(0, separator);
  const hex = value.slice(separator + 1);
  if (algorithm !== ALGORITHM || !isOid(hex)) return null;
  return hex;
};
