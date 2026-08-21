/** Typed bech32m encodings for PrincipalIDs and RepoIDs. */
import { Result } from "effect";

import { Invalid } from "../git/Error.ts";
import { isRepoId, type RepoId } from "../trust/Genesis.ts";
import { principalId, type PrincipalId } from "../trust/Principal.ts";

const CHARSET = "qpzry9x8gf2tvdw0s3jn54khce6mua7l";
const BECH32M = 0x2bc830a3;
const generators = [0x3b6a57b2, 0x26508e6d, 0x1ea119fa, 0x3d4233dd, 0x2a1462b3];
const text = new TextEncoder();
const decoder = new TextDecoder();

const invalid = (reason: string) => Result.fail(new Invalid({ field: "identifier", reason }));

const polymod = (values: ReadonlyArray<number>): number => {
  let checksum = 1;
  for (const value of values) {
    const top = checksum >>> 25;
    checksum = (((checksum & 0x1ffffff) << 5) ^ value) >>> 0;
    for (let bit = 0; bit < 5; bit++) {
      const generator = generators[bit];
      if (generator !== undefined && ((top >>> bit) & 1) !== 0) {
        checksum = (checksum ^ generator) >>> 0;
      }
    }
  }
  return checksum >>> 0;
};

const expand = (hrp: string): ReadonlyArray<number> => {
  const high = Array.from(hrp, (character) => character.charCodeAt(0) >>> 5);
  const low = Array.from(hrp, (character) => character.charCodeAt(0) & 31);
  return [...high, 0, ...low];
};

const checksum = (hrp: string, words: ReadonlyArray<number>): ReadonlyArray<number> => {
  const value = (polymod([...expand(hrp), ...words, 0, 0, 0, 0, 0, 0]) ^ BECH32M) >>> 0;
  return Array.from({ length: 6 }, (_, index) => (value >>> (5 * (5 - index))) & 31);
};

const convertBits = (
  values: ReadonlyArray<number>,
  from: number,
  to: number,
  pad: boolean,
): ReadonlyArray<number> | null => {
  let accumulator = 0;
  let bits = 0;
  const result: number[] = [];
  const maximum = (1 << to) - 1;
  const fromMaximum = (1 << from) - 1;
  for (const value of values) {
    if (value < 0 || value >>> from !== 0) return null;
    accumulator = ((accumulator << from) | (value & fromMaximum)) >>> 0;
    bits += from;
    while (bits >= to) {
      bits -= to;
      result.push((accumulator >>> bits) & maximum);
    }
  }
  if (pad) {
    if (bits > 0) result.push((accumulator << (to - bits)) & maximum);
  } else if (bits >= from || ((accumulator << (to - bits)) & maximum) !== 0) {
    return null;
  }
  return result;
};

const idBytes = (id: RepoId): Uint8Array => {
  const encoded = id.slice("SHA256:".length);
  const binary = atob(`${encoded}=`);
  return Uint8Array.from(binary, (character) => character.charCodeAt(0));
};

const idOf = (bytes: ReadonlyArray<number>): RepoId | null => {
  if (bytes.length !== 32) return null;
  let binary = "";
  for (const byte of bytes) binary += String.fromCharCode(byte);
  const value = `SHA256:${btoa(binary).replace(/=+$/, "")}`;
  return isRepoId(value) ? value : null;
};

const validHint = (hint: string): boolean => {
  try {
    const url = new URL(hint);
    return url.protocol === "https:" || url.protocol === "http:";
  } catch {
    return false;
  }
};

const encode = (
  hrp: "gid" | "grepo",
  id: RepoId,
  hints: ReadonlyArray<string>,
): Result.Result<string, Invalid> => {
  if (hints.length > 4) return invalid("an identifier carries at most four location hints");

  const payload: number[] = [0, 32, ...idBytes(id)];
  for (const hint of hints) {
    if (!validHint(hint)) return invalid(`'${hint}' is not an HTTP URL`);
    const bytes = text.encode(hint);
    if (bytes.length > 255) return invalid("a location hint may be at most 255 bytes");
    payload.push(1, bytes.length, ...bytes);
  }

  const words = convertBits(payload, 8, 5, true);
  if (words === null) return invalid("identifier payload could not be encoded");
  const values = [...words, ...checksum(hrp, words)];
  return Result.succeed(`${hrp}1${values.map((value) => CHARSET[value]).join("")}`);
};

export const encodePrincipal = (input: {
  readonly id: PrincipalId;
  readonly hints?: ReadonlyArray<string>;
}): Result.Result<string, Invalid> => encode("gid", input.id, input.hints ?? []);

export const encodeRepository = (input: {
  readonly id: RepoId;
  readonly hints?: ReadonlyArray<string>;
}): Result.Result<string, Invalid> => encode("grepo", input.id, input.hints ?? []);

export type DecodedIdentifier =
  | { readonly kind: "principal"; readonly id: PrincipalId; readonly hints: ReadonlyArray<string> }
  | { readonly kind: "repository"; readonly id: RepoId; readonly hints: ReadonlyArray<string> };

export const decodeIdentifier = (encoded: string): Result.Result<DecodedIdentifier, Invalid> => {
  if (encoded.length > 2048) return invalid("identifier is too long");
  if (encoded !== encoded.toLowerCase() && encoded !== encoded.toUpperCase()) {
    return invalid("identifier mixes letter case");
  }
  const source = encoded.toLowerCase();
  const separator = source.lastIndexOf("1");
  if (separator <= 0 || separator + 7 > source.length) return invalid("identifier is not bech32m");
  const hrp = source.slice(0, separator);
  if (hrp !== "gid" && hrp !== "grepo") return invalid(`unknown identifier type '${hrp}'`);

  const values: number[] = [];
  for (const character of source.slice(separator + 1)) {
    const value = CHARSET.indexOf(character);
    if (value === -1) return invalid(`'${character}' is not a bech32 character`);
    values.push(value);
  }
  if (polymod([...expand(hrp), ...values]) !== BECH32M) {
    return invalid("identifier checksum does not match");
  }

  const bytes = convertBits(values.slice(0, -6), 5, 8, false);
  if (bytes === null) return invalid("identifier has invalid bit padding");

  let id: RepoId | null = null;
  const hints: string[] = [];
  for (let at = 0; at < bytes.length;) {
    const type = bytes[at++];
    const length = bytes[at++];
    if (type === undefined || length === undefined || at + length > bytes.length) {
      return invalid("identifier has a truncated TLV field");
    }
    const value = bytes.slice(at, at + length);
    at += length;
    if (type === 0) {
      if (id !== null) return invalid("identifier carries more than one identity");
      id = idOf(value);
      if (id === null) return invalid("identifier carries a malformed SHA-256 identity");
      continue;
    }
    if (type === 1) {
      const hint = decoder.decode(Uint8Array.from(value));
      if (!validHint(hint)) return invalid("identifier carries a malformed location hint");
      hints.push(hint);
    }
  }
  if (id === null) return invalid("identifier carries no identity");
  if (hints.length > 4) return invalid("identifier carries more than four location hints");

  return hrp === "gid"
    ? Result.succeed({ kind: "principal", id: principalId(id), hints })
    : Result.succeed({ kind: "repository", id, hints });
};
