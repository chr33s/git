/**
 * Conservative public-unicast classification. Special-purpose ranges are
 * denied as whole blocks, including their globally reachable exceptions.
 * Sources: IANA IPv4/IPv6 Special-Purpose Address Registries (2025-10-09).
 * Translation/tunneling prefixes are not ordinary public IPv6 destinations.
 */
export const publicAddress = (address: string): boolean => {
  if (!address.includes(":")) {
    const parts = address.split(".");
    if (
      parts.length !== 4 ||
      parts.some(
        (part) => !/^\d+$/.test(part) || String(Number(part)) !== part || Number(part) > 255,
      )
    )
      return false;
    const [a = 0, b = 0, c = 0] = parts.map(Number);
    return !(
      a === 0 ||
      a === 10 ||
      a === 127 ||
      a >= 224 ||
      (a === 100 && b >= 64 && b <= 127) ||
      (a === 169 && b === 254) ||
      (a === 172 && b >= 16 && b <= 31) ||
      (a === 192 && (b === 168 || (b === 0 && (c === 0 || c === 2)) || (b === 88 && c === 99))) ||
      (a === 198 && (b === 18 || b === 19 || (b === 51 && c === 100))) ||
      (a === 203 && b === 0 && c === 113)
    );
  }
  try {
    const normalized = new URL(`https://[${address}]`).hostname.slice(1, -1);
    const [first = "", second = ""] = normalized.split(":");
    const a = Number.parseInt(first || "0", 16);
    const b = Number.parseInt(second || "0", 16);
    // Only 2000::/3; this also excludes mapped, NAT64, unique-local,
    // link-local, multicast, unspecified, and loopback addresses.
    return (
      a >= 0x2000 &&
      a < 0x4000 &&
      !(a === 0x2001 && (b < 0x200 || b === 0xdb8)) &&
      a !== 0x2002 &&
      !(a === 0x3fff && b < 0x1000)
    );
  } catch {
    return false;
  }
};
