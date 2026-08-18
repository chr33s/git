/**
 * Relative time, for the "2h ago" stamps the design uses throughout.
 *
 * The fixtures carry those strings literally ("20h ago", "1w ago"); anything
 * read from the server carries a real `Date` instead, and this is what turns
 * one into the other so both look the same on screen.
 */

interface Unit {
  readonly seconds: number;
  readonly short: string;
}

/** Coarsest first, so the first match is the one to use. */
const UNITS: readonly Unit[] = [
  { seconds: 604800, short: "w" },
  { seconds: 86400, short: "d" },
  { seconds: 3600, short: "h" },
  { seconds: 60, short: "m" },
];

/**
 * `at` relative to now, in the design's shorthand.
 *
 * Anything under a minute reads "just now" rather than "0m ago", and a future
 * timestamp — a commit with a skewed clock — reads "just now" too rather than
 * a negative age.
 */
export const ago = (at: Date, now = new Date()): string => {
  const seconds = Math.floor((now.getTime() - at.getTime()) / 1000);
  if (seconds < 60) return "just now";
  for (const unit of UNITS) {
    if (seconds >= unit.seconds) {
      return `${String(Math.floor(seconds / unit.seconds))}${unit.short} ago`;
    }
  }
  return "just now";
};

/** `Maya Kessler` → `MK`, for the avatar circles. */
export const initials = (name: string): string => {
  const words = name
    .trim()
    .split(/\s+/)
    .filter((word) => word.length > 0);
  const first = words[0];
  if (first === undefined) return "··";
  const last = words.length > 1 ? words[words.length - 1] : undefined;
  const second = last === undefined ? first.slice(1, 2) : last.slice(0, 1);
  return (first.slice(0, 1) + second).toUpperCase();
};

/** Midnight local, for bucketing commits into calendar days. */
export const startOfDay = (at: Date): Date =>
  new Date(at.getFullYear(), at.getMonth(), at.getDate());

/** Whole days between two midnights. */
export const daysBetween = (from: Date, to: Date): number =>
  Math.round((startOfDay(to).getTime() - startOfDay(from).getTime()) / 86400000);
