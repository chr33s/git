/**
 * What a session record may not carry.
 *
 * Prompts are where credentials leak: a token pasted into an instruction, a
 * connection string in a traceback, an internal host in an aside. Redaction
 * (hub §21) is the way back, and it is recovery rather than hygiene — a
 * tombstone reaches every replica that syncs, but only after the bytes already
 * went there.
 *
 * So the cheap, unambiguous layers run before the record is accepted. They are
 * deliberately the ones with no judgement in them: a high-entropy string in a
 * credential-shaped place, a password inside a URL, a connection string, a
 * provider's own token prefix. Anything needing judgement — names, addresses,
 * a repository's own house patterns — belongs in the rules a repository
 * publishes, not in a list compiled here (docs/agents.md §11).
 *
 * It is heuristic and says so. What keeps a transcript out of a canonical
 * record is the distillation rule and the size bound; this catches the
 * accident on top of that, and catching most of them is worth more than
 * catching none.
 */

/** Where a secret was found, and what kind it looked like. */
export interface Finding {
  readonly kind: string;
  /** The matched text, shortened: a refusal must not repeat the secret. */
  readonly hint: string;
}

/**
 * Shannon entropy per character, which is what separates a token from prose.
 *
 * English runs around 3 bits and base64 secrets well above 4. The threshold is
 * high enough that ordinary words, paths and identifiers stay under it.
 */
const entropy = (value: string): number => {
  const counts = new Map<string, number>();
  for (const character of value) counts.set(character, (counts.get(character) ?? 0) + 1);

  let bits = 0;
  for (const count of counts.values()) {
    const share = count / value.length;
    bits -= share * Math.log2(share);
  }
  return bits;
};

/** Enough of a match to name it, and not enough to repeat it. */
const hintOf = (value: string): string =>
  value.length <= 12 ? `${value.slice(0, 4)}…` : `${value.slice(0, 8)}…${value.slice(-2)}`;

/**
 * Token prefixes providers publish precisely so scanners can find them.
 *
 * Named rather than pattern-guessed: a prefix its issuer documents is the one
 * signal here that has no false positives worth arguing about.
 */
const PREFIXES = [
  "ghp_",
  "gho_",
  "ghs_",
  "github_pat_",
  "sk-ant-",
  "sk-",
  "xoxb-",
  "xoxp-",
  "AKIA",
  "sbp_",
  "sb_secret_",
  "glpat-",
  "npm_",
];

/** A credential inside a URL: `scheme://user:secret@host`. */
const CREDENTIALED_URI = /\b[a-z][a-z0-9+.-]*:\/\/[^\s/:@]+:([^\s/@]{4,})@/gi;

/**
 * `KEY=value` and `"key": "value"`, where the key says what the value is.
 *
 * The closing quote is optional and matched *before* the separator, because a
 * JSON key wears one: `"api_key": "…"` is the shape a leaked config is pasted
 * in, and a pattern that only knew `KEY=value` walked straight past it.
 */
const NAMED_SECRET =
  /\b(?:pass(?:word|wd)?|secret|token|api[_-]?key|access[_-]?key|private[_-]?key)\b["']?\s*[:=]\s*["']?([^\s"',;]{8,})/gi;

/** Connection strings, which carry their credentials inline by design. */
const CONNECTION = /\b(?:postgres(?:ql)?|mysql|mongodb(?:\+srv)?|redis|amqp|jdbc):\/\/\S+:\S+@/gi;

/** Anything long, dense and unbroken enough to be a key rather than a word. */
const DENSE = /\b[A-Za-z0-9+/_=-]{32,}\b/g;

/**
 * What one payload trips, in the order a reader meets it.
 *
 * Every layer here is always on, because every layer here is a fact rather
 * than an opinion. The opinionated ones — PII, a repository's own patterns —
 * are configuration, and configuration belongs in `refs/meta/policy` where
 * every replica applies the same rules.
 */
/**
 * Whether the value being scanned is an identifier somebody else minted.
 *
 * Every pattern rule still applies — a `ghp_` prefix is a token wherever it is
 * put — but the entropy rule does not, because an opaque base62 id of
 * thirty-two characters is indistinguishable from a token by entropy alone.
 * Scanned as prose, an OTel trace id or a provider's conversation id refused
 * the whole record, with no override and nothing written, and it did so for
 * some providers and not others: hex- and UUID-shaped ids stay under the
 * threshold and base62 ones do not.
 *
 * Named here rather than in either caller so both hold the same line.
 * `telemetry/Records.record` and `context/Exposure.expose` each write to an
 * append-only ref that this version cannot rewind, and each had its own idea
 * of which fields this covers.
 */
export interface Reading {
  readonly opaque?: boolean;
}

export const scan = (text: string, reading: Reading = {}): ReadonlyArray<Finding> => {
  const found: Array<Finding> = [];
  const seen = new Set<string>();

  const note = (kind: string, value: string) => {
    if (seen.has(value)) return;
    seen.add(value);
    found.push({ kind, hint: hintOf(value) });
  };

  for (const prefix of PREFIXES) {
    // *Every* occurrence, not the first. A prompt that names the prefix in
    // prose — "AWS keys start with AKIA. Mine is AKIA…" — put a bare word at
    // the first position, and a scan that stopped there walked straight past
    // the key beside it. Nothing else covers a 20-character token: `DENSE`
    // wants 32, and `NAMED_SECRET` wants a `key=` in front.
    for (let at = text.indexOf(prefix); at >= 0; at = text.indexOf(prefix, at + 1)) {
      const rest = text.slice(at).split(/[\s"',;]/)[0] ?? prefix;
      // The prefix alone is a word — `sk-` appears in prose — so it counts
      // only with a body behind it long enough to be the token it announces.
      if (rest.length >= prefix.length + 8) note("provider token", rest);
    }
  }

  for (const [, secret] of text.matchAll(CREDENTIALED_URI)) {
    if (secret !== undefined) note("credential in a URL", secret);
  }
  for (const [match] of text.matchAll(CONNECTION)) note("connection string", match);
  for (const [, secret] of text.matchAll(NAMED_SECRET)) {
    // A named value that is plainly a placeholder is what a prompt about
    // configuration looks like, and refusing those would make this useless for
    // the conversations that most need recording.
    if (secret !== undefined && entropy(secret) > 3 && !/^[<${]/.test(secret)) {
      note("named credential", secret);
    }
  }
  for (const [match] of text.matchAll(DENSE)) {
    if (reading.opaque !== true && entropy(match) > 4.5) note("high-entropy string", match);
  }

  return found;
};
