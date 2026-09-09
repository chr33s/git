/**
 * The anchor resolver every host deploys.
 *
 * Portable, and named for what it is rather than for where it runs. It began
 * as `Anchor.node.ts` on the assumption that a resolver is platform-bound —
 * and the cost of that assumption was a Cloudflare-served repository
 * answering `rebaseline-required` for every note the CLI had ever signed,
 * because the two ends computed `typescript-syntax@1` and `file@1` for the
 * same source. Worse than the noise: §24 makes `rebaseline-required` advisory
 * by default, so `/hub/notes/check` reported `actionable: false` and a merge
 * check went green with every real drift invisible behind it.
 *
 * There is no `node:` import here and never was one. If a resolver ever does
 * need one — tree-sitter, say — that resolver is the one that earns the
 * suffix, and the hosts that cannot load it fall back to `Anchor.file`.
 *
 * Hand-written rather than tree-sitter, and the reason is §13's `unverifiable`
 * state: a resolver that guesses at a language it cannot really read produces
 * anchors that land on the wrong region, and a wrong region reports `fresh`
 * for a constraint whose code changed underneath it. Silence is the safe
 * failure here and confidence is the dangerous one, so each language is placed
 * in one of three tiers by what can be read about it *reliably*:
 *
 *   structured  regions and normalization — symbol anchors resolve
 *   normalized  comments and formatting are identifiable, declarations are
 *               not — `@file` normalizes, every symbol anchor is Unsupported
 *   opaque      neither — `@file` is byte-exact, as `Anchor.file` gives it
 *
 * Java, C and C++ sit in the middle tier deliberately. Their comment syntax is
 * unambiguous, so §11's "ignore formatting and comments" is honest for a whole
 * file; their declarations are not something a regular expression reads
 * correctly, and a note anchored to `class Repository` in a C++ header would
 * be answering about whatever the pattern happened to match.
 */
import { Effect, Layer } from "effect";

import {
  AnchorResolver,
  FILE_ANCHOR,
  fingerprint,
  type Anchor,
  type AnchorResolution,
} from "./Anchor.ts";

const encoder = new TextEncoder();
const decoder = new TextDecoder();

interface Region extends Anchor {
  readonly start: number;
  readonly end: number;
  /**
   * Where the declaration ends and the implementation begins, or `null` where
   * the two cannot be told apart — §11's `signatureHash = null`.
   */
  readonly signatureEnd: number | null;
}

/** What a language's comments and strings look like, for the normalizer. */
interface Syntax {
  readonly line: ReadonlyArray<string>;
  readonly block: readonly [string, string] | null;
  readonly quotes: ReadonlyArray<string>;
  /** Python's triple quotes, which must not read as three empty strings. */
  readonly triple: boolean;
  /**
   * Whether `'` opens a character literal rather than a string.
   *
   * In Rust and C it does — and it is also the start of a lifetime
   * (`&'static str`) and a digit separator (`1'000`), neither of which is a
   * quote at all. Read as one, the apostrophe swallows everything to the next
   * apostrophe, which is how `fn a`'s region came to cover `fn b`.
   */
  readonly charLiteral: boolean;
  /** Characters that continue an identifier past `[A-Za-z0-9_]`. */
  readonly word: RegExp;
  /**
   * Whether `/` can open a regular-expression literal.
   *
   * Only JavaScript and TypeScript have one, and reading `/["']/` as division
   * left the quote inside it opening a string scan that ran past the
   * declaration's closing brace — which is how `function f`'s region came to
   * swallow `function g` whole.
   */
  readonly regex: boolean;
  /**
   * Whether a backtick opens a template literal with `${…}` interpolation.
   *
   * Go's backtick is a raw string and holds no code, so it stays an ordinary
   * quote. JavaScript's holds expressions, and a backtick or brace inside one
   * belongs to that expression rather than to the template — read as a plain
   * quote, `` `a ${["`"].join("")} b` `` closed at the wrong backtick.
   */
  readonly template: boolean;
}

const C_STYLE: Syntax = {
  charLiteral: false,
  line: ["//"],
  block: ["/*", "*/"],
  quotes: ['"', "'", "`"],
  triple: false,
  word: /[A-Za-z0-9_$]/,
  regex: false,
  template: false,
};

/**
 * C-family syntax plus the two constructs only JavaScript and TypeScript have.
 *
 * The backtick leaves `quotes` because `atomAt` reads it as a template rather
 * than as a quote; Go keeps it in `C_STYLE`, where a backtick really does open
 * a string that holds no code.
 */
const SCRIPT: Syntax = {
  ...C_STYLE,
  quotes: ['"', "'"],
  regex: true,
  template: true,
};

const HASH_STYLE: Syntax = {
  charLiteral: false,
  line: ["#"],
  block: null,
  quotes: ['"', "'"],
  triple: false,
  word: /[A-Za-z0-9_]/,
  regex: false,
  template: false,
};

const PYTHON: Syntax = { ...HASH_STYLE, triple: true };

/** C-family syntax where `'` is a character literal; see `Syntax.charLiteral`. */
const NATIVE: Syntax = { ...C_STYLE, charLiteral: true };

const CSS_STYLE: Syntax = {
  charLiteral: false,
  line: [],
  block: ["/*", "*/"],
  quotes: ['"', "'"],
  triple: false,
  word: /[A-Za-z0-9_-]/,
  regex: false,
  template: false,
};

const MARKUP: Syntax = {
  charLiteral: false,
  line: [],
  block: ["<!--", "-->"],
  quotes: ['"', "'"],
  triple: false,
  word: /[A-Za-z0-9_-]/,
  regex: false,
  template: false,
};

/**
 * Line numbers for a file, by offset.
 *
 * Built once and searched, rather than counted from zero per lookup. Each
 * region asks twice — its start and its end — so a 5,000-line file with 300
 * declarations ran 600 full-prefix scans of its own text to answer
 * `note anchors`, which is a read an agent makes before every edit.
 */
const linesOf = (text: string): ReadonlyArray<number> => {
  const starts = [0];
  for (let at = text.indexOf("\n"); at >= 0; at = text.indexOf("\n", at + 1)) starts.push(at + 1);
  return starts;
};

/** The 1-based line holding `offset`, by binary search over `starts`. */
const lineIn = (starts: ReadonlyArray<number>, offset: number): number => {
  let low = 0;
  let high = starts.length - 1;
  while (low < high) {
    const middle = Math.ceil((low + high) / 2);
    if ((starts[middle] ?? 0) <= offset) low = middle;
    else high = middle - 1;
  }
  return low + 1;
};

const lineEnd = (text: string, offset: number): number => {
  const found = text.indexOf("\n", offset);
  return found < 0 ? text.length : found;
};

/**
 * The index just past a comment, string or character literal starting at `at`,
 * or `null` where none does.
 *
 * One primitive rather than a state machine per scanner. There were two, they
 * had drifted, and both were wrong in the same way for the same reason: "is
 * this brace structure, or is it inside something" is one question, and
 * answering it twice means answering it differently once.
 */
interface Atom {
  readonly end: number;
  /** A comment normalizes away; a string is a token the source depends on. */
  readonly kind: "comment" | "text";
}

/**
 * Where a `'` character literal ends, or `-1` if this `'` opens none.
 *
 * `'a'` and `'\n'` are literals; `'static` is a lifetime and `1'000` a digit
 * separator, and both must read as ordinary punctuation. The difference is
 * decidable by looking at what follows, which is why it is decided here rather
 * than by a quote set that cannot express it.
 */
const characterEnd = (text: string, at: number): number => {
  let held = at + 1;
  if (text[held] === "\\") {
    held += 1;
    if (text[held] === "u" && text[held + 1] === "{") {
      const closed = text.indexOf("}", held);
      if (closed < 0) return -1;
      held = closed + 1;
    } else if (text[held] === "x") {
      held += 3;
    } else {
      held += 1;
    }
  } else {
    // One code point, so an astral character counts as the single char it is.
    held += text.slice(held, held + 2)[0]?.length ?? 1;
  }
  return text[held] === "'" ? held + 1 : -1;
};

/**
 * Whether the `/` at `at` opens a regular-expression literal.
 *
 * Decided by what precedes it, which is the only thing that separates `/re/`
 * from division without parsing the whole expression. A value ends the token
 * before a division — an identifier, a number, a closing bracket, a string —
 * and everything else leaves the parser in expression position, where a `/`
 * can only be a regex.
 *
 * Wrong in the regex direction costs at most one line, because `regexEnd`
 * refuses to cross a newline. Wrong in the division direction is the bug this
 * exists to fix, so the doubtful cases are read as regexes.
 */
const REGEX_KEYWORDS = new Set([
  "return",
  "typeof",
  "instanceof",
  "in",
  "of",
  "new",
  "delete",
  "void",
  "case",
  "do",
  "else",
  "yield",
  "await",
  "throw",
]);

const opensRegex = (text: string, at: number): boolean => {
  let back = at - 1;
  while (back >= 0 && /\s/.test(text[back] ?? "")) back--;
  if (back < 0) return true;
  const char = text[back] ?? "";
  if (/[A-Za-z0-9_$]/.test(char)) {
    let from = back;
    while (from >= 0 && /[A-Za-z0-9_$]/.test(text[from] ?? "")) from--;
    return REGEX_KEYWORDS.has(text.slice(from + 1, back + 1));
  }
  return !")]".includes(char);
};

/**
 * The index past a regex literal, or `-1` where the `/` opens none.
 *
 * A `[…]` class holds a literal `/`, so the delimiter only closes outside one.
 * A literal cannot span a line, and refusing to is what keeps a misread
 * division from swallowing the rest of a declaration.
 */
const regexEnd = (text: string, at: number): number => {
  let held = at + 1;
  let escaped = false;
  let klass = false;
  while (held < text.length) {
    const seen = text[held] ?? "";
    if (seen === "\n") return -1;
    held += 1;
    if (escaped) escaped = false;
    else if (seen === "\\") escaped = true;
    else if (seen === "[") klass = true;
    else if (seen === "]") klass = false;
    else if (seen === "/" && !klass) {
      while (/[dgimsuvy]/.test(text[held] ?? "")) held += 1;
      return held;
    }
  }
  return -1;
};

/**
 * The index past a template literal, interpolations included.
 *
 * `${…}` holds an expression rather than text, so its braces are matched with
 * the same primitive the rest of the scanner uses — which is what lets a
 * backtick, a brace or a comment inside one belong to the expression instead
 * of closing the template early.
 */
const templateEnd = (text: string, at: number, syntax: Syntax): number => {
  let held = at + 1;
  let escaped = false;
  while (held < text.length) {
    const seen = text[held] ?? "";
    if (escaped) {
      escaped = false;
      held += 1;
      continue;
    }
    if (seen === "\\") {
      escaped = true;
      held += 1;
      continue;
    }
    if (seen === "`") return held + 1;
    if (seen === "$" && text[held + 1] === "{") {
      held = matchingBrace(text, held + 1, syntax);
      continue;
    }
    held += 1;
  }
  return text.length;
};

const TRIPLES = ['"""', "'''"];

const atomAt = (text: string, at: number, syntax: Syntax): Atom | null => {
  const char = text[at] ?? "";
  const rest = text.slice(at, at + 4);

  const line = syntax.line.find((mark) => rest.startsWith(mark));
  if (line !== undefined) {
    const stop = text.indexOf("\n", at + line.length);
    return { end: stop < 0 ? text.length : stop, kind: "comment" };
  }
  if (syntax.block !== null && rest.startsWith(syntax.block[0])) {
    const [open, close] = syntax.block;
    const stop = text.indexOf(close, at + open.length);
    return { end: stop < 0 ? text.length : stop + close.length, kind: "comment" };
  }
  const triple = syntax.triple ? TRIPLES.find((mark) => rest.startsWith(mark)) : undefined;
  if (triple !== undefined) {
    const stop = text.indexOf(triple, at + 3);
    // A docstring is a string expression a reader depends on, not a comment.
    return { end: stop < 0 ? text.length : stop + 3, kind: "text" };
  }
  if (syntax.template && char === "`") {
    return { end: templateEnd(text, at, syntax), kind: "text" };
  }
  if (syntax.regex && char === "/" && opensRegex(text, at)) {
    const stop = regexEnd(text, at);
    // A `/` that opens nothing is ordinary punctuation, which is what division
    // is — returning `null` leaves the caller to step over it as one character.
    if (stop >= 0) return { end: stop, kind: "text" };
    return null;
  }
  if (!syntax.quotes.includes(char)) return null;

  if (char === "'" && syntax.charLiteral) {
    const stop = characterEnd(text, at);
    return stop < 0 ? null : { end: stop, kind: "text" };
  }

  let held = at + 1;
  let escaped = false;
  while (held < text.length) {
    const seen = text[held] ?? "";
    held += 1;
    if (escaped) escaped = false;
    else if (seen === "\\") escaped = true;
    else if (seen === char) break;
  }
  return { end: held, kind: "text" };
};

/** Find the matching brace, stepping over every comment and string on the way. */
const matchingBrace = (text: string, open: number, syntax: Syntax): number => {
  let depth = 0;
  for (let at = open; at < text.length; at++) {
    const atom = atomAt(text, at, syntax);
    if (atom !== null) {
      at = atom.end - 1;
      continue;
    }
    const char = text[at];
    if (char === "{") depth++;
    if (char === "}" && --depth === 0) return at + 1;
  }
  return text.length;
};

/** The next index holding something that is not whitespace or a comment. */
const nextSignificant = (text: string, from: number, syntax: Syntax): number => {
  for (let at = from; at < text.length; at++) {
    if (/\s/.test(text[at] ?? "")) continue;
    const atom = atomAt(text, at, syntax);
    if (atom?.kind === "comment") {
      at = atom.end - 1;
      continue;
    }
    return at;
  }
  return -1;
};

/**
 * The `{` that opens a declaration's body, or `-1` where it has none.
 *
 * Not the first `{` after the name, which is what this used to take. A
 * parameter's object type (`f(o: { a: string })`), a generic constraint
 * (`f<T extends { a: 1 }>`) and an object return type (`f(): { a: string }`)
 * all put braces in front of the body, and taking the first one anchored the
 * note to a fragment of the signature — a region the implementation is not in,
 * which then reported `fresh` however the body was rewritten. That is the one
 * outcome §15 exists to make impossible, reached through the resolver.
 *
 * So: the first brace outside every bracket, and where that brace turns out to
 * be a type annotation — another brace follows it — the one after that.
 */
const bodyBrace = (text: string, start: number, syntax: Syntax): number => {
  let round = 0;
  let square = 0;
  let angle = 0;
  for (let at = start; at < text.length; at++) {
    const atom = atomAt(text, at, syntax);
    if (atom !== null) {
      at = atom.end - 1;
      continue;
    }
    const char = text[at];
    if (char === "(") round++;
    else if (char === ")") round--;
    else if (char === "[") square++;
    else if (char === "]") square--;
    // Only in the generic position, before the parameters: elsewhere `<` and
    // `>` are comparisons, and `->` is a Rust return arrow.
    else if (char === "<" && round === 0 && square === 0) angle++;
    else if (char === ">" && angle > 0) angle--;
    else if (char === ";" && round === 0 && square === 0) {
      // `declare function f(): void;` — a declaration with no body at all.
      // Without this the scan ran on and claimed the next function's.
      return -1;
    } else if (char === "{" && round === 0 && square === 0 && angle === 0) {
      const closed = matchingBrace(text, at, syntax);
      const after = nextSignificant(text, closed, syntax);
      if (after !== -1 && text[after] === "{") {
        at = closed - 1;
        continue;
      }
      return at;
    }
  }
  return -1;
};

/** What, at a line's end, says the statement has not finished yet. */
const CONTINUES = /[=,+\-*/%&|^?:<>!~.([{]$/;

/**
 * What, at the next line's start, says the same thing.
 *
 * Both directions are needed and neither is enough alone. A codebase formatted
 * the way this one is breaks a ternary before its `?` and a call chain before
 * its `.`, so the line above ends on an ordinary identifier and only the line
 * below says the statement is still going. Reading the end alone stopped
 * `const pick = a\n  ? … : …` at its first line, which is the same
 * under-covered region this function exists to prevent.
 */
const CONTINUED_BY = /^[?:.,=+\-*/%&|^<>]/;

/**
 * Where a declaration that carries no brace body ends.
 *
 * `const covers = (…) => { … }` and `type X = { … }` used to end at the first
 * line, on the reasoning that their braces are an initializer rather than a
 * body. The reasoning is right and the conclusion was backwards: a region that
 * stops at the declaration line fingerprints the declaration line, so the
 * whole implementation could be rewritten underneath it and every hash stayed
 * put. `fresh` for changed code is the one answer §15 exists to make
 * impossible, and this reached it for the commonest shape in a TypeScript
 * codebase.
 *
 * So the region runs to the end of the statement: the `;` outside every
 * bracket, or the line whose brackets are balanced and which ends on nothing
 * that continues it. What such a declaration cannot offer is a *signature* —
 * there is no point at which its declaration stops and its value begins — and
 * §11's answer to that is `signatureHash = null`, which `braceRegions` sets.
 */
const statementEnd = (text: string, start: number, syntax: Syntax): number => {
  let round = 0;
  let square = 0;
  let curly = 0;
  let significant = start;
  for (let at = start; at < text.length; at++) {
    const atom = atomAt(text, at, syntax);
    if (atom !== null) {
      if (atom.kind === "text") significant = atom.end - 1;
      at = atom.end - 1;
      continue;
    }
    const char = text[at] ?? "";
    if (char === "(") round++;
    else if (char === ")") round--;
    else if (char === "[") square++;
    else if (char === "]") square--;
    else if (char === "{") curly++;
    else if (char === "}") curly--;
    const nested = round > 0 || square > 0 || curly > 0;
    if (char === ";" && !nested) return at + 1;
    if (char === "\n" && !nested) {
      if (CONTINUES.test(text[significant] ?? "")) continue;
      const next = nextSignificant(text, at + 1, syntax);
      if (next === -1 || !CONTINUED_BY.test(text[next] ?? "")) return at;
      at = next - 1;
      continue;
    }
    if (!/\s/.test(char)) significant = at;
  }
  return text.length;
};

const PAIRS = ["=>", "==", "!=", "<=", ">=", "&&", "||", "??", "?.", "++", "--", "**", "::", ":="];

/**
 * Comments and formatting disappear; semantic tokens remain separated.
 *
 * The separator matters: joined, `return x` and `returnx` normalize to the
 * same bytes, and a note would read a renamed identifier as no change at all.
 */
const semantic = (text: string, syntax: Syntax): Uint8Array => {
  const tokens: string[] = [];
  for (let at = 0; at < text.length;) {
    const char = text[at] ?? "";
    if (/\s/.test(char)) {
      at++;
      continue;
    }
    // Comments go; strings, character literals and docstrings stay whole, or
    // their contents parse as code and a renamed variable inside a message
    // reads as a changed implementation.
    const atom = atomAt(text, at, syntax);
    if (atom !== null) {
      if (atom.kind === "text") tokens.push(text.slice(at, atom.end));
      at = atom.end;
      continue;
    }
    if (syntax.word.test(char)) {
      let token = "";
      while (at < text.length && syntax.word.test(text[at] ?? "")) {
        token += text[at];
        at++;
      }
      tokens.push(token);
      continue;
    }
    const pair = `${char}${text[at + 1] ?? ""}`;
    if (PAIRS.includes(pair)) {
      tokens.push(pair);
      at += 2;
    } else {
      tokens.push(char);
      at++;
    }
  }
  return encoder.encode(tokens.join(" "));
};

/**
 * Regions from declarations whose body is a brace block.
 *
 * `declaration` must capture the kind and the name in that order, and must be
 * anchored to a line start: a pattern that matches mid-line finds `fn` inside
 * a string or a comment and anchors a note to it.
 */
const braceRegions = (
  text: string,
  declaration: RegExp,
  syntax: Syntax,
  bodied: (kind: string) => boolean,
): ReadonlyArray<Region> => {
  const starts = linesOf(text);
  const regions: Region[] = [];
  for (const match of text.matchAll(declaration)) {
    const kind = match[1];
    const name = match[2];
    const start = match.index;
    if (kind === undefined || name === undefined || start === undefined) continue;
    // Only where the *kind* can carry one: `type X = { … }` and `const X = …`
    // hold a value rather than a body, so there is no brace to match from and
    // `statementEnd` reads to the end of the declaration instead.
    const open = bodied(kind) ? bodyBrace(text, start, syntax) : -1;
    const hasBody = open >= 0;
    const end = hasBody ? matchingBrace(text, open, syntax) : statementEnd(text, start, syntax);
    regions.push({
      value: `${kind} ${name}`,
      startLine: lineIn(starts, start),
      endLine: lineIn(starts, Math.max(start, end - 1)),
      start,
      end,
      // A declaration with no brace body has no point at which its signature
      // stops and its value begins, and saying so is what `null` is for.
      signatureEnd: hasBody ? open : null,
    });
  }
  return regions;
};

const TYPESCRIPT_DECLARATION =
  /^(?:[ \t]*(?:export\s+(?:default\s+)?|declare\s+|abstract\s+|async\s+)*)?(function|class|interface|type|const)\s+([A-Za-z_$][\w$]*)\b/gm;

const RUST_DECLARATION =
  /^(?:[ \t]*(?:pub(?:\([^)]*\))?\s+|async\s+|unsafe\s+|const\s+|default\s+)*)?(fn|struct|enum|trait|mod|impl|type|static)\s+([A-Za-z_][\w]*)\b/gm;

/** Go declares at column zero, and a method carries a receiver before its name. */
const GO_DECLARATION = /^(func|type|var|const)\s+(?:\([^)]*\)\s*)?([A-Za-z_]\w*)\b/gm;

const SHELL_DECLARATION = /^[ \t]*(?:function\s+)?([A-Za-z_][\w-]*)\s*\(\s*\)\s*\{/gm;

const typescriptRegions = (text: string): ReadonlyArray<Region> =>
  braceRegions(text, TYPESCRIPT_DECLARATION, SCRIPT, (kind) => kind !== "type" && kind !== "const");

const rustRegions = (text: string): ReadonlyArray<Region> =>
  braceRegions(
    text,
    RUST_DECLARATION,
    NATIVE,
    (kind) => kind !== "type" && kind !== "static" && kind !== "const",
  );

const goRegions = (text: string): ReadonlyArray<Region> =>
  braceRegions(text, GO_DECLARATION, C_STYLE, (kind) => kind === "func");

/** `name() { ... }`, which is the one shape every POSIX shell agrees on. */
const shellRegions = (text: string): ReadonlyArray<Region> => {
  const starts = linesOf(text);
  const regions: Region[] = [];
  for (const match of text.matchAll(SHELL_DECLARATION)) {
    const name = match[1];
    const start = match.index;
    if (name === undefined || start === undefined) continue;
    const open = text.indexOf("{", start);
    if (open < 0) continue;
    const end = matchingBrace(text, open, HASH_STYLE);
    regions.push({
      value: `function ${name}`,
      startLine: lineIn(starts, start),
      endLine: lineIn(starts, Math.max(start, end - 1)),
      start,
      end,
      signatureEnd: open,
    });
  }
  return regions;
};

/**
 * Python regions, delimited by indentation rather than braces.
 *
 * A declaration ends at the first later line that is neither blank nor
 * indented past the declaration's own column — which is exactly the rule the
 * language itself uses, so it approximates nothing.
 */
const pythonRegions = (text: string): ReadonlyArray<Region> => {
  const starts = linesOf(text);
  const regions: Region[] = [];
  const declaration = /^([ \t]*)(?:async[ \t]+)?(def|class)[ \t]+([A-Za-z_]\w*)\b/gm;
  for (const match of text.matchAll(declaration)) {
    const indent = match[1];
    const kind = match[2];
    const name = match[3];
    const start = match.index;
    if (indent === undefined || kind === undefined || name === undefined || start === undefined) {
      continue;
    }

    // The header runs to the colon that closes it at bracket depth zero, which
    // is what lets a signature span lines.
    let depth = 0;
    let header = text.length;
    for (let at = start; at < text.length; at++) {
      const char = text[at];
      if (char === "(" || char === "[" || char === "{") depth++;
      else if (char === ")" || char === "]" || char === "}") depth--;
      else if (char === ":" && depth === 0) {
        header = at + 1;
        break;
      }
    }

    let end = text.length;
    for (let at = lineEnd(text, header) + 1; at < text.length;) {
      const stop = lineEnd(text, at);
      const line = text.slice(at, stop);
      const own = line.length - line.trimStart().length;
      if (line.trim() !== "" && own <= indent.length) {
        end = at;
        break;
      }
      at = stop + 1;
    }

    regions.push({
      value: `${kind} ${name}`,
      startLine: lineIn(starts, start),
      endLine: lineIn(starts, Math.max(start, end - 1)),
      start,
      end,
      signatureEnd: header,
    });
  }
  return regions;
};

/**
 * The half-open offset ranges covered by fenced code blocks.
 *
 * A `#` inside a fence is shell, or C, or a diff — anything but a heading. Read
 * as one it became an anchor of its own *and* truncated the section it sits in,
 * so an edit anywhere below it in that section reported `fresh`. CommonMark's
 * rule: three or more backticks or tildes, indented at most three spaces, closed
 * by at least as many of the same character or by the end of the document.
 */
const FENCE = /^[ \t]{0,3}(`{3,}|~{3,})[^\n]*$/gm;

const fences = (text: string): ReadonlyArray<readonly [number, number]> => {
  const spans: Array<readonly [number, number]> = [];
  let open: { readonly at: number; readonly mark: string } | null = null;
  for (const match of text.matchAll(FENCE)) {
    const mark = match[1];
    const start = match.index;
    if (mark === undefined || start === undefined) continue;
    if (open === null) {
      open = { at: start, mark };
      continue;
    }
    if (mark[0] !== open.mark[0] || mark.length < open.mark.length) continue;
    spans.push([open.at, lineEnd(text, start)]);
    open = null;
  }
  if (open !== null) spans.push([open.at, text.length]);
  return spans;
};

const markdownRegions = (text: string): ReadonlyArray<Region> => {
  const starts = linesOf(text);
  const fenced = fences(text);
  const headings: Array<{ start: number; level: number; title: string; lineEnd: number }> = [];
  const heading = /^(#{1,6})[ \t]+(.+?)[ \t]*#*[ \t]*$/gm;
  for (const match of text.matchAll(heading)) {
    const hashes = match[1];
    const title = match[2];
    const start = match.index;
    if (hashes === undefined || title === undefined || start === undefined) continue;
    if (fenced.some(([from, to]) => start >= from && start < to)) continue;
    headings.push({ start, level: hashes.length, title, lineEnd: lineEnd(text, start) });
  }
  return headings.map((heading, index) => {
    const next = headings.slice(index + 1).find((candidate) => candidate.level <= heading.level);
    const end = next?.start ?? text.length;
    return {
      value: `${"#".repeat(heading.level)} ${heading.title}`,
      startLine: lineIn(starts, heading.start),
      endLine: lineIn(starts, Math.max(heading.start, end - 1)),
      start: heading.start,
      end,
      signatureEnd: heading.lineEnd,
    };
  });
};

const BLOCK_TAGS = ["script", "style", "template"] as const;

/**
 * A single-file component's blocks, which §10 names `#script`, `#style` and
 * `#template`.
 *
 * The open tag is the signature: `lang="ts"` or `scoped` changes what the
 * block *is*, which is a contract change, while editing between the tags is
 * an implementation change.
 */
const componentRegions = (text: string): ReadonlyArray<Region> => {
  const starts = linesOf(text);
  const regions: Region[] = [];
  for (const tag of BLOCK_TAGS) {
    // Depth-counted rather than closed at the first `</tag>`: Vue nests
    // `<template #slot>` inside `<template>`, and ending the outer block at
    // the inner close left the real markup outside the fingerprinted region —
    // where rewriting it reported `fresh`.
    const marks = new RegExp(`<(/?)${tag}(?:\\s[^>]*)?>`, "gi");
    let opened: { start: number; inner: number } | null = null;
    let depth = 0;
    for (const match of text.matchAll(marks)) {
      const closing = match[1] === "/";
      const at = match.index;
      if (at === undefined) continue;
      if (!closing) {
        if (depth === 0) opened = { start: at, inner: at + match[0].length };
        depth++;
        continue;
      }
      if (depth === 0) continue;
      depth--;
      if (depth !== 0 || opened === null) continue;
      const end = at + match[0].length;
      regions.push({
        value: `#${tag}`,
        startLine: lineIn(starts, opened.start),
        endLine: lineIn(starts, Math.max(opened.start, end - 1)),
        start: opened.start,
        end,
        signatureEnd: opened.inner,
      });
      opened = null;
    }
    // An unclosed block still names a region: the file is being edited, and
    // reporting nothing would read as the block having been deleted.
    if (depth > 0 && opened !== null) {
      regions.push({
        value: `#${tag}`,
        startLine: lineIn(starts, opened.start),
        endLine: lineIn(starts, Math.max(opened.start, text.length - 1)),
        start: opened.start,
        end: text.length,
        signatureEnd: opened.inner,
      });
    }
  }
  return regions.sort((left, right) => left.start - right.start);
};

/**
 * What this resolver can say about a path, and how much of it.
 *
 * `regions` absent means the middle tier: comments and formatting are read
 * reliably, declarations are not, so `@file` normalizes and every symbol
 * anchor answers `Unsupported` rather than guessing.
 */
interface Language {
  readonly name: string;
  readonly syntax: Syntax;
  readonly regions?: (text: string) => ReadonlyArray<Region>;
  /** The syntax a region's own contents normalize under, where it differs. */
  readonly within?: (region: Region) => Syntax;
}

const LANGUAGES: ReadonlyArray<readonly [RegExp, Language]> = [
  [/\.(?:[cm]?[jt]sx?)$/, { name: "typescript", syntax: SCRIPT, regions: typescriptRegions }],
  [/\.rs$/, { name: "rust", syntax: NATIVE, regions: rustRegions }],
  [/\.go$/, { name: "go", syntax: C_STYLE, regions: goRegions }],
  [/\.py[iw]?$/, { name: "python", syntax: PYTHON, regions: pythonRegions }],
  [/\.(?:sh|bash|zsh|ksh)$/, { name: "shell", syntax: HASH_STYLE, regions: shellRegions }],
  [/\.(?:md|mdx|markdown)$/, { name: "markdown", syntax: MARKUP, regions: markdownRegions }],
  [
    /\.(?:vue|svelte)$/,
    {
      name: "component",
      syntax: MARKUP,
      regions: componentRegions,
      within: (region) =>
        region.value === "#style" ? CSS_STYLE : region.value === "#script" ? SCRIPT : MARKUP,
    },
  ],
  // Comments and formatting only; see this module's opening note.
  [/\.(?:java|kt|kts|scala|swift|cs)$/, { name: "jvm-family", syntax: C_STYLE }],
  [/\.(?:c|h|cc|cpp|cxx|hpp|hh|m|mm)$/, { name: "c-family", syntax: NATIVE }],
];

const languageOf = (path: string): Language | null => {
  const lower = path.toLowerCase();
  return LANGUAGES.find(([pattern]) => pattern.test(lower))?.[1] ?? null;
};

const publicAnchor = (region: Region): Anchor => ({
  value: region.value,
  startLine: region.startLine,
  endLine: region.endLine,
});

/**
 * How many lines the text holds.
 *
 * `split("\n").length` counts the empty segment a trailing newline leaves
 * behind, so every normally-terminated file reported one line too many and
 * `note anchors` printed `@file 1-4` for a three-line file.
 */
const lines = (text: string): number =>
  Math.max(1, text.split("\n").length - (text.endsWith("\n") ? 1 : 0));

export const syntax = Layer.sync(AnchorResolver, () => {
  const anchors = Effect.fn("hub.Anchor.syntax.anchors")((path: string, content: Uint8Array) => {
    const text = decoder.decode(content);
    const language = languageOf(path);
    return Effect.succeed([
      { value: FILE_ANCHOR, startLine: 1, endLine: lines(text) },
      ...(language?.regions?.(text) ?? []).map(publicAnchor),
    ]);
  });

  const resolve = Effect.fn("hub.Anchor.syntax.resolve")(function* (
    path: string,
    content: Uint8Array,
    requested: string,
  ): Effect.fn.Return<AnchorResolution> {
    const text = decoder.decode(content);
    const language = languageOf(path);

    if (requested === FILE_ANCHOR) {
      return {
        _tag: "Found",
        anchor: { value: FILE_ANCHOR, startLine: 1, endLine: lines(text) },
        fingerprint: yield* fingerprint({
          resolver: language === null ? "file@1" : `${language.name}-syntax@1`,
          normalization: language === null ? "exact-v1" : "semantic-v1",
          raw: content,
          content: language === null ? content : semantic(text, language.syntax),
        }),
      };
    }
    if (language?.regions === undefined) return { _tag: "Unsupported" };

    const regions = language.regions(text);
    const exact = regions.filter((region) => region.value === requested);
    // A bare name is accepted on the way in and qualified on the way out —
    // §10 — so `verify` finds `function verify` and refuses two of them.
    const candidates =
      exact.length > 0
        ? exact
        : regions.filter((region) => region.value.split(" ").slice(1).join(" ") === requested);
    if (candidates.length === 0) return { _tag: "Missing" };
    if (candidates.length > 1) {
      return { _tag: "Ambiguous", candidates: candidates.map(publicAnchor) };
    }
    const region = candidates[0];
    if (region === undefined) return { _tag: "Missing" };

    const syntax = language.within?.(region) ?? language.syntax;
    const rawText = text.slice(region.start, region.end);
    return {
      _tag: "Found",
      anchor: publicAnchor(region),
      fingerprint: yield* fingerprint({
        resolver: `${language.name}-syntax@1`,
        normalization: "semantic-v1",
        raw: encoder.encode(rawText),
        content: semantic(rawText, syntax),
        signature:
          region.signatureEnd === null
            ? null
            : semantic(text.slice(region.start, region.signatureEnd), syntax),
      }),
    };
  });

  return AnchorResolver.of({ name: "syntax", version: "1", anchors, resolve });
});
