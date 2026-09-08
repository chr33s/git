/** A bounded regex subset evaluated as an NFA, without backtracking. */
type Atom =
  | {
      readonly kind: "character";
      readonly expression: RegExp;
      readonly optional: boolean;
      readonly repeat: boolean;
    }
  | { readonly kind: "assertion"; readonly accepts: (line: string, at: number) => boolean };

const word = (character: string | undefined): boolean =>
  character !== undefined && /[a-zA-Z0-9_]/.test(character);

/** Compile literals, character classes, anchors and one repetition per pattern. */
export const linearRegex = (pattern: string, ignoreCase: boolean): ((line: string) => boolean) => {
  if (pattern.length > 200) throw new Error("pattern exceeds 200 characters");
  // Syntax validation only: this expression is never executed.
  new RegExp(pattern, ignoreCase ? "i" : "");
  const alternatives: Atom[][] = [[]];
  let atoms = alternatives[0];
  if (atoms === undefined) throw new Error("missing pattern");
  let repetitions = 0;
  for (let at = 0; at < pattern.length;) {
    const start = at;
    const character = pattern[at++];
    if (character === "|") {
      atoms = [];
      alternatives.push(atoms);
      continue;
    }
    if (character === "(" || character === ")")
      throw new Error("groups are not supported; use a literal search");
    if (character === "^" || character === "$") {
      atoms.push({
        kind: "assertion",
        accepts:
          character === "^"
            ? (_line, index) => index === 0
            : (line, index) =>
                index === line.length ||
                (index === line.length - 1 && /[\n\r\u2028\u2029]/.test(line[index] ?? "")),
      });
      continue;
    }
    if (character === "[") {
      if (pattern[at] === "^") at++;
      while (at < pattern.length && pattern[at] !== "]") {
        if (pattern[at] === "\\") at++;
        at++;
      }
      if (pattern[at++] !== "]") throw new Error("unterminated character class");
    } else if (character === "\\") {
      const escaped = pattern[at++];
      if (escaped === "b" || escaped === "B") {
        atoms.push({
          kind: "assertion",
          accepts: (line, index) =>
            (word(line[index - 1]) !== word(line[index])) === (escaped === "b"),
        });
        continue;
      }
      if (escaped === undefined || /[0-9ckpP]/.test(escaped))
        throw new Error("unsupported escape; use a literal search");
      if (escaped === "x" || escaped === "u") {
        const length = escaped === "x" ? 2 : 4;
        if (!new RegExp(`^[0-9a-fA-F]{${length}}$`).test(pattern.slice(at, at + length)))
          throw new Error("unsupported escape");
        at += length;
      }
    } else if (character !== undefined && /[*+?{}]/.test(character)) {
      throw new Error("unexpected repetition");
    }
    const expression = new RegExp(`^(?:${pattern.slice(start, at)})$`, ignoreCase ? "i" : "");
    let minimum = 1;
    let maximum = 1;
    const quantifier = pattern[at];
    if (quantifier !== undefined && /[*+?{]/.test(quantifier)) {
      if (++repetitions > 1) throw new Error("at most one repetition is supported");
      at++;
      if (quantifier === "*") {
        minimum = 0;
        maximum = Infinity;
      } else if (quantifier === "+") maximum = Infinity;
      else if (quantifier === "?") minimum = 0;
      else {
        const end = pattern.indexOf("}", at);
        const parts = /^(\d+)(?:,(\d*))?$/.exec(pattern.slice(at, end));
        if (end < 0 || parts === null) throw new Error("invalid repetition");
        minimum = Number(parts[1]);
        maximum = parts[2] === undefined ? minimum : parts[2] === "" ? Infinity : Number(parts[2]);
        at = end + 1;
        if (minimum > 200 || (maximum !== Infinity && maximum > 200))
          throw new Error("repetition exceeds 200 states");
      }
    }
    for (let index = 0; index < minimum; index++)
      atoms.push({ kind: "character", expression, optional: false, repeat: false });
    if (maximum === Infinity)
      atoms.push({ kind: "character", expression, optional: true, repeat: true });
    else
      for (let index = minimum; index < maximum; index++)
        atoms.push({ kind: "character", expression, optional: true, repeat: false });
  }
  return (line) =>
    alternatives.some((sequence) => {
      let active = new Set<number>();
      for (let at = 0; at <= line.length; at++) {
        active.add(0);
        const next = new Set<number>();
        // Epsilon edges only point forward, so this single pass closes them.
        for (let index = 0; index <= sequence.length; index++) {
          if (!active.has(index)) continue;
          const atom = sequence[index];
          if (atom === undefined) return true;
          if (atom.kind === "assertion") {
            if (atom.accepts(line, at)) active.add(index + 1);
          } else {
            if (atom.optional) active.add(index + 1);
            const character = line[at];
            if (character !== undefined && atom.expression.test(character))
              next.add(atom.repeat ? index : index + 1);
          }
        }
        active = next;
      }
      return false;
    });
};
