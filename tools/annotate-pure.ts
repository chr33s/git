/**
 * Mark the library's top-level calls pure, so a bundler can drop what is unused.
 *
 * A module-level `const unpack = Effect.fn(...)(...)` or
 * `class X extends Context.Service()("key")` is a call, and a bundler must
 * assume a call has side effects — so it keeps the call, and everything the
 * call's arguments reach, even when nothing imports `unpack`. A consumer that
 * only pushes would carry the pack reader and the whole inflater. Effect's own
 * build annotates the same way; `/*#__PURE__*\/` is understood by esbuild,
 * Rollup, Rolldown and Terser.
 *
 * Every annotated call in `dist/lib` is a definition: the library has no
 * module-level registration, which `src/Library.test.ts` keeps true by
 * enumerating the files that ship.
 */
import * as fs from "node:fs";
import * as path from "node:path";

const root = process.argv[2] ?? "dist/lib";

const files = (directory: string): ReadonlyArray<string> =>
  fs.readdirSync(directory, { withFileTypes: true }).flatMap((entry) => {
    const full = path.join(directory, entry.name);
    if (entry.isDirectory()) return files(full);
    return entry.name.endsWith(".js") ? [full] : [];
  });

let annotated = 0;
for (const file of files(root)) {
  const source = fs.readFileSync(file, "utf8");
  const output = source
    // `const name = callee(` and `const name = new Callee(` at module level.
    .replace(
      /^((?:export )?const [\w$]+ = )(?=(?:new )?[A-Za-z_$][\w$.]*\()/gm,
      (_, declaration: string) => {
        annotated++;
        return `${declaration}/*#__PURE__*/ `;
      },
    )
    // `class X extends Factory()(...)`: both calls, inner and outer.
    .replace(/extends ([A-Za-z_$][\w$.]*\(\))\(/g, (_, factory: string) => {
      annotated++;
      return `extends /*#__PURE__*/ (/*#__PURE__*/ ${factory})(`;
    });
  if (output !== source) fs.writeFileSync(file, output);
}

if (annotated === 0) throw new Error(`no top-level calls found under ${root}; was it built?`);
console.log(`annotated ${annotated} top-level calls under ${root}`);
