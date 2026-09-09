/**
 * Each language the resolver claims to read, held to the same three claims.
 *
 * A language is only worth adding if all three hold: it finds the declaration
 * a note names, formatting and comments do not move `contentHash`, and a
 * changed declaration moves `signatureHash` while a changed body does not.
 * Anything that cannot answer all three belongs in a lower tier, where a
 * symbol anchor is `Unsupported` and nothing is silently wrong.
 */
import assert from "node:assert/strict";
import { describe, it } from "@effect/vitest";
import { Effect } from "effect";

import { AnchorResolver, type Fingerprint } from "./Anchor.ts";
import { syntax } from "./Anchor.syntax.ts";

const encoder = new TextEncoder();

const resolve = (path: string, source: string, anchor: string) =>
  Effect.gen(function* () {
    return yield* (yield* AnchorResolver).resolve(path, encoder.encode(source), anchor);
  }).pipe(Effect.provide(syntax));

const anchors = (path: string, source: string) =>
  Effect.gen(function* () {
    return yield* (yield* AnchorResolver).anchors(path, encoder.encode(source));
  }).pipe(Effect.provide(syntax));

const found = Effect.fn("test.found")(function* (path: string, source: string, anchor: string) {
  const resolved = yield* resolve(path, source, anchor);
  assert.equal(resolved._tag, "Found", `${path}#${anchor} did not resolve`);
  if (resolved._tag !== "Found") throw new Error("unreachable");
  return resolved;
});

/**
 * The three claims, asked at once.
 *
 * `formatted` must differ from `source` only in whitespace and comments;
 * `body` must change the implementation and `contract` the declaration.
 */
const holds = Effect.fn("test.holds")(function* (input: {
  readonly path: string;
  readonly anchor: string;
  readonly value?: string;
  readonly source: string;
  readonly formatted: string;
  readonly body: string;
  readonly contract: string;
}) {
  const original = yield* found(input.path, input.source, input.anchor);
  assert.equal(original.anchor.value, input.value ?? input.anchor);

  const formatted = (yield* found(input.path, input.formatted, input.anchor)).fingerprint;
  assert.equal(
    formatted.contentHash,
    original.fingerprint.contentHash,
    "formatting and comments must not read as a change",
  );
  assert.notEqual(
    formatted.rawHash,
    original.fingerprint.rawHash,
    "the test's own formatted source is identical; it proves nothing",
  );

  const body = (yield* found(input.path, input.body, input.anchor)).fingerprint;
  assert.notEqual(body.contentHash, original.fingerprint.contentHash, "a changed body is a change");
  assert.equal(
    body.signatureHash,
    original.fingerprint.signatureHash,
    "a changed body is not a contract change",
  );

  const contract = (yield* found(input.path, input.contract, input.anchor)).fingerprint;
  assert.notEqual(
    contract.signatureHash,
    original.fingerprint.signatureHash,
    "a changed declaration is a contract change",
  );
  return original.fingerprint satisfies Fingerprint;
});

describe("anchored source, by language", () => {
  it.effect("reads Rust declarations", () =>
    Effect.gen(function* () {
      yield* holds({
        path: "src/auth.rs",
        anchor: "fn verify",
        source: "pub fn verify(token: &str) -> bool {\n    token.len() > 0\n}\n",
        formatted:
          "pub fn verify(token: &str) -> bool {\n    // still constant-time\n\n    token.len() > 0\n}\n",
        body: "pub fn verify(token: &str) -> bool {\n    token.len() > 1\n}\n",
        contract: "pub fn verify(token: &[u8]) -> bool {\n    token.len() > 0\n}\n",
      });

      const listed = yield* anchors(
        "src/auth.rs",
        "pub struct Store {\n    keys: Vec<String>,\n}\n\nimpl Store {\n    fn len(&self) -> usize {\n        self.keys.len()\n    }\n}\n",
      );
      assert.deepEqual(
        listed.map((anchor) => anchor.value),
        ["@file", "struct Store", "impl Store", "fn len"],
      );
    }),
  );

  it.effect("reads Go declarations, receiver and all", () =>
    Effect.gen(function* () {
      yield* holds({
        path: "auth.go",
        anchor: "func Verify",
        source: "func Verify(token string) bool {\n\treturn len(token) > 0\n}\n",
        formatted:
          "func Verify(token string) bool {\n\t// still constant-time\n\treturn len(token) > 0\n}\n",
        body: "func Verify(token string) bool {\n\treturn len(token) > 1\n}\n",
        contract: "func Verify(token []byte) bool {\n\treturn len(token) > 0\n}\n",
      });

      // A method's receiver sits between `func` and the name; the anchor is
      // still the name, which is what a note would have been written against.
      const method = yield* found(
        "store.go",
        "func (s *Store) Len() int {\n\treturn len(s.keys)\n}\n",
        "func Len",
      );
      assert.equal(method.anchor.value, "func Len");
    }),
  );

  it.effect("reads Python, whose regions end where the indentation does", () =>
    Effect.gen(function* () {
      yield* holds({
        path: "auth.py",
        anchor: "def verify",
        source: "def verify(token):\n    return len(token) > 0\n",
        formatted: "def verify(token):\n    # still constant-time\n\n    return len(token) > 0\n",
        body: "def verify(token):\n    return len(token) > 1\n",
        contract: "def verify(token, *, strict):\n    return len(token) > 0\n",
      });

      // A region runs to the next declaration at or above its own column, so
      // the blank lines between two of them belong to the earlier one — the
      // same reading Markdown headings get.
      const listed = yield* anchors(
        "auth.py",
        "def first():\n    pass\n\n\nclass Store:\n    def put(self, key):\n        pass\n\n\ndef last():\n    pass\n",
      );
      assert.deepEqual(
        listed.map((anchor) => [anchor.value, anchor.startLine, anchor.endLine]),
        [
          ["@file", 1, 11],
          ["def first", 1, 4],
          ["class Store", 5, 9],
          ["def put", 6, 9],
          ["def last", 10, 11],
        ],
      );

      // A docstring is a value the function returns to a reader, not a
      // comment, so changing it changes the content hash.
      const bare = yield* found("d.py", "def f():\n    return 1\n", "def f");
      const documented = yield* found(
        "d.py",
        'def f():\n    """What it does."""\n    return 1\n',
        "def f",
      );
      assert.notEqual(bare.fingerprint.contentHash, documented.fingerprint.contentHash);
    }),
  );

  it.effect("reads shell functions", () =>
    Effect.gen(function* () {
      // Asked directly rather than through `holds`: a shell function's whole
      // declaration is its name, so there is no contract to change without
      // changing which anchor is being named.
      const original = yield* found(
        "scripts/release.sh",
        "publish() {\n  npm publish\n}\n",
        "function publish",
      );
      const commented = yield* found(
        "scripts/release.sh",
        "publish() {\n  # tag first\n\n  npm publish\n}\n",
        "function publish",
      );
      const changed = yield* found(
        "scripts/release.sh",
        "publish() {\n  npm publish --dry-run\n}\n",
        "function publish",
      );
      assert.equal(original.fingerprint.contentHash, commented.fingerprint.contentHash);
      assert.notEqual(original.fingerprint.rawHash, commented.fingerprint.rawHash);
      assert.notEqual(original.fingerprint.contentHash, changed.fingerprint.contentHash);

      // `function name()` and `name()` are one declaration, spelled two ways.
      const keyword = yield* found(
        "scripts/release.sh",
        "function publish() {\n  npm publish\n}\n",
        "function publish",
      );
      assert.equal(keyword.anchor.value, "function publish");

      // A `#!` line is a comment to the normalizer, which is right: it is not
      // part of any function's body.
      const listed = yield* anchors(
        "scripts/release.sh",
        "#!/usr/bin/env bash\nbuild() {\n  npm run build\n}\n",
      );
      assert.deepEqual(
        listed.map((anchor) => anchor.value),
        ["@file", "function build"],
      );
    }),
  );

  it.effect("reads a single-file component's blocks", () =>
    Effect.gen(function* () {
      const source =
        '<template>\n  <p>{{ name }}</p>\n</template>\n\n<script>\nexport default { data: () => ({ name: "a" }) }\n</script>\n\n<style scoped>\np { color: red; }\n</style>\n';

      const listed = yield* anchors("src/Card.vue", source);
      assert.deepEqual(
        listed.map((anchor) => anchor.value),
        ["@file", "#template", "#script", "#style"],
      );

      const script = yield* found("src/Card.vue", source, "#script");
      const commented = yield* found(
        "src/Card.vue",
        source.replace("export default", "// the default export\nexport default"),
        "#script",
      );
      // The script block normalizes as script: a `//` comment inside it is
      // formatting, even though the file around it is markup.
      assert.equal(script.fingerprint.contentHash, commented.fingerprint.contentHash);

      const rebodied = yield* found(
        "src/Card.vue",
        source.replace('name: "a"', 'name: "b"'),
        "#script",
      );
      assert.notEqual(script.fingerprint.contentHash, rebodied.fingerprint.contentHash);
      assert.equal(script.fingerprint.signatureHash, rebodied.fingerprint.signatureHash);

      // The open tag is the block's contract, so `scoped` coming off it is a
      // different kind of change from editing the rules inside.
      const style = yield* found("src/Card.vue", source, "#style");
      const unscoped = yield* found(
        "src/Card.vue",
        source.replace("<style scoped>", "<style>"),
        "#style",
      );
      assert.notEqual(style.fingerprint.signatureHash, unscoped.fingerprint.signatureHash);
    }),
  );

  it.effect("declines a symbol anchor in the languages it only normalizes", () =>
    Effect.gen(function* () {
      for (const path of ["Repository.java", "repo.cpp", "repo.h", "Repo.kt", "Repo.swift"]) {
        const resolved = yield* resolve(path, "class Repository {};\n", "class Repository");
        assert.equal(resolved._tag, "Unsupported", `${path} should decline symbol anchors`);
        // Only `@file` is offered, so nothing suggests an anchor that would
        // then fail to resolve.
        assert.deepEqual(
          (yield* anchors(path, "class Repository {};\n")).map((anchor) => anchor.value),
          ["@file"],
        );
      }

      // And that whole file still normalizes past comments, which is the
      // whole reason these sit above the opaque tier.
      const plain = yield* found("Repo.java", "class Repo {}\n", "@file");
      const noisy = yield* found("Repo.java", "/* header */\nclass Repo {}\n", "@file");
      assert.equal(plain.fingerprint.contentHash, noisy.fingerprint.contentHash);
      assert.equal(plain.fingerprint.normalization, "semantic-v1");
    }),
  );

  it.effect("finds the body past braces that belong to the signature", () =>
    Effect.gen(function* () {
      // Each of these puts a `{` in front of the body. Taking the first one
      // anchored the note to a fragment of the signature, so the whole
      // implementation fell outside the fingerprint and every rewrite of it
      // reported `fresh` — §15's one forbidden outcome, reached sideways.
      const signatures = [
        ["an object parameter type", "export function f(o: { a: string }): void {", "}"],
        ["an object return type", "export function f(): { a: string } {", "  return { a: '' }\n}"],
        ["a generic constraint", "export function f<T extends { a: 1 }>(x: T): void {", "}"],
        ["a destructured parameter", "export function f({ a, b }: Pair): void {", "}"],
      ] as const;

      for (const [what, head, tail] of signatures) {
        const before = yield* found("a.ts", `${head}\n  const held = 1\n${tail}\n`, "function f");
        const after = yield* found(
          "a.ts",
          `${head}\n  const held = 99999\n  launch()\n${tail}\n`,
          "function f",
        );
        assert.notEqual(
          before.fingerprint.contentHash,
          after.fingerprint.contentHash,
          `a rewritten body under ${what} must not read as unchanged`,
        );
        assert.equal(
          before.fingerprint.signatureHash,
          after.fingerprint.signatureHash,
          `${what} is not a contract change`,
        );
        assert.ok(before.anchor.endLine > before.anchor.startLine, `${what} spans its body`);
      }
    }),
  );

  it.effect("does not let a bodyless declaration claim the next one's body", () =>
    Effect.gen(function* () {
      const source =
        "declare function first(a: string): void;\n\nexport function second(): number {\n  return 1\n}\n";
      const first = yield* found("a.ts", source, "function first");
      const second = yield* found("a.ts", source, "function second");
      // The declaration ends at its semicolon; without that the scan ran on
      // and fingerprinted `second`'s body as `first`'s.
      assert.equal(first.anchor.endLine, 1);
      assert.equal(second.anchor.startLine, 3);
      assert.notEqual(first.fingerprint.contentHash, second.fingerprint.contentHash);
    }),
  );

  it.effect("reads a Rust lifetime as punctuation rather than a quote", () =>
    Effect.gen(function* () {
      // A lone `'` opened a string that swallowed the closing brace, so `fn a`
      // absorbed everything to the next apostrophe — `fn b` included.
      const source =
        'pub fn a() -> u8 {\n    let s: &\'static str = "x";\n    1\n}\n\npub fn b() -> u8 {\n    2\n}\n';
      const a = yield* found("a.rs", source, "fn a");
      assert.deepEqual([a.anchor.startLine, a.anchor.endLine], [1, 4]);

      // And the proof that it stops there: editing `fn b` leaves `fn a` alone.
      const edited = yield* found("a.rs", source.replace("    2", "    3"), "fn a");
      assert.equal(a.fingerprint.contentHash, edited.fingerprint.contentHash);

      // A real character literal is still a literal, and still a token.
      const held = yield* found("a.rs", "fn c() -> char {\n    'x'\n}\n", "fn c");
      const other = yield* found("a.rs", "fn c() -> char {\n    'y'\n}\n", "fn c");
      assert.notEqual(held.fingerprint.contentHash, other.fingerprint.contentHash);
    }),
  );

  it.effect("counts the lines a file has, not the newlines it ends with", () =>
    Effect.gen(function* () {
      const three = yield* found("a.ts", "const a = 1\nconst b = 2\nconst c = 3\n", "@file");
      assert.deepEqual([three.anchor.startLine, three.anchor.endLine], [1, 3]);
      // No trailing newline is still three lines.
      const bare = yield* found("a.ts", "const a = 1\nconst b = 2\nconst c = 3", "@file");
      assert.equal(bare.anchor.endLine, 3);
      // And an empty file is one line rather than none.
      assert.equal((yield* found("a.ts", "", "@file")).anchor.endLine, 1);
    }),
  );

  it.effect("keeps a nested block from closing the one that contains it", () =>
    Effect.gen(function* () {
      const source =
        "<template>\n  <Foo><template #header>x</template></Foo>\n  <p>the real markup</p>\n</template>\n";
      const whole = yield* found("C.vue", source, "#template");
      assert.deepEqual([whole.anchor.startLine, whole.anchor.endLine], [1, 4]);

      // Closing at the inner `</template>` left the markup below it outside
      // the region, where rewriting it reported `fresh`.
      const rewritten = yield* found(
        "C.vue",
        source.replace("the real markup", "something else entirely"),
        "#template",
      );
      assert.notEqual(whole.fingerprint.contentHash, rewritten.fingerprint.contentHash);
    }),
  );

  it.effect("keeps a renamed identifier from normalizing into its neighbour", () =>
    Effect.gen(function* () {
      // The normalizer separates tokens; joined, `return x` and `returnx`
      // would hash alike and a rename would read as no change at all.
      const before = yield* found("a.ts", "function f() { return x }", "function f");
      const after = yield* found("a.ts", "function f() { returnx }", "function f");
      assert.notEqual(before.fingerprint.contentHash, after.fingerprint.contentHash);
    }),
  );

  it.effect("reads an arrow-function constant's body, not only its first line", () =>
    Effect.gen(function* () {
      // `export const f = (…) => { … }` is the commonest declaration shape in
      // this codebase, and a region that stopped at the declaration line left
      // every hash unmoved however the body was rewritten — `fresh` for
      // changed code, which is the one answer §15 forbids.
      const source = "export const covers = (a: string, b: string) => {\n  return a === b;\n};\n";
      const original = yield* found("a.ts", source, "const covers");
      assert.deepEqual([original.anchor.startLine, original.anchor.endLine], [1, 3]);

      const rewritten = yield* found(
        "a.ts",
        source.replace("return a === b;", 'throw new Error("different");'),
        "const covers",
      );
      assert.notEqual(original.fingerprint.contentHash, rewritten.fingerprint.contentHash);
      // No point separates such a declaration from its value, and §11 says so
      // with a null rather than by hashing the declaration line twice.
      assert.equal(original.fingerprint.signatureHash, null);
    }),
  );

  it.effect("ends a value declaration at the statement, not at the next one", () =>
    Effect.gen(function* () {
      const source =
        "const half = (a: number, b: number) => a / b;\nconst other = (c: number) => c;\n";
      const listed = yield* anchors("a.ts", source);
      assert.deepEqual(
        listed.map((anchor) => `${anchor.value} ${anchor.startLine}-${anchor.endLine}`),
        ["@file 1-2", "const half 1-1", "const other 2-2"],
      );
    }),
  );

  it.effect("does not let a quote inside a regex literal open a string", () =>
    Effect.gen(function* () {
      // Read as division, the `/` left `["]` opening a string scan that ran
      // past `f`'s closing brace and swallowed `g` whole — so a note on `f`
      // fingerprinted code it does not describe and drifted on every edit to
      // its neighbour.
      const source =
        'function f() {\n  return /["]/.test("a");\n}\n\nfunction g() {\n  return 1;\n}\n';
      const listed = yield* anchors("a.ts", source);
      assert.deepEqual(
        listed.map((anchor) => `${anchor.value} ${anchor.startLine}-${anchor.endLine}`),
        ["@file 1-7", "function f 1-3", "function g 5-7"],
      );
    }),
  );

  it.effect("does not let a backtick inside an interpolation close its template", () =>
    Effect.gen(function* () {
      const source =
        'function h() {\n  return `a ${["`"].join("")} b`;\n}\n\nfunction i() {\n  return 2;\n}\n';
      const listed = yield* anchors("a.ts", source);
      assert.deepEqual(
        listed.map((anchor) => `${anchor.value} ${anchor.startLine}-${anchor.endLine}`),
        ["@file 1-7", "function h 1-3", "function i 5-7"],
      );
    }),
  );

  it.effect("does not read a hash inside a fenced code block as a heading", () =>
    Effect.gen(function* () {
      // It became an anchor of its own and truncated the section holding it,
      // so an edit below the fence under `# Title` reported `fresh`.
      const source = "# Title\n\n```sh\n# not a heading\necho hi\n```\n\nprose\n\n## Real\n";
      const listed = yield* anchors("m.md", source);
      assert.deepEqual(
        listed.map((anchor) => anchor.value),
        ["@file", "# Title", "## Real"],
      );

      const original = yield* found("m.md", source, "# Title");
      const edited = yield* found("m.md", source.replace("echo hi", "echo there"), "# Title");
      assert.notEqual(original.fingerprint.contentHash, edited.fingerprint.contentHash);
    }),
  );
});
