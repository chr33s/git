/**
 * The protocol's two halves without a git binary in sight: what `uploadPack`
 * acknowledges during negotiation, and what `receivePack` reports back.
 *
 * Both take a web `Request` and answer a `Response`, so a test can speak
 * pkt-lines at them directly. The interop suite proves stock git can hold
 * these conversations; this file pins the exact acknowledgments each round
 * shape earns, which a passing clone would hide, and the shape of the push
 * *report*: a push is not all-or-nothing unless the client asked for that, so
 * a command this server refuses has to come back as one `ng` line beside the
 * `ok`s for everything that worked — an HTTP error would tell the client
 * nothing about which ref was at fault and would silently drop the rest.
 */
import assert from "node:assert/strict";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { createGzip } from "node:zlib";
import { describe, it } from "@effect/vitest";

import { Effect, Layer } from "effect";

import { EMPTY_TREE_OID, type Signature } from "../git/Format.ts";
import { stores } from "../git/Memory.ts";
import { stores as nodeStores } from "../git/Node.ts";
import { DELIM, FLUSH, pkt } from "../git/Pkt.ts";
import * as GitRepository from "../git/Repository.ts";
import { Repository } from "../git/Repository.ts";
import { RefStore, type Oid } from "../git/Store.ts";
import * as Auth from "./Auth.ts";
import * as Policy from "./Policy.ts";
import * as Protocol from "./Protocol.ts";

// These repositories have no genesis, so the policy boundary refuses writes to
// them unless the host says otherwise — which is the choice `serve --open`
// makes, and which every scratch repository in this file relies on.
const live = GitRepository.layer.pipe(
  Layer.provide(GitRepository.hooksNoop),
  Layer.provideMerge(stores),
  Layer.provideMerge(Policy.anonymousWrites(true)),
);

const decoder = new TextDecoder();
const ZERO = "0".repeat(40);

const alice: Signature = {
  name: "Alice",
  email: "alice@example.com",
  at: new Date(1_700_000_000_000),
  offset: 0,
};

/** Each test gets its own stores, so there is no shared state to reset. */
const scenario = <A, E>(effect: Effect.Effect<A, E, Repository>) =>
  Effect.runPromise(effect.pipe(Effect.provide(live)));

/** main: a <- b <- c, plus an unrelated root on `side`. */
const history = Effect.gen(function* () {
  const repository = yield* Repository;
  const commitOn = (branch: string, message: string) =>
    repository.commit({ branch, tree: EMPTY_TREE_OID, message, author: alice });
  const a = yield* commitOn("main", "a");
  const b = yield* commitOn("main", "b");
  const c = yield* commitOn("main", "c");
  const side = yield* commitOn("side", "elsewhere");
  return { a, b, c, side };
});

/** Request bodies framed by the same `pkt` the server and client use. */
const body = (parts: ReadonlyArray<Uint8Array>): Uint8Array<ArrayBuffer> => {
  const out = new Uint8Array(parts.reduce((sum, part) => sum + part.length, 0));
  let offset = 0;
  for (const part of parts) {
    out.set(part, offset);
    offset += part.length;
  }
  return out;
};

const v0Request = (input: {
  readonly wants: ReadonlyArray<Oid>;
  readonly capabilities?: string;
  readonly haves?: ReadonlyArray<Oid>;
  readonly done?: boolean;
}): Request =>
  new Request("http://host/repo/git-upload-pack", {
    method: "POST",
    body: body([
      ...input.wants.map((oid, index) =>
        pkt(
          `want ${oid}${index === 0 && input.capabilities !== undefined ? ` ${input.capabilities}` : ""}\n`,
        ),
      ),
      FLUSH,
      ...(input.haves ?? []).map((oid) => pkt(`have ${oid}\n`)),
      input.done === true ? pkt("done\n") : FLUSH,
    ]),
  });

const v2Request = (args: ReadonlyArray<string>): Request =>
  new Request("http://host/repo/git-upload-pack", {
    method: "POST",
    headers: { "git-protocol": "version=2" },
    body: body([pkt("command=fetch"), DELIM, ...args.map((arg) => pkt(`${arg}\n`)), FLUSH]),
  });

/**
 * The response's pkt-lines up to where a packfile (or the body) ends.
 *
 * Hand-parsed for the same reason `client/Fetch.ts`'s prelude is:
 * `PktReader` treats a header it cannot parse as corruption, and by the
 * time it says so the four bytes are gone — here those four bytes are the
 * pack's own magic, which is exactly what the assertions need to see.
 */
interface Preamble {
  readonly lines: ReadonlyArray<string>;
  readonly sawPack: boolean;
}

const linesOf = (bytes: Uint8Array): Preamble => {
  const lines: string[] = [];
  let at = 0;
  while (at + 4 <= bytes.length) {
    const header = decoder.decode(bytes.subarray(at, at + 4));
    if (header === "PACK") return { lines, sawPack: true };
    if (!/^[0-9a-f]{4}$/.test(header)) break;
    const length = Number.parseInt(header, 16);
    if (length < 4) {
      at += 4;
      continue;
    }
    lines.push(decoder.decode(bytes.subarray(at + 4, at + length)).replace(/\n$/, ""));
    at += length;
  }
  return { lines, sawPack: false };
};

const answer = (request: Request) =>
  Effect.gen(function* () {
    const response = yield* Protocol.uploadPack(request);
    const bytes = new Uint8Array(yield* Effect.promise(() => response.arrayBuffer()));
    return linesOf(bytes);
  });

const pktLine = (line: string) => `${(line.length + 4).toString(16).padStart(4, "0")}${line}`;

const push = (commands: ReadonlyArray<string>): Request =>
  new Request("http://git.test/r/git-receive-pack", {
    method: "POST",
    body: `${commands.map(pktLine).join("")}0000`,
  });

describe("Protocol negotiation", () => {
  it("advertises multi_ack_detailed on upload-pack", async () => {
    const text = await scenario(
      Effect.gen(function* () {
        yield* history;
        const response = yield* Protocol.advertise("git-upload-pack");
        return decoder.decode(new Uint8Array(yield* Effect.promise(() => response.arrayBuffer())));
      }),
    );
    assert.match(text, /multi_ack_detailed/);
  });

  it("acknowledges every common have, says ready, and ends the round with NAK", async () => {
    const { a, b, lines } = await scenario(
      Effect.gen(function* () {
        const { a, b, c } = yield* history;
        const { lines } = yield* answer(
          v0Request({ wants: [c], capabilities: "multi_ack_detailed", haves: [a, b] }),
        );
        return { a, b, lines };
      }),
    );
    assert.deepEqual(lines, [`ACK ${a} common`, `ACK ${b} common`, `ACK ${b} ready`, "NAK"]);
  });

  it("withholds ready while a want cannot reach the common set", async () => {
    const { a, lines } = await scenario(
      Effect.gen(function* () {
        const { a, c, side } = yield* history;
        const { lines } = yield* answer(
          v0Request({ wants: [c, side], capabilities: "multi_ack_detailed", haves: [a] }),
        );
        return { a, lines };
      }),
    );
    assert.deepEqual(lines, [`ACK ${a} common`, "NAK"]);
  });

  it("answers a plain round with a single bare ACK when the capability is absent", async () => {
    const { a, lines } = await scenario(
      Effect.gen(function* () {
        const { a, b, c } = yield* history;
        const { lines } = yield* answer(v0Request({ wants: [c], haves: [a, b] }));
        return { a, lines };
      }),
    );
    // First common only: a single-ACK client stops offering at one ACK, so
    // acknowledging more would be lines nothing reads.
    assert.deepEqual(lines, [`ACK ${a}`]);
  });

  it("restates common acks before the final ACK on the done round", async () => {
    const { a, lines, sawPack } = await scenario(
      Effect.gen(function* () {
        const { a, c } = yield* history;
        const result = yield* answer(
          v0Request({ wants: [c], capabilities: "multi_ack_detailed", haves: [a], done: true }),
        );
        return { a, ...result };
      }),
    );
    assert.deepEqual(lines, [`ACK ${a} common`, `ACK ${a}`]);
    assert.equal(sawPack, true);
  });

  it("v2: acknowledges without ready while the common set cannot cover the wants", async () => {
    const { a, lines, sawPack } = await scenario(
      Effect.gen(function* () {
        const { a, c, side } = yield* history;
        const result = yield* answer(v2Request([`want ${c}`, `want ${side}`, `have ${a}`]));
        return { a, ...result };
      }),
    );
    assert.deepEqual(lines, ["acknowledgments", `ACK ${a}`]);
    assert.equal(sawPack, false);
  });

  it("v2: says ready and streams the pack once the wants are covered", async () => {
    const { a, lines } = await scenario(
      Effect.gen(function* () {
        const { a, c } = yield* history;
        const result = yield* answer(v2Request([`want ${c}`, `have ${a}`]));
        return { a, ...result };
      }),
    );
    assert.deepEqual(lines.slice(0, 4), ["acknowledgments", `ACK ${a}`, "ready", "packfile"]);
  });
});

describe("receive-pack", () => {
  it.live("refuses a ref name per-ref and applies the rest of the push", () =>
    Effect.gen(function* () {
      const repository = yield* Repository;
      const refs = yield* RefStore;

      const commit = yield* repository.commit({
        branch: "main",
        tree: EMPTY_TREE_OID,
        message: "one",
        author: alice,
      });
      yield* repository.setRef({ name: "refs/heads/gone", to: commit });

      // One delete this server accepts, and one command naming a file in the
      // repository root rather than a ref. `HEAD` passes git's character
      // rules, which is exactly why the check has to be about *where* the
      // name points and not only about how it is spelled.
      const response = yield* Protocol.receivePack(
        push([`${commit} ${ZERO} refs/heads/gone\n`, `${ZERO} ${commit} HEAD\n`]),
      );
      const report = decoder.decode(
        new Uint8Array(yield* Effect.promise(() => response.arrayBuffer())),
      );

      assert.equal(response.status, 200);
      assert.ok(report.includes("unpack ok"), report);
      assert.ok(report.includes("ok refs/heads/gone"), report);
      assert.ok(report.includes("ng HEAD funny refname"), report);

      // The good command took effect and the refused one did not: HEAD is
      // still the symbolic ref it was, not a file holding an oid.
      assert.equal(yield* refs.read("refs/heads/gone"), null);
      assert.equal(yield* repository.head, "refs/heads/main");
    }).pipe(Effect.provide(live)),
  );

  it.live("reads the pack of a push it refuses, so the client sees the report", () =>
    Effect.gen(function* () {
      const repository = yield* Repository;
      const commit = yield* repository.commit({
        branch: "main",
        tree: EMPTY_TREE_OID,
        message: "one",
        author: alice,
      });

      // A create for a refused name: the client has already begun sending
      // its pack. Answering without reading it abandons the request body,
      // and the client sees a torn-down connection instead of this report.
      const body = `${pktLine(`${ZERO} ${commit} HEAD\n`)}0000PACK-and-then-some-bytes`;
      const response = yield* Protocol.receivePack(
        new Request("http://git.test/r/git-receive-pack", { method: "POST", body }),
      );
      const report = decoder.decode(
        new Uint8Array(yield* Effect.promise(() => response.arrayBuffer())),
      );

      assert.equal(response.status, 200);
      assert.ok(report.includes("ng HEAD"), report);
    }).pipe(Effect.provide(live)),
  );

  it.live("fails an atomic push whole when one of its names is refused", () =>
    Effect.gen(function* () {
      const repository = yield* Repository;
      const refs = yield* RefStore;

      const commit = yield* repository.commit({
        branch: "main",
        tree: EMPTY_TREE_OID,
        message: "one",
        author: alice,
      });
      yield* repository.setRef({ name: "refs/heads/gone", to: commit });

      const response = yield* Protocol.receivePack(
        push([`${commit} ${ZERO} refs/heads/gone\0atomic\n`, `${ZERO} ${commit} HEAD\n`]),
      );
      const report = decoder.decode(
        new Uint8Array(yield* Effect.promise(() => response.arrayBuffer())),
      );

      assert.ok(report.includes("ng refs/heads/gone"), report);
      assert.ok(report.includes("ng HEAD"), report);
      // Atomic means the delete did not happen either.
      assert.equal(yield* refs.read("refs/heads/gone"), commit);
    }).pipe(Effect.provide(live)),
  );

  it.live(
    "inflates a compressed body as it reads it, not into memory first",
    () =>
      Effect.gen(function* () {
        const repository = yield* Repository;
        const refs = yield* RefStore;

        const commit = yield* repository.commit({
          branch: "main",
          tree: EMPTY_TREE_OID,
          message: "one",
          author: alice,
        });
        yield* repository.setRef({ name: "refs/heads/gone", to: commit });

        // A quarter of a gigabyte of highly compressible padding behind one
        // valid command — the shape of a zip bomb, and about what deflate's
        // ~1000:1 gets from a request a client could send in a second. The
        // server has to read all of it, because the command comes first; what
        // it must not do is hold it. Buffering every byte a single
        // `gunzip.write` produced is what this replaced, and on a Durable
        // Object with 128 MiB that was an isolate reset per request.
        const padding = new Uint8Array(1024 * 1024);
        const gzip = createGzip();
        const compressed: Array<Uint8Array> = [];
        gzip.on("data", (chunk: Uint8Array) => compressed.push(chunk));
        gzip.write(`${pktLine(`${commit} ${ZERO} refs/heads/gone\n`)}0000`);
        for (let written = 0; written < 256; written++) gzip.write(padding);
        yield* Effect.promise(() => new Promise<void>((resolve) => gzip.end(() => resolve())));

        let peak = 0;
        const sample = setInterval(() => {
          peak = Math.max(peak, process.memoryUsage().arrayBuffers);
        }, 1);
        const before = process.memoryUsage().arrayBuffers;

        const response = yield* Protocol.receivePack(
          new Request("http://git.test/r/git-receive-pack", {
            method: "POST",
            body: Buffer.concat(compressed),
            headers: { "content-encoding": "gzip" },
          }),
        ).pipe(Effect.ensuring(Effect.sync(() => clearInterval(sample))));

        const report = decoder.decode(
          new Uint8Array(yield* Effect.promise(() => response.arrayBuffer())),
        );

        // The command it was carrying was read and applied…
        assert.ok(report.includes("ok refs/heads/gone"), report);
        assert.equal(yield* refs.read("refs/heads/gone"), null);
        // …without the 256 MiB behind it ever being held at once.
        assert.ok(peak - before < 64 * 1024 * 1024, `peak grew by ${peak - before} bytes`);
      }).pipe(Effect.provide(live)),
    { timeout: 60_000 },
  );

  it("advertises a detached HEAD as the commit it holds", async () => {
    const root = await fs.mkdtemp(path.join(os.tmpdir(), "advertise-"));
    try {
      const onDisk = GitRepository.layer.pipe(
        Layer.provide(GitRepository.hooksNoop),
        Layer.provideMerge(nodeStores(root)),
        Layer.provideMerge(Policy.anonymousWrites(true)),
      );

      const advertised = await Effect.runPromise(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const commit = yield* repository.commit({
            branch: "main",
            tree: EMPTY_TREE_OID,
            message: "one",
            author: alice,
          });

          // What `git checkout <sha>`, a rebase or a bisect leaves behind in a
          // served repository: HEAD holds the commit, not the name of a ref.
          yield* Effect.promise(() => fs.writeFile(path.join(root, "HEAD"), `${commit}\n`));

          const response = yield* Protocol.advertise("git-upload-pack");
          return {
            commit,
            text: decoder.decode(
              new Uint8Array(yield* Effect.promise(() => response.arrayBuffer())),
            ),
          };
        }).pipe(Effect.provide(onDisk)),
      );

      // The HEAD line is there, and no `symref=HEAD:<oid>` claims the commit
      // is the name of a ref.
      assert.ok(advertised.text.includes(`${advertised.commit} HEAD`), advertised.text);
      assert.equal(advertised.text.includes("symref=HEAD:"), false, advertised.text);
    } finally {
      await fs.rm(root, { recursive: true, force: true });
    }
  });

  it("refuses a push whose ref name escapes the repository", async () => {
    // The delete-only traversal from the security audit: `next` is the zero
    // oid, so no pack body is needed and the command reaches the ref store on
    // its own. On a filesystem backend the name is joined onto the repository
    // root, so `../../victim` would have unlinked a file outside it — and the
    // reflog append would have written outside it too.
    const sandbox = await fs.mkdtemp(path.join(os.tmpdir(), "receive-pack-escape-"));
    const root = path.join(sandbox, "nested", "repo");
    await fs.mkdir(root, { recursive: true });
    const victim = path.join(sandbox, "victim");
    await fs.writeFile(victim, "precious\n");

    try {
      const onDisk = GitRepository.layer.pipe(
        Layer.provide(GitRepository.hooksNoop),
        Layer.provideMerge(nodeStores(root)),
        Layer.provideMerge(Policy.anonymousWrites(true)),
      );

      const reports = await Effect.runPromise(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const commit = yield* repository.commit({
            branch: "main",
            tree: EMPTY_TREE_OID,
            message: "one",
            author: alice,
          });

          const said: string[] = [];
          for (const name of ["../../victim", "refs/heads/../../../victim", "refs/../../victim"]) {
            // Both directions: the delete that needs no pack, and the create
            // that would write an oid into the escaped path.
            for (const command of [`${commit} ${ZERO} ${name}\n`, `${ZERO} ${commit} ${name}\n`]) {
              const response = yield* Protocol.receivePack(push([command]));
              said.push(
                decoder.decode(new Uint8Array(yield* Effect.promise(() => response.arrayBuffer()))),
              );
            }
          }
          return said;
        }).pipe(Effect.provide(onDisk)),
      );

      // Every one refused per-ref, as a report the client can read — not as a
      // torn-down connection and not as a 500.
      for (const report of reports) {
        assert.ok(report.includes("funny refname"), report);
      }

      // And nothing outside the repository moved.
      assert.equal(await fs.readFile(victim, "utf8"), "precious\n");
      assert.deepEqual((await fs.readdir(sandbox)).sort(), ["nested", "victim"]);
      assert.deepEqual(await fs.readdir(path.join(sandbox, "nested")), ["repo"]);
    } finally {
      await fs.rm(sandbox, { recursive: true, force: true });
    }
  });

  it("reports a ref the store cannot write as a per-ref failure", async () => {
    const root = await fs.mkdtemp(path.join(os.tmpdir(), "receive-pack-"));
    try {
      const onDisk = GitRepository.layer.pipe(
        Layer.provide(GitRepository.hooksNoop),
        Layer.provideMerge(nodeStores(root)),
        Layer.provideMerge(Policy.anonymousWrites(true)),
      );

      const report = await Effect.runPromise(
        Effect.gen(function* () {
          const repository = yield* Repository;
          const commit = yield* repository.commit({
            branch: "main",
            tree: EMPTY_TREE_OID,
            message: "one",
            author: alice,
          });
          // `refs/heads/feature` is a legal ref name, but on a filesystem
          // backend that path is now a directory — git calls this "cannot lock
          // ref", and it has to reach the client as `ng`, not as a 500 with a
          // JSON body no git client can parse.
          yield* repository.setRef({ name: "refs/heads/feature/x", to: commit });

          const response = yield* Protocol.receivePack(
            push([`${ZERO} ${ZERO} refs/heads/feature\n`]),
          );
          return {
            status: response.status,
            text: decoder.decode(
              new Uint8Array(yield* Effect.promise(() => response.arrayBuffer())),
            ),
          };
        }).pipe(Effect.provide(onDisk)),
      );

      assert.equal(report.status, 200);
      assert.ok(report.text.includes("ng refs/heads/feature"), report.text);
    } finally {
      await fs.rm(root, { recursive: true, force: true });
    }
  });
});

/**
 * What each advertisement is allowed to name.
 *
 * v0's `info/refs` hid the hub and trust namespaces; v2's `ls-refs` — which is
 * the advertisement modern git actually asks for — did not, so the hiding had
 * no effect on any real client.
 */
describe("the push report", () => {
  it("says one thing per ref, and nothing about a ref it never submitted", async () => {
    // `allFailed` mapped over the client's whole command list, while `refused`
    // already held an `ng` for every ref the policy gate declined — so a
    // failure after the gate emitted two `ng` lines for one ref, and a status
    // line for a ref the store was never asked about.
    const text = await scenario(
      Effect.gen(function* () {
        const repository = yield* Repository;
        const commit = yield* repository.commit({
          branch: "main",
          tree: EMPTY_TREE_OID,
          message: "one",
          author: alice,
        });

        yield* repository.setRef({ name: "refs/heads/topic", to: commit });

        // Deletions, so there is no pack body to go wrong: one ordinary branch
        // and one ref the namespace rules refuse outright.
        const response = yield* Protocol.receivePack(
          push([
            `${commit} ${ZERO} refs/heads/topic\n`,
            `${commit} ${ZERO} refs/meta/trust/genesis\n`,
          ]),
        );
        const bytes = new Uint8Array(yield* Effect.promise(() => response.arrayBuffer()));
        return linesOf(bytes).lines;
      }),
    );

    const named = text.filter((line) => /^(ok|ng) refs\//.test(line));
    const genesis = named.filter((line) => line.includes("refs/meta/trust/genesis"));
    assert.equal(genesis.length, 1, `one line per ref: ${named.join(" | ")}`);
    assert.ok(genesis[0]?.startsWith("ng "), `the genesis must be refused: ${named.join(" | ")}`);
    assert.ok(
      named.some((line) => line.startsWith("ok refs/heads/topic")),
      `the other ref must still land: ${named.join(" | ")}`,
    );
  });
});

describe("an atomic push the policy refuses", () => {
  it("reports every command, rather than nothing at all", async () => {
    // `Policy.gate` returns no allowed updates for a refused atomic batch, so
    // reporting on *those* produced zero `ng` lines and the client saw a bare
    // "unpack ok" for a push that had been refused outright.
    const report = await scenario(
      Effect.gen(function* () {
        const repository = yield* Repository;
        const commit = yield* repository.commit({
          branch: "main",
          tree: EMPTY_TREE_OID,
          message: "one",
          author: alice,
        });
        yield* repository.setRef({ name: "refs/heads/topic", to: commit });

        const request = new Request("http://git.test/r/git-receive-pack", {
          method: "POST",
          body: `${[
            `${commit} ${ZERO} refs/heads/topic\0report-status atomic\n`,
            `${commit} ${ZERO} refs/meta/trust/genesis\n`,
          ]
            .map(pktLine)
            .join("")}0000`,
        });
        const response = yield* Protocol.receivePack(request);
        const bytes = new Uint8Array(yield* Effect.promise(() => response.arrayBuffer()));
        return { lines: linesOf(bytes).lines, now: yield* repository.resolve("refs/heads/topic") };
      }),
    );

    const named = report.lines.filter((line) => /^(ok|ng) refs\//.test(line));
    assert.equal(named.length, 2, `every command needs a line: ${report.lines.join(" | ")}`);
    assert.ok(
      named.every((line) => line.startsWith("ng ")),
      `atomic means none of them applied: ${named.join(" | ")}`,
    );
    assert.notEqual(report.now, null, "and nothing was deleted");
  });
});

describe("what a signed envelope binds", () => {
  it("refuses a ref it never named before the pack is unpacked", async () => {
    // The envelope names the refs a native client is moving and where to, and
    // checking it needs nothing from the pack — unlike the force-push rule,
    // which cannot tell a fast-forward from a rewrite until the objects are
    // present. Left to the gate, a push naming refs the signature never
    // covered had its whole pack unpacked and persisted first.
    //
    // The body is deliberately not a pack: a refusal that arrives before the
    // unpack cannot have noticed, and one that arrives after says so.
    const report = await scenario(
      Effect.gen(function* () {
        const response = yield* Protocol.receivePack(
          new Request("http://git.test/r/git-receive-pack", {
            method: "POST",
            body:
              `${pktLine(`${ZERO} ${"a".repeat(40)} refs/heads/elsewhere\0report-status`)}` +
              `0000not a pack at all`,
          }),
        ).pipe(
          Effect.provide(
            Auth.requester({
              ...Auth.anonymous,
              capabilities: ["source.push"],
              envelope: {
                type: "auth.request",
                version: 1,
                repo: Auth.anonymous.projection.repoId,
                operation: "git-receive-pack",
                commands: [{ ref: "refs/heads/agreed", from: null, to: "a".repeat(40) }],
                nonce: "n",
                expiresAt: new Date(Date.now() + 60_000).toISOString(),
              },
            }),
          ),
        );
        const bytes = new Uint8Array(yield* Effect.promise(() => response.arrayBuffer()));
        return linesOf(bytes).lines;
      }),
    );

    const line = report.find((entry) => entry.includes("refs/heads/elsewhere"));
    assert.match(line ?? "", /^ng /, report.join(" | "));
    assert.match(line ?? "", /did not name this ref/, report.join(" | "));
    assert.ok(
      !report.some((entry) => entry.includes("unpacker")),
      `refused before the unpack: ${report.join(" | ")}`,
    );
    // One line per ref. `respond` concatenates the refusals it accumulated
    // with whatever `allFailed` produced, so a ref refused before that point
    // used to get an `ng` here as well — twice, with two different reasons.
    assert.equal(
      report.filter((entry) => entry.includes("refs/heads/elsewhere")).length,
      1,
      report.join(" | "),
    );
  });

  it("applies none of an atomic push when one ref was never named", async () => {
    // Atomic is all-or-nothing, and an uncovered ref is part of the all. The
    // covered half was filtered out of what the gate saw and then landed on
    // its own, which is the one thing the capability promises will not happen.
    const outcome = await scenario(
      Effect.gen(function* () {
        const repository = yield* Repository;
        const commit = yield* repository.commit({
          branch: "agreed",
          tree: EMPTY_TREE_OID,
          message: "one",
          author: alice,
        });

        const response = yield* Protocol.receivePack(
          new Request("http://git.test/r/git-receive-pack", {
            method: "POST",
            body:
              `${pktLine(`${commit} ${ZERO} refs/heads/agreed\0report-status atomic`)}` +
              `${pktLine(`${ZERO} ${"a".repeat(40)} refs/heads/elsewhere`)}0000`,
          }),
        ).pipe(
          Effect.provide(
            Auth.requester({
              ...Auth.anonymous,
              capabilities: ["source.push", "source.delete"],
              envelope: {
                type: "auth.request",
                version: 1,
                repo: Auth.anonymous.projection.repoId,
                operation: "git-receive-pack",
                commands: [{ ref: "refs/heads/agreed", from: commit, to: null }],
                nonce: "n",
                expiresAt: new Date(Date.now() + 60_000).toISOString(),
              },
            }),
          ),
        );
        const bytes = new Uint8Array(yield* Effect.promise(() => response.arrayBuffer()));
        return {
          lines: linesOf(bytes).lines,
          still: yield* repository.resolve("refs/heads/agreed"),
        };
      }),
    );

    const named = outcome.lines.filter((line) => /^(ok|ng) refs\//.test(line));
    assert.equal(named.length, 2, `every command needs a line: ${outcome.lines.join(" | ")}`);
    assert.ok(
      named.every((line) => line.startsWith("ng ")),
      `atomic means none of them applied: ${named.join(" | ")}`,
    );
    assert.notEqual(outcome.still, null, "and the covered half did not land on its own");
  });
});

describe("advertisement hiding", () => {
  const lsRefs = (prefixes: ReadonlyArray<string> = []): Request =>
    new Request("http://host/repo/git-upload-pack", {
      method: "POST",
      headers: { "git-protocol": "version=2" },
      body: body([
        pkt("command=ls-refs"),
        DELIM,
        ...prefixes.map((prefix) => pkt(`ref-prefix ${prefix}\n`)),
        FLUSH,
      ]),
    });

  const namedBy = (request: Request) =>
    Effect.gen(function* () {
      const response = yield* Protocol.uploadPack(request);
      const bytes = new Uint8Array(yield* Effect.promise(() => response.arrayBuffer()));
      return linesOf(bytes).lines.map((line) => line.split(" ")[1] ?? "");
    });

  const withHubRefs = Effect.fn("test.withHubRefs")(function* () {
    const repository = yield* Repository;
    const commit = yield* repository.commit({
      branch: "main",
      tree: EMPTY_TREE_OID,
      message: "one",
      author: alice,
    });
    yield* repository.setRef({ name: "refs/meta/trust/log", to: commit });
    yield* repository.setRef({ name: "refs/meta/trust/genesis", to: commit });
    yield* repository.setRef({ name: "refs/hub/pr/1", to: commit });
    return commit;
  });

  it("hides hub and trust refs from a v2 ls-refs that asked for everything", async () => {
    const refs = await scenario(
      Effect.gen(function* () {
        yield* withHubRefs();
        return yield* namedBy(lsRefs());
      }),
    );

    assert.ok(refs.includes("refs/heads/main"), refs.join("\n"));
    // The identity stays: it is what lets any client compute the RepoID and
    // check it against what they trust, and hiding it would make verification
    // need permission.
    assert.ok(refs.includes("refs/meta/trust/genesis"), refs.join("\n"));
    assert.ok(!refs.includes("refs/meta/trust/log"), `trust log leaked: ${refs.join("\n")}`);
    assert.ok(!refs.includes("refs/hub/pr/1"), `hub ref leaked: ${refs.join("\n")}`);
  });

  it("hides them from `ref-prefix refs/`, which is what everything looks like", async () => {
    const refs = await scenario(
      Effect.gen(function* () {
        yield* withHubRefs();
        return yield* namedBy(lsRefs(["refs/"]));
      }),
    );
    assert.ok(refs.includes("refs/heads/main"), refs.join("\n"));
    assert.ok(!refs.includes("refs/hub/pr/1"), `hub ref leaked: ${refs.join("\n")}`);
  });

  it("answers a client that names the namespace, which is the only way to fetch it", async () => {
    // Hiding is about sparing a stock clone an event per comment, not about
    // withholding state. v0 hides these too, so a `ref-prefix` that names the
    // namespace is the *only* way a replica can ever learn these oids — and
    // without it `hub enable` and `Replication.pull` reported success having
    // fetched no grants and no revocations.
    const refs = await scenario(
      Effect.gen(function* () {
        yield* withHubRefs();
        return yield* namedBy(lsRefs(["refs/meta/trust/", "refs/hub/"]));
      }),
    );

    assert.ok(refs.includes("refs/meta/trust/log"), refs.join("\n"));
    assert.ok(refs.includes("refs/hub/pr/1"), refs.join("\n"));
  });

  it("does not hand a namespace to a prefix that merely starts the same way", async () => {
    // `ls-remote 'refs/h*'` names nothing — three characters, and not the
    // namespace — so answering it returned the whole of `refs/hub/`: exactly
    // the state the hiding exists to withhold from a client that did not ask.
    const refs = await scenario(
      Effect.gen(function* () {
        yield* withHubRefs();
        return yield* namedBy(lsRefs(["refs/h", "refs/m"]));
      }),
    );

    assert.ok(!refs.includes("refs/hub/pr/1"), `hub ref leaked: ${refs.join("\n")}`);
    assert.ok(!refs.includes("refs/meta/trust/log"), `trust log leaked: ${refs.join("\n")}`);
  });

  it("answers one that names it without a trailing slash, which is what git sends", async () => {
    // git derives the prefix from the configured refspec, so `refs/hub/*`
    // yields `refs/hub` — no slash. The un-hiding test was written against
    // `hiddenFromAdvertisement`, which asks whether a *ref* is hidden, and
    // `refs/hub` is not one: the answer came back empty for a client that had
    // asked by name, and a mirror reported a replication that fetched nothing.
    const refs = await scenario(
      Effect.gen(function* () {
        yield* withHubRefs();
        return yield* namedBy(lsRefs(["refs/meta/trust", "refs/hub"]));
      }),
    );

    assert.ok(refs.includes("refs/meta/trust/log"), refs.join("\n"));
    assert.ok(refs.includes("refs/hub/pr/1"), refs.join("\n"));
  });

  it("still hides them from a prefix that only overlaps `refs/`", async () => {
    // The prefix has to reach the namespace, not merely share a start with it:
    // `refs/heads/` names no hidden ref, and answering it with hub state would
    // undo the hiding for every stock client that fetches branches by prefix.
    const refs = await scenario(
      Effect.gen(function* () {
        yield* withHubRefs();
        return yield* namedBy(lsRefs(["refs/heads/", "refs/hub"]));
      }),
    );

    assert.ok(refs.includes("refs/heads/main"), refs.join("\n"));
    assert.ok(refs.includes("refs/hub/pr/1"), "the named namespace still answers");
    assert.ok(!refs.includes("refs/meta/trust/log"), `trust log leaked: ${refs.join("\n")}`);
  });

  it("still shows them to a pusher, who has to know what it is replacing", async () => {
    // receive-pack's old-oid is how a stale push is caught, so hiding these
    // from the push advertisement would make every hub ref writable exactly
    // once and then never again.
    const text = await scenario(
      Effect.gen(function* () {
        yield* withHubRefs();
        const response = yield* Protocol.advertise("git-receive-pack");
        return decoder.decode(new Uint8Array(yield* Effect.promise(() => response.arrayBuffer())));
      }),
    );

    assert.ok(text.includes("refs/meta/trust/log"), text);
    assert.ok(text.includes("refs/hub/pr/1"), text);
  });
});
