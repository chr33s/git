/**
 * Git smart-HTTP endpoints.
 *
 * Web `Request` in, web `Response` out, `Repository` the only requirement —
 * no store, no platform type reaches a handler, so the same functions serve
 * from a Durable Object and from `node:http`. Protocol v0, stateless-rpc, as
 * the smart-HTTP transport defines it:
 *
 *   GET  …/info/refs?service=git-{upload,receive}-pack   advertisement
 *   POST …/git-upload-pack                               wants/haves -> pack
 *   POST …/git-receive-pack                              commands + pack -> report
 *
 * Negotiation speaks `multi_ack_detailed`: every common `have` is tagged
 * `ACK <oid> common` so the client keeps offering past the first hit, and
 * `ACK <oid> ready` goes out once `Repository.canServe` proves a pack cut at
 * the common set is complete. Deliberately not advertised: `thin-pack` (the
 * writer emits full objects) and plain `multi_ack`, which `_detailed`
 * supersedes.
 */
import { createGunzip } from "node:zlib";

import { concatBytes as concat } from "../git/Format.ts";
import { Effect, Stream } from "effect";

import { type GitError, Invalid, PackCorrupt, type StorageFailure } from "../git/Error.ts";
import { bandChunks, DELIM, FLUSH, pkt, PktReader } from "../git/Pkt.ts";
import { type ReceiveResult, Repository } from "../git/Repository.ts";
import * as Refspec from "../git/Refspec.ts";
import * as Policy from "./Policy.ts";
import * as Redaction from "../hub/Redaction.ts";
import { checkRefAddress, checkRefName, isOid, type Oid, type RefUpdate } from "../git/Store.ts";

const decoder = new TextDecoder();

const ZERO_OID = "0".repeat(40);
const AGENT = "agent=chr33s-git/0";

const corrupt = (reason: string) => new PackCorrupt({ reason });

/**
 * The objects a fetch needs, computed strictly and retried once.
 *
 * A redacted payload is absent by design while the tree naming it survives, so
 * a strict closure fails the moment anything has been redacted — and computing
 * "which absences a tombstone accounts for" up front would fold the trust log
 * and walk every pull request on *every* fetch, including the overwhelming
 * majority that never touch a hub ref at all.
 *
 * So the strict walk goes first and pays nothing, and only a walk that actually
 * hits a missing object asks what the tombstones say. An absence no tombstone
 * covers fails the retry too, which is the corruption it is.
 */
const planFor = (request: {
  readonly wants: ReadonlyArray<Oid>;
  readonly haves: ReadonlyArray<Oid>;
  readonly clientShallow: ReadonlyArray<Oid>;
  readonly depth: number | undefined;
  readonly since: Date | undefined;
  readonly notRefs: ReadonlyArray<string>;
}) =>
  Effect.gen(function* () {
    const repository = yield* Repository;

    // A deepening fetch assembles its object set without reading blobs, so it
    // cannot notice a redacted one and would succeed with it in the plan — and
    // the failure would then land in `packOids`, after the 200 and the
    // boundary lines had gone out. There is nothing to retry from there, so
    // that path pays for the exclusion up front. It is also the rare one:
    // ordinary clones and fetches take the strict walk below and pay nothing.
    const deepening =
      request.depth !== undefined || request.since !== undefined || request.notRefs.length > 0;
    if (deepening) {
      const exclude = yield* Redaction.excluded().pipe(Effect.orElseSucceed(() => new Set<Oid>()));
      return yield* repository.fetch({ ...request, exclude });
    }

    // Only a *missing object* is worth a second look; a storage fault is not
    // something tombstones explain, and retrying it would pay for a trust fold
    // to fail the same way twice.
    const plan = yield* repository
      .fetch(request)
      .pipe(Effect.catchTag("ObjectNotFound", () => Effect.succeed(null)));
    if (plan !== null) return plan;

    const exclude = yield* Redaction.excluded().pipe(Effect.orElseSucceed(() => new Set<Oid>()));
    return yield* repository.fetch({ ...request, exclude });
  });

/** A text pkt-line, the conventional trailing newline stripped. */
const text = (payload: Uint8Array): string => {
  const decoded = decoder.decode(payload);
  return decoded.endsWith("\n") ? decoded.slice(0, -1) : decoded;
};

/**
 * The request body as chunks. git compresses large negotiation bodies, and
 * announces it the standard way; a pack body is already deflated per object,
 * so pushes arrive identity-encoded.
 */
const body = (request: Request): AsyncIterable<Uint8Array> => {
  const raw = request.body;
  if (raw === null) return (async function* () {})();
  if (request.headers.get("content-encoding")?.includes("gzip") !== true) return raw;

  return (async function* () {
    const gunzip = createGunzip();

    // Backpressure waits on `drain` *or* on the failure that means `drain`
    // will never arrive: corrupt input mid-write would otherwise hang the
    // request until the platform times the whole slot out.
    const drain = () =>
      new Promise<void>((resolve, reject) => {
        const settle = (error?: Error) => {
          gunzip.off("drain", onDrain);
          gunzip.off("error", onError);
          gunzip.off("close", onDrain);
          if (error === undefined) resolve();
          else reject(error);
        };
        const onDrain = () => settle();
        const onError = (error: Error) => settle(error);
        gunzip.once("drain", onDrain);
        gunzip.once("error", onError);
        gunzip.once("close", onDrain);
      });

    /**
     * The writes run beside the reads, not before them.
     *
     * Draining the transform into an array and yielding the array afterwards
     * — which is what this did — is not backpressure at all: attaching a
     * `data` handler puts the readable side in flowing mode, so zlib expands
     * as fast as it can and every byte lands in memory first. Deflate reaches
     * about 1000:1, so one 64 KiB request chunk becomes ~64 MiB and a few of
     * them exhaust a 128 MiB Durable Object. Iterating the transform leaves
     * it paused between reads, and then a full readable buffer stalls the
     * transform, `write` returns false, and the whole pipe is bounded by the
     * two high-water marks however compressible the body is.
     */
    // The failure travels through `gunzip`, which the loop below is reading:
    // destroying it there is what turns a broken body into a thrown error at
    // the consumer instead of a body that simply stops.
    const pumped = (async () => {
      try {
        for await (const chunk of raw) {
          if (!gunzip.write(chunk)) await drain();
        }
        gunzip.end();
      } catch (error) {
        gunzip.destroy(error instanceof Error ? error : new Error(String(error)));
      }
    })().catch(() => undefined);

    try {
      // SAFETY: a zlib transform is a readable stream, which node makes async
      // iterable at runtime; only the bundled lib declarations omit it.
      for await (const chunk of gunzip as AsyncIterable<Uint8Array>) yield chunk;
    } finally {
      gunzip.destroy();
      await pumped;
    }
  })();
};

const headers = (type: string) => ({
  "cache-control": "no-cache",
  "content-type": type,
});

/** `Git-Protocol: version=2`, which is how a client opts in to v2. */
const isVersionTwo = (request: Request): boolean =>
  (request.headers.get("git-protocol") ?? "")
    .split(":")
    .some((value) => value.trim() === "version=2");

/**
 * Protocol v2's advertisement: capabilities only, no refs.
 *
 * That is the whole point of v2 — a repository with fifty thousand refs no
 * longer sends all of them before the client can say it only wants one. The
 * refs come back from `ls-refs`, filtered by prefix.
 */
const advertiseV2 = (): Response =>
  new Response(
    concat([
      pkt("version 2\n"),
      pkt(`${AGENT}\n`),
      pkt("ls-refs=unborn\n"),
      pkt("fetch=shallow\n"),
      pkt("object-format=sha1\n"),
      FLUSH,
    ]),
    { headers: headers("application/x-git-upload-pack-advertisement") },
  );

/** `GET /info/refs?service=…` — refs, capabilities on the first line. */
export const advertise = (
  service: "git-upload-pack" | "git-receive-pack",
): Effect.Effect<Response, GitError, Repository> =>
  Effect.gen(function* () {
    const repository = yield* Repository;
    const refs = yield* repository.refs;
    const head = yield* repository.head;

    // A detached HEAD holds the commit rather than the name of a ref — git
    // leaves one behind after `checkout <sha>`, a rebase or a bisect — and
    // there is no symbolic target to announce for it.
    const detached = isOid(head);
    const symref = detached ? "" : ` symref=HEAD:${head}`;

    const caps =
      service === "git-upload-pack"
        ? `multi_ack_detailed shallow deepen-since deepen-not side-band-64k${symref} ${AGENT}`
        : `report-status delete-refs atomic side-band-64k ${AGENT}`;

    const lines: string[] = [];
    if (service === "git-upload-pack") {
      const target = detached ? head : refs.find(([name]) => name === head)?.[1];
      if (target !== undefined) lines.push(`${target} HEAD`);
    }
    // Hub and trust refs are left out of the v0 advertisement. A repository
    // with a year of review history has more hub refs than branches, and a
    // stock `git clone` would pay for all of them on every fetch to get
    // something it cannot read. Clients that want them ask by name — the
    // refspec is what carries the request, and `want` still serves the oids.
    // The genesis stays visible: it is one commit, and it is what lets any
    // client compute the RepoID and check it against what it trusts.
    for (const [name, oid] of refs) {
      // Only from the fetch advertisement. A client pushing has to know what
      // it is replacing — receive-pack's old-oid is how a stale push is
      // caught — so hiding these here would make every hub ref writable
      // exactly once and then never again.
      if (service === "git-upload-pack" && Refspec.hiddenFromAdvertisement(name)) continue;
      lines.push(`${oid} ${name}`);
    }

    const parts: Uint8Array[] = [pkt(`# service=${service}\n`), FLUSH];
    if (lines.length === 0) {
      // An empty repository still advertises capabilities, on a line whose
      // ref name is the placeholder git defined for exactly this case.
      parts.push(pkt(`${ZERO_OID} capabilities^{}\0${caps}\n`));
    } else {
      lines.forEach((line, index) => {
        parts.push(pkt(index === 0 ? `${line}\0${caps}\n` : `${line}\n`));
      });
    }
    parts.push(FLUSH);

    return new Response(concat(parts), {
      headers: headers(`application/x-${service}-advertisement`),
    });
  });

const step = <A>(run: () => Promise<A>) =>
  Effect.tryPromise({
    try: run,
    catch: (cause) =>
      cause instanceof PackCorrupt || cause instanceof Invalid
        ? cause
        : corrupt(cause instanceof Error ? cause.message : JSON.stringify(cause)),
  });

/**
 * The offered haves this repository holds, in offered order — the one
 * negotiation computation v0 and v2 share. `all` is the dialect switch:
 * single-ACK stops at the first hit (nothing reads a second), multi-ack
 * checks the whole prefix with bounded concurrency.
 */
const commonOf = (
  repository: Repository["Service"],
  haves: ReadonlyArray<Oid>,
  all: boolean,
): Effect.Effect<ReadonlyArray<Oid>, StorageFailure> =>
  Effect.gen(function* () {
    if (!all) {
      for (const have of haves) {
        if (yield* repository.contains(have)) return [have];
      }
      return [];
    }
    const held = yield* Effect.forEach(haves, (have) => repository.contains(have), {
      concurrency: 16,
    });
    return haves.filter((_, index) => held[index] === true);
  });

/** `POST /git-upload-pack` — wants and haves in, ACK/NAK and a pack out. */
export const uploadPack = (request: Request): Effect.Effect<Response, GitError, Repository> =>
  Effect.gen(function* () {
    const repository = yield* Repository;

    // v2 is a different conversation, and the client announces it in a
    // header rather than in the body — so the version is known before a
    // single pkt-line is read.
    if (isVersionTwo(request)) {
      const parsed = yield* readV2(new PktReader(body(request)));
      if (parsed.command === "ls-refs") return yield* lsRefs(parsed);
      if (parsed.command === "fetch") return yield* fetchV2(parsed);
      return yield* new Invalid({
        field: "command",
        reason: `unknown v2 command '${parsed.command}'`,
      });
    }

    const reader = new PktReader(body(request));

    const wants: Oid[] = [];
    const haves: Oid[] = [];
    const clientShallow: Oid[] = [];
    const notRefs: string[] = [];
    let depth: number | undefined;
    let since: Date | undefined;
    let sideband = false;
    let multiAck = false;
    let done = false;
    let first = true;

    yield* step(async () => {
      for (;;) {
        const item = await reader.next();
        if (item === "eof") return;
        // v2's separators cannot appear in a v0 body, and treating them as
        // nothing keeps the reader total rather than throwing on a byte
        // sequence this branch simply does not expect.
        if (item === "flush" || item === "delim" || item === "end") continue;
        let line = text(item);

        // Capabilities ride on the first `want`, space-separated after the
        // oid — unlike receive-pack, which puts them after a NUL. Reading
        // this wrong is silent: the client negotiates side-band and the
        // server answers with a raw pack.
        if (first && line.startsWith("want ")) {
          const capabilities = line.slice(45).trim();
          if (capabilities !== "") {
            const requested = capabilities.split(" ");
            sideband = requested.includes("side-band-64k");
            multiAck = requested.includes("multi_ack_detailed");
            line = line.slice(0, 45).trimEnd();
          }
          first = false;
        }

        const [keyword] = line.split(" ", 1);
        const argument = line.slice((keyword?.length ?? 0) + 1);
        const oid = argument.slice(0, 40);

        if (keyword === "want" && isOid(oid)) wants.push(oid);
        else if (keyword === "have" && isOid(oid)) haves.push(oid);
        else if (keyword === "shallow" && isOid(oid)) clientShallow.push(oid);
        else if (keyword === "deepen") {
          const value = Number.parseInt(argument, 10);
          // `deepen 0` is git's way of saying "no depth limit".
          if (!Number.isInteger(value) || value < 0) {
            throw new Invalid({ field: "deepen", reason: `bad depth '${argument}'` });
          }
          if (value > 0) depth = value;
        } else if (keyword === "deepen-since") {
          const seconds = Number.parseInt(argument, 10);
          if (!Number.isInteger(seconds)) {
            throw new Invalid({ field: "deepen-since", reason: `bad timestamp '${argument}'` });
          }
          since = new Date(seconds * 1000);
        } else if (keyword === "deepen-not") {
          notRefs.push(argument);
        } else if (line === "done") {
          done = true;
          return;
        } else {
          throw new Invalid({ field: "upload-pack", reason: `unexpected line '${line}'` });
        }
      }
    });

    if (wants.length === 0) {
      return yield* new Invalid({ field: "upload-pack", reason: "no 'want' lines" });
    }

    const deepening = depth !== undefined || since !== undefined || notRefs.length > 0;

    // Which of the offered haves this repository possesses. Without
    // multi_ack the first hit is the whole answer — a single-ACK client
    // stops offering the moment it gets one, so checking further haves would
    // be work nothing reads. With `multi_ack_detailed` every hit matters —
    // each one is a base the pack can be cut at — so all of them are
    // checked, concurrently: the offered prefix regrows every stateless
    // round, and paying one storage round-trip per have back-to-back would
    // stack that latency ahead of the first ACK byte.
    const common = yield* commonOf(repository, haves, multiAck);
    const last = common.at(-1);

    // A plain negotiation round is answered without a fetch plan at all: it
    // replies acknowledgments and waits to be asked again, so computing the
    // closure here would be a full walk per round, thrown away each time.
    // `canServe` is the exception, and a deliberate one — its bounded walk
    // is what earns the `ready` that ends the conversation early.
    if (!done && !deepening) {
      const lines = multiAck
        ? [
            ...common.map((oid) => pkt(`ACK ${oid} common\n`)),
            ...(last !== undefined && (yield* repository.canServe(wants, common))
              ? [pkt(`ACK ${last} ready\n`)]
              : []),
            // Every non-done round ends with NAK — the round's terminator
            // in the multi_ack dialects, not a contradiction of the ACKs.
            pkt("NAK\n"),
          ]
        : [last === undefined ? pkt("NAK\n") : pkt(`ACK ${last}\n`)];
      return new Response(concat(lines), {
        headers: headers("application/x-git-upload-pack-result"),
      });
    }

    // `fetch` reads an undefined `depth` or `since` exactly as it reads their
    // absence, and an empty `notRefs` as no stops — the locals pass through.
    const plan = yield* planFor({ wants, haves, clientShallow, depth, since, notRefs });

    /**
     * The boundary section comes before anything else, and only when the
     * client asked to deepen — it is how the client learns which commits to
     * record as having hidden parents.
     */
    const boundary = !deepening
      ? []
      : [
          ...plan.shallow.map((oid) => pkt(`shallow ${oid}\n`)),
          ...plan.unshallow.map((oid) => pkt(`unshallow ${oid}\n`)),
          FLUSH,
        ];

    if (!done) {
      // A deepen round is answered with the boundary section and its flush
      // *alone*, and the difference is not cosmetic: fetch-pack reads exactly
      // that much before deciding what to ask for next, and a trailing NAK
      // leaves it reading a pack that is not there.
      return new Response(concat(boundary), {
        headers: headers("application/x-git-upload-pack-result"),
      });
    }

    // The done round restates the acknowledgments a stateless server never
    // saw itself send: `common` lines first under multi_ack_detailed —
    // fetch-pack reads them again before the final line — then the bare
    // `ACK`/`NAK` that says the pack follows.
    const acks = multiAck ? common.map((oid) => pkt(`ACK ${oid} common\n`)) : [];
    const prelude = concat([
      ...boundary,
      ...acks,
      last === undefined ? pkt("NAK\n") : pkt(`ACK ${last}\n`),
    ]);
    const pack = repository.packOids(plan.oids);

    const packStream = Stream.concat(
      Stream.fromIterable([prelude]),
      // Multiplexed when asked for: pack bytes on band 1, and a flush to
      // close the stream, which an unmultiplexed body does not need.
      sideband
        ? Stream.concat(
            pack.pipe(Stream.flatMap((bytes) => Stream.fromIterable(bandChunks(bytes)))),
            Stream.fromIterable([FLUSH]),
          )
        : pack,
    );

    return new Response(Stream.toReadableStream(packStream), {
      headers: headers("application/x-git-upload-pack-result"),
    });
  });

/** A v2 request: a command, then capabilities, then `0001`, then arguments. */
interface V2Request {
  readonly command: string;
  readonly args: ReadonlyArray<string>;
}

const readV2 = (reader: PktReader) =>
  step(async (): Promise<V2Request> => {
    let command = "";
    const args: string[] = [];
    let afterDelim = false;

    for (;;) {
      const item = await reader.next();
      if (item === "eof" || item === "flush") break;
      if (item === "delim") {
        afterDelim = true;
        continue;
      }
      if (item === "end") break;

      const line = text(item);
      if (afterDelim) args.push(line);
      else if (line.startsWith("command=")) command = line.slice(8);
    }

    return { command, args };
  });

/**
 * `ls-refs`: the refs a client asks for, rather than every ref there is.
 *
 * `peel` and `symrefs` are opt-in for the same reason — an annotated tag's
 * target and HEAD's symbolic name both cost a read, and most callers want
 * neither.
 */
const lsRefs = (request: V2Request): Effect.Effect<Response, GitError, Repository> =>
  Effect.gen(function* () {
    const repository = yield* Repository;
    const refs = yield* repository.refs;
    const head = yield* repository.head;

    const prefixes = request.args
      .filter((line) => line.startsWith("ref-prefix "))
      .map((line) => line.slice(11));
    const peel = request.args.includes("peel");
    const symrefs = request.args.includes("symrefs");
    const unborn = request.args.includes("unborn");

    const matches = (name: string) =>
      prefixes.length === 0 || prefixes.some((prefix) => name.startsWith(prefix));

    const lines: Uint8Array[] = [];

    // Detached: HEAD is the commit itself, and has no symbolic target.
    const detached = isOid(head);
    const target = symrefs && !detached ? ` symref-target:${head}` : "";
    const headOid = detached ? head : refs.find(([name]) => name === head)?.[1];
    if (matches("HEAD")) {
      if (headOid !== undefined) {
        lines.push(pkt(`${headOid} HEAD${target}\n`));
      } else if (unborn) {
        // A repository with no commits still has a HEAD, and v2 has a way to
        // say so — which is what lets a clone of an empty repo set up the
        // right branch name rather than guessing.
        lines.push(pkt(`unborn HEAD${target}\n`));
      }
    }

    for (const [name, oid] of refs) {
      if (!matches(name)) continue;
      // v2's advertisement, and so subject to the same hiding as v0's — with
      // the exception v2 is able to express and v0 is not. Hiding is about
      // sparing a stock clone an event per comment, not about withholding
      // state: a client that *names* the namespace is asking for it, and this
      // is the only way it can ever be fetched, since v0 withholds it too.
      //
      // The prefix has to name a hidden namespace itself, not merely overlap
      // one. `ref-prefix refs/` is what "everything" looks like, and answering
      // it with hub state would be the default this exists to avoid.
      if (
        Refspec.hiddenFromAdvertisement(name) &&
        !prefixes.some((prefix) => name.startsWith(prefix) && Refspec.namesHiddenNamespace(prefix))
      ) {
        continue;
      }
      let line = `${oid} ${name}`;
      if (peel && name.startsWith("refs/tags/")) {
        const peeled = yield* repository.readTag(oid).pipe(
          Effect.map((tag) => tag.object),
          // A lightweight tag points straight at its target: nothing to peel.
          Effect.catchTag("ObjectNotFound", () => Effect.succeed(null)),
        );
        if (peeled !== null) line += ` peeled:${peeled}`;
      }
      lines.push(pkt(`${line}\n`));
    }

    lines.push(FLUSH);
    return new Response(concat(lines), {
      headers: headers("application/x-git-upload-pack-result"),
    });
  });

/**
 * `fetch`: the same negotiation as v0, in named sections.
 *
 * The packfile section is always multiplexed in v2 — side-band is not a
 * capability here, it is the format — so the `sideband` flag v0 carries has
 * no counterpart.
 */
const fetchV2 = (request: V2Request): Effect.Effect<Response, GitError, Repository> =>
  Effect.gen(function* () {
    const repository = yield* Repository;

    const wants: Oid[] = [];
    const haves: Oid[] = [];
    const clientShallow: Oid[] = [];
    const notRefs: string[] = [];
    let depth: number | undefined;
    let since: Date | undefined;
    let done = false;

    for (const line of request.args) {
      const [keyword] = line.split(" ", 1);
      const argument = line.slice((keyword?.length ?? 0) + 1);
      const oid = argument.slice(0, 40);

      if (keyword === "want" && isOid(oid)) wants.push(oid);
      else if (keyword === "have" && isOid(oid)) haves.push(oid);
      else if (keyword === "shallow" && isOid(oid)) clientShallow.push(oid);
      else if (keyword === "deepen") {
        const value = Number.parseInt(argument, 10);
        if (Number.isInteger(value) && value > 0) depth = value;
      } else if (keyword === "deepen-since") {
        const seconds = Number.parseInt(argument, 10);
        if (Number.isInteger(seconds)) since = new Date(seconds * 1000);
      } else if (keyword === "deepen-not") notRefs.push(argument);
      else if (line === "done") done = true;
    }

    if (wants.length === 0) {
      return yield* new Invalid({ field: "fetch", reason: "no 'want' lines" });
    }

    const deepening = depth !== undefined || since !== undefined || notRefs.length > 0;

    // Without `done` the client is still negotiating: acknowledge what is
    // common, and only continue into a pack when `canServe` proves one can
    // be cut at the common set. `ready` on the first shared commit — the
    // eager version — would end negotiation with whatever base happened to
    // come first, which is single-ACK's fat-pack mistake in v2 syntax. The
    // acknowledgments section is built once; the round's outcome only
    // decides how it closes — flush and come back, or `ready` and the pack
    // in the same response.
    const acks: Uint8Array[] = [];
    if (!done) {
      const common = yield* commonOf(repository, haves, true);
      const ready = common.length > 0 && (yield* repository.canServe(wants, common));

      acks.push(pkt("acknowledgments\n"));
      if (common.length === 0) acks.push(pkt("NAK\n"));
      else for (const oid of common) acks.push(pkt(`ACK ${oid}\n`));

      if (!ready) {
        return new Response(concat([...acks, FLUSH]), {
          headers: headers("application/x-git-upload-pack-result"),
        });
      }
      acks.push(pkt("ready\n"), DELIM);
    }

    // As in the v0 round: `fetch` treats undefined options and an empty
    // `notRefs` exactly like their absence, so the locals pass through.
    const plan = yield* planFor({ wants, haves, clientShallow, depth, since, notRefs });

    const prelude: Uint8Array[] = [...acks];
    if (deepening) {
      prelude.push(pkt("shallow-info\n"));
      for (const oid of plan.shallow) prelude.push(pkt(`shallow ${oid}\n`));
      for (const oid of plan.unshallow) prelude.push(pkt(`unshallow ${oid}\n`));
      prelude.push(DELIM);
    }
    prelude.push(pkt("packfile\n"));

    const packStream = Stream.concat(
      Stream.fromIterable([concat(prelude)]),
      Stream.concat(
        repository
          .packOids(plan.oids)
          .pipe(Stream.flatMap((bytes) => Stream.fromIterable(bandChunks(bytes)))),
        Stream.fromIterable([FLUSH]),
      ),
    );

    return new Response(Stream.toReadableStream(packStream), {
      headers: headers("application/x-git-upload-pack-result"),
    });
  });

const report = (results: ReadonlyArray<ReceiveResult>, unpacked: string): Uint8Array<ArrayBuffer> =>
  concat([
    pkt(`unpack ${unpacked}\n`),
    ...results.map((result) =>
      pkt(result.ok ? `ok ${result.ref}\n` : `ng ${result.ref} ${result.reason ?? "failed"}\n`),
    ),
    FLUSH,
  ]);

/** `POST /git-receive-pack` — ref commands and a pack in, report-status out. */
export const receivePack = (request: Request): Effect.Effect<Response, GitError, Repository> =>
  Effect.gen(function* () {
    const repository = yield* Repository;
    const reader = new PktReader(body(request));

    const updates: RefUpdate[] = [];
    /** Commands refused by name, reported per-ref rather than as a failure. */
    const refused: ReceiveResult[] = [];
    let atomic = false;
    let sideband = false;
    let first = true;

    yield* step(async () => {
      for (;;) {
        const item = await reader.next();
        if (item === "eof" || item === "flush" || item === "delim" || item === "end") return;
        const line = text(item);

        let command = line;
        if (first) {
          const nul = line.indexOf("\0");
          if (nul !== -1) {
            command = line.slice(0, nul);
            const caps = line.slice(nul + 1).split(" ");
            atomic = caps.includes("atomic");
            sideband = caps.includes("side-band-64k");
          }
          first = false;
        }

        const old = command.slice(0, 40);
        const next = command.slice(41, 81);
        const name = command.slice(82);
        // The all-zero id is checked first: it is forty hex digits too, but it
        // means "no object" — `null` on the update — rather than naming one.
        const expected = old === ZERO_OID ? null : isOid(old) ? old : undefined;
        const value = next === ZERO_OID ? null : isOid(next) ? next : undefined;
        if (expected === undefined || value === undefined || name.length === 0) {
          throw new Invalid({ field: "receive-pack", reason: `malformed command '${command}'` });
        }
        // A name the stores would refuse is reported the way git reports it —
        // `ng <ref> funny refname`, the rest of the push applied — because
        // failing the request instead would silently drop the good commands.
        // A delete is held only to the addressing rules, so a ref written
        // under a laxer version can still be removed.
        const problem = next === ZERO_OID ? checkRefAddress(name) : checkRefName(name);
        if (problem !== null) {
          refused.push({
            ref: name,
            from: expected,
            to: null,
            ok: false,
            reason: `funny refname: ${problem}`,
          });
          continue;
        }

        updates.push({ name, value, expected, reason: "push" });
      }
    });

    /**
     * Read the pack the client is sending and throw it away.
     *
     * A response written while the client is still streaming its body leaves
     * that body unconsumed, and the connection is torn down rather than
     * finished — so the client sees "the remote end hung up unexpectedly"
     * instead of the report explaining which ref was refused.
     */
    const drain = step(async () => {
      for await (const _ of reader.rest()) {
        // The bytes are not wanted; finishing the request is.
      }
    });

    if (updates.length === 0 && refused.length === 0) {
      yield* drain;
      return yield* new Invalid({ field: "receive-pack", reason: "no commands" });
    }

    const respond = (applied: ReadonlyArray<ReceiveResult>, unpacked: string) => {
      // The refused commands ride along in the same report, so a client sees
      // one `ng` per bad ref beside the `ok`s for everything that worked.
      const status = report([...applied, ...refused], unpacked);
      // A client that asked for side-band expects the report on band 1, and
      // a flush to close the stream; sending it raw would desynchronise it.
      const payload = sideband ? concat([...bandChunks(status), FLUSH]) : status;
      return new Response(payload, {
        headers: headers("application/x-git-receive-pack-result"),
      });
    };
    /**
     * One `ng` per ref, for a failure that took the whole push down.
     *
     * Takes the commands it is reporting on rather than reading `updates`,
     * because after the policy gate those two lists differ: `refused` already
     * carries an `ng` for every ref the gate declined, so reporting the client's
     * full list again emitted two `ng` lines for the same ref — and a status
     * line for a ref that was never submitted to the store at all.
     */
    const allFailed = (
      commands: ReadonlyArray<RefUpdate>,
      reason: string,
    ): ReadonlyArray<ReceiveResult> =>
      commands.map((update) => ({
        ref: update.name,
        from: update.expected ?? null,
        to: null,
        ok: false,
        reason,
      }));

    // An atomic push is all-or-nothing, and a refused name is part of the all:
    // applying the rest would be exactly what the capability promises not to.
    if (refused.length > 0 && (atomic || updates.length === 0)) {
      yield* drain;
      return respond(allFailed(updates, "atomic push refused: funny refname"), "ok");
    }

    // A refused command may have had a pack behind it even when every command
    // this server accepted was a delete: the client sends one body, and the
    // report only reaches it if that body is read first.
    if (refused.length > 0 && !updates.some((update) => update.value !== null)) {
      yield* drain;
    }

    // The object phase. A pack arrives whenever any command creates or moves
    // a ref; a delete-only push sends none.
    if (updates.some((update) => update.value !== null)) {
      // What can be judged before the body is read, is. The full rules need
      // the objects — a fast-forward cannot be told from a force push until
      // the pack is unpacked, so `Policy.gate` has to run after — but a
      // credential scoped to *delete* a branch is not one that may create or
      // move one, and the guard charges receive-pack either. Left entirely to
      // the gate, such a caller had their whole pack persisted before being
      // refused, which is the object half of the write the refusal is about.
      const refusal = yield* Policy.mayWrite("source.push").pipe(
        Effect.orElseSucceed(() => "the repository's policy could not be evaluated"),
      );
      if (refusal !== null) {
        yield* drain;
        return respond(allFailed(updates, refusal), "ok");
      }

      const unpacked = yield* repository
        .unpack(Stream.fromAsyncIterable(reader.rest(), (cause) => corrupt(String(cause))))
        .pipe(
          Effect.map(() => null),
          Effect.catch((error) => Effect.succeed(error)),
        );
      if (unpacked !== null) {
        const reason = unpacked instanceof PackCorrupt ? unpacked.reason : unpacked._tag;
        // The unpacker stopped part-way, so the rest of the pack is still
        // arriving; the report only reaches the client if it is read first.
        yield* drain;
        return respond(allFailed(updates, "unpacker error"), reason);
      }

      // No full connectivity check, but never point a ref at an object this
      // repository does not hold.
      for (const update of updates) {
        if (update.value !== null && !(yield* repository.contains(update.value))) {
          yield* drain;
          return respond(allFailed(updates, "missing necessary objects"), "ok");
        }
      }
    }

    // Every mutable ref update converges here. The guard has already said who
    // the requester is; this is where what they may do meets what the branch
    // requires, and the compare-and-swap that carries the verdict is applied
    // with it rather than after it.
    const judged = yield* Policy.gate(updates, atomic);
    if (atomic && judged.refused.length > 0) {
      // The client's whole list, not `judged.updates`: an atomic batch with a
      // refusal in it has *no* allowed updates, so reporting on those produces
      // no `ng` lines at all — a push refused in complete silence. Atomic means
      // every command failed, and the report has to say so for each of them.
      return respond(allFailed(updates, judged.refused[0]!.reason), "ok");
    }
    // Non-atomic: the refusals ride back in the same report as the `ok`s, so a
    // client is told which refs the policy declined and why, rather than
    // seeing them silently missing from the answer.
    for (const entry of judged.refused) {
      const command = updates.find((update) => update.name === entry.ref);
      refused.push({
        ref: entry.ref,
        from: command?.expected ?? null,
        to: command?.value ?? null,
        ok: false,
        reason: entry.reason,
      });
    }

    const results = yield* repository.receive(judged.updates, { atomic }).pipe(
      Effect.catchTags({
        HookRejected: (error) => Effect.succeed(allFailed(judged.updates, error.message)),
        Invalid: (error) => Effect.succeed(allFailed(judged.updates, error.reason)),
        // No `StorageFailure` catch: the stores report a ref they could not
        // write as an unapplied result carrying its own reason, which comes
        // through as `ng <ref> cannot lock ref` below. What is left here is a
        // backend that is down — and answering that with 200 and a per-ref
        // conflict would tell the client to retry forever and hide the outage
        // from whoever is watching the status codes.
      }),
    );

    return respond(results, "ok");
  });

/**
 * Route a request whose repository is already resolved — the caller scoped
 * the `Repository` layer, so the path prefix in front of these suffixes is
 * its business. `null` means "not a protocol request".
 */
export const handle = (request: Request): Effect.Effect<Response | null, GitError, Repository> =>
  Effect.suspend(() => {
    const url = new URL(request.url);
    const segments = url.pathname.split("/").filter((segment) => segment !== "");
    const last = segments.at(-1);

    if (request.method === "GET" && last === "refs" && segments.at(-2) === "info") {
      const service = url.searchParams.get("service");
      if (service === "git-upload-pack" || service === "git-receive-pack") {
        // v2 advertises capabilities and no refs; the refs come from
        // `ls-refs`, which is the saving the version exists for.
        if (service === "git-upload-pack" && isVersionTwo(request)) {
          return Effect.succeed(advertiseV2());
        }
        return advertise(service);
      }
      // The dumb protocol is not served here.
      return Effect.succeed(null);
    }
    if (request.method === "POST" && last === "git-upload-pack") return uploadPack(request);
    if (request.method === "POST" && last === "git-receive-pack") return receivePack(request);
    return Effect.succeed(null);
  });
