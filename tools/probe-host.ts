/** Read-only synthetic health check against a private, fixed canary repository. */
import { createHash } from "node:crypto";
import { Config, Effect, Redacted, Schema } from "effect";
import { RawObject, RefsResponse } from "../src/server/ApiContract.ts";
import { routeOf } from "../src/server/Route.ts";

export interface ProbeConfiguration {
  readonly url: string;
  readonly token: string;
  readonly ref: string;
  readonly oid: string;
  readonly lfsOid?: string;
}

interface Check {
  readonly name: "anonymous" | "refs" | "object" | "lfs";
  readonly outcome: "healthy" | "status" | "content" | "unavailable";
  readonly status?: number;
  readonly durationMs: number;
}

const maxBytes = 1024 * 1024;
const read = async (response: Response) => {
  const reader = response.body?.getReader();
  if (reader === undefined) return new Uint8Array();
  const chunks: Uint8Array[] = [];
  let size = 0;
  try {
    for (;;) {
      const chunk = await reader.read();
      if (chunk.done) break;
      size += chunk.value.byteLength;
      if (size > maxBytes) throw new Error("probe response budget exceeded");
      chunks.push(chunk.value);
    }
  } finally {
    await reader.cancel();
    reader.releaseLock();
  }
  const bytes = new Uint8Array(size);
  let offset = 0;
  for (const chunk of chunks) {
    bytes.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return bytes;
};

/** The caller owns scheduling and credentials; the report contains neither. */
export const probe = async (config: ProbeConfiguration) => {
  const url = new URL(config.url);
  const route = routeOf(url.pathname);
  if (
    (url.protocol !== "https:" &&
      !(url.protocol === "http:" && ["127.0.0.1", "[::1]", "localhost"].includes(url.hostname))) ||
    url.username ||
    url.password ||
    url.search ||
    url.hash ||
    route === null ||
    route.rest !== "" ||
    !/^[a-f0-9]{40}$/.test(config.oid) ||
    !config.ref.startsWith("refs/") ||
    config.token.length === 0 ||
    /[\r\n]/.test(config.token) ||
    (config.lfsOid !== undefined && !/^[a-f0-9]{64}$/.test(config.lfsOid))
  )
    throw new Error("invalid probe configuration");
  const base = url.href.replace(/\/$/, "");
  const checks: Check[] = [];
  const deadline = AbortSignal.timeout(30_000);
  const check = async (
    name: Check["name"],
    suffix: string,
    expected: number,
    authenticated: boolean,
    validate: (response: Response) => Promise<boolean>,
  ) => {
    const started = performance.now();
    try {
      const response = await fetch(base + suffix, {
        redirect: "manual",
        signal: AbortSignal.any([deadline, AbortSignal.timeout(10_000)]),
        headers: authenticated ? { authorization: `Bearer ${config.token}` } : {},
      });
      let outcome: Check["outcome"];
      if (response.status !== expected) {
        await response.body?.cancel();
        outcome = "status";
      } else outcome = (await validate(response)) ? "healthy" : "content";
      checks.push({
        name,
        outcome,
        status: response.status,
        durationMs: performance.now() - started,
      });
    } catch {
      checks.push({ name, outcome: "unavailable", durationMs: performance.now() - started });
    }
  };
  await check("anonymous", "/info/refs?service=git-upload-pack", 401, false, async (response) => {
    await response.body?.cancel();
    return true;
  });
  await check("refs", "/refs", 200, true, async (response) => {
    const refs = Schema.decodeSync(Schema.fromJsonString(RefsResponse))(
      new TextDecoder().decode(await read(response)),
    );
    return refs.refs.some((ref) => ref.name === config.ref && ref.oid === config.oid);
  });
  await check("object", `/object/${config.oid}`, 200, true, async (response) => {
    const object = Schema.decodeSync(Schema.fromJsonString(RawObject))(
      new TextDecoder().decode(await read(response)),
    );
    const bytes = Buffer.from(object.content, "base64");
    return (
      object.oid === config.oid &&
      object.size === bytes.byteLength &&
      createHash("sha1")
        .update(`${object.type} ${bytes.byteLength}\0`)
        .update(bytes)
        .digest("hex") === config.oid
    );
  });
  if (config.lfsOid !== undefined) {
    const oid = config.lfsOid;
    await check(
      "lfs",
      `/info/lfs/objects/${oid}`,
      200,
      true,
      async (response) =>
        createHash("sha256")
          .update(await read(response))
          .digest("hex") === oid,
    );
  }
  return {
    component: "HostProbe",
    event: "probe_result",
    healthy: checks.every((check) => check.outcome === "healthy"),
    checks,
  };
};

if (import.meta.main) {
  try {
    const config = await Effect.runPromise(
      Effect.gen(function* () {
        const url = yield* Config.String("GIT_PROBE_URL");
        const token = Redacted.value(yield* Config.Redacted("GIT_PROBE_TOKEN"));
        const ref = yield* Config.String("GIT_PROBE_REF").pipe(
          Config.withDefault("refs/heads/main"),
        );
        const oid = yield* Config.String("GIT_PROBE_OID");
        const lfsOid = yield* Config.String("GIT_PROBE_LFS_OID").pipe(Config.withDefault(""));
        return lfsOid === "" ? { url, token, ref, oid } : { url, token, ref, oid, lfsOid };
      }),
    );
    const report = await probe(config);
    console.log(JSON.stringify(report));
    process.exitCode = report.healthy ? 0 : 1;
  } catch {
    console.error(
      JSON.stringify({ component: "HostProbe", event: "probe_failed", reason: "configuration" }),
    );
    process.exitCode = 2;
  }
}
