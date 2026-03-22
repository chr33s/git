import { build } from "esbuild";
import { spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import { mkdir, rm, writeFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { Miniflare } from "miniflare";
import { chromium, type LaunchOptions } from "playwright";

import wrangler from "../wrangler.json" with { type: "json" };

export const __filename = fileURLToPath(import.meta.url);
export const __dirname = dirname(__filename);

export const root = join(__dirname, "..");
const tmp = join(root, ".tmp");

async function bundleWorker() {
  const result = await build({
    bundle: true,
    entryPoints: [join(root, wrangler.main)],
    external: ["cloudflare:workers"],
    format: "esm",
    platform: "neutral",
    target: "esnext",
    write: false,
  });
  const output = result.outputFiles?.[0];
  if (!output) throw new Error("Failed to bundle worker");
  return output.text;
}

function parseCommand(command: string) {
  const args: string[] = [];
  let current = "";
  let inQuotes = false;
  let quoteChar = "";

  for (let i = 0; i < command.length; i++) {
    const char = command[i];

    if (!inQuotes && (char === '"' || char === "'")) {
      inQuotes = true;
      quoteChar = char;
    } else if (inQuotes && char === quoteChar) {
      inQuotes = false;
      quoteChar = "";
    } else if (!inQuotes && char === " ") {
      if (current.trim()) {
        args.push(current.trim());
        current = "";
      }
    } else {
      current += char;
    }
  }

  if (current.trim()) {
    args.push(current.trim());
  }

  return args;
}

export function exec(command: string, options: { cwd: string }): Promise<Buffer> {
  const args = parseCommand(command);
  const cmd = args.shift();

  if (!cmd) {
    return Promise.reject(new Error("Invalid command"));
  }

  return new Promise((resolve, reject) => {
    const child = spawn(cmd, args, {
      cwd: options.cwd,
      stdio: ["ignore", "pipe", "pipe"],
    });

    const stdout: Buffer[] = [];
    let stderr = "";

    child.stdout.on("data", (data: Buffer) => {
      stdout.push(data);
    });

    child.stderr.on("data", (data: Buffer) => {
      stderr += data.toString();
    });

    child.on("close", (code: number | null) => {
      if (code === 0) {
        resolve(Buffer.concat(stdout));
      } else {
        reject(new Error(stderr || `Command exited with code ${code}`));
      }
    });

    child.on("error", reject);
  });
}

export async function run(command: string, options: { cwd: string }): Promise<string> {
  const result = await exec(command, options);
  return result.toString();
}

export async function cli() {
  const dir = join(tmp, "git");

  return {
    bin,
    dir,
    run,
    seed,
    setup,
    async before() {
      await mkdir(dir, { recursive: true });
    },
    async after() {
      await rm(dir, { force: true, recursive: true });
    },
  };

  async function bin(command: string, options: { cwd: string }): Promise<string> {
    return run(`node ${root}/src/cli.ts ${command}`, options);
  }

  async function seed(repoURL: string) {
    const cwd = await setup();
    await writeFile(join(cwd, "README.md"), "# Test Repository");
    await run("git add .", { cwd });
    await run('git commit -m "Initial commit"', { cwd });
    await run(`git remote add origin ${repoURL}`, { cwd });
    await run("git push -u origin main", { cwd }); // GIT_CURL_VERBOSE=1
    return cwd;
  }

  async function setup() {
    const cwd = join(dir, randomUUID());
    await mkdir(cwd, { recursive: true });

    await run("git init -b main --no-template", { cwd });
    await run('git config user.name "Test User"', { cwd });
    await run('git config user.email "test@example.com"', { cwd });

    return cwd;
  }
}

export async function globalTeardown() {
  await rm(tmp, { recursive: true, force: true }).catch();
}

export function pktLine(text: string) {
  return (text.length + 4).toString(16).padStart(4, "0") + text;
}

export async function playwright(options?: LaunchOptions) {
  const dir = join(tmp, "playwright");
  await mkdir(dir, { recursive: true });
  const context = await chromium.launchPersistentContext(dir, options);
  const page = await context.newPage();

  return {
    context,
    page,
    async before() {},
    async after() {
      await context.close();
      await rm(dir, { recursive: true, force: true }).catch();
    },
  };
}

export function toStream(data: Uint8Array) {
  return new ReadableStream<Uint8Array>({
    start(controller) {
      controller.enqueue(data);
      controller.close();
    },
  });
}

export interface WorkerOptions {
  port?: number;
  persistPath?: string;
}

export async function worker(options?: WorkerOptions) {
  const dir = options?.persistPath ?? join(tmp, "miniflare");
  await mkdir(dir, { recursive: true });

  const script = await bundleWorker();
  const mf = new Miniflare({
    modules: true,
    script,
    compatibilityDate: wrangler.compatibility_date,
    compatibilityFlags: wrangler.compatibility_flags,
    port: options?.port,
    durableObjects: {
      GIT_SERVER: { className: "GitServer", useSQLite: true },
    },
    r2Buckets: ["GIT_OBJECTS"],
    durableObjectsPersist: dir,
    r2Persist: dir,
  });

  const url = await mf.ready;
  const env = await mf.getBindings<Env>();

  return {
    env,
    cf: {},
    ctx: {
      waitUntil: () => {},
      passThroughOnException: () => {},
    },
    caches: await mf.getCaches(),
    dispose: () => mf.dispose(),
    mf,
    dir,
    url,
    dispatchFetch: (input: string, init?: RequestInit) => mf.dispatchFetch(input, init as any),
    async before() {
      await mf.ready;
    },
    async after() {
      await mf.dispose();
      await rm(dir, { recursive: true, force: true }).catch();
    },
  };
}
