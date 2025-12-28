import { spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import { mkdir, rm, writeFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { chromium, type LaunchOptions } from "playwright";
import { getPlatformProxy, unstable_startWorker as startWorker } from "wrangler";

export const __filename = fileURLToPath(import.meta.url);
export const __dirname = dirname(__filename);

export const root = join(__dirname, "..");
const tmp = join(root, ".tmp");

export async function cli() {
	const dir = join(tmp, "git");
	let pid: number | undefined;

	return {
		bin,
		dir,
		run,
		seed,
		server,
		setup,
		async before() {
			await mkdir(dir, { recursive: true });
		},
		async after() {
			await rm(dir, { force: true, recursive: true });
			if (pid) process.kill(pid);
		},
	};

	async function bin(command: string, options: { cwd: string }): Promise<string> {
		return run(`node ${root}/src/cli.ts ${command}`, options);
	}

	async function run(command: string, options: { cwd: string }): Promise<string> {
		return new Promise((resolve, reject) => {
			// Parse command with quotes
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

			const cmd = args[0];
			const cmdArgs = args.slice(1);

			if (!cmd) {
				reject(new Error("Invalid command"));
				return;
			}

			const child = spawn(cmd, cmdArgs, {
				cwd: options.cwd,
				// shell: true,
				stdio: ["pipe", "pipe", "pipe"],
			});

			let stdout = "";
			let stderr = "";

			child.stdout.on("data", (data: Buffer) => {
				stdout += data.toString();
			});

			child.stderr.on("data", (data: Buffer) => {
				stderr += data.toString();
			});

			child.on("close", (code: number | null) => {
				if (code === 0) {
					resolve(stdout);
				} else {
					reject(stderr);
				}
			});

			child.on("error", (error: Error) => {
				reject(error.toString());
			});
		});
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

	async function server() {
		return new Promise((resolve, reject) => {
			const child = spawn(
				"npx",
				["wrangler", "dev", `--persist-to=${join(root, ".tmp/wrangler")}`],
				{
					cwd: root,
					// shell: true,
					stdio: ["pipe", "pipe", "pipe"],
				},
			);
			child.stdout.on("data", (data: Buffer) => {
				const string = data.toString();
				if (string.includes("Ready on")) {
					pid = child.pid;
					resolve(true);
				}
			});
			child.stderr.on("data", (data: Buffer) => {
				reject(data.toString());
			});
			child.on("close", (code: number | null) => {
				if (code === 0) {
					resolve(true);
				} else {
					reject(false);
				}
			});
			child.on("error", (error: Error) => {
				reject(error.toString());
			});
		});
	}

	async function setup() {
		const cwd = join(dir, randomUUID());
		await mkdir(cwd, { recursive: true });

		await run("git init -b main", { cwd });
		await run('git config user.name "Test User"', { cwd });
		await run('git config user.email "test@example.com"', { cwd });

		return cwd;
	}
}

export async function globalTeardown() {
	await rm(tmp, { recursive: true, force: true }).catch();
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

export async function worker(options?: Parameters<typeof startWorker>[0]) {
	const dir = join(tmp, "wrangler");
	const config = join(root, "wrangler.json");
	const instance = await startWorker(
		merge(
			{
				config,
				dev: {
					multiworkerPrimary: true,
					persist: dir,
					watch: false,
				},
			},
			options,
		),
	);
	const platform = await getPlatformProxy<Env>({
		configPath: config,
		persist: { path: dir },
	});

	return {
		...platform,
		dir,
		instance,
		async before() {
			await instance.ready;
		},
		async after() {
			await instance?.dispose();
			await platform?.dispose();
			await rm(dir, { recursive: true, force: true }).catch();
		},
	};
}

function merge(target: Record<string, any>, source?: Record<string, any>) {
	if (!source) return target;
	for (const key of Object.keys(source)) {
		if (source[key] instanceof Object && key in target && target[key] instanceof Object) {
			Object.assign(source[key], merge(target[key], source[key]));
		}
	}
	return { ...target, ...source };
}
