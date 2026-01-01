import { basename } from "node:path";
import { parseArgs, type ParseArgsOptionDescriptor, type ParseArgsOptionsConfig } from "node:util";

import { FsStorage } from "./cli.storage.ts";
import { Client } from "./client.ts";
import pkg from "../package.json" with { type: "json" };

interface CommandOption extends ParseArgsOptionDescriptor {
	description: string;
}

interface CommandOptions extends ParseArgsOptionsConfig {
	[longOption: string]: CommandOption;
}

interface Command {
	command: string;
	description: string;
	handler: (args: string[], options: CommandOptions) => Promise<void>;
	options: CommandOptions;
}

export class Cli {
	commands: Command[] = [
		{
			command: "clone",
			description: "Clone a repository from URL",
			handler: (...args) => this.#clone(...args),
			options: {
				local: {
					description: "Clone from a local path",
					short: "l",
					type: "boolean",
				},
				shared: {
					description: "Make a shallow clone for space savings",
					short: "s",
					type: "boolean",
				},
				bare: {
					description: "Create a bare repository",
					type: "boolean",
				},
				mirror: {
					description: "Set up a mirror repository",
					type: "boolean",
				},
				origin: {
					description: "Name of the remote to track",
					short: "o",
					type: "string",
				},
				branch: {
					description: "Checkout specified branch",
					short: "b",
					type: "string",
				},
				depth: {
					description: "Create a shallow clone with specified depth",
					type: "string",
				},
				"single-branch": {
					description: "Clone only one branch",
					type: "boolean",
				},
				"no-tags": {
					description: "Do not clone tags",
					type: "boolean",
				},
				quiet: {
					description: "Quiet mode",
					short: "q",
					type: "boolean",
				},
			},
		},
		{
			command: "init",
			description: "Initialize a new repository",
			handler: (...args) => this.#init(...args),
			options: {
				quiet: {
					description: "Only print error messages",
					short: "q",
					type: "boolean",
				},
				bare: {
					description: "Create a bare repository",
					type: "boolean",
				},
				template: {
					description: "Template directory to use",
					type: "string",
				},
				"separate-git-dir": {
					description: "Separate git directory from working directory",
					type: "string",
				},
				"initial-branch": {
					description: "Initial branch name",
					short: "b",
					type: "string",
				},
				shared: {
					description: "Make repository shared among users",
					type: "string",
				},
			},
		},
		{
			command: "add", // [<pathspec>...]
			description: "Add file contents to the index",
			handler: (...args) => this.#add(...args),
			options: {
				verbose: {
					description: "Be verbose",
					short: "v",
					type: "boolean",
				},
				"dry-run": {
					description: "Show what would be added",
					short: "n",
					type: "boolean",
				},
				force: {
					description: "Allow adding ignored files",
					short: "f",
					type: "boolean",
				},
				interactive: {
					description: "Interactive mode",
					short: "i",
					type: "boolean",
				},
				patch: {
					description: "Interactively choose hunks to add",
					short: "p",
					type: "boolean",
				},
				edit: {
					description: "Edit the diff before adding",
					short: "e",
					type: "boolean",
				},
				all: {
					description: "Add all changes",
					short: "A",
					type: "boolean",
				},
				update: {
					description: "Only add tracked files",
					short: "u",
					type: "boolean",
				},
				"intent-to-add": {
					description: "Record that path will be added",
					short: "N",
					type: "boolean",
				},
				refresh: {
					description: "Only refresh stat information",
					type: "boolean",
				},
				sparse: {
					description: "Update index entries outside sparse-checkout",
					type: "boolean",
				},
			},
		},
		{
			command: "mv",
			description: "Move or rename a file, a directory, or a symlink",
			handler: (...args) => this.#mv(...args),
			options: {
				verbose: {
					description: "Be verbose",
					short: "v",
					type: "boolean",
				},
				"dry-run": {
					description: "Show what would be moved",
					short: "n",
					type: "boolean",
				},
				force: {
					description: "Force overwrite",
					short: "f",
					type: "boolean",
				},
				sparse: {
					description: "Allow moving sparse entries",
					type: "boolean",
				},
			},
		},
		{
			command: "restore",
			description: "Restore working tree files",
			handler: (...args) => this.#restore(...args),
			options: {
				source: {
					description: "Restore from specified tree",
					short: "s",
					type: "string",
				},
				staged: {
					description: "Restore to the index",
					short: "S",
					type: "boolean",
				},
				worktree: {
					description: "Restore to the working tree",
					short: "W",
					type: "boolean",
				},
				ours: {
					description: "Keep our version during conflicts",
					type: "boolean",
				},
				theirs: {
					description: "Take their version during conflicts",
					type: "boolean",
				},
				quiet: {
					description: "Suppress output",
					short: "q",
					type: "boolean",
				},
				patch: {
					description: "Interactively choose hunks",
					short: "p",
					type: "boolean",
				},
			},
		},
		{
			command: "rm",
			description: "Remove files from the working tree and from the index",
			handler: (...args) => this.#rm(...args),
			options: {
				"dry-run": {
					description: "Show what would be removed",
					short: "n",
					type: "boolean",
				},
				force: {
					description: "Force removal",
					short: "f",
					type: "boolean",
				},
				cached: {
					description: "Only remove from index",
					type: "boolean",
				},
				recursive: {
					description: "Allow recursive removal",
					short: "r",
					type: "boolean",
				},
				quiet: {
					description: "Suppress output",
					short: "q",
					type: "boolean",
				},
				"pathspec-from-file": {
					description: "Read pathspec from file",
					type: "string",
				},
			},
		},
		{
			command: "commit", // [-m <msg>]
			description: "Record changes to the repository",
			handler: (...args) => this.#commit(...args),
			options: {
				message: {
					description: "Commit message",
					short: "m",
					type: "string",
				},
				all: {
					description: "Commit all changes",
					short: "a",
					type: "boolean",
				},
				patch: {
					description: "Choose hunks to commit",
					short: "p",
					type: "boolean",
				},
				"dry-run": {
					description: "Show what would be committed",
					type: "boolean",
				},
				amend: {
					description: "Amend the previous commit",
					type: "boolean",
				},
				file: {
					description: "Read message from file",
					short: "F",
					type: "string",
				},
				author: {
					description: "Override the commit author",
					type: "string",
				},
				date: {
					description: "Override the author date",
					type: "string",
				},
				signoff: {
					description: "Add Signed-off-by trailer",
					short: "s",
					type: "boolean",
				},
				verbose: {
					description: "Show diff in commit message",
					short: "v",
					type: "boolean",
				},
				quiet: {
					description: "Suppress output",
					short: "q",
					type: "boolean",
				},
				"allow-empty": {
					description: "Allow empty commits",
					type: "boolean",
				},
				"allow-empty-message": {
					description: "Allow empty commit messages",
					type: "boolean",
				},
				"no-verify": {
					description: "Bypass pre-commit hooks",
					short: "n",
					type: "boolean",
				},
			},
		},
		{
			command: "status",
			description: "Show the working tree status",
			handler: (...args) => this.#status(...args),
			options: {
				short: {
					description: "Short format",
					short: "s",
					type: "boolean",
				},
				branch: {
					description: "Show branch info",
					short: "b",
					type: "boolean",
				},
				verbose: {
					description: "Verbose output",
					short: "v",
					type: "boolean",
				},
				long: {
					description: "Long format",
					type: "boolean",
				},
				porcelain: {
					description: "Machine-readable format",
					type: "string",
				},
				"untracked-files": {
					description: "Show untracked files",
					short: "u",
					type: "string",
				},
				"ignore-submodules": {
					description: "Ignore submodules",
					type: "string",
				},
				ignored: {
					description: "Show ignored files",
					type: "string",
				},
				renames: {
					description: "Show rename information",
					type: "boolean",
				},
				"find-renames": {
					description: "Find renames with threshold",
					type: "string",
				},
			},
		},
		{
			command: "log",
			description: "Show commit logs",
			handler: (...args) => this.#log(...args),
			options: {
				oneline: {
					description: "Condensed format",
					type: "boolean",
				},
				decorate: {
					description: "Show references",
					type: "string",
				},
				graph: {
					description: "Draw graph",
					type: "boolean",
				},
				all: {
					description: "All branches and tags",
					type: "boolean",
				},
				author: {
					description: "Filter by author",
					type: "string",
				},
				since: {
					description: "Start date",
					type: "string",
				},
				until: {
					description: "End date",
					type: "string",
				},
				"max-count": {
					description: "Limit number of commits",
					short: "n",
					type: "string",
				},
				patch: {
					description: "Show patches",
					short: "p",
					type: "boolean",
				},
				stat: {
					description: "Show statistics",
					type: "boolean",
				},
				reverse: {
					description: "Reverse order",
					type: "boolean",
				},
			},
		},
		{
			command: "show",
			description: "Show various types of objects",
			handler: (...args) => this.#show(...args),
			options: {
				pretty: {
					description: "Pretty format",
					type: "string",
				},
				format: {
					description: "Custom format",
					type: "string",
				},
				"name-only": {
					description: "Show only names",
					type: "boolean",
				},
				"name-status": {
					description: "Show status of changed files",
					type: "boolean",
				},
				stat: {
					description: "Show statistics",
					type: "boolean",
				},
				patch: {
					description: "Show patches",
					short: "p",
					type: "boolean",
				},
				oneline: {
					description: "Condensed format",
					type: "boolean",
				},
			},
		},
		{
			command: "branch",
			description: "List, create, or delete branches",
			handler: (...args) => this.#branch(...args),
			options: {
				list: {
					description: "List branches",
					short: "l",
					type: "boolean",
				},
				all: {
					description: "Show all branches",
					short: "a",
					type: "boolean",
				},
				remote: {
					description: "Show remote branches",
					short: "r",
					type: "boolean",
				},
				verbose: {
					description: "Be verbose",
					short: "v",
					type: "boolean",
				},
				delete: {
					description: "Delete a branch",
					short: "d",
					type: "boolean",
				},
				"force-delete": {
					description: "Force delete a branch",
					short: "D",
					type: "boolean",
				},
				move: {
					description: "Move or rename branch",
					short: "m",
					type: "boolean",
				},
				copy: {
					description: "Copy a branch",
					short: "c",
					type: "boolean",
				},
				upstream: {
					description: "Set upstream branch",
					short: "u",
					type: "string",
				},
				track: {
					description: "Set tracking branch",
					short: "t",
					type: "boolean",
				},
				merged: {
					description: "List merged branches",
					type: "boolean",
				},
				"no-merged": {
					description: "List unmerged branches",
					type: "boolean",
				},
			},
		},
		{
			command: "checkout", // [-b <branch>]
			description: "Switch branches or restore working tree files",
			handler: (...args) => this.#checkout(...args),
			options: {
				branch: {
					description: "Create and checkout new branch",
					short: "b",
					type: "string",
				},
				create: {
					description: "Create new branch",
					short: "c",
					type: "string",
				},
				detach: {
					description: "Detach HEAD",
					short: "d",
					type: "boolean",
				},
				force: {
					description: "Force checkout",
					short: "f",
					type: "boolean",
				},
				merge: {
					description: "Perform merge",
					short: "m",
					type: "boolean",
				},
				orphan: {
					description: "Create orphan branch",
					type: "string",
				},
				patch: {
					description: "Interactively choose hunks",
					short: "p",
					type: "boolean",
				},
				quiet: {
					description: "Suppress output",
					short: "q",
					type: "boolean",
				},
				track: {
					description: "Set tracking branch",
					short: "t",
					type: "boolean",
				},
				"no-track": {
					description: "Do not set tracking branch",
					type: "boolean",
				},
				progress: {
					description: "Show progress",
					type: "boolean",
				},
			},
		},
		{
			command: "switch",
			description: "Switch branches",
			handler: (...args) => this.#switch(...args),
			options: {
				create: {
					description: "Create new branch",
					short: "c",
					type: "string",
				},
				detach: {
					description: "Detach HEAD",
					short: "d",
					type: "boolean",
				},
				"force-create": {
					description: "Force create branch",
					short: "C",
					type: "string",
				},
				"discard-changes": {
					description: "Discard local changes",
					short: "f",
					type: "boolean",
				},
				merge: {
					description: "Merge changes",
					short: "m",
					type: "boolean",
				},
				"no-track": {
					description: "Do not track",
					type: "boolean",
				},
				quiet: {
					description: "Suppress output",
					short: "q",
					type: "boolean",
				},
				progress: {
					description: "Show progress",
					type: "boolean",
				},
			},
		},
		{
			command: "merge",
			description: "Join two or more development histories together",
			handler: (...args) => this.#merge(...args),
			options: {
				commit: {
					description: "Create merge commit",
					type: "boolean",
				},
				"no-commit": {
					description: "Do not create merge commit",
					type: "boolean",
				},
				"no-ff": {
					description: "Always create merge commit",
					type: "boolean",
				},
				"ff-only": {
					description: "Only fast-forward",
					type: "boolean",
				},
				squash: {
					description: "Squash commits",
					type: "boolean",
				},
				strategy: {
					description: "Merge strategy",
					short: "s",
					type: "string",
				},
				"strategy-option": {
					description: "Strategy option",
					short: "X",
					type: "string",
				},
				message: {
					description: "Merge message",
					short: "m",
					type: "string",
				},
				edit: {
					description: "Edit merge message",
					short: "e",
					type: "boolean",
				},
				quiet: {
					description: "Suppress output",
					short: "q",
					type: "boolean",
				},
				verbose: {
					description: "Be verbose",
					short: "v",
					type: "boolean",
				},
				stat: {
					description: "Show statistics",
					type: "boolean",
				},
				"no-stat": {
					description: "Do not show statistics",
					short: "n",
					type: "boolean",
				},
				abort: {
					description: "Abort merge",
					type: "boolean",
				},
			},
		},
		{
			command: "rebase",
			description: "Reapply commits on top of another base tip",
			handler: (...args) => this.#rebase(...args),
			options: {
				interactive: {
					description: "Interactive rebase",
					short: "i",
					type: "boolean",
				},
				"keep-empty": {
					description: "Keep empty commits",
					type: "boolean",
				},
				"no-keep-empty": {
					description: "Remove empty commits",
					type: "boolean",
				},
				root: {
					description: "Rebase from root",
					type: "boolean",
				},
				onto: {
					description: "Onto specified branch",
					type: "string",
				},
				continue: {
					description: "Continue rebase",
					type: "boolean",
				},
				abort: {
					description: "Abort rebase",
					type: "boolean",
				},
				quit: {
					description: "Quit rebase",
					type: "boolean",
				},
				skip: {
					description: "Skip current commit",
					type: "boolean",
				},
				"edit-todo": {
					description: "Edit todo list",
					type: "boolean",
				},
				merge: {
					description: "Use merge strategy",
					short: "m",
					type: "boolean",
				},
				strategy: {
					description: "Merge strategy",
					short: "s",
					type: "string",
				},
				"strategy-option": {
					description: "Strategy option",
					short: "X",
					type: "string",
				},
				quiet: {
					description: "Suppress output",
					short: "q",
					type: "boolean",
				},
				verbose: {
					description: "Be verbose",
					short: "v",
					type: "boolean",
				},
			},
		},
		{
			command: "reset",
			description: "Reset current HEAD to the specified state",
			handler: (...args) => this.#reset(...args),
			options: {
				soft: {
					description: "Keep staged changes",
					type: "boolean",
				},
				mixed: {
					description: "Keep working tree changes",
					type: "boolean",
				},
				hard: {
					description: "Discard all changes",
					type: "boolean",
				},
				merge: {
					description: "Merge mode",
					type: "boolean",
				},
				keep: {
					description: "Keep mode",
					type: "boolean",
				},
				quiet: {
					description: "Suppress output",
					short: "q",
					type: "boolean",
				},
				"pathspec-from-file": {
					description: "Read pathspec from file",
					type: "string",
				},
				"pathspec-file-nul": {
					description: "Use NUL to terminate pathspec",
					type: "boolean",
				},
			},
		},
		{
			command: "tag",
			description: "Create, list, delete or verify a tag object signed with GPG",
			handler: (...args) => this.#tag(...args),
			options: {
				list: {
					description: "List tags",
					short: "l",
					type: "boolean",
				},
				annotate: {
					description: "Create annotated tag",
					short: "a",
					type: "boolean",
				},
				sign: {
					description: "Sign tag with GPG",
					short: "s",
					type: "boolean",
				},
				delete: {
					description: "Delete tags",
					short: "d",
					type: "boolean",
				},
				verify: {
					description: "Verify tag signatures",
					type: "boolean",
				},
				message: {
					description: "Tag message",
					short: "m",
					type: "string",
				},
				force: {
					description: "Force tag creation",
					short: "f",
					type: "boolean",
				},
				contains: {
					description: "Find tags containing commit",
					type: "string",
				},
				merged: {
					description: "List merged tags",
					type: "boolean",
				},
				"no-merged": {
					description: "List unmerged tags",
					type: "boolean",
				},
			},
		},
		{
			command: "fetch", // [-r remote]
			description: "Download objects and refs from remote",
			handler: (...args) => this.#fetch(...args),
			options: {
				all: {
					description: "Fetch all remotes",
					type: "boolean",
				},
				remote: {
					description: "Remote repository",
					short: "r",
					type: "string",
				},
				"dry-run": {
					description: "Show what would be fetched",
					type: "boolean",
				},
				force: {
					description: "Force fetch",
					short: "f",
					type: "boolean",
				},
				tags: {
					description: "Fetch all tags",
					short: "t",
					type: "boolean",
				},
				"no-tags": {
					description: "Do not fetch tags",
					type: "boolean",
				},
				depth: {
					description: "Shallow clone with depth",
					type: "string",
				},
				deepen: {
					description: "Deepen shallow clone",
					type: "string",
				},
				unshallow: {
					description: "Convert shallow to full clone",
					type: "boolean",
				},
				prune: {
					description: "Remove deleted remote refs",
					short: "p",
					type: "boolean",
				},
				"prune-tags": {
					description: "Remove deleted remote tags",
					short: "P",
					type: "boolean",
				},
				quiet: {
					description: "Suppress output",
					short: "q",
					type: "boolean",
				},
				verbose: {
					description: "Be verbose",
					short: "v",
					type: "boolean",
				},
				jobs: {
					description: "Number of parallel jobs",
					short: "j",
					type: "string",
				},
			},
		},
		{
			command: "pull",
			description: "Fetch from and integrate with another repository or a local branch",
			handler: (...args) => this.#pull(...args),
			options: {
				rebase: {
					description: "Use rebase instead of merge",
					short: "r",
					type: "boolean",
				},
				"no-rebase": {
					description: "Use merge (default)",
					type: "boolean",
				},
				all: {
					description: "Fetch all remotes",
					type: "boolean",
				},
				force: {
					description: "Force pull",
					short: "f",
					type: "boolean",
				},
				tags: {
					description: "Fetch all tags",
					short: "t",
					type: "boolean",
				},
				"no-tags": {
					description: "Do not fetch tags",
					type: "boolean",
				},
				quiet: {
					description: "Suppress output",
					short: "q",
					type: "boolean",
				},
				verbose: {
					description: "Be verbose",
					short: "v",
					type: "boolean",
				},
				depth: {
					description: "Shallow clone with depth",
					type: "string",
				},
				deepen: {
					description: "Deepen shallow clone",
					type: "string",
				},
				unshallow: {
					description: "Convert shallow to full clone",
					type: "boolean",
				},
				jobs: {
					description: "Number of parallel jobs",
					short: "j",
					type: "string",
				},
			},
		},
		{
			command: "push",
			description: "Update remote refs along with associated objects",
			handler: (...args) => this.#push(...args),
			options: {
				all: {
					description: "Push all branches",
					type: "boolean",
				},
				prune: {
					description: "Remove deleted branches",
					type: "boolean",
				},
				mirror: {
					description: "Mirror all refs",
					type: "boolean",
				},
				delete: {
					description: "Delete remote refs",
					short: "d",
					type: "boolean",
				},
				tags: {
					description: "Push all tags",
					type: "boolean",
				},
				"follow-tags": {
					description: "Push follow tags",
					type: "boolean",
				},
				"dry-run": {
					description: "Show what would be pushed",
					short: "n",
					type: "boolean",
				},
				force: {
					description: "Force push",
					short: "f",
					type: "boolean",
				},
				"force-with-lease": {
					description: "Safer force push",
					type: "boolean",
				},
				"set-upstream": {
					description: "Set upstream branch",
					short: "u",
					type: "boolean",
				},
				repo: {
					description: "Repository URL or name",
					type: "string",
				},
				quiet: {
					description: "Suppress output",
					short: "q",
					type: "boolean",
				},
				verbose: {
					description: "Be verbose",
					short: "v",
					type: "boolean",
				},
				progress: {
					description: "Show progress",
					type: "boolean",
				},
			},
		},
		{
			command: "remote",
			description: "Manage set of tracked repositories",
			handler: (...args) => this.#remote(...args),
			options: {
				verbose: {
					description: "Be verbose",
					short: "v",
					type: "boolean",
				},
				add: {
					description: "Add new remote",
					type: "boolean",
				},
				delete: {
					description: "Delete remote",
					type: "boolean",
				},
				rename: {
					description: "Rename remote",
					type: "boolean",
				},
				"set-url": {
					description: "Change remote URL",
					type: "boolean",
				},
				"get-url": {
					description: "Get remote URL",
					type: "boolean",
				},
				"set-branches": {
					description: "Set tracked branches",
					type: "boolean",
				},
				"set-head": {
					description: "Set default HEAD",
					type: "boolean",
				},
				show: {
					description: "Show remote details",
					type: "boolean",
				},
				track: {
					description: "Set tracking branch",
					short: "t",
					type: "boolean",
				},
				prune: {
					description: "Prune stale remotes",
					type: "boolean",
				},
			},
		},
	];
	#options: ParseArgsOptionsConfig = {
		help: { short: "h", type: "boolean" },
		version: { short: "v", type: "boolean" },
	};
	#client: Client;

	constructor() {
		const storage = new FsStorage();
		const config = { repoName: basename(process.cwd()) };
		this.#client = new Client(config, storage);
	}

	async run(args: string[] = []) {
		const { values, positionals } = parseArgs({
			allowPositionals: true,
			args,
			options: this.#options,
			strict: false, // Allow subcommand parsing
		});
		if (values.version) return this.#version();
		const command = this.commands.find((cmd) => cmd.command === positionals[0]);
		if (command) {
			if (values.help) return this.#help(command);
			try {
				return await command.handler(args.slice(1), command.options);
			} catch (error) {
				this.#write("err", `Error: ${error instanceof Error ? error.message : String(error)}`);
				process.exit(1);
			}
		}
		return this.#help();
	}

	async #init(args: string[], options: CommandOptions) {
		const { values, positionals } = parseArgs({
			allowPositionals: true,
			args,
			options,
		});

		const path = positionals[0] ?? ".";
		await this.#client.init();
		if (!values.quiet) {
			this.#write("out", `Initialized empty Git repository in ${path}/.git/`);
		}
	}

	async #clone(args: string[], options: CommandOptions) {
		const { positionals } = parseArgs({
			allowPositionals: true,
			args,
			options,
		});

		const url = positionals[0];
		if (!url) {
			this.#write("err", "Error: repository URL required");
			process.exit(1);
		}

		await this.#client.clone(url);
		this.#write("out", `Cloned repository from ${url}`);
	}

	async #status(args: string[], options: CommandOptions) {
		const { values } = parseArgs({
			allowPositionals: true,
			args,
			options,
		});

		const status = await this.#client.status();

		if (values.short) {
			for (const file of status.staged) this.#write("out", `A  ${file}`);
			for (const file of status.modified) this.#write("out", ` M ${file}`);
			for (const file of status.untracked) this.#write("out", `?? ${file}`);
		} else {
			const branch = "main"; // TODO: get actual branch
			this.#write("out", `On branch ${branch}\n`);

			if (status.staged.length > 0) {
				this.#write("out", "Changes to be committed:");
				for (const file of status.staged) this.#write("out", `\tnew file:   ${file}`);
				this.#write("out", "");
			}

			if (status.modified.length > 0) {
				this.#write("out", "Changes not staged for commit:");
				for (const file of status.modified) this.#write("out", `\tmodified:   ${file}`);
				this.#write("out", "");
			}

			if (status.untracked.length > 0) {
				this.#write("out", "Untracked files:");
				for (const file of status.untracked) this.#write("out", `\t${file}`);
				this.#write("out", "");
			}

			if (
				status.staged.length === 0 &&
				status.modified.length === 0 &&
				status.untracked.length === 0
			) {
				this.#write("out", "working tree clean");
			}
		}
	}

	async #log(args: string[], options: CommandOptions) {
		const { values } = parseArgs({
			allowPositionals: true,
			args,
			options,
		});

		const commits = await this.#client.log();

		if (commits.length === 0) {
			this.#write("out", "fatal: your current branch has no commits yet");
			return;
		}

		for (const commit of commits) {
			if (values.oneline) {
				this.#write("out", `${commit.oid.slice(0, 7)} ${commit.message.split("\n")[0]}`);
			} else {
				this.#write("out", `commit ${commit.oid}`);
				this.#write("out", `Author: ${commit.author}\n`);
				this.#write("out", `    ${commit.message.split("\n")[0]}\n`);
			}
		}
	}

	async #show(args: string[], options: CommandOptions) {
		const { positionals } = parseArgs({
			allowPositionals: true,
			args,
			options,
		});

		const ref = positionals[0] ?? "HEAD";

		const obj = await this.#client.show(ref);
		this.#write("out", `commit ${ref}`);
		this.#write("out", new TextDecoder().decode(obj.data));
	}

	async #add(args: string[], options: CommandOptions) {
		const { positionals, values } = parseArgs({
			allowPositionals: true,
			args,
			options,
		});

		if (positionals.length === 0) {
			this.#write("err", "Error: please specify paths to add");
			process.exit(1);
		}

		for (const path of positionals) {
			try {
				await this.#client.add(path);
				if (!values.quiet) this.#write("out", `added '${path}'`);
			} catch (error) {
				this.#write(
					"err",
					`Error adding '${path}': ${error instanceof Error ? error.message : String(error)}`,
				);
			}
		}
	}

	async #mv(args: string[], options: CommandOptions) {
		const { positionals } = parseArgs({
			allowPositionals: true,
			args,
			options,
		});

		if (positionals.length < 2) {
			this.#write("err", "Error: source and destination required");
			process.exit(1);
		}

		await this.#client.mv(positionals[0]!, positionals[1]!);
		this.#write("out", `renamed ${positionals[0]} -> ${positionals[1]}`);
	}

	async #restore(args: string[], options: CommandOptions) {
		const { positionals } = parseArgs({
			allowPositionals: true,
			args,
			options,
		});

		if (positionals.length === 0) {
			this.#write("err", "Error: path required");
			process.exit(1);
		}

		await this.#client.restore(positionals[0]!);
		this.#write("out", `restored '${positionals[0]}'`);
	}

	async #rm(_args: string[], _options: CommandOptions) {
		this.#write("out", "Not Implemented");
	}

	async #commit(args: string[], options: CommandOptions) {
		const { values } = parseArgs({
			allowPositionals: true,
			args,
			options,
		});

		if (!values.message && !values.file) {
			this.#write("err", "Error: commit message required (-m or -F)");
			process.exit(1);
		}

		let message: string;
		if (values.file) {
			this.#write("err", "Error: -F flag not yet implemented");
			process.exit(1);
		} else {
			message = values.message as string;
		}

		const commitOid = await this.#client.commit(message);
		if (!values.quiet) {
			this.#write("out", `[main ${commitOid.slice(0, 7)}] ${message.split("\n")[0]}`);
		}
	}

	async #checkout(args: string[], options: CommandOptions) {
		const { positionals, values } = parseArgs({
			allowPositionals: true,
			args,
			options,
		});

		const target = positionals[0];
		if (!target) {
			this.#write("err", "Error: target branch or commit required");
			process.exit(1);
		}

		if (values.branch) {
			this.#write("err", "Error: -b flag not yet fully implemented");
			process.exit(1);
		}
		await this.#client.checkout(target);
		this.#write("out", `Switched to branch '${target}'`);
	}

	async #switch(args: string[], options: CommandOptions) {
		const { positionals } = parseArgs({
			allowPositionals: true,
			args,
			options,
		});

		const target = positionals[0];
		if (!target) {
			this.#write("err", "Error: branch name required");
			process.exit(1);
		}

		await this.#client.switch(target);
		this.#write("out", `Switched to branch '${target}'`);
	}

	async #branch(args: string[], options: CommandOptions) {
		const { positionals, values } = parseArgs({
			allowPositionals: true,
			args,
			options,
		});

		if (values.delete || values["force-delete"]) {
			for (const branch of positionals) {
				await this.#client.branch();
				this.#write("out", `Deleted branch ${branch}`);
			}
		} else if (positionals.length > 0) {
			const branchName = positionals[0];
			await this.#client.branch(branchName);
			this.#write("out", `Created branch ${branchName}`);
		} else {
			const branches = await this.#client.branch();
			for (const branch of branches || []) {
				this.#write("out", `  ${branch}`);
			}
		}
	}

	async #merge(args: string[], options: CommandOptions) {
		const { positionals } = parseArgs({
			allowPositionals: true,
			args,
			options,
		});

		const branch = positionals[0];
		if (!branch) {
			this.#write("err", "Error: branch name required");
			process.exit(1);
		}

		const result = await this.#client.merge(branch);
		this.#write("out", `Merged ${branch} (commit ${result.mergeCommitOid.slice(0, 7)})`);
	}

	async #rebase(args: string[], options: CommandOptions) {
		const { positionals } = parseArgs({
			allowPositionals: true,
			args,
			options,
		});

		const onto = positionals[0];
		if (!onto) {
			this.#write("err", "Error: target branch required");
			process.exit(1);
		}

		const result = await this.#client.rebase(onto);
		this.#write("out", `Successfully rebased ${result.replayed} commits onto ${onto}`);
	}

	async #reset(args: string[], options: CommandOptions) {
		const { positionals, values } = parseArgs({
			allowPositionals: true,
			args,
			options,
		});

		const ref = positionals[0] ?? "HEAD";
		const hard = values.hard as boolean | undefined;

		await this.#client.reset(hard ?? false, ref);
		this.#write("out", `Reset to ${ref}`);
	}

	async #tag(args: string[], options: CommandOptions) {
		const { positionals } = parseArgs({
			allowPositionals: true,
			args,
			options,
		});

		const tagName = positionals[0];
		if (!tagName) {
			this.#write("err", "Error: tag name required");
			process.exit(1);
		}

		await this.#client.tag(tagName);
		this.#write("out", `Created tag ${tagName}`);
	}

	async #fetch(args: string[], options: CommandOptions) {
		const { values } = parseArgs({
			allowPositionals: true,
			args,
			options,
		});

		const remote = (values.remote as string) ?? "origin";

		await this.#client.fetch(remote);
		this.#write("out", `Fetched from ${remote}`);
	}

	async #pull(args: string[], options: CommandOptions) {
		const { values } = parseArgs({
			allowPositionals: true,
			args,
			options,
		});

		const remote = (values.remote as string) ?? "origin";
		const branch = (values.branch as string) ?? "main";

		await this.#client.pull(remote, branch);
		this.#write("out", `Pulled from ${remote}/${branch}`);
	}

	async #push(args: string[], options: CommandOptions) {
		const { values } = parseArgs({
			allowPositionals: true,
			args,
			options,
		});

		const remote = (values.repo as string) ?? "origin";
		const branch = "main"; // TODO: get current branch
		const force = (values.force as boolean) ?? false;

		const result = await this.#client.push(remote, branch, force);
		if (result.success) {
			this.#write("out", `Successfully pushed ${result.pushed} objects to ${remote}/${branch}`);
		}
	}

	async #remote(args: string[], options: CommandOptions) {
		const { positionals, values } = parseArgs({
			allowPositionals: true,
			args,
			options,
		});

		if (values.add) {
			const name = positionals[0];
			const url = positionals[1];

			if (!name || !url) {
				this.#write("err", "Error: remote name and URL required");
				process.exit(1);
			}

			await this.#client.remote("add", name, url);
			if (!values.verbose) {
				this.#write("out", `Added remote '${name}' -> ${url}`);
			}
		} else if (values.delete) {
			const name = positionals[0];

			if (!name) {
				this.#write("err", "Error: remote name required");
				process.exit(1);
			}

			await this.#client.remote("remove", name);
			this.#write("out", `Deleted remote '${name}'`);
		} else if (values["set-url"]) {
			const name = positionals[0];
			const url = positionals[1];

			if (!name || !url) {
				this.#write("err", "Error: remote name and new URL required");
				process.exit(1);
			}

			await this.#client.remote("set-url", name, url);
			this.#write("out", `Updated remote '${name}' URL to ${url}`);
		} else if (values["get-url"]) {
			const name = positionals[0];

			if (!name) {
				this.#write("err", "Error: remote name required");
				process.exit(1);
			}

			try {
				const url = await this.#client.getRemote(name);

				if (url) {
					this.#write("out", url);
				} else {
					this.#write("err", `Error: remote '${name}' not found`);
					process.exit(1);
				}
			} catch {
				this.#write("err", "Error: no remotes configured");
				process.exit(1);
			}
		} else if (values.show) {
			const name = positionals[0] ?? "origin";

			try {
				const url = await this.#client.getRemote(name);

				if (url) {
					this.#write("out", `* remote ${name}`);
					this.#write("out", `  Fetch URL: ${url}`);
					this.#write("out", `  Push URL: ${url}`);
				} else {
					this.#write("err", `Error: remote '${name}' not found`);
					process.exit(1);
				}
			} catch {
				this.#write("err", "Error: no remotes configured");
				process.exit(1);
			}
		} else {
			// List remotes
			try {
				const remotes = await this.#client.getAllRemotes();
				const entries = Object.entries(remotes);

				if (entries.length === 0) {
					// No output for empty remote list
					return;
				}

				for (const [name, url] of entries) {
					if (values.verbose) {
						this.#write("out", `${name}\t${url} (fetch)`);
						this.#write("out", `${name}\t${url} (push)`);
					} else {
						this.#write("out", name);
					}
				}
			} catch {
				// No remotes configured yet
			}
		}
	}

	async #help(command?: Command) {
		let print = `${pkg.name} - ${pkg.description}\n\n`;
		print += `Usage: npx .git ${command?.command ?? "<command>"} [arguments]\n\n`;

		if (!command) {
			print += `Commands:\n`;
			for (const { command, description } of this.commands) {
				print += `\t${command}\t\t${description}\n`;
			}
		} else {
			print += `${command.description}\n`;
		}

		print += `\nOptions:\n`;
		for (const [option, info] of Object.entries(command?.options ?? this.#options)) {
			print += `\t${info.short ? `-${info.short}` : ""}\t--${option}\t\t${(info as CommandOption).description}\n`;
		}
		this.#write("out", print);
	}

	async #version() {
		this.#write("out", pkg.version);
	}

	#write(std: "out" | "err", message: string): void {
		switch (std) {
			case "out":
				process.stdout.write(`${message}\n`);
				break;
			case "err":
				process.stderr.write(`${message}\n`);
				break;
			default:
				throw new Error(`Invalid std: ${String(std)}`) as never;
		}
	}
}

if (import.meta.main) {
	try {
		const args = process.argv.slice(2);
		const cli = new Cli();
		await cli.run(args);
	} catch (error) {
		process.stderr.write(`Error: ${error instanceof Error ? error.message : String(error)}\n`);
		process.exit(1);
	}
}
