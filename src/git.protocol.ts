export interface GitRepoInfo {
	host: string;
	repo: string;
}

export interface GitRef {
	oid?: string;
	name: string;
	target?: string;
}

export class GitProtocol {
	async discoverRefs(repo: GitRepoInfo): Promise<GitRef[]> {
		const url = `https://${repo.host}/${repo.repo}.git/info/refs?service=git-upload-pack`;

		const response = await fetch(url);
		if (!response.ok) {
			throw new Error(`Failed to discover refs: ${response.statusText}`);
		}

		const text = await response.text();
		return this.#parseRefs(text);
	}

	async fetchPack(
		repo: GitRepoInfo,
		wants: string[],
		haves: string[],
	): Promise<ReadableStream<Uint8Array>> {
		const url = `https://${repo.host}/${repo.repo}.git/git-upload-pack`;

		// Build request body
		const lines: string[] = [];

		// Want lines
		for (let i = 0; i < wants.length; i++) {
			if (i === 0) {
				lines.push(`0032want ${wants[i]}\n`);
			} else {
				lines.push(`0032want ${wants[i]}\n`);
			}
		}

		// Have lines
		for (const have of haves) {
			lines.push(`0032have ${have}\n`);
		}

		// Done
		lines.push("0009done\n");

		const body = lines.join("");

		const response = await fetch(url, {
			method: "POST",
			headers: {
				"Content-Type": "application/x-git-upload-pack-request",
				Accept: "application/x-git-upload-pack-result",
			},
			body,
		});

		if (!response.ok) {
			throw new Error(`Failed to fetch pack: ${response.statusText}`);
		}

		if (!response.body) {
			throw new Error("No response body");
		}

		return response.body;
	}

	async pushPack(
		repo: GitRepoInfo,
		refs: Array<{ ref: string; old: string; new: string }>,
		packData: Uint8Array,
	) {
		const url = `https://${repo.host}/${repo.repo}.git/git-receive-pack`;

		// Build request
		const lines: string[] = [];

		for (const ref of refs) {
			lines.push(`${ref.old} ${ref.new} ${ref.ref}\0`);
		}

		lines.push("0000");

		const header = new TextEncoder().encode(lines.join(""));
		const body = new Uint8Array(header.length + packData.length);
		body.set(header);
		body.set(packData, header.length);

		const response = await fetch(url, {
			method: "POST",
			headers: {
				"Content-Type": "application/x-git-receive-pack-request",
				Accept: "application/x-git-receive-pack-result",
			},
			body,
		});

		if (!response.ok) {
			throw new Error(`Failed to push: ${response.statusText}`);
		}
	}

	#parseRefs(data: string): GitRef[] {
		const refs: GitRef[] = [];
		const lines = data.split("\n");

		for (const line of lines) {
			if (line.length === 0 || line.startsWith("#")) continue;

			// Remove pkt-line length prefix if present
			const content = line.replace(/^[0-9a-f]{4}/, "");

			const match = content.match(/^([0-9a-f]{40})\s+(.+?)(\^\{\})?$/);
			if (match && match[1] && match[2]) {
				refs.push({
					oid: match[1],
					name: match[2],
				});
			}
		}

		// Parse symbolic refs
		const headLine = lines.find((l) => l.includes("symref=HEAD"));
		if (headLine) {
			const match = headLine.match(/symref=HEAD:(.+?)\s/);
			if (match) {
				refs.push({
					name: "HEAD",
					target: match[1],
				});
			}
		}

		return refs;
	}
}
