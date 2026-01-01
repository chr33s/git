import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { after, before, describe, it } from "node:test";

import * as helpers from "./test.helpers.ts";

const cli = await helpers.cli();
const worker = await helpers.worker({ port: 8080 });

before(async () => {
	await cli.before();
	await worker.before();
});
after(async () => {
	await cli.after();
	await worker.after();
});

void describe("cli", () => {
	void it("git clone", async () => {
		const cd1 = await cli.setup();

		await writeFile(join(cd1, "README.md"), "# Test Repository");
		await mkdir(join(cd1, "src"), { recursive: true });
		await writeFile(join(cd1, "src/main.js"), "console.log('Hello World');");

		await cli.run("git add .", { cwd: cd1 });
		const res1 = await cli.run('git commit -m "Initial commit"', { cwd: cd1 });
		assert.ok(res1.includes("Initial commit"));

		const cloneRepo = `test-repo-${randomUUID().slice(0, 8)}`;
		const repoUrl = `${worker.url}${cloneRepo}.git`;

		await cli.run(`git remote add origin ${repoUrl}`, { cwd: cd1 });
		await cli.run("git push -u origin main", { cwd: cd1 });

		const cd2 = await cli.setup();
		await cli.run(`git -c init.defaultBranch=main clone ${repoUrl} cloned-repo`, { cwd: cd2 });

		// Verify clone contents
		const clonedDir = join(cd2, "cloned-repo");
		const res2 = await readFile(join(clonedDir, "README.md"), "utf-8");
		assert.equal(res2, "# Test Repository");
		const res3 = await readFile(join(clonedDir, "src/main.js"), "utf-8");
		assert.equal(res3, "console.log('Hello World');");
	});

	void it("git push", async () => {
		const cd1 = await cli.setup();

		await writeFile(join(cd1, "file1.txt"), "test content");
		await cli.run("git add .", { cwd: cd1 });
		await cli.run('git commit -m "commit for push test"', { cwd: cd1 });

		const pushRepo = `push-test-repo-${randomUUID().slice(0, 8)}`;
		const repoUrl = `${worker.url}${pushRepo}.git`;
		await cli.run(`git remote add origin ${repoUrl}`, { cwd: cd1 });
		await cli.run("git push -u origin main", { cwd: cd1 });
		const output = await cli.run("git ls-remote origin", { cwd: cd1 });
		assert.match(output, /refs\/heads\/main/, "Push should create refs on remote");
	});

	void it("git fetch", async () => {
		const cd1 = await cli.setup();
		const cd2 = await cli.setup();

		await writeFile(join(cd1, "version.txt"), "v1.0");
		await cli.run("git add .", { cwd: cd1 });
		await cli.run('git commit -m "version 1.0"', { cwd: cd1 });
		await writeFile(join(cd1, "version.txt"), "v1.1");
		await cli.run('git commit -am "version 1.1"', { cwd: cd1 });

		const fetchRepo = `fetch-test-repo-${randomUUID().slice(0, 8)}`;
		const repoUrl = `${worker.url}${fetchRepo}.git`;
		await cli.run(`git remote add origin ${repoUrl}`, { cwd: cd1 });
		await cli.run("git push -u origin main", { cwd: cd1 });

		await cli.run(`git -c init.defaultBranch=main clone ${repoUrl} cloned-repo`, { cwd: cd2 });
		const clonedDir2 = join(cd2, "cloned-repo");

		await cli.run("git fetch origin", { cwd: clonedDir2 });
		const output = await cli.run("git branch -r", { cwd: clonedDir2 });
		assert.match(output, /origin\/main/, "Fetch should retrieve main branch");
	});

	void it("git pull", async () => {
		const cd1 = await cli.setup();
		const cd2 = await cli.setup();

		// Setup initial repository with multiple commits
		await writeFile(join(cd1, "data.txt"), "initial data");
		await cli.run("git add .", { cwd: cd1 });
		await cli.run('git commit -m "initial commit"', { cwd: cd1 });
		await writeFile(join(cd1, "data.txt"), "updated data");
		await cli.run('git commit -am "update data"', { cwd: cd1 });

		const pullRepo = `pull-test-repo-${randomUUID().slice(0, 8)}`;
		const repoUrl = `${worker.url}${pullRepo}.git`;
		await cli.run(`git remote add origin ${repoUrl}`, { cwd: cd1 });
		await cli.run("git push -u origin main", { cwd: cd1 });

		// Setup second repository and add it as remote
		await cli.run(`git -c init.defaultBranch=main clone ${repoUrl} cloned-repo`, { cwd: cd2 });
		const clonedDir3 = join(cd2, "cloned-repo");

		// Pull changes into the second repository
		await cli.run("git pull origin main", { cwd: clonedDir3 });

		// Verify content was pulled correctly
		const dataTxt = await readFile(join(clonedDir3, "data.txt"), "utf-8");
		assert.match(dataTxt, /updated data/, "Pull should retrieve updated content");

		// Verify commit history was pulled
		const logOutput = await cli.run("git log --oneline", { cwd: clonedDir3 });
		assert.match(logOutput, /update data/, "Pull should include latest commits");
		assert.match(logOutput, /initial commit/, "Pull should preserve history");

		// Verify branch was set up correctly
		const branchOutput = await cli.run("git branch -r", { cwd: clonedDir3 });
		assert.match(branchOutput, /origin\/main/, "Pull should set up remote tracking");
	});
});
