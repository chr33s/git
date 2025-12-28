export default {
	fetch(request, env) {
		try {
			request.signal?.throwIfAborted();

			const repo = new URL(request.url).pathname
				.match(/^\/([a-z0-9-_.]+?)(?:\.git)?(?:\/|$)/)
				?.at(1);
			if (!repo) throw new Error("No repository name provided in URL");

			const gitRepository = env.GIT_SERVER.getByName(repo);
			return gitRepository.fetch(request);
		} catch (error: any) {
			// Handle AbortError specifically
			if (error.name === "AbortError") {
				console.info("Request aborted:", error.message);
				return new Response(null, { status: 499 }); // Client Closed Request
			}

			console.error("index.fetch:", error);

			return Response.json(
				{ error: error.message ?? "Internal Server Error" },
				{ status: error.status ?? 500 },
			);
		}
	},
} satisfies ExportedHandler<Env>;

export { Server as GitServer } from "./server.ts";
