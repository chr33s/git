/**
 * The Worker: a router that resolves `/:repo` to a DO stub and forwards.
 * Same job as `git/Durable.ts`'s default export, with one difference —
 * `repos` is a typed stub derived from the DO class, so a renamed method is
 * a compile error rather than a 500 at the edge.
 *
 * Its own module because the bundler requires this file's *default export*
 * to be the worker layer, and `alchemy.run.ts`'s default export must be the
 * stack. The class stays importable on its own; consumers that bind it do
 * not pull in `.make()` — the bundler tree-shakes it.
 */
import * as Alchemy from "alchemy/Cloudflare";
import { Effect } from "effect";
import { HttpServerRequest, HttpServerResponse } from "effect/unstable/http";

import Repos from "./host/Cloudflare.ts";
import { Repo } from "./host/Cloudflare.ts";

/** The Worker's public contract: it serves HTTP, nothing more. */
export type GitShape = {
  readonly fetch: Effect.Effect<
    HttpServerResponse.HttpServerResponse,
    never,
    HttpServerRequest.HttpServerRequest
  >;
};

/** `Repo` in the third slot: this Worker hosts the DO, so it is contract. */
export class Git extends Alchemy.Worker<Git, GitShape, Repo>()("git") {}

export default Git.make(
  {
    main: import.meta.url,
    compatibility: { date: "2025-12-10", flags: ["nodejs_compat"] },
  },
  Effect.gen(function* () {
    const repos = yield* Repo;

    return {
      fetch: Effect.gen(function* () {
        const request = yield* HttpServerRequest.HttpServerRequest;
        const name = new URL(request.url, "http://x").pathname.split("/")[1];
        if (name === undefined || name === "") {
          return HttpServerResponse.text("No repository in URL", { status: 400 });
        }

        const response = yield* repos
          .getByName(name)
          .fetch(new Request(request.url, { method: request.method }));
        return HttpServerResponse.raw(response);
      }),
    };
    // The host Worker's layer also provides its DO's live implementation —
    // that is what makes `yield* Repo` above resolve.
  }).pipe(Effect.provide(Repos)),
);
