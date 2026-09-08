/**
 * `git+ knowledge check` — the one operation ordinary file tooling cannot do.
 *
 * Concepts are ordinary Markdown: they are edited, diffed, reviewed and moved
 * with `$EDITOR`, `git diff` and `git log`, so there is deliberately no
 * `list`, `show`, `import` or `export` here (docs/knowledge.md §14). What Git+
 * adds is the check — does this Concept's declared provenance still verify,
 * are the repository files it depends on still the ones it named, has its
 * deadline passed — and each of those is reported on its own axis.
 *
 * Read-only in the sense that matters: it stages nothing, writes no Concept,
 * persists no Memory and appends no exposure. Capturing a dirty view does
 * materialize blobs, which is stated in `--help` rather than glossed as "no
 * disk writes" (docs/context-pack.knowledge.md §12.1).
 */
import { Console, Effect } from "effect";
import { Argument, Command, Flag } from "effect/unstable/cli";

import { Invalid } from "../git/Error.ts";
import { Repository } from "../git/Repository.ts";
import * as Pack from "../context/Pack.ts";
import * as Check from "../knowledge/Check.ts";
import * as Concept from "../knowledge/Concept.ts";
import {
  membershipOrNull,
  mustResolve,
  repoFlag,
  rootFlag,
  withDiscovered,
  withWork,
  workFlag,
} from "./shared.ts";

const render = (report: Check.Report): ReadonlyArray<string> => {
  const lines = [
    `bundle    ${report.bundle}${report.bundleAbsent ? " (absent)" : ""}`,
    `view      ${report.view.tree}`,
    `profile   okf ${Concept.OKF_VERSION} @ ${report.profile.okfRevision.slice(0, 12)} · ${report.profile.checkerVersion}`,
    `evaluated ${report.evaluatedAt} · ${report.completeness}`,
  ];
  for (const concept of report.concepts) {
    lines.push("");
    lines.push(`${concept.id}  ${concept.blob}`);
    lines.push(
      `  structure ${concept.structure.state}   lifecycle ${concept.lifecycle}   temporal ${concept.temporal.state}`,
    );
    for (const citation of concept.citations) {
      lines.push(`  cites     ${citation.state.padEnd(18)} ${citation.record}`);
    }
    for (const dependency of concept.repositoryEvidence) {
      lines.push(`  evidence  ${dependency.state.padEnd(18)} ${dependency.path}`);
    }
    for (const source of concept.external) {
      lines.push(
        `  external  ${source.state.padEnd(18)} ${source.id} (snapshot ${source.retention})`,
      );
    }
    // Both halves, because they are different questions: whether automatic
    // recall may use it, and why not (§7).
    lines.push(
      `  recall    ${concept.eligibility.recall ? "eligible" : "excluded"}${
        concept.eligibility.reasons.length === 0
          ? ""
          : ` (${concept.eligibility.reasons.join(", ")})`
      }`,
    );
    for (const diagnosed of concept.diagnostics) {
      lines.push(`  ${diagnosed.severity.padEnd(7)} ${diagnosed.code}: ${diagnosed.message}`);
    }
  }
  for (const diagnosed of report.diagnostics) {
    lines.push(`${diagnosed.severity.padEnd(9)} ${diagnosed.code}: ${diagnosed.message}`);
  }
  // And say what to do about it. `hub enable` does not fetch session refs
  // by default, so on a fresh clone every citation reads `unavailable` — and
  // a report that said only that left an operator with nothing verified and
  // no next step, while the same situation in `session` names the command.
  if (
    report.concepts.some((concept) =>
      concept.citations.some((citation) => citation.state === "unavailable"),
    )
  ) {
    lines.push(
      "",
      "! some cited records are not in this replica, so their provenance was not verified; fetch the session refs with `git+ hub enable --refs 'refs/hub/session/*'`",
    );
  }
  return lines;
};

const check = Command.make(
  "check",
  {
    root: rootFlag,
    repo: repoFlag,
    work: workFlag,
    ref: Flag.string("ref").pipe(
      Flag.withDefault(""),
      Flag.withDescription("Check this committed revision instead of the working view"),
    ),
    bundle: Flag.string("bundle").pipe(
      Flag.withDefault(Concept.BUNDLE),
      Flag.withDescription("Repository-relative bundle root"),
    ),
    strict: Flag.boolean("strict").pipe(
      Flag.withDefault(false),
      Flag.withDescription(
        "Also fail on changed dependencies, expired deadlines and unverified citations",
      ),
    ),
    json: Flag.boolean("json").pipe(
      Flag.withDefault(false),
      Flag.withDescription("One parseable report on stdout"),
    ),
    at: Flag.string("at").pipe(
      Flag.withDefault(""),
      Flag.withDescription("The evaluation instant, as an ISO 8601 datetime with an offset"),
    ),
    concept: Argument.string("concept").pipe(Argument.optional),
  },
  ({ at, bundle, concept, json, ref, repo, root, strict, work }) =>
    Effect.gen(function* () {
      // Escape is refused at the boundary rather than resolved: a bundle
      // outside the chosen view is not this repository's knowledge (§5.1).
      const root_ = bundle.replace(/\/+$/u, "");
      if (root_ === "" || root_.startsWith("/") || root_.split("/").includes("..")) {
        return yield* new Invalid({
          field: "bundle",
          reason: "--bundle must be a repository-relative path inside the view",
        });
      }

      // The clock is an argument, never ambient: INV-14 makes an ambient clock
      // a way for one pinned input to produce two answers, and `stale_after`
      // is exactly the field that would move.
      const instant = at === "" ? null : Concept.instantOf(at);
      if (instant !== null && instant.state === "invalid") {
        return yield* new Invalid({ field: "at", reason: instant.reason });
      }
      const evaluationTime = instant === null ? new Date() : new Date(instant.at);

      const named = concept._tag === "Some" ? concept.value : null;

      // One check for both views below, so the inputs it is given cannot drift
      // between the committed and the working-tree branch.
      const checkView = Effect.fn("knowledge.checkView")(function* (view: Pack.View) {
        const { repo: identity, trust } = yield* membershipOrNull();
        return yield* Check.check({
          view,
          bundle: root_,
          concept: named,
          evaluationTime,
          repo: identity,
          trust,
        });
      });

      const run = Effect.gen(function* () {
        const repository = yield* Repository;
        const base = yield* mustResolve(repository, ref === "" ? "HEAD" : ref);
        return yield* checkView(yield* Pack.committed(base));
      });

      // A bare repository cannot invent a working tree, and an explicit `--ref`
      // means "that committed revision, not my edits" (§12.1). Everything else
      // checks the permitted effective working view, dirty knowledge changes
      // included, because that is what an author about to commit is asking
      // about.
      const report =
        repo !== "" || ref !== ""
          ? yield* withDiscovered(root, repo, run)
          : yield* withWork(
              work,
              Effect.gen(function* () {
                const repository = yield* Repository;
                const head = yield* repository.resolve(yield* repository.head);
                if (head === null) {
                  return yield* new Invalid({
                    field: "ref",
                    reason: "this checkout has no commit yet; there is no view to check",
                  });
                }
                return yield* checkView(yield* Pack.capture(head));
              }),
            );

      if (json) {
        // The `concept` field is the parse result, which is this module's
        // input and not part of the response contract (§12.2).
        yield* Console.log(
          JSON.stringify(
            {
              ...report,
              concepts: report.concepts.map((checked) => {
                const { concept, ...rest } = checked;
                void concept;
                return rest;
              }),
            },
            null,
            2,
          ),
        );
      } else {
        for (const line of render(report)) yield* Console.log(line);
      }

      const gate = Check.gate(report, strict);
      if (!gate.ok) {
        return yield* new Invalid({
          field: "knowledge",
          reason: gate.failures.join("; "),
        });
      }
    }),
);

export const knowledgeCommand = Command.make("knowledge", {}, () =>
  Console.log("Usage: git+ knowledge check [<concept>]"),
).pipe(
  Command.withSubcommands([
    check.pipe(
      Command.withDescription(
        "Check a Concept bundle's structure, provenance, dependencies and freshness",
      ),
    ),
  ]),
);
