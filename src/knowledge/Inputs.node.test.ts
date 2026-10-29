import assert from "node:assert/strict";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { it } from "@effect/vitest";
import { Effect, Layer } from "effect";
import * as Node from "../git/Node.ts";
import * as GitRepository from "../git/Repository.ts";
import { knowledgeFixture } from "../testing/Knowledge.ts";

it.live(
  "one manifest produces identical evidence in processes with different clocks, locales and timezones",
  () =>
    Effect.promise(async () => {
      const root = await fs.mkdtemp(path.join(os.tmpdir(), "knowledge-inputs-"));
      const directory = path.join(root, "repository");
      try {
        const fixture = await Effect.runPromise(
          knowledgeFixture().pipe(
            Effect.provide(
              GitRepository.layer.pipe(
                Layer.provide(GitRepository.hooksNoop),
                Layer.provide(Node.stores(directory)),
              ),
            ),
          ),
        );
        const retained = path.join(root, "inputs.json");
        await fs.writeFile(retained, JSON.stringify(fixture.evidence));
        const module = (name: string) => new URL(name, import.meta.url).href;
        const script = `import * as fs from 'node:fs/promises';
      import { Effect, Layer, Schema } from 'effect';
      import * as Node from ${JSON.stringify(module("../git/Node.ts"))};
      import * as GitRepository from ${JSON.stringify(module("../git/Repository.ts"))};
      import { Repository } from ${JSON.stringify(module("../git/Repository.ts"))};
      import * as Proposal from ${JSON.stringify(module("./Proposal.ts"))};
      import * as Maintenance from ${JSON.stringify(module("./Maintenance.ts"))};
      import * as Outcomes from ${JSON.stringify(module("./Outcomes.ts"))};
      const instant=Number(process.argv[1]);
      const OriginalDate=Date;
      globalThis.Date=class extends OriginalDate {
        constructor(...args) {super(...(args.length===0 ? [instant] : args));}
        static now() {return instant;}
      };
      const result=await Effect.runPromise(Effect.gen(function* () {
        const evidence=yield* Schema.decodeEffect(Schema.fromJsonString(Proposal.Evidence))(yield* Effect.promise(()=>fs.readFile(${JSON.stringify(retained)}, 'utf8')));
        const plan=yield* Maintenance.plan({manifest:evidence.manifest, history:[], maximum:25});
        const checks=yield* Effect.forEach(plan.chosen, selection=>Maintenance.check(evidence.manifest, selection));
        const outcomes=yield* Effect.forEach(evidence.manifest.outcomeHeads, ({ref})=>Outcomes.evaluate({manifest:evidence.manifest,ref,actors:evidence.actors}));
        return {plan,checks,outcomes};
      }).pipe(Effect.provide(GitRepository.layer.pipe(Layer.provide(GitRepository.hooksNoop), Layer.provide(Node.stores(${JSON.stringify(directory)}))))));
      process.stdout.write(JSON.stringify(result));`;
        const execute = promisify(execFile);
        const results = await Promise.all(
          [
            { at: "2025-01-01T00:00:00Z", timezone: "Pacific/Honolulu", locale: "en_US.UTF-8" },
            { at: "2035-01-01T00:00:00Z", timezone: "Asia/Ho_Chi_Minh", locale: "tr_TR.UTF-8" },
          ].map(({ at, timezone, locale }) =>
            execute(
              process.execPath,
              ["--input-type=module", "-e", script, String(Date.parse(at))],
              {
                env: { ...process.env, TZ: timezone, LANG: locale, LC_ALL: locale },
                maxBuffer: 4 * 1024 * 1024,
              },
            ),
          ),
        );
        assert.equal(results[0]?.stdout, results[1]?.stdout);
        assert.match(results[0]?.stdout ?? "", /supported|accepted/u);
      } finally {
        await fs.rm(root, { recursive: true, force: true });
      }
    }),
);
