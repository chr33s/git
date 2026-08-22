/**
 * Refspec parsing and mapping — the rule that decides what a fetched ref is
 * called at this end.
 *
 * Every input here is a name the *remote* chose, which is what makes the
 * `$`-expansion case below a correctness question rather than a curiosity.
 */
import assert from "node:assert/strict";
import { describe, it } from "@effect/vitest";

import { Effect, Result } from "effect";

import * as Refspec from "./Refspec.ts";

const parsed = (text: string): Refspec.Refspec => {
  const result = Refspec.parse(text);
  assert.equal(Result.isSuccess(result), true, `${text} should parse`);
  // SAFETY: asserted successful on the line above.
  return (result as Result.Success<Refspec.Refspec, never>).success;
};

describe("Refspec", () => {
  it.effect("maps a wildcard source onto a wildcard destination", () =>
    Effect.sync(() => {
      const spec = parsed("+refs/heads/*:refs/remotes/origin/*");
      assert.equal(Refspec.map(spec, "refs/heads/main"), "refs/remotes/origin/main");
      assert.equal(Refspec.map(spec, "refs/heads/feature/x"), "refs/remotes/origin/feature/x");
      assert.equal(Refspec.map(spec, "refs/tags/v1"), null);
    }),
  );

  it.effect("refuses an empty match, which would leave an empty path component", () =>
    Effect.sync(() => {
      const spec = parsed("refs/heads/*:refs/remotes/origin/*");
      assert.equal(Refspec.map(spec, "refs/heads/"), null);
    }),
  );

  it.effect("does not expand `$` patterns out of a ref name the remote chose", () =>
    Effect.sync(() => {
      // `String.replace` expands `$&`, "$`", `$'` and `$$` inside its second
      // argument, and the second argument here is part of a name that arrived
      // over the wire. Left as a replacement string, "refs/heads/x$`y" spliced
      // the whole matched prefix into the middle of the destination, and
      // `refs/heads/a$&b` produced a destination containing a `*`.
      const spec = parsed("refs/heads/*:refs/remotes/origin/*");
      assert.equal(Refspec.map(spec, "refs/heads/x$`y"), "refs/remotes/origin/x$`y");
      assert.equal(Refspec.map(spec, "refs/heads/a$&b"), "refs/remotes/origin/a$&b");
      assert.equal(Refspec.map(spec, "refs/heads/c$'d"), "refs/remotes/origin/c$'d");
      assert.equal(Refspec.map(spec, "refs/heads/e$$f"), "refs/remotes/origin/e$$f");
    }),
  );

  it.effect("takes the first spec that covers a ref", () =>
    Effect.sync(() => {
      const specs = [parsed("refs/heads/main:refs/heads/main"), ...Refspec.DEFAULT_FETCH];
      assert.equal(Refspec.resolve(specs, "refs/heads/main")?.destination, "refs/heads/main");
      assert.equal(Refspec.resolve(specs, "refs/nope/x"), null);
    }),
  );

  it.effect("replicates the branch rules alongside the trust they are enforced with", () =>
    Effect.sync(() => {
      // A replica holding the membership but not the rules answers `OPEN` to
      // every question `Policy.rulesOf` asks, so a mirror sharing the same trust
      // graph would let through exactly the pushes the origin protects.
      assert.notEqual(
        Refspec.resolve(Refspec.HUB_FETCH, "refs/meta/policy"),
        null,
        "the policy ref must be in the hub refspecs",
      );
      assert.notEqual(Refspec.resolve(Refspec.HUB_FETCH, Refspec.TRUST_LOG), null);
      assert.notEqual(Refspec.resolve(Refspec.HUB_FETCH, Refspec.SOCIAL_LOG), null);
      assert.notEqual(Refspec.resolve(Refspec.HUB_FETCH, "refs/hub/pr/1"), null);
      // And it is hidden from a plain advertisement, so naming it is the only
      // way it ever arrives.
      assert.equal(Refspec.hiddenFromAdvertisement("refs/meta/policy"), true);
    }),
  );

  it.effect("knows which namespaces only grow, and which are withheld from a clone", () =>
    Effect.sync(() => {
      assert.equal(Refspec.isAppendOnly("refs/hub/pr/1"), true);
      assert.equal(Refspec.isAppendOnly(Refspec.TRUST_LOG), true);
      assert.equal(Refspec.isAppendOnly(Refspec.SOCIAL_LOG), true);
      assert.equal(Refspec.isAppendOnly("refs/heads/main"), false);

      assert.equal(Refspec.hiddenFromAdvertisement("refs/hub/pr/1"), true);
      assert.equal(Refspec.hiddenFromAdvertisement(Refspec.TRUST_LOG), true);
      assert.equal(Refspec.hiddenFromAdvertisement(Refspec.SOCIAL_LOG), true);
      assert.equal(Refspec.hiddenFromAdvertisement("refs/quarantine/inbox/1"), true);
      // Identity is never hidden: verifying it would otherwise need permission.
      assert.equal(Refspec.hiddenFromAdvertisement(Refspec.TRUST_GENESIS), false);
      assert.equal(Refspec.hiddenFromAdvertisement("refs/heads/main"), false);
    }),
  );

  it.effect("recognizes a ref-prefix that asks for a hidden namespace", () =>
    Effect.sync(() => {
      // git writes the prefix its refspec gives it, and a refspec written
      // `refs/hub/*` yields `refs/hub` — no trailing slash. Answered with
      // `hiddenFromAdvertisement`, which asks whether a *ref* is hidden, that
      // prefix matched nothing: the client that had named the namespace by hand
      // was told there was nothing in it, and since v0 hides it too, could never
      // discover it at all.
      assert.equal(Refspec.namesHiddenNamespace("refs/hub"), true);
      assert.equal(Refspec.namesHiddenNamespace("refs/hub/"), true);
      assert.equal(Refspec.namesHiddenNamespace("refs/hub/pr/1"), true);
      assert.equal(Refspec.namesHiddenNamespace("refs/meta"), true);
      assert.equal(Refspec.namesHiddenNamespace("refs/meta/trust/"), true);
      assert.equal(Refspec.namesHiddenNamespace("refs/social"), true);
      assert.equal(Refspec.namesHiddenNamespace("refs/social/"), true);
      assert.equal(Refspec.namesHiddenNamespace("refs/quarantine"), true);

      // A prefix that merely starts the same way names nothing, and answering it
      // hands `ls-remote 'refs/h*'` the whole namespace for three characters.
      assert.equal(Refspec.namesHiddenNamespace("refs/h"), false);
      assert.equal(Refspec.namesHiddenNamespace("refs/hu"), false);
      assert.equal(Refspec.namesHiddenNamespace("refs/m"), false);
      // "Everything" is what the hiding exists to answer with less.
      assert.equal(Refspec.namesHiddenNamespace("refs/"), false);
      assert.equal(Refspec.namesHiddenNamespace(""), false);
      assert.equal(Refspec.namesHiddenNamespace("refs/heads/"), false);
    }),
  );

  it.effect("names the namespaces a refspec has to ask for by hand", () =>
    Effect.sync(() => {
      // The asking side of the same rule, and the half that went missing: the
      // probe was built by appending a character to the refspec's own prefix and
      // asking whether *that* was a hidden ref, so a refspec broad enough to
      // cover everything — `+refs/*:refs/*` — produced `refs/x`, which is not,
      // and no `ls-refs` was sent at all. Since v0 hides these too, the fetch
      // took no hub or trust state and reported success.
      assert.deepEqual(Refspec.hiddenPrefixes("refs/hub/"), ["refs/hub/"]);
      assert.deepEqual(Refspec.hiddenPrefixes("refs/hub/pr/"), ["refs/hub/pr/"]);
      assert.deepEqual(Refspec.hiddenPrefixes("refs/meta/trust/"), ["refs/meta/trust/"]);
      assert.deepEqual(Refspec.hiddenPrefixes("refs/social/"), ["refs/social/"]);
      assert.deepEqual(Refspec.hiddenPrefixes("refs/quarantine/"), ["refs/quarantine/"]);
      // A refspec that covers everything covers all three, and has to name them.
      assert.deepEqual(Refspec.hiddenPrefixes("refs/"), [
        "refs/hub/",
        "refs/meta/",
        "refs/quarantine/",
        "refs/social/",
      ]);
      assert.deepEqual(Refspec.hiddenPrefixes(""), [
        "refs/hub/",
        "refs/meta/",
        "refs/quarantine/",
        "refs/social/",
      ]);
      // And one that covers neither asks for nothing.
      assert.deepEqual(Refspec.hiddenPrefixes("refs/heads/"), []);
      assert.deepEqual(Refspec.hiddenPrefixes("refs/tags/"), []);
    }),
  );

  it.effect("asks for the head of a source, wherever its wildcard is", () =>
    Effect.sync(() => {
      // A refspec may put its `*` in the middle — `map` supports exactly that —
      // and the probe is a *prefix*, which `ls-refs` compares with `startsWith`.
      // Cut at the last character instead of the first wildcard, the ask kept
      // its `*`, matched nothing, and the fetch reported a replication of zero
      // refs as a success.
      const spec = (text: string): Refspec.Refspec => {
        const parsed = Refspec.parse(text);
        assert.ok(Result.isSuccess(parsed), text);
        return parsed.success;
      };

      assert.deepEqual(Refspec.probes(spec("refs/hub/*/head:refs/local/*")), ["refs/hub/"]);
      assert.deepEqual(Refspec.probes(spec("refs/hub/*:refs/hub/*")), ["refs/hub/"]);
      assert.deepEqual(Refspec.probes(spec("refs/*:refs/*")), [
        "refs/hub/",
        "refs/meta/",
        "refs/quarantine/",
        "refs/social/",
      ]);
      // A source with no wildcard is its own head — and the rules file is
      // hidden from the advertisement too, so it is asked for by name.
      assert.deepEqual(Refspec.probes(spec("refs/meta/policy:refs/meta/policy")), [
        "refs/meta/policy",
      ]);
      assert.deepEqual(Refspec.probes(spec("refs/heads/*:refs/heads/*")), []);
    }),
  );
});
