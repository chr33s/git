/**
 * Refspec parsing and mapping — the rule that decides what a fetched ref is
 * called at this end.
 *
 * Every input here is a name the *remote* chose, which is what makes the
 * `$`-expansion case below a correctness question rather than a curiosity.
 */
import assert from "node:assert/strict";
import { describe, it } from "@effect/vitest";

import { Result } from "effect";

import * as Refspec from "./Refspec.ts";

const parsed = (text: string): Refspec.Refspec => {
  const result = Refspec.parse(text);
  assert.equal(Result.isSuccess(result), true, `${text} should parse`);
  // SAFETY: asserted successful on the line above.
  return (result as Result.Success<Refspec.Refspec, never>).success;
};

describe("Refspec", () => {
  it("maps a wildcard source onto a wildcard destination", () => {
    const spec = parsed("+refs/heads/*:refs/remotes/origin/*");
    assert.equal(Refspec.map(spec, "refs/heads/main"), "refs/remotes/origin/main");
    assert.equal(Refspec.map(spec, "refs/heads/feature/x"), "refs/remotes/origin/feature/x");
    assert.equal(Refspec.map(spec, "refs/tags/v1"), null);
  });

  it("refuses an empty match, which would leave an empty path component", () => {
    const spec = parsed("refs/heads/*:refs/remotes/origin/*");
    assert.equal(Refspec.map(spec, "refs/heads/"), null);
  });

  it("does not expand `$` patterns out of a ref name the remote chose", () => {
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
  });

  it("takes the first spec that covers a ref", () => {
    const specs = [parsed("refs/heads/main:refs/heads/main"), ...Refspec.DEFAULT_FETCH];
    assert.equal(Refspec.resolve(specs, "refs/heads/main")?.destination, "refs/heads/main");
    assert.equal(Refspec.resolve(specs, "refs/nope/x"), null);
  });

  it("replicates the branch rules alongside the trust they are enforced with", () => {
    // A replica holding the membership but not the rules answers `OPEN` to
    // every question `Policy.rulesOf` asks, so a mirror sharing the same trust
    // graph would let through exactly the pushes the origin protects.
    assert.notEqual(
      Refspec.resolve(Refspec.HUB_FETCH, "refs/meta/policy"),
      null,
      "the policy ref must be in the hub refspecs",
    );
    assert.notEqual(Refspec.resolve(Refspec.HUB_FETCH, Refspec.TRUST_LOG), null);
    assert.notEqual(Refspec.resolve(Refspec.HUB_FETCH, "refs/hub/pr/1"), null);
    // And it is hidden from a plain advertisement, so naming it is the only
    // way it ever arrives.
    assert.equal(Refspec.hiddenFromAdvertisement("refs/meta/policy"), true);
  });

  it("knows which namespaces only grow, and which are withheld from a clone", () => {
    assert.equal(Refspec.isAppendOnly("refs/hub/pr/1"), true);
    assert.equal(Refspec.isAppendOnly(Refspec.TRUST_LOG), true);
    assert.equal(Refspec.isAppendOnly("refs/heads/main"), false);

    assert.equal(Refspec.hiddenFromAdvertisement("refs/hub/pr/1"), true);
    assert.equal(Refspec.hiddenFromAdvertisement(Refspec.TRUST_LOG), true);
    // Identity is never hidden: verifying it would otherwise need permission.
    assert.equal(Refspec.hiddenFromAdvertisement(Refspec.TRUST_GENESIS), false);
    assert.equal(Refspec.hiddenFromAdvertisement("refs/heads/main"), false);
  });

  it("recognizes a ref-prefix that asks for a hidden namespace", () => {
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
    // A prefix on the way to one counts, since the client walking down to it
    // asks with each: `refs/h` reaches `refs/hub`.
    assert.equal(Refspec.namesHiddenNamespace("refs/h"), true);

    // "Everything" is what the hiding exists to answer with less.
    assert.equal(Refspec.namesHiddenNamespace("refs/"), false);
    assert.equal(Refspec.namesHiddenNamespace(""), false);
    assert.equal(Refspec.namesHiddenNamespace("refs/heads/"), false);
  });
});
