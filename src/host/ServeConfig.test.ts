/**
 * What `GIT_HOSTS`/`--hosts` accepts, and what it says about what it will not.
 *
 * The allowlist is the whole audience check: an entry that cannot match is not
 * a smaller allowlist, it is a server that refuses every host-bound credential
 * while looking configured. So the interesting cases here are the ones that
 * *look* like they would work — a pasted URL, a stray capital — and the value
 * of the refusal is the message, which is the only thing an operator behind a
 * proxy has to go on.
 */
import assert from "node:assert/strict";
import { describe, it } from "@effect/vitest";

import { Result } from "effect";

import { merge, parseHosts } from "./ServeConfig.ts";

const ENVIRONMENT = { root: "repos", port: 8080, hostname: "127.0.0.1", hosts: "" };

const accepted = (raw: string): ReadonlyArray<string> => {
  const parsed = parseHosts(raw);
  assert.ok(Result.isSuccess(parsed), `'${raw}' was refused: ${String(parsed)}`);
  return parsed.success;
};

const refused = (raw: string): string => {
  const parsed = parseHosts(raw);
  assert.ok(Result.isFailure(parsed), `'${raw}' was accepted as ${String(parsed)}`);
  return parsed.failure;
};

describe("the trusted-host list", () => {
  it("takes a comma-separated list and lowercases it for a Host header", () => {
    // A `Host` header is matched case-insensitively, so the list is stored in
    // the form the comparison uses rather than the form somebody typed.
    assert.deepEqual(accepted("Git.Example.COM"), ["git.example.com"]);
    assert.deepEqual(accepted("a.example.com,b.example.com"), ["a.example.com", "b.example.com"]);
  });

  it("reads a blank value as no list rather than a list of one nothing", () => {
    // The default: an unset `GIT_HOSTS` must leave the allowlist empty, not
    // holding `""` — which would match a header nothing sends and quietly
    // stand in for a configured entry.
    assert.deepEqual(accepted(""), []);
    assert.deepEqual(accepted("   "), []);
    assert.deepEqual(accepted("a.example.com,"), ["a.example.com"]);
    assert.deepEqual(accepted("a.example.com,,b.example.com"), ["a.example.com", "b.example.com"]);
  });

  it("keeps a port, which names a different destination", () => {
    assert.deepEqual(accepted("git.example.com:8443"), ["git.example.com:8443"]);
    assert.deepEqual(accepted("localhost:8080"), ["localhost:8080"]);
  });

  it("takes an IPv6 literal in the brackets a Host header carries", () => {
    assert.deepEqual(accepted("[::1]:8080"), ["[::1]:8080"]);
    assert.deepEqual(accepted("[2001:db8::1]"), ["[2001:db8::1]"]);
    // Unbracketed, `::1` and a port concatenate to something no client sends.
    assert.match(refused("::1:8080"), /not a host authority/);
  });

  it("refuses a pasted URL and says what to write instead", () => {
    // The plausible mistake, since a registered remote *is* written as a URL.
    // Accepted, it would be an entry no request could ever match.
    const said = refused("https://git.example.com");
    assert.match(said, /not a host authority/);
    assert.match(said, /write 'git\.example\.com'/);

    assert.match(
      refused("https://git.example.com:8443/repo.git"),
      /write 'git\.example\.com:8443'/,
    );
  });

  it("refuses the other shapes that cannot appear in a Host header", () => {
    for (const entry of [
      "git.example.com/repo", // a path
      "user@git.example.com", // user info
      "git.example.com:", // a colon naming no port
      "git.example.com:999999", // a port no socket has
      "git example.com", // a space
    ]) {
      assert.match(refused(entry), /not a host authority/, entry);
    }
  });

  it("names the offending entry rather than the whole list", () => {
    // An operator with six hosts configured needs to know which one, and a
    // message quoting the list back says nothing they did not already type.
    const said = refused("a.example.com,https://b.example.com,c.example.com");
    assert.match(said, /'https:\/\/b\.example\.com'/);
    assert.doesNotMatch(said, /c\.example\.com/);
  });
});

describe("resolving the trusted-host list", () => {
  it("takes GIT_HOSTS when no flag overrides it", () => {
    const resolved = merge({ ...ENVIRONMENT, hosts: "git.example.com, other.example.com" }, {});
    assert.ok(Result.isSuccess(resolved), String(resolved));
    assert.deepEqual(resolved.success.hosts, ["git.example.com", "other.example.com"]);
  });

  it("refuses a malformed GIT_HOSTS rather than starting on a dead list", () => {
    const resolved = merge({ ...ENVIRONMENT, hosts: "https://git.example.com" }, {});
    assert.ok(Result.isFailure(resolved));
    assert.match(resolved.failure, /^GIT_HOSTS: /);
  });

  it("lets an explicit --hosts override a malformed GIT_HOSTS", () => {
    // The documented precedence has to hold in the case that matters: an
    // operator whose environment is wrong reaching for the flag to fix it.
    // Validated eagerly, the bad environment value failed the read first and
    // the flag that was there to correct it could never be reached.
    const resolved = merge(
      { ...ENVIRONMENT, hosts: "https://git.example.com" },
      { hosts: ["git.example.com"] },
    );
    assert.ok(Result.isSuccess(resolved), String(resolved));
    assert.deepEqual(resolved.success.hosts, ["git.example.com"]);
  });

  it("defaults to no trusted hosts, which trusts only the bound address", () => {
    const resolved = merge(ENVIRONMENT, {});
    assert.ok(Result.isSuccess(resolved), String(resolved));
    assert.deepEqual(resolved.success.hosts, []);
  });

  it("lets every other flag win over its environment value too", () => {
    const resolved = merge(
      { root: "from-env", port: 1, hostname: "env.example.com", hosts: "" },
      { root: "from-flag", port: 2, hostname: "flag.example.com" },
    );
    assert.ok(Result.isSuccess(resolved), String(resolved));
    assert.deepEqual(resolved.success, {
      root: "from-flag",
      port: 2,
      hostname: "flag.example.com",
      hosts: [],
    });
  });
});
