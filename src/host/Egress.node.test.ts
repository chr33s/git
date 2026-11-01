import assert from "node:assert/strict";
import * as http from "node:http";
import { describe, it } from "@effect/vitest";
import { ConfigProvider, Effect, Predicate } from "effect";
import { publicAddress } from "../server/EgressAddress.ts";
import { checkedLookup, configuration, make, origins } from "./Egress.node.ts";

describe("host egress policy", () => {
  it.effect("classifies public unicast conservatively, including embedded IPv4", () =>
    Effect.sync(() => {
      for (const address of [
        "1.1.1.1",
        "8.8.8.8",
        "100.63.255.255",
        "100.128.0.1",
        "172.15.255.255",
        "172.32.0.1",
        "2001:4860:4860::8888",
        "2606:4700:4700::1111",
      ]) {
        assert.equal(publicAddress(address), true, address);
      }
      for (const address of [
        "0.0.0.0",
        "10.0.0.1",
        "100.64.0.1",
        "100.127.255.255",
        "127.0.0.1",
        "127.99.1.1",
        "169.254.169.254",
        "172.16.0.1",
        "172.31.255.255",
        "192.168.1.1",
        "192.0.0.9",
        "192.0.2.1",
        "192.88.99.1",
        "198.18.0.1",
        "198.19.0.1",
        "198.51.100.1",
        "203.0.113.1",
        "224.0.0.1",
        "240.0.0.1",
        "255.255.255.255",
        "::",
        "::1",
        "::ffff:127.0.0.1",
        "::ffff:8.8.8.8",
        "64:ff9b::7f00:1",
        "fc00::1",
        "fd00::1",
        "fe80::1",
        "ff02::1",
        "2001::1",
        "2001:db8::1",
        "2002:7f00:1::",
        "3fff::1",
        "bad",
        "0177.0.0.1",
        "256.1.1.1",
        "fe80::1%lo0",
      ]) {
        assert.equal(publicAddress(address), false, address);
      }
    }),
  );

  it.effect("requires explicit origins without credentials or path policy ambiguity", () =>
    Effect.gen(function* () {
      assert.deepEqual([...origins(["https://EXAMPLE.com:443/"])], ["https://example.com"]);
      for (const value of [
        "https://example.com/path",
        "http://u:p@localhost",
        "file:///",
        "https://example.com?x=1",
        "https://example.com#x",
        "https://*.example.com",
      ]) {
        const result = yield* configuration().pipe(
          Effect.provide(
            ConfigProvider.layer(ConfigProvider.fromUnknown({ GIT_EGRESS_PRIVATE_ORIGINS: value })),
          ),
          Effect.result,
        );
        assert.equal(result._tag, "Failure", value);
      }
    }),
  );

  it.live("rechecks changed DNS answers and rejects mixed private/public answers", () =>
    Effect.promise(async () => {
      let address = "1.1.1.1";
      let calls = 0;
      const lookup = checkedLookup({ allowPrivate: false }, async () => {
        calls++;
        return [{ address, family: 4 }];
      });
      const invoke = () =>
        new Promise((resolve, reject) =>
          lookup("remote.test", { all: true }, (error, records) =>
            error ? reject(error) : resolve(records),
          ),
        );
      assert.deepEqual(await invoke(), [{ address: "1.1.1.1", family: 4 }]);
      address = "127.0.0.1";
      await assert.rejects(invoke(), /network policy denied/);
      assert.equal(calls, 2);
      const mixed = checkedLookup({ allowPrivate: false }, async () => [
        { address: "1.1.1.1", family: 4 },
        { address: "fd00::1", family: 6 },
      ]);
      await assert.rejects(
        new Promise((resolve, reject) =>
          mixed("remote.test", { all: true }, (error, records) =>
            error ? reject(error) : resolve(records),
          ),
        ),
        /network policy denied/,
      );
      // Plain HTTP granted for local development must not become public HTTP
      // when localhost's DNS answer changes. No connection is made by this test.
      const local = checkedLookup(
        { allowPrivate: false, allowLoopback: true, onlyLoopback: true },
        async () => [{ address: "1.1.1.1", family: 4 }],
      );
      await assert.rejects(
        new Promise((resolve, reject) =>
          local("localhost", { all: true }, (error, records) =>
            error ? reject(error) : resolve(records),
          ),
        ),
        /network policy denied/,
      );
    }),
  );

  it.live(
    "allows configured local development, follows same-origin redirects, and never forwards credentials across origins",
    () =>
      Effect.promise(async () => {
        let hits = 0;
        let connections = 0;
        let crossHits = 0;
        const other = http.createServer((_req, res) => {
          crossHits++;
          res.end("unexpected");
        });
        await new Promise<void>((resolve) => other.listen(0, "127.0.0.1", resolve));
        const otherAddress = other.address();
        assert.ok(otherAddress !== null && !Predicate.isString(otherAddress));
        const otherUrl = `http://127.0.0.1:${otherAddress.port}`;
        const server = http.createServer((req, res) => {
          hits++;
          res.setHeader("connection", "close");
          if (req.url === "/redirect" || req.url === "/cross") {
            res.writeHead(302, { location: req.url === "/cross" ? otherUrl : "/target" });
            res.end();
          } else {
            res.end(req.headers.authorization ?? "no credential");
          }
        });
        server.on("connection", () => {
          connections++;
        });
        await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
        const address = server.address();
        assert.ok(address !== null && !Predicate.isString(address));
        const url = `http://127.0.0.1:${address.port}`;
        const denied = make(new Set());
        const allowed = make(origins([url, otherUrl]));
        let resolvedAddress = "127.0.0.1";
        let lookups = 0;
        const changingDns = make(new Set(), {
          allowLoopback: true,
          resolve: async () => {
            lookups++;
            return [{ address: resolvedAddress, family: 4 }];
          },
        });
        try {
          await assert.rejects(denied.fetch(url));
          await assert.rejects(denied.fetch(`https://127.0.0.1:${address.port}`));
          await assert.rejects(denied.fetch(`https://2130706433:${address.port}`));
          await assert.rejects(denied.fetch(`https://0177.0.0.1:${address.port}`));
          assert.equal(hits, 0, "literal private destinations must not reach the listener");
          assert.equal(
            connections,
            0,
            "refusal must precede even a TLS handshake to the private address",
          );
          const response = await allowed.fetch(url + "/redirect", {
            headers: { authorization: "Bearer fixture-secret" },
          });
          assert.equal(response.url, url + "/target");
          assert.equal(response.redirected, true);
          assert.equal(response.clone().url, response.url);
          assert.equal(await response.text(), "Bearer fixture-secret");
          await assert.rejects(
            allowed.fetch(url + "/cross", { headers: { authorization: "Bearer fixture-secret" } }),
          );
          assert.equal(crossHits, 0);
          await assert.rejects(
            allowed.fetch(url + "/redirect", { method: "POST", body: "one-shot write" }),
          );
          const dnsUrl = `http://localhost:${address.port}/dns`;
          assert.equal(await (await changingDns.fetch(dnsUrl)).text(), "no credential");
          const beforeRebind = hits;
          const beforeConnections = connections;
          resolvedAddress = "169.254.169.254";
          await assert.rejects(changingDns.fetch(dnsUrl));
          assert.equal(lookups, 2, "a new connection must resolve and validate again");
          assert.equal(hits, beforeRebind);
          assert.equal(connections, beforeConnections);
        } finally {
          await Promise.all([denied.close(), allowed.close(), changingDns.close()]);
          await Promise.all([
            new Promise<void>((resolve) => server.close(() => resolve())),
            new Promise<void>((resolve) => other.close(() => resolve())),
          ]);
        }
      }),
  );
});
