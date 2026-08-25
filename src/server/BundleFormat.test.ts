import assert from "node:assert/strict";
import { describe, it } from "@effect/vitest";

import { Effect, Result } from "effect";

import type { Oid } from "../git/Oid.ts";
import {
  artifactId,
  artifactPath,
  catchupArtifacts,
  cloneArtifacts,
  decodeManifest,
  encodeConfigList,
  encodeHeader,
  encodeManifest,
  encodeProtocolList,
  familyState,
  isBundleId,
  listEntries,
  nextToken,
  objectIdOf,
  parseHeader,
  publishArtifact,
} from "./BundleFormat.ts";

// SAFETY: forty hex characters by construction.
const oid = (char: string): Oid => char.repeat(40) as Oid;

describe("BundleFormat identifiers", () => {
  it.effect("accepts git's bundle-list identifier grammar", () =>
    Effect.sync(() => {
      assert.equal(isBundleId("full-1"), true);
      assert.equal(isBundleId("ninc-1700"), true);
      assert.equal(isBundleId("1full"), false);
      assert.equal(isBundleId("full_1"), false);
      assert.equal(artifactId("full", null, 7n), "full-7");
      assert.equal(artifactId("incremental", "blob:none", 8n), "ninc-8");
      assert.equal(objectIdOf(null, 7n, "abc"), "full/7-abc.bundle");
    }),
  );

  it.effect("issues strictly increasing creation tokens", () =>
    Effect.sync(() => {
      const now = new Date(1_700_000_000_000);
      assert.equal(nextToken(null, now), 1_700_000_000_000n);
      assert.equal(nextToken(1_700_000_000_000n, now), 1_700_000_000_001n);
      assert.equal(nextToken(1n, now), 1_700_000_000_000n);
    }),
  );
});

describe("BundleFormat header", () => {
  it.effect("round-trips v2 refs and prerequisites", () =>
    Effect.sync(() => {
      const bytes = encodeHeader({
        version: 2,
        filter: null,
        refs: { "refs/heads/main": oid("a") },
        prerequisites: [oid("b")],
      });
      const parsed = parseHeader(bytes);
      assert.equal(Result.isSuccess(parsed), true);
      if (Result.isFailure(parsed)) return;
      assert.equal(parsed.success.header.version, 2);
      assert.equal(parsed.success.header.refs["refs/heads/main"], oid("a"));
      assert.deepEqual(parsed.success.header.prerequisites, [oid("b")]);
      assert.equal(bytes[parsed.success.packOffset], undefined);
    }),
  );

  it.effect("writes a v3 filter capability for blob:none", () =>
    Effect.sync(() => {
      const bytes = encodeHeader({
        version: 3,
        filter: "blob:none",
        refs: { "refs/heads/main": oid("a") },
        prerequisites: [],
      });
      const text = new TextDecoder().decode(bytes);
      assert.match(text, /^# v3 git bundle\n/);
      assert.match(text, /@filter=blob:none\n/);
      const parsed = parseHeader(bytes);
      assert.equal(Result.isSuccess(parsed) && parsed.success.header.filter === "blob:none", true);
    }),
  );
});

describe("BundleFormat lists", () => {
  const artifact = {
    id: "full-1",
    kind: "full" as const,
    filter: null,
    creationToken: 1n,
    refs: { "refs/heads/main": oid("a") },
    prerequisites: [],
    objectId: "full/1-aa.bundle",
    bytes: 10,
    checksum: "aa",
    createdAt: "2024-01-01T00:00:00.000Z",
  };
  const incremental = {
    ...artifact,
    id: "inc-2",
    kind: "incremental" as const,
    creationToken: 2n,
    objectId: "full/2-bb.bundle",
    checksum: "bb",
  };

  it.effect("keeps the full base off the catch-up list", () =>
    Effect.sync(() => {
      const published = publishArtifact(null, artifact);
      const chained = publishArtifact(published, incremental);
      const family = familyState(chained, null);
      assert.deepEqual(
        cloneArtifacts(family).map((entry) => entry.id),
        ["full-1", "inc-2"],
      );
      assert.deepEqual(
        catchupArtifacts(family).map((entry) => entry.id),
        ["inc-2"],
      );
    }),
  );

  it.effect("orders protocol and config lists by creationToken", () =>
    Effect.sync(() => {
      const entries = listEntries([incremental, artifact], (item) => `http://h/${item.objectId}`);
      assert.deepEqual(
        entries.map((entry) => entry.id),
        ["full-1", "inc-2"],
      );
      const protocol = encodeProtocolList(entries);
      assert.ok(protocol.includes("bundle.version=1"));
      assert.ok(protocol.includes("bundle.full-1.uri=http://h/full/1-aa.bundle"));
      assert.ok(protocol.includes("bundle.inc-2.creationToken=2"));
      const ini = encodeConfigList(entries);
      assert.match(ini, /\[bundle "full-1"\]/);
      assert.match(ini, /heuristic = creationToken/);
    }),
  );

  it.effect("round-trips a manifest and parses artifact paths", () =>
    Effect.sync(() => {
      const manifest = publishArtifact(null, artifact);
      const again = decodeManifest(encodeManifest(manifest));
      assert.equal(again?.families[0]?.full?.id, "full-1");
      assert.deepEqual(artifactPath("/repo/bundles/full/1-aa.bundle"), {
        family: "full",
        file: "1-aa.bundle",
      });
      assert.equal(artifactPath("/repo/bundles/clone"), null);
    }),
  );
});
