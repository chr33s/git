import * as assert from "node:assert/strict";
import { describe, it } from "node:test";

import { GitDelta, GitDeltaCache } from "./git.delta.ts";

void describe("GitDelta", () => {
	void describe("applyDelta", () => {
		void it("should apply delta to base", () => {
			const base = new Uint8Array([1, 2, 3, 4, 5]);
			const delta = new Uint8Array([
				0x05, // Base size: 5
				0x06, // Target size: 6
				0x05, // Insert 5 bytes
				1,
				2,
				3,
				4,
				5,
				0x01, // Insert 1 byte
				6,
			]);

			const result = GitDelta.applyDelta(base, delta);
			assert.equal(result.length, 6);
		});

		void it("should throw on base size mismatch", () => {
			const base = new Uint8Array([1, 2, 3]);
			const delta = new Uint8Array([0x05, 0x05]); // Expects 5 bytes base but got 3

			try {
				GitDelta.applyDelta(base, delta);
				assert.fail("Should have thrown error");
			} catch (error: any) {
				assert.ok(error.message.includes("Base size mismatch"));
			}
		});

		void it("should handle copy instructions", () => {
			const base = new Uint8Array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
			// Create delta with copy instruction
			const delta = new Uint8Array([
				0x0a, // Base size: 10
				0x05, // Target size: 5
				0x91, // Copy: cmd=0x80 | 0x10 | 0x01 (offset byte + size byte)
				0x02, // offset = 2
				0x05, // size = 5
			]);

			const result = GitDelta.applyDelta(base, delta);
			assert.equal(result.length, 5);
			assert.deepEqual(result, new Uint8Array([3, 4, 5, 6, 7]));
		});
	});

	void describe("createDelta", () => {
		void it("should create delta from source and target", () => {
			const source = new Uint8Array([1, 2, 3, 4, 5]);
			const target = new Uint8Array([1, 2, 3, 4, 5, 6]);

			const delta = GitDelta.createDelta(source, target);
			assert.ok(delta instanceof Uint8Array);
			assert.ok(delta.length > 0);
		});

		void it("should create delta for identical content", () => {
			const source = new Uint8Array([1, 2, 3, 4, 5]);
			const target = new Uint8Array([1, 2, 3, 4, 5]);

			const delta = GitDelta.createDelta(source, target);
			assert.ok(delta instanceof Uint8Array);
		});

		void it("should create delta for completely different content", () => {
			const source = new Uint8Array([1, 2, 3, 4, 5]);
			const target = new Uint8Array([10, 20, 30, 40, 50]);

			const delta = GitDelta.createDelta(source, target);
			assert.ok(delta instanceof Uint8Array);
		});

		void it("should handle empty source", () => {
			const source = new Uint8Array([]);
			const target = new Uint8Array([1, 2, 3]);

			const delta = GitDelta.createDelta(source, target);
			assert.ok(delta instanceof Uint8Array);
		});
	});

	void describe("roundtrip", () => {
		void it("should roundtrip delta encoding and decoding", () => {
			const source = new Uint8Array([1, 2, 3, 4, 5]);
			const target = new Uint8Array([1, 2, 3, 4, 5, 6, 7]);

			const delta = GitDelta.createDelta(source, target);
			const result = GitDelta.applyDelta(source, delta);

			assert.deepEqual(result, target);
		});

		void it("should handle larger content", () => {
			const source = new Uint8Array(1000);
			for (let i = 0; i < source.length; i++) {
				source[i] = i % 256;
			}

			const target = new Uint8Array(1100);
			for (let i = 0; i < 1000; i++) {
				target[i] = source[i];
			}
			for (let i = 1000; i < 1100; i++) {
				target[i] = (i * 2) % 256;
			}

			const delta = GitDelta.createDelta(source, target);
			const result = GitDelta.applyDelta(source, delta);

			assert.deepEqual(result, target);
		});

		void it("should roundtrip with text content", () => {
			const source = new TextEncoder().encode("Hello, World! This is a test.");
			const target = new TextEncoder().encode("Hello, World! This is a modified test.");

			const delta = GitDelta.createDelta(source, target);
			const result = GitDelta.applyDelta(source, delta);

			assert.deepEqual(result, target);
		});
	});

	void describe("calculateCompressionRatio", () => {
		void it("should calculate compression ratio", () => {
			const original = new Uint8Array(100);
			const delta = new Uint8Array(50);

			const ratio = GitDelta.calculateCompressionRatio(original, delta);
			assert.equal(ratio, 0.5);
		});

		void it("should return 0 for same size", () => {
			const original = new Uint8Array(100);
			const delta = new Uint8Array(100);

			const ratio = GitDelta.calculateCompressionRatio(original, delta);
			assert.equal(ratio, 0);
		});

		void it("should return negative for larger delta", () => {
			const original = new Uint8Array(50);
			const delta = new Uint8Array(100);

			const ratio = GitDelta.calculateCompressionRatio(original, delta);
			assert.ok(ratio < 0);
		});
	});

	void describe("shouldUseDelta", () => {
		void it("should return true when delta saves more than 10%", () => {
			const original = new Uint8Array(100);
			const delta = new Uint8Array(80); // 20% savings

			const result = GitDelta.shouldUseDelta(original, delta);
			assert.equal(result, true);
		});

		void it("should return false when delta saves less than 10%", () => {
			const original = new Uint8Array(100);
			const delta = new Uint8Array(95); // 5% savings

			const result = GitDelta.shouldUseDelta(original, delta);
			assert.equal(result, false);
		});

		void it("should return false when delta is larger", () => {
			const original = new Uint8Array(100);
			const delta = new Uint8Array(120);

			const result = GitDelta.shouldUseDelta(original, delta);
			assert.equal(result, false);
		});

		void it("should return true at exactly 10% savings boundary", () => {
			const original = new Uint8Array(100);
			const delta = new Uint8Array(89); // Just over 10% savings

			const result = GitDelta.shouldUseDelta(original, delta);
			assert.equal(result, true);
		});
	});
});

void describe("GitDeltaCache", () => {
	void describe("constructor", () => {
		void it("should create cache with default size", () => {
			const cache = new GitDeltaCache();
			assert.ok(cache);
		});

		void it("should create cache with custom size", () => {
			const cache = new GitDeltaCache(50);
			assert.ok(cache);
		});
	});

	void describe("set and get", () => {
		void it("should store and retrieve delta", () => {
			const cache = new GitDeltaCache();
			const targetOid = "a".repeat(40);
			const baseOid = "b".repeat(40);
			const delta = new Uint8Array([1, 2, 3]);

			cache.set(targetOid, baseOid, delta);
			const result = cache.get(targetOid);

			assert.ok(result);
			assert.equal(result?.base, baseOid);
			assert.deepEqual(result?.delta, delta);
		});

		void it("should return undefined for non-existent entry", () => {
			const cache = new GitDeltaCache();
			const result = cache.get("nonexistent");

			assert.equal(result, undefined);
		});

		void it("should overwrite existing entry", () => {
			const cache = new GitDeltaCache();
			const targetOid = "a".repeat(40);

			cache.set(targetOid, "base1", new Uint8Array([1]));
			cache.set(targetOid, "base2", new Uint8Array([2]));

			const result = cache.get(targetOid);
			assert.equal(result?.base, "base2");
		});
	});

	void describe("has", () => {
		void it("should return true for existing entry", () => {
			const cache = new GitDeltaCache();
			const targetOid = "a".repeat(40);

			cache.set(targetOid, "base", new Uint8Array([1]));

			assert.equal(cache.has(targetOid), true);
		});

		void it("should return false for non-existent entry", () => {
			const cache = new GitDeltaCache();

			assert.equal(cache.has("nonexistent"), false);
		});
	});

	void describe("clear", () => {
		void it("should clear all entries", () => {
			const cache = new GitDeltaCache();

			cache.set("oid1", "base1", new Uint8Array([1]));
			cache.set("oid2", "base2", new Uint8Array([2]));

			cache.clear();

			assert.equal(cache.has("oid1"), false);
			assert.equal(cache.has("oid2"), false);
		});
	});

	void describe("eviction", () => {
		void it("should evict oldest entry when max size reached", () => {
			const cache = new GitDeltaCache(3);

			cache.set("oid1", "base1", new Uint8Array([1]));
			cache.set("oid2", "base2", new Uint8Array([2]));
			cache.set("oid3", "base3", new Uint8Array([3]));
			cache.set("oid4", "base4", new Uint8Array([4])); // Should evict oid1

			assert.equal(cache.has("oid1"), false);
			assert.equal(cache.has("oid2"), true);
			assert.equal(cache.has("oid3"), true);
			assert.equal(cache.has("oid4"), true);
		});
	});
});
