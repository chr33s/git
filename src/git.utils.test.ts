import * as assert from "node:assert/strict";
import { describe, it } from "node:test";

import * as utils from "./git.utils.ts";

void describe("bytesToHex", () => {
	void it("should convert empty bytes to empty string", () => {
		const result = utils.bytesToHex(new Uint8Array([]));
		assert.strictEqual(result, "");
	});

	void it("should convert single byte to hex string", () => {
		const result = utils.bytesToHex(new Uint8Array([0xff]));
		assert.strictEqual(result, "ff");
	});

	void it("should convert multiple bytes to hex string", () => {
		const result = utils.bytesToHex(new Uint8Array([0x00, 0x0f, 0xf0, 0xff]));
		assert.strictEqual(result, "000ff0ff");
	});

	void it("should pad single digit hex values", () => {
		const result = utils.bytesToHex(new Uint8Array([0x01, 0x0a]));
		assert.strictEqual(result, "010a");
	});
});

void describe("hexToBytes", () => {
	void it("should convert empty hex string to empty bytes", () => {
		const result = utils.hexToBytes("");
		assert.strictEqual(result.length, 0);
	});

	void it("should convert hex string to bytes", () => {
		const result = utils.hexToBytes("ff00");
		assert.deepStrictEqual(result, new Uint8Array([0xff, 0x00]));
	});

	void it("should convert lowercase hex string", () => {
		const result = utils.hexToBytes("aabbccdd");
		assert.deepStrictEqual(result, new Uint8Array([0xaa, 0xbb, 0xcc, 0xdd]));
	});

	void it("should convert uppercase hex string", () => {
		const result = utils.hexToBytes("AABBCCDD");
		assert.deepStrictEqual(result, new Uint8Array([0xaa, 0xbb, 0xcc, 0xdd]));
	});

	void it("should throw on odd length hex string", () => {
		assert.throws(() => utils.hexToBytes("abc"), {
			message: "Invalid hex string length",
		});
	});
});

void describe("bytesToHex and hexToBytes round-trip", () => {
	void it("should round-trip conversion", () => {
		const original = new Uint8Array([0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0]);
		const hex = utils.bytesToHex(original);
		const result = utils.hexToBytes(hex);
		assert.deepStrictEqual(result, original);
	});
});

void describe("concatenateUint8Arrays", () => {
	void it("should concatenate empty array", () => {
		const result = utils.concatenateUint8Arrays([]);
		assert.strictEqual(result.length, 0);
	});

	void it("should concatenate single array", () => {
		const arr = new Uint8Array([1, 2, 3]);
		const result = utils.concatenateUint8Arrays([arr]);
		assert.deepStrictEqual(result, arr);
	});

	void it("should concatenate multiple arrays", () => {
		const arr1 = new Uint8Array([1, 2]);
		const arr2 = new Uint8Array([3, 4]);
		const arr3 = new Uint8Array([5, 6]);
		const result = utils.concatenateUint8Arrays([arr1, arr2, arr3]);
		assert.deepStrictEqual(result, new Uint8Array([1, 2, 3, 4, 5, 6]));
	});

	void it("should handle arrays of different sizes", () => {
		const arr1 = new Uint8Array([1]);
		const arr2 = new Uint8Array([2, 3, 4]);
		const arr3 = new Uint8Array([5]);
		const result = utils.concatenateUint8Arrays([arr1, arr2, arr3]);
		assert.deepStrictEqual(result, new Uint8Array([1, 2, 3, 4, 5]));
	});
});

void describe("createSha1", () => {
	void it("should create SHA1 hash of empty data", async () => {
		const result = await utils.createSha1(new Uint8Array([]));
		assert.strictEqual(result, "da39a3ee5e6b4b0d3255bfef95601890afd80709");
	});

	void it("should create SHA1 hash of data", async () => {
		const data = new TextEncoder().encode("hello");
		const result = await utils.createSha1(data);
		assert.strictEqual(result, "aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d");
	});

	void it("should create consistent SHA1 hash", async () => {
		const data = new TextEncoder().encode("test data");
		const result1 = await utils.createSha1(data);
		const result2 = await utils.createSha1(data);
		assert.strictEqual(result1, result2);
	});

	void it("should create different hash for different data", async () => {
		const result1 = await utils.createSha1(new TextEncoder().encode("test1"));
		const result2 = await utils.createSha1(new TextEncoder().encode("test2"));
		assert.notStrictEqual(result1, result2);
	});
});

void describe("compressData and decompressData", () => {
	void it("should compress and decompress empty data", async () => {
		const data = new Uint8Array([1, 2, 3]);
		const compressed = await utils.compressData(data);
		const decompressed = await utils.decompressData(compressed);
		assert.deepStrictEqual(decompressed, data);
	});

	void it("should compress and decompress data", async () => {
		const data = new TextEncoder().encode("hello world");
		const compressed = await utils.compressData(data);
		const decompressed = await utils.decompressData(compressed);
		assert.deepStrictEqual(decompressed, data);
	});

	void it("should compress data to smaller size", async () => {
		const data = new TextEncoder().encode("a".repeat(100));
		const compressed = await utils.compressData(data);
		assert.ok(compressed.length < data.length);
	});

	void it("should decompress compressed data correctly", async () => {
		const original = new TextEncoder().encode("test data for compression");
		const compressed = await utils.compressData(original);
		const decompressed = await utils.decompressData(compressed);
		assert.deepStrictEqual(decompressed, original);
	});
});

void describe("createStreamFromData", () => {
	void it("should create readable stream from data", async () => {
		const data = new Uint8Array([1, 2, 3]);
		const stream = utils.createStreamFromData(data);
		const reader = stream.getReader();
		const { value, done } = await reader.read();
		assert.deepStrictEqual(value, data);
		assert.strictEqual(done, false);
		const { done: done2 } = await reader.read();
		assert.strictEqual(done2, true);
	});
});

void describe("collectStream", () => {
	void it("should collect from null stream", async () => {
		const result = await utils.collectStream(null);
		assert.deepStrictEqual(result, new Uint8Array(0));
	});

	void it("should collect data from stream", async () => {
		const data = new Uint8Array([1, 2, 3, 4, 5]);
		const stream = utils.createStreamFromData(data);
		const result = await utils.collectStream(stream);
		assert.deepStrictEqual(result, data);
	});
});

void describe("deflateData and deflateStream", () => {
	void it("should deflate data", async () => {
		const data = new TextEncoder().encode("test data");
		const deflated = await utils.deflateData(data);
		assert.ok(deflated.length > 0);
	});

	void it("should deflate stream", async () => {
		const data = new TextEncoder().encode("test stream data");
		const stream = utils.createStreamFromData(data);
		const deflatedStream = utils.deflateStream(stream);
		const result = await utils.collectStream(deflatedStream);
		assert.ok(result.length > 0);
	});
});

void describe("readVarInt", () => {
	void it("should read single byte varint", () => {
		const data = new Uint8Array([0x7f]);
		const result = utils.readVarInt(data, 0);
		assert.deepStrictEqual(result, { value: 0x7f, bytesRead: 1 });
	});

	void it("should read multi-byte varint", () => {
		const data = new Uint8Array([0x80, 0x01]);
		const result = utils.readVarInt(data, 0);
		assert.deepStrictEqual(result, { value: 128, bytesRead: 2 });
	});

	void it("should read varint from offset", () => {
		const data = new Uint8Array([0xff, 0xff, 0x7f]);
		const result = utils.readVarInt(data, 2);
		assert.deepStrictEqual(result, { value: 0x7f, bytesRead: 1 });
	});

	void it("should throw on incomplete varint", () => {
		const data = new Uint8Array([0x80]);
		assert.throws(() => utils.readVarInt(data, 0), {
			message: "Incomplete varint",
		});
	});

	void it("should read zero", () => {
		const data = new Uint8Array([0x00]);
		const result = utils.readVarInt(data, 0);
		assert.deepStrictEqual(result, { value: 0, bytesRead: 1 });
	});
});

void describe("writeVarInt", () => {
	void it("should write single byte varint", () => {
		const result = utils.writeVarInt(0x7f);
		assert.deepStrictEqual(result, new Uint8Array([0x7f]));
	});

	void it("should write multi-byte varint", () => {
		const result = utils.writeVarInt(128);
		assert.deepStrictEqual(result, new Uint8Array([0x80, 0x01]));
	});

	void it("should write zero", () => {
		const result = utils.writeVarInt(0);
		assert.deepStrictEqual(result, new Uint8Array([0x00]));
	});

	void it("should write large value", () => {
		const result = utils.writeVarInt(16384);
		assert.ok(result.length > 2);
	});
});

void describe("readVarInt and writeVarInt round-trip", () => {
	void it("should round-trip varint", () => {
		const values = [0, 1, 127, 128, 255, 256, 16384, 2097151];
		for (const value of values) {
			const written = utils.writeVarInt(value);
			const read = utils.readVarInt(written, 0);
			assert.strictEqual(read.value, value);
		}
	});
});

void describe("isValidSha", () => {
	void it("should accept valid SHA", () => {
		assert.strictEqual(utils.isValidSha("da39a3ee5e6b4b0d3255bfef95601890afd80709"), true);
	});

	void it("should accept uppercase SHA", () => {
		assert.strictEqual(utils.isValidSha("DA39A3EE5E6B4B0D3255BFEF95601890AFD80709"), true);
	});

	void it("should accept mixed case SHA", () => {
		assert.strictEqual(utils.isValidSha("Da39A3Ee5e6B4b0D3255bFeF95601890AfD80709"), true);
	});

	void it("should reject short SHA", () => {
		assert.strictEqual(utils.isValidSha("da39a3ee5e6b4b0d3255bfef95601890afd8070"), false);
	});

	void it("should reject long SHA", () => {
		assert.strictEqual(utils.isValidSha("da39a3ee5e6b4b0d3255bfef95601890afd807090"), false);
	});

	void it("should reject non-hex SHA", () => {
		assert.strictEqual(utils.isValidSha("ga39a3ee5e6b4b0d3255bfef95601890afd80709"), false);
	});

	void it("should reject empty SHA", () => {
		assert.strictEqual(utils.isValidSha(""), false);
	});
});

void describe("applyDelta", () => {
	void it("should apply empty delta", () => {
		const base = new Uint8Array([1, 2, 3]);
		const delta = new Uint8Array([
			0x03, // base size
			0x00, // target size (empty result)
		]);
		const result = utils.applyDelta(base, delta);
		assert.deepStrictEqual(result, new Uint8Array(0));
	});

	void it("should insert data", () => {
		const base = new Uint8Array([1, 2, 3]);
		const delta = new Uint8Array([
			0x03, // base size
			0x04, // target size
			0x04, // insert instruction: insert 4 bytes
			0x0a,
			0x0b,
			0x0c,
			0x0d,
		]);
		const result = utils.applyDelta(base, delta);
		assert.deepStrictEqual(result, new Uint8Array([0x0a, 0x0b, 0x0c, 0x0d]));
	});

	void it("should throw on base size mismatch", () => {
		const base = new Uint8Array([1, 2, 3]);
		const delta = new Uint8Array([0x05, 0x03]); // base size says 5, but base is 3
		assert.throws(() => utils.applyDelta(base, delta), {
			message: /Base size mismatch/,
		});
	});

	void it("should throw on result size mismatch", () => {
		const base = new Uint8Array([1, 2, 3]);
		const delta = new Uint8Array([
			0x03, // base size
			0x05, // target size (says 5)
			0x03, // insert 3 bytes
			0x0a,
			0x0b,
			0x0c,
			// only inserted 3, but target size says 5
		]);
		assert.throws(() => utils.applyDelta(base, delta), {
			message: /Result size mismatch/,
		});
	});
});
