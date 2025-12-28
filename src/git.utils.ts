export function bytesToHex(bytes: Uint8Array): string {
	return Array.from(bytes)
		.map((b) => b.toString(16).padStart(2, "0"))
		.join("");
}

export function hexToBytes(hex: string): Uint8Array {
	if (hex.length % 2 !== 0) {
		throw new Error("Invalid hex string length");
	}
	const bytes = new Uint8Array(hex.length / 2);
	for (let i = 0; i < hex.length; i += 2) {
		bytes[i / 2] = parseInt(hex.substring(i, i + 2), 16);
	}
	return bytes;
}

export function concatenateUint8Arrays(arrays: Uint8Array[]): Uint8Array {
	const totalLength = arrays.reduce((sum, arr) => sum + arr.length, 0);
	const result = new Uint8Array(totalLength);
	let offset = 0;

	for (const arr of arrays) {
		result.set(arr, offset);
		offset += arr.length;
	}

	return result;
}

export async function createSha1(data: Uint8Array): Promise<string> {
	const buffer =
		data.buffer instanceof ArrayBuffer
			? data.buffer.slice(data.byteOffset, data.byteOffset + data.byteLength)
			: new ArrayBuffer(data.length);
	if (!(data.buffer instanceof ArrayBuffer)) {
		new Uint8Array(buffer).set(data);
	}
	const hashBuffer = await crypto.subtle.digest("SHA-1", buffer);
	return bytesToHex(new Uint8Array(hashBuffer));
}

export async function compressData(data: Uint8Array): Promise<Uint8Array> {
	const cs = new CompressionStream("deflate");
	const writer = cs.writable.getWriter();

	const buffer =
		data.buffer instanceof ArrayBuffer
			? data.buffer.slice(data.byteOffset, data.byteOffset + data.byteLength)
			: new ArrayBuffer(data.length);
	if (!(data.buffer instanceof ArrayBuffer)) {
		new Uint8Array(buffer).set(data);
	}

	await writer.write(new Uint8Array(buffer));
	await writer.close();

	const reader = cs.readable.getReader();
	const chunks: Uint8Array[] = [];

	while (true) {
		const { done, value } = await reader.read();
		if (done) break;
		chunks.push(value);
	}

	return concatenateUint8Arrays(chunks);
}

export async function decompressData(compressed: Uint8Array): Promise<Uint8Array> {
	const ds = new DecompressionStream("deflate");
	const writer = ds.writable.getWriter();

	const buffer =
		compressed.buffer instanceof ArrayBuffer
			? compressed.buffer.slice(
					compressed.byteOffset,
					compressed.byteOffset + compressed.byteLength,
				)
			: new ArrayBuffer(compressed.length);
	if (!(compressed.buffer instanceof ArrayBuffer)) {
		new Uint8Array(buffer).set(compressed);
	}

	await writer.write(new Uint8Array(buffer));
	await writer.close();

	const reader = ds.readable.getReader();
	const chunks: Uint8Array[] = [];

	while (true) {
		const { done, value } = await reader.read();
		if (done) break;
		chunks.push(value);
	}

	return concatenateUint8Arrays(chunks);
}

export function createStreamFromData(data: Uint8Array): ReadableStream<Uint8Array> {
	return new ReadableStream<Uint8Array>({
		start(controller) {
			controller.enqueue(data);
			controller.close();
		},
	});
}

export async function collectStream(body: ReadableStream<Uint8Array> | null): Promise<Uint8Array> {
	if (!body) {
		return new Uint8Array(0);
	}
	const chunks: Uint8Array[] = [];
	await body.pipeTo(
		new WritableStream({
			write(chunk) {
				chunks.push(chunk);
			},
		}),
	);
	return concatenateUint8Arrays(chunks);
}

export async function deflateData(data: Uint8Array): Promise<Uint8Array> {
	const rs = createStreamFromData(data);
	const ds = deflateStream(rs);
	return collectStream(ds);
}

export function deflateStream(stream: ReadableStream<Uint8Array>): ReadableStream<Uint8Array> {
	const cs = new CompressionStream("deflate");
	return (stream as any).pipeThrough(cs) as ReadableStream<Uint8Array>;
}

export function readVarInt(data: Uint8Array, offset: number): { value: number; bytesRead: number } {
	let value = 0;
	let shift = 0;
	let bytesRead = 0;

	while (offset + bytesRead < data.length) {
		const byte = data[offset + bytesRead];
		if (byte === undefined) break;
		bytesRead++;
		value |= (byte & 0x7f) << shift;

		if (!(byte & 0x80)) {
			return { value, bytesRead };
		}

		shift += 7;
	}

	throw new Error("Incomplete varint");
}

export function writeVarInt(value: number): Uint8Array {
	const bytes: number[] = [];

	while (value >= 0x80) {
		bytes.push((value & 0x7f) | 0x80);
		value >>= 7;
	}
	bytes.push(value & 0x7f);

	return new Uint8Array(bytes);
}

export function isValidSha(sha: string): boolean {
	return /^[0-9a-f]{40}$/i.test(sha);
}

export function applyDelta(base: Uint8Array, delta: Uint8Array): Uint8Array {
	let deltaOffset = 0;

	// Read base object size from delta
	const baseSize = readVarInt(delta, deltaOffset);
	deltaOffset += baseSize.bytesRead;

	if (baseSize.value !== base.length) {
		throw new Error(`Base size mismatch: expected ${baseSize.value}, got ${base.length}`);
	}

	// Read target object size from delta
	const targetSize = readVarInt(delta, deltaOffset);
	deltaOffset += targetSize.bytesRead;

	const result = new Uint8Array(targetSize.value);
	let resultOffset = 0;

	// Process delta instructions
	while (deltaOffset < delta.length) {
		const cmd = delta[deltaOffset++];
		if (cmd === undefined) break;

		if (cmd & 0x80) {
			// Copy instruction
			let copyOffset = 0;
			let copySize = 0;

			if (cmd & 0x01) copyOffset |= delta[deltaOffset++] || 0;
			if (cmd & 0x02) copyOffset |= (delta[deltaOffset++] || 0) << 8;
			if (cmd & 0x04) copyOffset |= (delta[deltaOffset++] || 0) << 16;
			if (cmd & 0x08) copyOffset |= (delta[deltaOffset++] || 0) << 24;

			if (cmd & 0x10) copySize |= delta[deltaOffset++] || 0;
			if (cmd & 0x20) copySize |= (delta[deltaOffset++] || 0) << 8;
			if (cmd & 0x40) copySize |= (delta[deltaOffset++] || 0) << 16;

			if (copySize === 0) copySize = 0x10000;

			// Copy from base to result
			result.set(base.slice(copyOffset, copyOffset + copySize), resultOffset);
			resultOffset += copySize;
		} else if (cmd > 0) {
			// Insert instruction
			const insertSize = cmd;
			result.set(delta.slice(deltaOffset, deltaOffset + insertSize), resultOffset);
			deltaOffset += insertSize;
			resultOffset += insertSize;
		} else {
			throw new Error("Invalid delta instruction");
		}
	}

	if (resultOffset !== targetSize.value) {
		throw new Error(`Result size mismatch: expected ${targetSize.value}, got ${resultOffset}`);
	}

	return result;
}
