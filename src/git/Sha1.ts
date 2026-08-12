/**
 * Streaming SHA-1.
 *
 * Web Crypto only digests complete buffers, and the pack trailer is a hash
 * over a stream that must not be buffered — so the one algorithm git's wire
 * format needs incrementally is written out here. Platform-neutral by
 * construction: no imports at all.
 */
export class Sha1 {
  #h0 = 0x67452301;
  #h1 = 0xefcdab89;
  #h2 = 0x98badcfe;
  #h3 = 0x10325476;
  #h4 = 0xc3d2e1f0;
  readonly #block = new Uint8Array(64);
  readonly #words = new Int32Array(80);
  #blockLength = 0;
  #total = 0;

  update(bytes: Uint8Array): this {
    this.#total += bytes.length;
    let offset = 0;
    while (offset < bytes.length) {
      const take = Math.min(64 - this.#blockLength, bytes.length - offset);
      this.#block.set(bytes.subarray(offset, offset + take), this.#blockLength);
      this.#blockLength += take;
      offset += take;
      if (this.#blockLength === 64) {
        this.#compress();
        this.#blockLength = 0;
      }
    }
    return this;
  }

  /** The 20-byte digest; the instance is spent afterwards. */
  digest(): Uint8Array {
    const total = this.#total;
    // 0x80, zeros to 56 mod 64, then the bit length as a 64-bit big-endian —
    // split as hi = total/2^29 and lo = (total mod 2^29)*8 to stay exact.
    const padLength = (((56 - ((total + 1) % 64)) % 64) + 64) % 64;
    const tail = new Uint8Array(1 + padLength + 8);
    tail[0] = 0x80;
    const hi = Math.floor(total / 2 ** 29);
    const lo = (total % 2 ** 29) * 8;
    const tailView = new DataView(tail.buffer);
    tailView.setUint32(tail.length - 8, hi);
    tailView.setUint32(tail.length - 4, lo);
    this.update(tail);

    const out = new Uint8Array(20);
    const view = new DataView(out.buffer);
    view.setUint32(0, this.#h0 >>> 0);
    view.setUint32(4, this.#h1 >>> 0);
    view.setUint32(8, this.#h2 >>> 0);
    view.setUint32(12, this.#h3 >>> 0);
    view.setUint32(16, this.#h4 >>> 0);
    return out;
  }

  digestHex(): string {
    return [...this.digest()].map((byte) => byte.toString(16).padStart(2, "0")).join("");
  }

  #compress(): void {
    const words = this.#words;
    const block = this.#block;
    for (let index = 0; index < 16; index++) {
      words[index] =
        (block[index * 4]! << 24) |
        (block[index * 4 + 1]! << 16) |
        (block[index * 4 + 2]! << 8) |
        block[index * 4 + 3]!;
    }
    for (let index = 16; index < 80; index++) {
      const word = words[index - 3]! ^ words[index - 8]! ^ words[index - 14]! ^ words[index - 16]!;
      words[index] = (word << 1) | (word >>> 31);
    }

    let a = this.#h0;
    let b = this.#h1;
    let c = this.#h2;
    let d = this.#h3;
    let e = this.#h4;

    for (let index = 0; index < 80; index++) {
      let f: number;
      let k: number;
      if (index < 20) {
        f = (b & c) | (~b & d);
        k = 0x5a827999;
      } else if (index < 40) {
        f = b ^ c ^ d;
        k = 0x6ed9eba1;
      } else if (index < 60) {
        f = (b & c) | (b & d) | (c & d);
        k = 0x8f1bbcdc;
      } else {
        f = b ^ c ^ d;
        k = 0xca62c1d6;
      }
      const temporary = (((a << 5) | (a >>> 27)) + f + e + k + words[index]!) | 0;
      e = d;
      d = c;
      c = (b << 30) | (b >>> 2);
      b = a;
      a = temporary;
    }

    this.#h0 = (this.#h0 + a) | 0;
    this.#h1 = (this.#h1 + b) | 0;
    this.#h2 = (this.#h2 + c) | 0;
    this.#h3 = (this.#h3 + d) | 0;
    this.#h4 = (this.#h4 + e) | 0;
  }
}
