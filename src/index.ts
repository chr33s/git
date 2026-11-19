/**
 * `@chr33s/git` — the Git wire protocol as a library.
 *
 * What the app's server, browser client and CLI are built from, and nothing
 * of the app itself: hashing, object and pack codecs, pkt-line framing, the
 * storage ports, typed errors, and a receive-pack client that needs no
 * repository (`Wire.pushObjects`). Portable — no `node:*` — and the only
 * dependency is `effect`, as a peer.
 *
 * Each namespace is also its own entry point (`@chr33s/git/Pkt`), and the
 * package is side-effect free, so a bundler keeps only what is imported.
 */
export * as Authorize from "./client/Authorize.ts";
export * as Errors from "./git/Error.ts";
export * as Format from "./git/Format.ts";
export * as Inflate from "./git/Inflate.ts";
export * as Oid from "./git/Oid.ts";
export * as Pack from "./git/Pack.ts";
export * as PackFile from "./git/PackFile.ts";
export * as PackIndex from "./git/PackIndex.ts";
export * as Packed from "./git/Packed.ts";
export * as Pkt from "./git/Pkt.ts";
export * as Refspec from "./git/Refspec.ts";
export * as Sha1 from "./git/Sha1.ts";
export * as Store from "./git/Store.ts";
export * as Transport from "./client/Transport.ts";
export * as Wire from "./client/Wire.ts";
