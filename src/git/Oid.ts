/** A 40-character lowercase hexadecimal git object id. */
export type Oid = string & { readonly Oid: unique symbol };

/** The one boundary that earns the `Oid` brand. */
export const isOid = (value: string): value is Oid => /^[0-9a-f]{40}$/.test(value);
