/**
 * The slice of SQLite these modules need, and nothing more.
 *
 * Row values are `unknown`-shaped rather than workerd's `SqlStorageValue`, so
 * a Durable Object's `storage.sql` and `node:sqlite` both satisfy it
 * structurally — which is what lets the same statements be exercised outside
 * the runtime that will run them.
 */
export interface Sql {
  exec<Row extends Record<string, ArrayBuffer | string | number | null>>(
    query: string,
    ...bindings: ReadonlyArray<string | number | null>
  ): { toArray(): Row[] };
}
