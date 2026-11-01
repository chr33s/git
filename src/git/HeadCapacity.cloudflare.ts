import { Config, Schema } from "effect";

/** Per-repository UTF-8 HEAD target budget, independent of the SQL ref ledger. */
export const defaults = 1024;
export const configuration = Config.schema(
  Schema.Int.check(Schema.isGreaterThan(0), Schema.isLessThanOrEqualTo(64 * 1024)),
  "GIT_CF_HEAD_BYTES",
).pipe(Config.withDefault(defaults));
