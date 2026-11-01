/** Local recovery fixture for the optional Artifacts namespace's real SQL services. */
import { Effect, Schema } from "effect";
import { Registry, Tokens } from "../artifacts/Namespace.ts";
import { sqlite } from "../artifacts/Sqlite.ts";

export const Credentials = Schema.Struct({
  read: Schema.String,
  write: Schema.String,
  revoked: Schema.String,
  expired: Schema.String,
});
export interface Credentials extends Schema.Schema.Type<typeof Credentials> {}
const services = (storage: DurableObjectStorage) =>
  sqlite({
    exec: storage.sql.exec.bind(storage.sql),
    transactionSync: storage.transactionSync.bind(storage),
  });

export const seedArtifacts = Effect.fn("BackupFixture.seedArtifacts")(function* (
  storage: DurableObjectStorage,
) {
  return yield* Effect.gen(function* () {
    const registry = yield* Registry;
    const tokens = yield* Tokens;
    yield* registry.create("private-repository", {
      description: "fixture private repository",
      defaultBranch: "main",
      readOnly: false,
      source: null,
    });
    yield* registry.create("pending-fork", {
      description: null,
      defaultBranch: "main",
      readOnly: true,
      source: "artifacts:fixture/private-repository",
      initializing: "fork",
    });
    const read = yield* tokens.issue("private-repository", "read", 3600);
    const write = yield* tokens.issue("private-repository", "write", 3600);
    const revoked = yield* tokens.issue("private-repository", "write", 3600);
    const expired = yield* tokens.issue("private-repository", "write", 3600);
    yield* tokens.revoke("private-repository", revoked.id);
    storage.sql.exec(
      "UPDATE tokens SET created_at = '2000-01-01T00:00:00.000Z', expires_at = '2000-01-02T00:00:00.000Z' WHERE id = ?",
      expired.id,
    );
    return {
      read: read.plaintext,
      write: write.plaintext,
      revoked: revoked.plaintext,
      expired: expired.plaintext,
    };
  }).pipe(Effect.provide(services(storage)));
});

export const inspectArtifacts = Effect.fn("BackupFixture.inspectArtifacts")(function* (
  storage: DurableObjectStorage,
  credentials: Credentials,
  mutate: boolean,
) {
  return yield* Effect.gen(function* () {
    const registry = yield* Registry;
    const tokens = yield* Tokens;
    if (mutate) yield* tokens.revoke("private-repository", credentials.write);
    return {
      repositories: yield* registry.list(),
      tokens: yield* tokens.list("private-repository"),
      read: yield* tokens.verify("private-repository", credentials.read),
      write: yield* tokens.verify("private-repository", credentials.write),
      revoked: yield* tokens.verify("private-repository", credentials.revoked),
      expired: yield* tokens.verify("private-repository", credentials.expired),
      wrongRepository: yield* tokens.verify("pending-fork", credentials.write),
    };
  }).pipe(Effect.provide(services(storage)));
});
