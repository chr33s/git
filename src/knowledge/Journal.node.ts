/** Private operational artifacts. They are never a source of knowledge authority. */
import * as fs from "node:fs";
import * as path from "node:path";
import { randomUUID } from "node:crypto";
import { Effect, Schema } from "effect";
import { Invalid, StorageFailure } from "../git/Error.ts";
import * as Maintenance from "./Maintenance.ts";
import * as Outcomes from "./Outcomes.ts";
import * as Assistance from "./Assistance.ts";
import * as LocalLease from "../host/LocalLease.node.ts";

export const Request = Schema.Struct({
  version: Schema.Literal(1),
  id: Schema.String,
  repository: Schema.String,
  target: Schema.String,
  trigger: Schema.Literals(["manual", "wake"]),
  event: Schema.NullOr(Schema.String),
  ref: Schema.NullOr(Schema.String),
  commit: Schema.NullOr(Schema.String),
  requestedAt: Schema.String,
});
export interface Request extends Schema.Schema.Type<typeof Request> {}
export const Limits = Schema.Struct({
  concepts: Schema.Int,
  outcomes: Schema.Int,
  timeoutMs: Schema.Int,
  evidenceBytes: Schema.Int,
});
export const Job = Schema.Struct({
  version: Schema.Literal(1),
  id: Schema.String,
  state: Schema.Literals([
    "requested",
    "running",
    "reported",
    "partial",
    "retryable-failure",
    "proposed",
  ]),
  plan: Maintenance.Plan,
  limits: Limits,
  requests: Schema.Array(Schema.String),
  cursor: Schema.Int,
  results: Schema.Array(Schema.Json),
  pendingOutcomes: Schema.Array(Schema.String),
  outcomes: Schema.Array(Outcomes.Report),
  actors: Schema.Array(Outcomes.Actor),
  assistance: Assistance.State,
  operatorDigest: Schema.String,
  candidate: Schema.NullOr(Schema.String),
  createdAt: Schema.String,
  updatedAt: Schema.String,
  elapsedMs: Schema.Finite,
  retry: Schema.NullOr(Schema.String),
  publication: Schema.Literals([
    "not-requested",
    "pending",
    "no-proposal",
    "superseded-input",
    "written",
    "unchanged",
    "conflict",
  ]),
});
export type Job = typeof Job.Type;
export const History = Schema.Struct({
  version: Schema.Literal(1),
  round: Schema.Int,
  entries: Schema.Array(Maintenance.History),
});

export const OutcomeHistory = Schema.Struct({
  version: Schema.Literal(1),
  entries: Schema.Array(
    Schema.Struct({
      ref: Schema.String,
      key: Schema.String,
      checkedAt: Schema.String,
      evidenceHead: Schema.String,
    }),
  ),
});

const file = <A>(operation: string, location: string, thunk: () => A) =>
  Effect.try({
    try: thunk,
    catch: (cause) => new StorageFailure({ operation, path: location, cause }),
  });
export const directory = (repository: string) => path.join(repository, "maintenance");
export const initialize = Effect.fn("knowledge.Journal.initialize")(function* (location: string) {
  yield* file("maintenance.initialize", location, () => {
    fs.mkdirSync(location, { recursive: true, mode: 0o700 });
    fs.chmodSync(location, 0o700);
    fs.mkdirSync(path.join(location, "requests"), { recursive: true, mode: 0o700 });
    fs.mkdirSync(path.join(location, "jobs"), { recursive: true, mode: 0o700 });
    fs.mkdirSync(path.join(location, "models"), { recursive: true, mode: 0o700 });
    fs.mkdirSync(path.join(location, "proposals"), { recursive: true, mode: 0o700 });
  });
});

export const read = Effect.fn("knowledge.Journal.read")(function* <S extends Schema.Top>(
  location: string,
  schema: S,
) {
  const text = yield* file("maintenance.read", location, () => {
    if (!fs.existsSync(location)) return null;
    if (fs.statSync(location).size > 16 * 1024 * 1024)
      throw new Error("operational artifact exceeds 16 MiB");
    return fs.readFileSync(location, "utf8");
  });
  if (text === null) return null;
  return yield* Schema.decodeEffect(Schema.fromJsonString(schema))(text).pipe(
    Effect.mapError(
      (cause) =>
        new Invalid({
          field: "maintenance.journal",
          reason: `invalid or unsupported artifact ${location}: ${cause.message}`,
        }),
    ),
  );
});

export const write = Effect.fn("knowledge.Journal.write")(function* (
  location: string,
  value: Schema.Json,
) {
  yield* file("maintenance.write", location, () => {
    const temporary = `${location}.${randomUUID()}.tmp`;
    try {
      const descriptor = fs.openSync(temporary, "wx", 0o600);
      try {
        fs.writeFileSync(descriptor, JSON.stringify(value));
        fs.fsyncSync(descriptor);
      } finally {
        fs.closeSync(descriptor);
      }
      fs.renameSync(temporary, location);
      const parent = fs.openSync(path.dirname(location), "r");
      try {
        fs.fsyncSync(parent);
      } finally {
        fs.closeSync(parent);
      }
    } finally {
      fs.rmSync(temporary, { force: true });
    }
  });
});

export const names = Effect.fn("knowledge.Journal.names")(function* (location: string) {
  return yield* file("maintenance.list", location, () =>
    fs.existsSync(location)
      ? fs
          .readdirSync(location)
          .filter((name) => /^[0-9a-f]{64}\.json$/u.test(name))
          .sort()
      : [],
  );
});
export const acknowledge = Effect.fn("knowledge.Journal.acknowledge")(function* (
  location: string,
  ids: ReadonlyArray<string>,
) {
  yield* file("maintenance.acknowledge", location, () => {
    for (const id of ids) {
      if (!/^[0-9a-f]{64}$/u.test(id)) throw new Error("invalid request identity");
      fs.rmSync(path.join(location, "requests", `${id}.json`), { force: true });
    }
    const descriptor = fs.openSync(path.join(location, "requests"), "r");
    try {
      fs.fsyncSync(descriptor);
    } finally {
      fs.closeSync(descriptor);
    }
  });
});

/** Separate from the wake cursor lock; the OS releases exclusion on process death. */
export const withLock = Effect.fn("knowledge.Journal.withLock")(function* <A, E, R>(
  location: string,
  effect: Effect.Effect<A, E, R>,
) {
  return yield* LocalLease.withLock(path.join(location, "worker.lock"), effect);
});
