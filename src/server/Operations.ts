/**
 * Observable host operations — not hub Tasks.
 *
 * An Operation wraps work another subsystem already knows how to run. It is
 * not a durable queue: if the process disappears, desired-state maintenance
 * decides whether the work still needs doing and starts a new operation.
 */
import { Context, Effect, Layer } from "effect";

import { Invalid, OperationNotFound } from "../git/Error.ts";

export type OperationState = "queued" | "running" | "succeeded" | "failed" | "cancelled";
export type OperationLevel = "debug" | "info" | "warning" | "error";

export interface OperationProgress {
  readonly current?: number;
  readonly total?: number;
  readonly unit?: string;
}

export interface OperationError {
  readonly tag: string;
  readonly message: string;
}

export interface Operation {
  readonly id: string;
  readonly repo: string;
  readonly kind: string;
  readonly state: OperationState;
  readonly createdAt: string;
  readonly startedAt?: string;
  readonly finishedAt?: string;
  readonly progress?: OperationProgress;
  readonly message?: string;
  readonly error?: OperationError;
  readonly cancellable: boolean;
}

export interface OperationEvent {
  readonly sequence: number;
  readonly at: string;
  readonly level: OperationLevel;
  readonly message: string;
  readonly progress?: OperationProgress;
}

export type OperationSse =
  | { readonly type: "progress"; readonly event: OperationEvent }
  | { readonly type: "message"; readonly event: OperationEvent }
  | { readonly type: "completed"; readonly operation: Operation }
  | { readonly type: "failed"; readonly operation: Operation };

export interface OperationHandle {
  readonly id: string;
  readonly progress: (progress: OperationProgress, message?: string) => Effect.Effect<void>;
  readonly info: (message: string) => Effect.Effect<void>;
  readonly warn: (message: string) => Effect.Effect<void>;
  /** Mark the commit point: cancellation may stop follow-up work but not undo. */
  readonly commit: Effect.Effect<void>;
}

const nowIso = () => new Date().toISOString();

const nextId = (): string => crypto.randomUUID();

interface Recorded {
  operation: Operation;
  events: OperationEvent[];
  committed: boolean;
  watchers: Set<(event: OperationSse) => void>;
}

const HISTORY_BOUND = 64;

export class Operations extends Context.Service<
  Operations,
  {
    readonly create: (input: {
      readonly repo: string;
      readonly kind: string;
      readonly cancellable?: boolean;
    }) => Effect.Effect<Operation>;
    readonly get: (id: string) => Effect.Effect<Operation, OperationNotFound>;
    readonly list: (query?: {
      readonly repo?: string;
      readonly state?: OperationState;
    }) => Effect.Effect<ReadonlyArray<Operation>>;
    readonly history: (
      id: string,
    ) => Effect.Effect<ReadonlyArray<OperationEvent>, OperationNotFound>;
    readonly start: (id: string) => Effect.Effect<void, OperationNotFound>;
    readonly progress: (
      id: string,
      progress: OperationProgress,
      message?: string,
    ) => Effect.Effect<void, OperationNotFound>;
    readonly message: (
      id: string,
      level: OperationLevel,
      message: string,
    ) => Effect.Effect<void, OperationNotFound>;
    readonly succeed: (id: string) => Effect.Effect<Operation, OperationNotFound>;
    readonly fail: (
      id: string,
      error: OperationError,
    ) => Effect.Effect<Operation, OperationNotFound>;
    readonly cancel: (id: string) => Effect.Effect<Operation, OperationNotFound | Invalid>;
    readonly markCommitted: (id: string) => Effect.Effect<void, OperationNotFound>;
    readonly watch: (
      id: string,
      listener: (event: OperationSse) => void,
    ) => Effect.Effect<() => void, OperationNotFound>;
  }
>()("server/Operations") {}

const terminal = (state: OperationState): boolean =>
  state === "succeeded" || state === "failed" || state === "cancelled";

export const memory = (): Operations["Service"] => {
  const records = new Map<string, Recorded>();

  const require = (id: string): Effect.Effect<Recorded, OperationNotFound> => {
    const recorded = records.get(id);
    return recorded === undefined
      ? Effect.fail(new OperationNotFound({ id }))
      : Effect.succeed(recorded);
  };

  const emit = (recorded: Recorded, event: OperationSse) => {
    for (const listener of recorded.watchers) listener(event);
  };

  const append = (
    recorded: Recorded,
    level: OperationLevel,
    message: string,
    progress?: OperationProgress,
  ): OperationEvent => {
    const event: OperationEvent =
      progress === undefined
        ? { sequence: recorded.events.length + 1, at: nowIso(), level, message }
        : { sequence: recorded.events.length + 1, at: nowIso(), level, message, progress };
    recorded.events.push(event);
    if (recorded.operation.state !== "queued" && recorded.operation.state !== "running") {
      if (recorded.events.length > HISTORY_BOUND) {
        recorded.events = recorded.events.slice(-HISTORY_BOUND);
      }
    }
    if (progress !== undefined) {
      recorded.operation = { ...recorded.operation, progress, message };
      emit(recorded, { type: "progress", event });
    } else {
      recorded.operation = { ...recorded.operation, message };
      emit(recorded, { type: "message", event });
    }
    return event;
  };

  return Operations.of({
    create: (input) =>
      Effect.sync(() => {
        const operation: Operation = {
          id: nextId(),
          repo: input.repo,
          kind: input.kind,
          state: "queued",
          createdAt: nowIso(),
          cancellable: input.cancellable !== false,
        };
        records.set(operation.id, {
          operation,
          events: [],
          committed: false,
          watchers: new Set(),
        });
        return operation;
      }),

    get: (id) => require(id).pipe(Effect.map((recorded) => recorded.operation)),

    list: (query) =>
      Effect.sync(() =>
        [...records.values()]
          .map((recorded) => recorded.operation)
          .filter((operation) => {
            if (query?.repo !== undefined && operation.repo !== query.repo) return false;
            if (query?.state !== undefined && operation.state !== query.state) return false;
            return true;
          })
          .sort((left, right) => right.createdAt.localeCompare(left.createdAt)),
      ),

    history: (id) => require(id).pipe(Effect.map((recorded) => recorded.events)),

    start: (id) =>
      require(id).pipe(
        Effect.map((recorded) => {
          if (recorded.operation.state !== "queued") return;
          recorded.operation = {
            ...recorded.operation,
            state: "running",
            startedAt: nowIso(),
          };
          append(recorded, "info", "started");
        }),
      ),

    progress: (id, progress, message) =>
      require(id).pipe(
        Effect.map((recorded) => {
          append(recorded, "info", message ?? recorded.operation.message ?? "", progress);
        }),
      ),

    message: (id, level, message) =>
      require(id).pipe(
        Effect.map((recorded) => {
          append(recorded, level, message);
        }),
      ),

    succeed: (id) =>
      require(id).pipe(
        Effect.map((recorded) => {
          if (!terminal(recorded.operation.state)) {
            recorded.operation = {
              ...recorded.operation,
              state: "succeeded",
              finishedAt: nowIso(),
              message: recorded.operation.message ?? "complete",
            };
            append(recorded, "info", "complete");
            emit(recorded, { type: "completed", operation: recorded.operation });
          }
          return recorded.operation;
        }),
      ),

    fail: (id, error) =>
      require(id).pipe(
        Effect.map((recorded) => {
          if (!terminal(recorded.operation.state)) {
            recorded.operation = {
              ...recorded.operation,
              state: "failed",
              finishedAt: nowIso(),
              error,
              message: error.message,
            };
            append(recorded, "error", error.message);
            emit(recorded, { type: "failed", operation: recorded.operation });
          }
          return recorded.operation;
        }),
      ),

    cancel: (id) =>
      require(id).pipe(
        Effect.flatMap((recorded) => {
          if (terminal(recorded.operation.state)) {
            return Effect.succeed(recorded.operation);
          }
          if (!recorded.operation.cancellable || recorded.committed) {
            return Effect.fail(
              new Invalid({
                field: "operation",
                reason: recorded.committed
                  ? "this operation has passed its commit point"
                  : "this operation cannot be cancelled",
              }),
            );
          }
          recorded.operation = {
            ...recorded.operation,
            state: "cancelled",
            finishedAt: nowIso(),
            message: "cancelled",
          };
          append(recorded, "warning", "cancelled");
          emit(recorded, { type: "failed", operation: recorded.operation });
          return Effect.succeed(recorded.operation);
        }),
      ),

    markCommitted: (id) =>
      require(id).pipe(
        Effect.map((recorded) => {
          recorded.committed = true;
        }),
      ),

    watch: (id, listener) =>
      require(id).pipe(
        Effect.map((recorded) => {
          recorded.watchers.add(listener);
          return () => {
            recorded.watchers.delete(listener);
          };
        }),
      ),
  });
};

export const memoryLayer = Layer.sync(Operations, memory);

export const handleOf = (operations: Operations["Service"], id: string): OperationHandle => ({
  id,
  progress: (progress, message) =>
    operations
      .progress(id, progress, message)
      .pipe(Effect.catchTag("OperationNotFound", () => Effect.void)),
  info: (message) =>
    operations
      .message(id, "info", message)
      .pipe(Effect.catchTag("OperationNotFound", () => Effect.void)),
  warn: (message) =>
    operations
      .message(id, "warning", message)
      .pipe(Effect.catchTag("OperationNotFound", () => Effect.void)),
  commit: operations.markCommitted(id).pipe(Effect.ignore),
});

/**
 * Create, start, run, and complete an operation around `work`.
 *
 * Interruption before the commit point marks the operation cancelled.
 * After the commit point the work is allowed to finish reporting success.
 */
export const run = Effect.fn("Operations.run")(function* <A, E, R>(
  input: {
    readonly repo: string;
    readonly kind: string;
    readonly cancellable?: boolean;
  },
  work: (handle: OperationHandle) => Effect.Effect<A, E, R>,
) {
  const operations = yield* Operations;
  const created = yield* operations.create(input);
  yield* operations.start(created.id);
  const handle = handleOf(operations, created.id);

  const finished = yield* work(handle).pipe(
    Effect.tap(() => operations.succeed(created.id)),
    Effect.tapCause(() => operations.fail(created.id, { tag: "Failed", message: "failed" })),
    Effect.onInterrupt(() =>
      operations.cancel(created.id).pipe(
        Effect.catchTag(["Invalid", "OperationNotFound"], () => Effect.void),
        Effect.asVoid,
      ),
    ),
  );

  const operation = yield* operations.get(created.id);
  return { operation, result: finished };
});

const sseFrame = (event: string, payload: string): string =>
  `event: ${event}\ndata: ${payload}\n\n`;

const encodeEvent = (event: OperationEvent): string =>
  JSON.stringify({
    sequence: event.sequence,
    at: event.at,
    level: event.level,
    message: event.message,
    progress: event.progress ?? null,
  });

const encodeOperation = (operation: Operation): string =>
  JSON.stringify({
    id: operation.id,
    repo: operation.repo,
    kind: operation.kind,
    state: operation.state,
    createdAt: operation.createdAt,
    message: operation.message ?? null,
  });

/** `text/event-stream` body for one operation, replaying history first. */
export const eventsResponse = Effect.fn("Operations.eventsResponse")(function* (id: string) {
  const operations = yield* Operations;
  const operation = yield* operations.get(id);
  const history = yield* operations.history(id);
  const encoder = new TextEncoder();
  let forward: ((event: OperationSse) => void) | undefined;
  const stop = yield* operations.watch(id, (event) => {
    if (forward !== undefined) forward(event);
  });

  const stream = new ReadableStream<Uint8Array>({
    start: (controller) => {
      const send = (event: OperationSse) => {
        if (event.type === "progress") {
          controller.enqueue(encoder.encode(sseFrame("progress", encodeEvent(event.event))));
        } else if (event.type === "message") {
          controller.enqueue(encoder.encode(sseFrame("message", encodeEvent(event.event))));
        } else if (event.type === "completed") {
          controller.enqueue(
            encoder.encode(sseFrame("completed", encodeOperation(event.operation))),
          );
          stop();
          controller.close();
        } else {
          controller.enqueue(encoder.encode(sseFrame("failed", encodeOperation(event.operation))));
          stop();
          controller.close();
        }
      };
      forward = send;

      for (const event of history) {
        controller.enqueue(
          encoder.encode(
            sseFrame(event.progress === undefined ? "message" : "progress", encodeEvent(event)),
          ),
        );
      }
      if (terminal(operation.state)) {
        controller.enqueue(
          encoder.encode(
            sseFrame(
              operation.state === "succeeded" ? "completed" : "failed",
              encodeOperation(operation),
            ),
          ),
        );
        stop();
        controller.close();
      }
    },
    cancel: () => {
      stop();
    },
  });

  return new Response(stream, {
    headers: {
      "content-type": "text/event-stream",
      "cache-control": "no-cache",
      connection: "keep-alive",
    },
  });
});

/** CLI line for one progress event. */
export const renderLine = (kind: string, event: OperationEvent): string => {
  const progress = event.progress;
  if (progress?.current !== undefined && progress.total !== undefined) {
    const unit = progress.unit === undefined ? "" : ` ${progress.unit}`;
    return `* ${kind}: ${event.message} ${progress.current.toLocaleString()} / ${progress.total.toLocaleString()}${unit}`;
  }
  if (progress?.current !== undefined && progress.total === undefined) {
    return `* ${kind}: ${event.message} ${progress.current}%`;
  }
  return `* ${kind}: ${event.message}`;
};

/**
 * Route `GET …/operations/:id/events`. `null` when the path is not that.
 */
export const handleEvents = Effect.fn("Operations.handleEvents")(function* (request: Request) {
  if (request.method !== "GET") return null;
  const segments = new URL(request.url).pathname.split("/").filter((segment) => segment !== "");
  const at = segments.lastIndexOf("operations");
  const id = segments[at + 1];
  if (at === -1 || segments[at + 2] !== "events" || id === undefined) return null;
  if (segments.length !== at + 3) return null;
  return yield* eventsResponse(id);
});
