/**
 * The client half of smart HTTP — enough to push.
 *
 * The mirror of `Fetch.ts`: an advertisement, then one POST carrying ref
 * commands and a pack, then a report. The conversation itself is `Wire.ts`;
 * what this adds is the history. The pack is built by the same `Repository`
 * walk the server uses to answer a fetch — `wants` are the new ref values,
 * `haves` are what the remote already advertises.
 *
 * The one decision made here rather than on the server is the
 * fast-forward check. A client holds the history; it can see that the remote's
 * current commit is not an ancestor of what would replace it, and say so
 * before spending a pack upload on a push the operator did not mean.
 */
import { Effect, Stream } from "effect";

import {
  Invalid,
  type ObjectNotFound,
  type PackCorrupt,
  type RequestLimitExceeded,
  type StorageFailure,
} from "../git/Error.ts";
import { Repository } from "../git/Repository.ts";
import { isOid, type Oid } from "../git/Store.ts";
import { absent } from "../hub/Redaction.ts";
import type { Authorize } from "./Authorize.ts";
import { type Command, discover, type PushResult, receivePack } from "./Wire.ts";

export type { PushResult } from "./Wire.ts";

export interface PushRef {
  readonly local: string;
  readonly remote: string;
  /** Delete the remote ref. */
  readonly delete?: boolean;
  /** Refuse the update unless the advertised remote value matches this lease. */
  readonly expected?: Oid | null;
}

/**
 * Push local refs to a remote over smart HTTP.
 *
 * Every requested ref gets a result, whether it reached the server or was
 * refused here; a rejection is a value, not a failure, because a push of five
 * branches where one lost a race is four successes.
 */
export const push = Effect.fn("Client.push")(function* (input: {
  readonly url: string;
  readonly refs: ReadonlyArray<PushRef>;
  readonly token?: string;
  readonly force?: boolean;
  readonly atomic?: boolean;
  /** How to answer a `Hub-SSH-v1` challenge; absent, a 401 stays a 401. */
  readonly authorize?: Authorize;
}) {
  const repository = yield* Repository;
  const { atomic, authorize, force, refs } = input;

  const advertisement = yield* discover(input);

  if (atomic === true && !advertisement.capabilities.has("atomic")) {
    return yield* new Invalid({
      field: "atomic",
      reason: "the remote does not support atomic push",
    });
  }

  const outcomes: Array<PushResult | null> = refs.map(() => null);
  const commands: Array<Command & { readonly index: number }> = [];

  for (const [index, request] of refs.entries()) {
    const name = request.remote;
    const advertised = advertisement.refs.get(name);
    const old = advertised ?? null;

    if (request.expected !== undefined && request.expected !== old) {
      outcomes[index] = { ref: name, ok: false, reason: "stale remote value" };
      continue;
    }

    if (request.delete === true) {
      // Nothing to delete is a failed command, not a silent success: the
      // caller named a ref the remote does not have.
      if (advertised === undefined) {
        outcomes[index] = { ref: name, ok: false, reason: "remote ref does not exist" };
        continue;
      }
      commands.push({ index, ref: name, old, next: null });
      continue;
    }

    const next = isOid(request.local) ? request.local : yield* repository.resolve(request.local);
    if (next === null) {
      return yield* new Invalid({ field: "local", reason: `unknown ref '${request.local}'` });
    }

    if (advertised === next) {
      outcomes[index] = { ref: name, ok: true, reason: "up to date" };
      continue;
    }

    /**
     * A remote value this history cannot reach — a diverged branch, or a
     * commit this clone never fetched — would be lost by the update. `force`
     * is the caller saying that is the intent.
     */
    if (advertised !== undefined && force !== true) {
      const fastForward = yield* repository.isAncestor(advertised, next);
      if (!fastForward) {
        outcomes[index] = { ref: name, ok: false, reason: "non-fast-forward" };
        continue;
      }
    }

    commands.push({ index, ref: name, old, next });
  }

  const settle = (): ReadonlyArray<PushResult> =>
    outcomes.map(
      (outcome, index) =>
        outcome ?? { ref: refs[index]?.remote ?? "", ok: false, reason: "no status reported" },
    );

  // Every requested ref was already where it belongs, or refused here. There
  // is no request to make, and an empty command list is one the server rejects.
  if (atomic === true && outcomes.some((outcome) => outcome?.ok === false)) {
    for (const command of commands)
      outcomes[command.index] = { ref: command.ref, ok: false, reason: "atomic push rejected" };
    return settle();
  }
  if (commands.length === 0) return settle();

  const wants = [
    ...new Set(commands.flatMap((command) => (command.next === null ? [] : [command.next]))),
  ];

  const pack =
    wants.length === 0
      ? Stream.empty
      : yield* Effect.gen(function* () {
          const request = { wants, haves: [...new Set(advertisement.refs.values())] };
          // Strict first, and retried once against what the tombstones account
          // for — the same shape the server's `planFor` uses, and for the same
          // reason. A redacted payload is absent by design while the tree
          // naming it survives, so a strict closure over a hub ref fails the
          // moment anything in it has been redacted: this client would have
          // been unable to push its own pull requests back. Computing the
          // exclusion up front instead would fold the trust log on every push,
          // including the overwhelming majority that touch no hub ref at all.
          const strict = yield* repository
            .fetch(request)
            .pipe(Effect.catchTag("ObjectNotFound", () => Effect.succeed(null)));
          // An absence no tombstone covers fails the retry too, which is the
          // corruption it is.
          const plan =
            strict ?? (yield* repository.fetch({ ...request, exclude: yield* absent() }));
          return repository.packOids(plan.oids);
        });

  const reported = yield* receivePack({
    advertisement,
    commands,
    pack,
    shallow: yield* repository.shallow,
    atomic,
    authorize,
  });
  for (const [position, command] of commands.entries()) {
    outcomes[command.index] = reported[position] ?? null;
  }

  return settle();
}) satisfies (input: {
  readonly url: string;
  readonly refs: ReadonlyArray<PushRef>;
  readonly token?: string;
  readonly force?: boolean;
  readonly atomic?: boolean;
  readonly authorize?: Authorize;
}) => Effect.Effect<
  ReadonlyArray<PushResult>,
  Invalid | PackCorrupt | ObjectNotFound | StorageFailure | RequestLimitExceeded,
  Repository
>;
