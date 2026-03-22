import type {
  GitReflogEntry,
  GitStorage,
  GitStorageRefChange,
  GitStorageRefChangeResult,
} from "./git.storage.ts";

export interface GitRef {
  name: string;
  oid: string;
}

export interface GitRefUpdate {
  ref: string;
  old: string | null;
  new: string | null;
  message?: string;
}

export interface GitRefUpdateResult {
  ref: string;
  ok: boolean;
  currentOid: string | null;
  error?: string;
}

const ZERO_OID = "0".repeat(40);
const REFLOG_LIMIT = 1000;

export class GitRefStore {
  #storage: GitStorage;

  constructor(storage: GitStorage) {
    this.#storage = storage;
  }

  async init() {
    await this.#storage.createDirectory(".git/refs");
    await this.#storage.createDirectory(".git/refs/heads");
    await this.#storage.createDirectory(".git/refs/tags");
    await this.#storage.createDirectory(".git/logs");
    await this.#storage.createDirectory(".git/logs/refs");
    await this.#storage.createDirectory(".git/logs/refs/heads");
    await this.#storage.createDirectory(".git/logs/refs/tags");
  }

  async readRef(refName: string) {
    const normalizedRef = this.#normalizeRefName(refName);
    return await this.#resolveRef(normalizedRef, new Set());
  }

  async readSymbolicRef(refName: string) {
    const normalizedRef = this.#normalizeRefName(refName);
    const rawValue = await this.#readRawRef(normalizedRef);

    if (!rawValue?.startsWith("ref: ")) {
      return null;
    }

    return this.#normalizeRefName(rawValue.slice(5).trim());
  }

  async writeRef(refName: string, oid: string, message: string = "update") {
    this.#assertValidOid(oid, "OID");

    const results = await this.applyRefUpdates([{ ref: refName, old: null, new: oid, message }], {
      compareOldOid: false,
    });
    const result = results[0];

    if (!result?.ok) {
      throw new Error(result?.error || `Failed to update ${refName}`);
    }
  }

  async writeSymbolicRef(refName: string, targetRef: string, message: string = "symbolic-ref") {
    const normalizedRef = this.#normalizeRefName(refName);
    const normalizedTarget = this.#normalizeRefName(targetRef);
    const path = this.#getRefPath(normalizedRef);

    const oldOid = await this.readRef(normalizedRef);

    await this.#storage.writeFile(path, new TextEncoder().encode(`ref: ${normalizedTarget}\n`));

    const newOid = await this.readRef(normalizedRef);
    if (oldOid || newOid) {
      await this.#appendReflog(normalizedRef, {
        message,
        newOid: newOid || ZERO_OID,
        oldOid: oldOid || ZERO_OID,
        timestamp: new Date().toISOString(),
      });
    }
  }

  async compareAndSwapRef(
    refName: string,
    expectedOld: string | null,
    newOid: string,
    message: string = "update",
  ) {
    this.#assertValidOid(newOid, "OID");
    if (expectedOld !== null && expectedOld !== ZERO_OID) {
      this.#assertValidOid(expectedOld, "expected old OID");
    }

    const results = await this.applyRefUpdates(
      [{ ref: refName, old: expectedOld, new: newOid, message }],
      { compareOldOid: true },
    );
    return results[0]?.ok || false;
  }

  async deleteRef(refName: string, message: string = "delete") {
    const normalizedRef = this.#normalizeRefName(refName);
    const currentOid = await this.readRef(normalizedRef);

    if (currentOid === null && (await this.#readRawRef(normalizedRef)) === null) {
      return;
    }

    const results = await this.applyRefUpdates(
      [{ ref: normalizedRef, old: currentOid, new: null, message }],
      { compareOldOid: false },
    );
    const result = results[0];

    if (!result?.ok) {
      throw new Error(result?.error || `Failed to delete ${refName}`);
    }
  }

  async applyRefUpdates(
    updates: GitRefUpdate[],
    options?: { atomic?: boolean; compareOldOid?: boolean },
  ) {
    const prepared: GitStorageRefChange[] = [];
    const metadata = new Map<
      string,
      { currentOid: string | null; expectedOid?: string | null; previousRaw: string | null }
    >();
    const validationErrors = new Map<string, string>();

    for (const update of updates) {
      try {
        const normalizedRef = this.#normalizeRefName(update.ref);
        const currentOid = await this.readRef(normalizedRef);
        const previousRaw = await this.#readRawRef(normalizedRef);
        const expectedOid =
          options?.compareOldOid === false
            ? undefined
            : update.old === ZERO_OID
              ? null
              : update.old;

        if (expectedOid !== undefined && expectedOid !== null) {
          this.#assertValidOid(expectedOid, "expected old OID");
        }

        let newValue: string | null = null;
        if (update.new !== null) {
          this.#assertValidOid(update.new, "OID");
          newValue = update.new;
        }

        prepared.push({
          expectedOid,
          newValue,
          path: this.#getRefPath(normalizedRef),
          refName: normalizedRef,
          reflogEntry: {
            message: update.message || (update.new === null ? "delete" : "update"),
            newOid: update.new || ZERO_OID,
            oldOid: currentOid || ZERO_OID,
            timestamp: new Date().toISOString(),
          },
        });
        metadata.set(normalizedRef, { currentOid, expectedOid, previousRaw });
      } catch (error) {
        validationErrors.set(update.ref, error instanceof Error ? error.message : String(error));
      }
    }

    if (validationErrors.size > 0) {
      const hasAtomicFailure = !!options?.atomic;
      return updates.map((update) => {
        const normalizedRef = this.#safeNormalizeRefName(update.ref);
        const metadataEntry = normalizedRef ? metadata.get(normalizedRef) : undefined;
        const ownError = validationErrors.get(update.ref);
        return {
          currentOid: metadataEntry?.currentOid || null,
          error: ownError || (hasAtomicFailure ? "atomic push failed" : undefined),
          ok: false,
          ref: update.ref,
        } satisfies GitRefUpdateResult;
      });
    }

    const storageResults = await this.#applyChanges(prepared, !!options?.atomic);
    const anyRejected = storageResults.some((result) => !result.applied);

    return prepared.map((change, index) => {
      const result = storageResults[index];
      const details = metadata.get(change.refName);
      const compareMismatch =
        details?.expectedOid !== undefined && details.expectedOid !== result?.currentOid;

      let error: string | undefined;
      if (!result?.applied) {
        error = compareMismatch
          ? "non-fast-forward"
          : anyRejected && options?.atomic
            ? "atomic push failed"
            : "update rejected";
      }

      return {
        currentOid: result?.currentOid || null,
        error,
        ok: !!result?.applied,
        ref: change.refName,
      } satisfies GitRefUpdateResult;
    });
  }

  async readReflog(refName: string) {
    const normalizedRef = this.#normalizeRefName(refName);

    if (this.#storage.readReflog) {
      return await this.#storage.readReflog(normalizedRef);
    }

    try {
      const data = await this.#storage.readFile(this.#getReflogPath(normalizedRef));
      const text = new TextDecoder().decode(data);

      return text
        .split("\n")
        .map((line) => line.trim())
        .filter(Boolean)
        .map((line) => this.#parseReflogLine(line));
    } catch {
      return [];
    }
  }

  async getAllRefs() {
    const refs: GitRef[] = [];

    try {
      await this.#walkRefs(".git/refs", "refs", refs);
    } catch {
      // Ignore missing refs directories.
    }

    return refs;
  }

  async #walkRefs(dirPath: string, prefix: string, refs: GitRef[]) {
    const entries = await this.#storage.listDirectory(dirPath);

    for (const entry of entries) {
      const fullRefName = `${prefix}/${entry}`;
      let oid: string | null = null;

      try {
        oid = await this.readRef(fullRefName);
      } catch {
        oid = null;
      }

      if (oid) {
        refs.push({ name: fullRefName, oid });
        continue;
      }

      const entryPath = `${dirPath}/${entry}`;
      try {
        await this.#walkRefs(entryPath, fullRefName, refs);
      } catch {
        // Ignore non-directory leaf nodes.
      }
    }
  }

  async #resolveRef(refName: string, visited: Set<string>): Promise<string | null> {
    if (visited.has(refName)) {
      throw new Error(`Symbolic ref loop detected for ${refName}`);
    }

    visited.add(refName);

    const rawValue = await this.#readRawRef(refName);
    if (rawValue === null) {
      return null;
    }

    if (rawValue.startsWith("ref: ")) {
      const targetRef = this.#normalizeRefName(rawValue.slice(5).trim());
      return await this.#resolveRef(targetRef, visited);
    }

    if (!this.#isValidOid(rawValue)) {
      throw new Error(`Invalid ref value stored for ${refName}`);
    }

    return rawValue;
  }

  async #readRawRef(refName: string) {
    try {
      const path = this.#getRefPath(refName);
      const data = await this.#storage.readFile(path);
      return new TextDecoder().decode(data).trim() || null;
    } catch {
      return null;
    }
  }

  async #applyChanges(changes: GitStorageRefChange[], atomic: boolean) {
    if (this.#storage.applyRefChanges) {
      return await this.#storage.applyRefChanges(changes, { atomic });
    }

    return await this.#applyChangesFallback(changes, atomic);
  }

  async #applyChangesFallback(changes: GitStorageRefChange[], atomic: boolean) {
    const current = new Map<string, { oid: string | null; raw: string | null }>();

    for (const change of changes) {
      current.set(change.refName, {
        oid: await this.readRef(change.refName),
        raw: await this.#readRawRef(change.refName),
      });
    }

    const mismatch = changes.some((change) => {
      if (change.expectedOid === undefined) {
        return false;
      }

      return current.get(change.refName)?.oid !== change.expectedOid;
    });

    if (atomic && mismatch) {
      return changes.map((change) => ({
        applied: false,
        currentOid: current.get(change.refName)?.oid || null,
        currentValue: current.get(change.refName)?.raw || null,
        refName: change.refName,
      })) satisfies GitStorageRefChangeResult[];
    }

    const results: GitStorageRefChangeResult[] = [];

    for (const change of changes) {
      const currentEntry = current.get(change.refName);
      if (change.expectedOid !== undefined && currentEntry?.oid !== change.expectedOid) {
        results.push({
          applied: false,
          currentOid: currentEntry?.oid || null,
          currentValue: currentEntry?.raw || null,
          refName: change.refName,
        });
        continue;
      }

      if (change.newValue === null) {
        try {
          await this.#storage.deleteFile(change.path);
        } catch {
          // Deleting a missing ref is a no-op.
        }
      } else {
        await this.#storage.writeFile(
          change.path,
          new TextEncoder().encode(`${change.newValue}\n`),
        );
      }

      if (change.reflogEntry) {
        await this.#appendReflog(change.refName, change.reflogEntry);
      }

      results.push({
        applied: true,
        currentOid: change.newValue,
        currentValue: change.newValue,
        refName: change.refName,
      });
    }

    return results;
  }

  async #appendReflog(refName: string, entry: GitReflogEntry) {
    if (this.#storage.readReflog) {
      const changes: GitStorageRefChange[] = [
        {
          newValue: await this.#readRawRef(refName),
          path: this.#getRefPath(refName),
          refName,
          reflogEntry: entry,
        },
      ];

      if (this.#storage.applyRefChanges) {
        await this.#storage.applyRefChanges(changes, { atomic: false });
        return;
      }
    }

    const reflogPath = this.#getReflogPath(refName);
    const entries = await this.readReflog(refName);
    entries.push(entry);
    const content = entries
      .slice(-REFLOG_LIMIT)
      .map((item) => this.#formatReflogEntry(item))
      .join("\n");
    await this.#storage.writeFile(
      reflogPath,
      new TextEncoder().encode(content.length > 0 ? `${content}\n` : ""),
    );
  }

  #formatReflogEntry(entry: GitReflogEntry) {
    return `${entry.oldOid} ${entry.newOid} ${entry.timestamp} ${entry.message}`;
  }

  #parseReflogLine(line: string) {
    const match = line.match(/^([0-9a-f]{40}) ([0-9a-f]{40}) (\S+) ?(.*)$/);
    if (!match || !match[1] || !match[2] || !match[3]) {
      throw new Error(`Invalid reflog entry: ${line}`);
    }

    return {
      message: match[4] || "",
      newOid: match[2],
      oldOid: match[1],
      timestamp: match[3],
    } satisfies GitReflogEntry;
  }

  #getRefPath(refName: string) {
    if (refName === "HEAD") {
      return ".git/HEAD";
    }

    return `.git/${refName}`;
  }

  #getReflogPath(refName: string) {
    if (refName === "HEAD") {
      return ".git/logs/HEAD";
    }

    return `.git/logs/${refName}`;
  }

  #normalizeRefName(refName: string) {
    const trimmedRefName = refName.trim();
    if (trimmedRefName === "HEAD") {
      return "HEAD";
    }

    const normalizedRef = trimmedRefName.startsWith("refs/")
      ? trimmedRefName
      : `refs/${trimmedRefName}`;

    this.#assertValidRefName(normalizedRef);
    return normalizedRef;
  }

  #safeNormalizeRefName(refName: string) {
    try {
      return this.#normalizeRefName(refName);
    } catch {
      return null;
    }
  }

  #assertValidRefName(refName: string) {
    if (refName === "HEAD") {
      return;
    }

    if (!refName.startsWith("refs/")) {
      throw new Error(`Invalid ref name '${refName}': must begin with refs/`);
    }

    if (refName.includes("..")) {
      throw new Error(`Invalid ref name '${refName}': cannot contain '..'`);
    }

    if (refName.includes("@{")) {
      throw new Error(`Invalid ref name '${refName}': cannot contain '@{'`);
    }

    if (/[ - ~^:?*[\\]/.test(refName)) {
      throw new Error(`Invalid ref name '${refName}': contains forbidden characters`);
    }

    if (refName.startsWith("/") || refName.endsWith("/") || refName.endsWith(".")) {
      throw new Error(`Invalid ref name '${refName}': invalid boundary character`);
    }

    if (refName.includes("//")) {
      throw new Error(`Invalid ref name '${refName}': cannot contain empty path segments`);
    }

    const segments = refName.split("/");
    if (segments.length < 3) {
      throw new Error(`Invalid ref name '${refName}': must include a namespace and name`);
    }

    for (const segment of segments) {
      if (!segment) {
        throw new Error(`Invalid ref name '${refName}': cannot contain empty path segments`);
      }

      if (segment === "." || segment === "..") {
        throw new Error(`Invalid ref name '${refName}': invalid path segment '${segment}'`);
      }

      if (segment.startsWith(".")) {
        throw new Error(
          `Invalid ref name '${refName}': segment '${segment}' cannot start with '.'`,
        );
      }

      if (segment.endsWith(".lock")) {
        throw new Error(
          `Invalid ref name '${refName}': segment '${segment}' cannot end with '.lock'`,
        );
      }
    }
  }

  #assertValidOid(oid: string, label: string) {
    if (!this.#isValidOid(oid)) {
      throw new Error(`Invalid ${label}: expected 40-character lowercase hexadecimal SHA-1`);
    }
  }

  #isValidOid(oid: string) {
    return /^[0-9a-f]{40}$/.test(oid);
  }
}
