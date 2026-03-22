import {
  type GitReflogEntry,
  type GitStorage,
  type GitStorageRefChange,
  type GitStorageRefChangeResult,
} from "./git.storage.ts";

type StoredRefRow = {
  oid: string | null;
  value: string;
};

export class CloudflareStorage implements GitStorage {
  #repoName?: string;
  #r2: R2Bucket;
  #sql: SqlStorage;
  #storage: DurableObjectStorage;

  constructor(ctx: DurableObjectState, env: Env) {
    this.#storage = ctx.storage;
    this.#sql = ctx.storage.sql;
    this.#r2 = env.GIT_OBJECTS;
  }

  async init(repoName: string) {
    this.#repoName = repoName;

    this.#sql.exec(/* SQL */ `
      CREATE TABLE IF NOT EXISTS git_files (
        repository TEXT NOT NULL,
        path TEXT NOT NULL,
        size INTEGER NOT NULL,
        last_modified DATETIME NOT NULL,
        r2_key TEXT NOT NULL,
        PRIMARY KEY (repository, path)
      );

      CREATE INDEX IF NOT EXISTS idx_git_files_repo
      ON git_files(repository);

      CREATE INDEX IF NOT EXISTS idx_git_files_path
      ON git_files(repository, path);

      CREATE TABLE IF NOT EXISTS git_refs (
        repository TEXT NOT NULL,
        ref_name TEXT NOT NULL,
        path TEXT NOT NULL,
        value TEXT NOT NULL,
        kind TEXT NOT NULL,
        target TEXT,
        oid TEXT,
        last_modified DATETIME NOT NULL,
        PRIMARY KEY (repository, ref_name)
      );

      CREATE INDEX IF NOT EXISTS idx_git_refs_path
      ON git_refs(repository, path);

      CREATE TABLE IF NOT EXISTS git_reflogs (
        repository TEXT NOT NULL,
        ref_name TEXT NOT NULL,
        seq INTEGER NOT NULL,
        old_oid TEXT NOT NULL,
        new_oid TEXT NOT NULL,
        timestamp TEXT NOT NULL,
        message TEXT NOT NULL,
        PRIMARY KEY (repository, ref_name, seq)
      );

      CREATE INDEX IF NOT EXISTS idx_git_reflogs_ref
      ON git_reflogs(repository, ref_name, seq);
    `);
  }

  async exists(path: string) {
    const repository = this.#requireRepository();

    const rows = this.#sql
      .exec(
        /* SQL */ `SELECT 1 FROM git_files WHERE repository = ? AND path = ? LIMIT 1`,
        repository,
        path,
      )
      .toArray();

    return rows.length > 0;
  }

  async readFile(path: string) {
    const repository = this.#requireRepository();
    const refName = this.#refNameFromPath(path);

    if (refName) {
      const row = this.#sql
        .exec(
          /* SQL */ `SELECT value FROM git_refs WHERE repository = ? AND ref_name = ?`,
          repository,
          refName,
        )
        .toArray()[0] as { value: string } | undefined;

      if (row?.value) {
        return new TextEncoder().encode(`${row.value}\n`);
      }
    }

    const key = this.#key(path);
    const object = await this.#r2.get(key);

    if (!object) {
      throw new Error(`File not found: ${path}`);
    }

    return new Uint8Array(await object.arrayBuffer());
  }

  async writeFile(path: string, data: Uint8Array) {
    const repository = this.#requireRepository();
    const refName = this.#refNameFromPath(path);

    if (refName) {
      const content = new TextDecoder().decode(data).trim();
      await this.#r2.put(this.#key(path), data);
      this.#upsertRef(repository, refName, path, content, new Date().toISOString());
      return;
    }

    const key = this.#key(path);
    const now = new Date().toISOString();

    await this.#r2.put(key, data);

    this.#sql.exec(
      /* SQL */ `
        INSERT OR REPLACE INTO git_files
        (repository, path, size, last_modified, r2_key)
        VALUES (?, ?, ?, ?, ?)
      `,
      repository,
      path,
      data.length,
      now,
      key,
    );
  }

  async deleteFile(path: string) {
    const repository = this.#requireRepository();
    const refName = this.#refNameFromPath(path);

    if (refName) {
      await this.#r2.delete(this.#key(path));
      this.#sql.exec(
        /* SQL */ `DELETE FROM git_refs WHERE repository = ? AND ref_name = ?`,
        repository,
        refName,
      );
      this.#sql.exec(
        /* SQL */ `DELETE FROM git_files WHERE repository = ? AND path = ?`,
        repository,
        path,
      );
      return;
    }

    const key = this.#key(path);
    await this.#r2.delete(key);

    this.#sql.exec(
      /* SQL */ `DELETE FROM git_files WHERE repository = ? AND path = ?`,
      repository,
      path,
    );
  }

  async createDirectory(_path: string) {
    // R2 doesn't require explicit directory creation.
  }

  async listDirectory(path: string) {
    const repository = this.#requireRepository();
    const pathPattern = this.#pattern(path);
    const normalizedPath = path.replace(/\/$/, "");
    const prefixLength = normalizedPath.length + 1;

    const rows = this.#sql
      .exec(
        /* SQL */ `
          SELECT path FROM git_files
          WHERE repository = ?
          AND path LIKE ?
        `,
        repository,
        pathPattern,
      )
      .toArray();

    const children = new Set<string>();
    for (const row of rows) {
      const fullPath = (row as { path: string }).path;
      const remainder = fullPath.slice(prefixLength);
      const slashIndex = remainder.indexOf("/");
      const child = slashIndex === -1 ? remainder : remainder.slice(0, slashIndex);
      if (child) {
        children.add(child);
      }
    }

    return Array.from(children);
  }

  async deleteDirectory(path: string) {
    const repository = this.#requireRepository();
    const pathPattern = this.#pattern(path);

    const files = this.#sql
      .exec(
        /* SQL */ `
          SELECT path, r2_key FROM git_files
          WHERE repository = ? AND path LIKE ?
        `,
        repository,
        pathPattern,
      )
      .toArray();

    await Promise.all(files.map((file) => this.#r2.delete((file as { r2_key: string }).r2_key)));

    this.#sql.exec(
      /* SQL */ `
        DELETE FROM git_files
        WHERE repository = ? AND path LIKE ?
      `,
      repository,
      pathPattern,
    );

    if (path === ".git/refs" || path.startsWith(".git/refs/")) {
      const refPrefix = path === ".git/refs" ? "refs/%" : `${path.slice(5).replace(/\/$/, "")}/%`;
      this.#sql.exec(
        /* SQL */ `DELETE FROM git_refs WHERE repository = ? AND ref_name LIKE ?`,
        repository,
        refPrefix,
      );
    }
  }

  async getFileInfo(path: string) {
    const repository = this.#requireRepository();

    const result = this.#sql
      .exec(
        /* SQL */ `
          SELECT size, last_modified FROM git_files
          WHERE repository = ? AND path = ?
        `,
        repository,
        path,
      )
      .one();

    if (!result) {
      throw new Error(`File not found: ${path}`);
    }

    return {
      size: result.size as number,
      lastModified: new Date(result.last_modified as string),
    };
  }

  async applyRefChanges(changes: GitStorageRefChange[], options?: { atomic?: boolean }) {
    const repository = this.#requireRepository();
    const atomic = !!options?.atomic;
    const uploads = changes
      .filter((change) => change.newValue !== null)
      .map((change) => ({
        data: new TextEncoder().encode(`${change.newValue}\n`),
        path: change.path,
      }));

    await Promise.all(uploads.map((upload) => this.#r2.put(this.#key(upload.path), upload.data)));

    const results = this.#storage.transactionSync(() => {
      const currentRows = new Map<string, StoredRefRow | null>();
      for (const change of changes) {
        currentRows.set(change.refName, this.#readStoredRef(repository, change.refName));
      }

      const hasMismatch = changes.some((change) => {
        if (change.expectedOid === undefined) {
          return false;
        }

        return (currentRows.get(change.refName)?.oid || null) !== change.expectedOid;
      });

      if (atomic && hasMismatch) {
        return changes.map((change) => ({
          applied: false,
          currentOid: currentRows.get(change.refName)?.oid || null,
          currentValue: currentRows.get(change.refName)?.value || null,
          refName: change.refName,
        }));
      }

      const results: GitStorageRefChangeResult[] = [];
      for (const change of changes) {
        const currentRow = currentRows.get(change.refName);
        if (change.expectedOid !== undefined && (currentRow?.oid || null) !== change.expectedOid) {
          results.push({
            applied: false,
            currentOid: currentRow?.oid || null,
            currentValue: currentRow?.value || null,
            refName: change.refName,
          });
          continue;
        }

        const now = new Date().toISOString();
        if (change.newValue === null) {
          this.#sql.exec(
            /* SQL */ `DELETE FROM git_refs WHERE repository = ? AND ref_name = ?`,
            repository,
            change.refName,
          );
          this.#sql.exec(
            /* SQL */ `DELETE FROM git_files WHERE repository = ? AND path = ?`,
            repository,
            change.path,
          );
        } else {
          this.#upsertRef(repository, change.refName, change.path, change.newValue, now);
        }

        if (change.reflogEntry) {
          this.#insertReflog(repository, change.refName, change.reflogEntry);
        }

        results.push({
          applied: true,
          currentOid: this.#parseRefValue(change.newValue || "").oid,
          currentValue: change.newValue,
          refName: change.refName,
        });
      }

      return results;
    });

    await Promise.all(
      changes
        .filter((change) => change.newValue === null)
        .map((change) => this.#r2.delete(this.#key(change.path))),
    );

    return results;
  }

  async readReflog(refName: string) {
    const repository = this.#requireRepository();

    const rows = this.#sql
      .exec(
        /* SQL */ `
        SELECT old_oid, new_oid, timestamp, message
        FROM git_reflogs
        WHERE repository = ? AND ref_name = ?
        ORDER BY seq ASC
      `,
        repository,
        refName,
      )
      .toArray();

    return rows.map((row) => ({
      message: (row as { message: string }).message,
      newOid: (row as { new_oid: string }).new_oid,
      oldOid: (row as { old_oid: string }).old_oid,
      timestamp: (row as { timestamp: string }).timestamp,
    })) satisfies GitReflogEntry[];
  }

  async listReflogRefs() {
    const repository = this.#requireRepository();

    const rows = this.#sql
      .exec(
        /* SQL */ `
        SELECT DISTINCT ref_name
        FROM git_reflogs
        WHERE repository = ?
      `,
        repository,
      )
      .toArray();

    return rows.map((row) => (row as { ref_name: string }).ref_name);
  }

  #pattern(path: string) {
    return path ? `${path.replace(/\/$/, "")}/%` : "%";
  }

  #key(path: string) {
    return `${this.#repoName}/${path}`;
  }

  #requireRepository() {
    if (!this.#repoName) {
      throw new Error("Storage not initialized");
    }

    return this.#repoName;
  }

  #refNameFromPath(path: string) {
    if (path === ".git/HEAD") {
      return "HEAD";
    }

    if (path.startsWith(".git/refs/")) {
      return path.slice(5);
    }

    return null;
  }

  #readStoredRef(repository: string, refName: string) {
    const row = this.#sql
      .exec(
        /* SQL */ `SELECT oid, value FROM git_refs WHERE repository = ? AND ref_name = ?`,
        repository,
        refName,
      )
      .toArray()[0] as { oid: string | null; value: string } | undefined;

    if (!row) {
      return null;
    }

    return {
      oid: row.oid || null,
      value: row.value,
    } satisfies StoredRefRow;
  }

  #upsertRef(repository: string, refName: string, path: string, value: string, now: string) {
    const parsed = this.#parseRefValue(value);
    const key = this.#key(path);

    this.#sql.exec(
      /* SQL */ `
        INSERT OR REPLACE INTO git_refs
        (repository, ref_name, path, value, kind, target, oid, last_modified)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
      `,
      repository,
      refName,
      path,
      value,
      parsed.kind,
      parsed.target,
      parsed.oid,
      now,
    );

    this.#sql.exec(
      /* SQL */ `
        INSERT OR REPLACE INTO git_files
        (repository, path, size, last_modified, r2_key)
        VALUES (?, ?, ?, ?, ?)
      `,
      repository,
      path,
      value.length + 1,
      now,
      key,
    );
  }

  #parseRefValue(value: string) {
    if (value.startsWith("ref: ")) {
      return {
        kind: "symbolic",
        oid: null,
        target: value.slice(5).trim(),
      } as const;
    }

    return {
      kind: "direct",
      oid: /^[0-9a-f]{40}$/.test(value) ? value : null,
      target: null,
    } as const;
  }

  #insertReflog(repository: string, refName: string, entry: GitReflogEntry) {
    const nextSeqRow = this.#sql
      .exec(
        /* SQL */ `
        SELECT COALESCE(MAX(seq), 0) + 1 AS next_seq
        FROM git_reflogs
        WHERE repository = ? AND ref_name = ?
      `,
        repository,
        refName,
      )
      .toArray()[0] as { next_seq: number } | undefined;

    const nextSeq = nextSeqRow?.next_seq || 1;

    this.#sql.exec(
      /* SQL */ `
        INSERT INTO git_reflogs
        (repository, ref_name, seq, old_oid, new_oid, timestamp, message)
        VALUES (?, ?, ?, ?, ?, ?, ?)
      `,
      repository,
      refName,
      nextSeq,
      entry.oldOid,
      entry.newOid,
      entry.timestamp,
      entry.message,
    );

    this.#sql.exec(
      /* SQL */ `
        DELETE FROM git_reflogs
        WHERE repository = ?
        AND ref_name = ?
        AND seq NOT IN (
          SELECT seq
          FROM git_reflogs
          WHERE repository = ? AND ref_name = ?
          ORDER BY seq DESC
          LIMIT 1000
        )
      `,
      repository,
      refName,
      repository,
      refName,
    );
  }
}
