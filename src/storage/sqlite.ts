import Database from "libsql/promise";
import { MAX_ATTEMPTS, RETRY_DELAY_MS, type Job, type Transaction } from "../core/contracts.js";
import type { StorageAdapter, StorageBackend } from "../storage.js";

export interface SqliteOptions {
  filename: string;
  busyTimeoutMs?: number;
}

const now = "CAST(unixepoch('subsec') * 1000 AS INTEGER)";

export function sqlite(options: SqliteOptions): StorageAdapter {
  return () => createSqliteStore(options);
}

async function createSqliteStore({ filename, busyTimeoutMs = 5_000 }: SqliteOptions): Promise<StorageBackend> {
  if (typeof filename !== "string" || !filename.trim()) throw new Error("SQLite storage requires a filename");
  if (!Number.isSafeInteger(busyTimeoutMs) || busyTimeoutMs < 0) {
    throw new TypeError("SQLite busyTimeoutMs must be a nonnegative integer");
  }
  const db = new Database(filename, { timeout: 0 }) as Database & { readonly inTransaction: boolean };
  let pending = Promise.resolve();
  let closing = false;
  let stopped: Promise<void> | undefined;

  async function acquire(sql: string): Promise<void> {
    const deadline = performance.now() + busyTimeoutMs;
    for (;;) {
      try { await db.exec(sql); return; }
      catch (error) {
        if (!(error instanceof Error) || !("code" in error) ||
            !String(error.code).startsWith("SQLITE_BUSY") || performance.now() >= deadline) throw error;
        await new Promise((resolve) => setTimeout(resolve, Math.min(25, Math.max(1, deadline - performance.now()))));
      }
    }
  }

  async function query<T>(sql: string, params: readonly unknown[] = []): Promise<T[]> {
    const statement = await db.prepare(sql);
    return await statement.all([...params]) as T[];
  }

  function transaction<T>(handler: (transaction: Transaction) => Promise<T>): Promise<T> {
    if (closing) return Promise.reject(new Error("SQLite storage is closed"));
    // SQLite has one writer. Serialize whole async handlers, including their awaits.
    const invocation = pending.then(async () => {
      await acquire("BEGIN IMMEDIATE");
      let failed = false;
      let queryError: unknown;
      function ensureCommittable(): void {
        if (failed) throw new Error("Transaction aborted after a caught SQL error", { cause: queryError });
      }
      const tx: Transaction = {
        async query<T>(sql: string, params: readonly unknown[] = []): Promise<T[]> {
          ensureCommittable();
          try { return await query<T>(sql, params); }
          catch (error) { failed = true; queryError = error; throw error; }
        },
        async publish(topic, payloadJson) {
          await tx.query(`
            INSERT INTO _simplebase_queue_jobs (destination_kind, topic, queue_name, payload_json, available_at_ms)
            SELECT 'subscription', topic, name, ?, ${now} FROM _simplebase_subscriptions WHERE topic = ?
          `, [payloadJson, topic]);
        },
        async enqueue(queue, payloadJson) {
          await tx.query(`
            INSERT INTO _simplebase_queue_jobs (queue_name, payload_json, available_at_ms) VALUES (?, ?, ${now})
          `, [queue, payloadJson]);
        },
        async nextJob(queues, bindings) {
          const [job] = await tx.query<Omit<Job, "payload_json"> & { payload_json: string }>(`
            SELECT CAST(id AS TEXT) AS id, queue_name, destination_kind, topic, payload_json
            FROM _simplebase_queue_jobs
            WHERE status = 'pending' AND available_at_ms <= ${now}
              AND ((destination_kind = 'queue' AND queue_name IN (SELECT value FROM json_each(?)))
                OR (destination_kind = 'subscription' AND EXISTS (
                  SELECT 1 FROM json_each(?) AS binding
                  WHERE json_extract(binding.value, '$.topic') = topic
                    AND json_extract(binding.value, '$.name') = queue_name
                )))
            ORDER BY available_at_ms, _simplebase_queue_jobs.id LIMIT 1
          `, [JSON.stringify(queues), JSON.stringify(bindings)]);
          return job && { ...job, payload_json: JSON.parse(job.payload_json) };
        },
        async attempt(handler, onError) {
          await db.exec("SAVEPOINT handler");
          try {
            await handler();
            ensureCommittable();
            await db.exec("RELEASE SAVEPOINT handler");
          } catch (error) {
            await db.exec("ROLLBACK TO SAVEPOINT handler; RELEASE SAVEPOINT handler");
            failed = false;
            queryError = undefined;
            await onError(error);
          }
        },
        async complete(id) {
          await tx.query(`
            UPDATE _simplebase_queue_jobs SET status = 'completed', attempt_count = attempt_count + 1,
              last_error = NULL, completed_at = ${now}, updated_at = ${now} WHERE id = ?
          `, [id]);
        },
        async fail(id, error) {
          await tx.query(`
            UPDATE _simplebase_queue_jobs SET attempt_count = attempt_count + 1,
              status = CASE WHEN attempt_count + 1 >= ? THEN 'failed' ELSE 'pending' END,
              last_error = ?, available_at_ms = ${now} + ?, updated_at = ${now}
            WHERE id = ? AND status = 'pending'
          `, [MAX_ATTEMPTS, error instanceof Error ? error.message : String(error), RETRY_DELAY_MS, id]);
        },
      };
      try {
        const result = await handler(tx);
        ensureCommittable();
        await db.exec("COMMIT");
        return result;
      } catch (error) {
        if (db.inTransaction) await db.exec("ROLLBACK");
        throw error;
      }
    });
    pending = invocation.then(() => {}, () => {});
    return invocation;
  }

  try {
    await db.exec("PRAGMA foreign_keys = ON; PRAGMA synchronous = FULL");
    await acquire("PRAGMA journal_mode = WAL");
    await transaction(async () => {
      await db.exec(`
        CREATE TABLE IF NOT EXISTS _simplebase_queue_jobs (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          queue_name TEXT NOT NULL,
          destination_kind TEXT NOT NULL DEFAULT 'queue' CHECK (destination_kind IN ('queue', 'subscription')),
          topic TEXT,
          payload_json TEXT NOT NULL CHECK (json_valid(payload_json)),
          status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'completed', 'failed')),
          available_at_ms INTEGER NOT NULL,
          attempt_count INTEGER NOT NULL DEFAULT 0,
          last_error TEXT,
          created_at INTEGER NOT NULL DEFAULT (${now}),
          updated_at INTEGER NOT NULL DEFAULT (${now}),
          completed_at INTEGER
        );
        CREATE TABLE IF NOT EXISTS _simplebase_subscriptions (
          topic TEXT NOT NULL, name TEXT NOT NULL, PRIMARY KEY (topic, name)
        );
        CREATE TABLE IF NOT EXISTS _simplebase_collections (
          collection_name TEXT NOT NULL, id TEXT NOT NULL,
          data TEXT NOT NULL CHECK (json_valid(data) AND json_type(data) = 'object' AND json_type(data, '$.id') IS NULL),
          PRIMARY KEY (collection_name, id)
        );
        CREATE TABLE IF NOT EXISTS _simplebase_kv (
          namespace TEXT NOT NULL, key TEXT NOT NULL, value TEXT NOT NULL CHECK (json_valid(value)),
          PRIMARY KEY (namespace, key)
        );
        CREATE INDEX IF NOT EXISTS idx_simplebase_queue_jobs_pending_due
          ON _simplebase_queue_jobs(available_at_ms, id) WHERE status = 'pending';
      `);
    });
  } catch (error) {
    db.close();
    throw error;
  }

  return {
    transaction,
    async bindSubscriptions(bindings) {
      await transaction(async (tx) => {
        for (const { topic, name } of bindings) {
          await tx.query("INSERT INTO _simplebase_subscriptions (topic, name) VALUES (?, ?) ON CONFLICT DO NOTHING", [topic, name]);
        }
      });
    },
    bind(db) {
      const collection = {
        async insert(name: string, id: string, documentJson: string) {
          await db.query("INSERT INTO _simplebase_collections (collection_name, id, data) VALUES (?, ?, ?)", [name, id, documentJson]);
        },
        async get(name: string, id: string) {
          const [row] = await db.query<{ data: string }>(
            "SELECT data FROM _simplebase_collections WHERE collection_name = ? AND id = ?", [name, id],
          );
          return row?.data;
        },
        async update(name: string, id: string, patchJson: string) {
          const rows = await db.query(`
            UPDATE _simplebase_collections SET data = (
              SELECT json_group_object(key, json(CASE type
                WHEN 'true' THEN 'true' WHEN 'false' THEN 'false' WHEN 'null' THEN 'null'
                WHEN 'object' THEN value WHEN 'array' THEN value ELSE json_quote(value) END))
              FROM (
                SELECT key, value, type FROM json_each(data)
                  WHERE key NOT IN (SELECT key FROM json_each(?))
                UNION ALL SELECT key, value, type FROM json_each(?)
              )
            ) WHERE collection_name = ? AND id = ? RETURNING id
          `, [patchJson, patchJson, name, id]);
          return rows.length > 0;
        },
        async delete(name: string, id: string) {
          return (await db.query("DELETE FROM _simplebase_collections WHERE collection_name = ? AND id = ? RETURNING id", [name, id])).length > 0;
        },
      };
      return {
        collection,
        kv: {
          async get(namespace, key) {
            const [row] = await db.query<{ value: string }>("SELECT value FROM _simplebase_kv WHERE namespace = ? AND key = ?", [namespace, key]);
            return row?.value;
          },
          async set(namespace, key, value) {
            await db.query(`INSERT INTO _simplebase_kv (namespace, key, value) VALUES (?, ?, ?)
              ON CONFLICT (namespace, key) DO UPDATE SET value = excluded.value`, [namespace, key, value]);
          },
          async delete(namespace, key) {
            return (await db.query("DELETE FROM _simplebase_kv WHERE namespace = ? AND key = ? RETURNING key", [namespace, key])).length > 0;
          },
        },
      };
    },
    close() {
      closing = true;
      return stopped ??= pending.then(() => { db.close(); });
    },
  };
}
