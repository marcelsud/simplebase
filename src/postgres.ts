import { Pool, type PoolClient } from "pg";

export const POLL_INTERVAL_MS = 250;
export const MAX_ATTEMPTS = 5;
const RETRY_DELAY_MS = 1_000;

export interface Job {
  id: string;
  queue_name: string;
  payload_json: unknown;
}

export function encodePayload(payload: unknown): string {
  const json = JSON.stringify(payload, (_key, value: unknown) => {
    if (
      value === undefined || typeof value === "function" ||
      typeof value === "symbol" || typeof value === "bigint" ||
      (typeof value === "number" && !Number.isFinite(value))
    ) {
      throw new TypeError("Event and job payloads must contain JSON values");
    }
    return value;
  });
  if (json === undefined) throw new TypeError("A payload is required");
  return json;
}

export async function createPostgresStore(url: string) {
  if (!url?.trim()) throw new Error("PostgreSQL storage requires a URL");
  const pool = new Pool({ connectionString: url });
  pool.on("error", (error) => console.error("PostgreSQL connection error", error));

  async function transaction<T>(handler: (client: PoolClient) => Promise<T>): Promise<T> {
    const client = await pool.connect();
    let connectionError: Error | undefined;
    const onError = (error: Error) => { connectionError = error; };
    client.on("error", onError);
    try {
      await client.query("BEGIN");
      const result = await handler(client);
      const commit = await client.query("COMMIT");
      if (commit.command !== "COMMIT") {
        throw new Error("Transaction rolled back after a caught SQL error");
      }
      return result;
    } catch (error) {
      try {
        await client.query("ROLLBACK");
      } catch (rollbackError) {
        connectionError = rollbackError instanceof Error ? rollbackError : new Error(String(rollbackError));
      }
      throw error;
    } finally {
      client.release(connectionError);
      client.removeListener("error", onError);
    }
  }

  try {
    await transaction(async (client) => {
      await client.query(`
        CREATE TABLE IF NOT EXISTS _simplebase_queue_jobs (
          id BIGSERIAL PRIMARY KEY,
          queue_name TEXT NOT NULL,
          payload_json JSONB NOT NULL,
          status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'completed', 'failed')),
          available_at_ms BIGINT NOT NULL,
          attempt_count INTEGER NOT NULL DEFAULT 0,
          last_error TEXT,
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          completed_at TIMESTAMPTZ
        )
      `);
      await client.query(`
        CREATE INDEX IF NOT EXISTS idx_simplebase_queue_jobs_pending_due
        ON _simplebase_queue_jobs(available_at_ms, id) WHERE status = 'pending'
      `);
    });
  } catch (error) {
    await pool.end();
    throw error;
  }

  return {
    transaction,
    async enqueue(client: PoolClient, queue: string, payload: unknown): Promise<void> {
      await client.query(
        "INSERT INTO _simplebase_queue_jobs (queue_name, payload_json, status, available_at_ms) VALUES ($1, $2::jsonb, 'pending', $3)",
        [queue, encodePayload(payload), Date.now()],
      );
    },
    async nextJob(client: PoolClient, queues: string[]): Promise<Job | undefined> {
      const { rows } = await client.query<Job>(`
        SELECT id, queue_name, payload_json FROM _simplebase_queue_jobs
        WHERE status = 'pending' AND queue_name = ANY($1::text[]) AND available_at_ms <= $2
        ORDER BY available_at_ms, id LIMIT 1 FOR UPDATE
      `, [queues, Date.now()]);
      return rows[0];
    },
    async complete(client: PoolClient, id: string): Promise<void> {
      await client.query(`
        UPDATE _simplebase_queue_jobs SET status = 'completed', attempt_count = attempt_count + 1,
        last_error = NULL, completed_at = NOW(), updated_at = NOW() WHERE id = $1
      `, [id]);
    },
    async fail(id: string, error: unknown): Promise<void> {
      const message = error instanceof Error ? error.message : String(error);
      await pool.query(`
        UPDATE _simplebase_queue_jobs SET attempt_count = attempt_count + 1,
        status = CASE WHEN attempt_count + 1 >= $2 THEN 'failed' ELSE 'pending' END,
        last_error = $3, available_at_ms = $4, updated_at = NOW()
        WHERE id = $1 AND status = 'pending'
      `, [id, MAX_ATTEMPTS, message, Date.now() + RETRY_DELAY_MS]);
    },
    close: () => pool.end(),
  };
}
