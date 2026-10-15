import { Pool, type PoolClient } from "pg";

export const POLL_INTERVAL_MS = 250;
export const MAX_ATTEMPTS = 5;
const RETRY_DELAY_MS = 1_000;

export interface Job {
  id: string;
  queue_name: string;
  destination_kind: "queue" | "subscription";
  topic: string | null;
  payload_json: unknown;
}

export interface SubscriptionBinding {
  topic: string;
  name: string;
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
      await client.query(`SELECT pg_advisory_xact_lock(
        hashtextextended(current_database() || ':' || current_schema() || ':simplebase:init', 0)
      )`);
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
      const { rows } = await client.query(`
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = current_schema() AND table_name = '_simplebase_queue_jobs'
          AND column_name = 'destination_kind'
      `);
      if (rows.length === 0) {
        await client.query(`ALTER TABLE _simplebase_queue_jobs
          ADD COLUMN destination_kind TEXT NOT NULL DEFAULT 'queue'
            CHECK (destination_kind IN ('queue', 'subscription')),
          ADD COLUMN topic TEXT
        `);
      }
      await client.query(`CREATE TABLE IF NOT EXISTS _simplebase_subscriptions (
        topic TEXT NOT NULL,
        name TEXT NOT NULL,
        PRIMARY KEY (topic, name)
      )`);
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
    async bindSubscriptions(bindings: SubscriptionBinding[]): Promise<void> {
      await transaction(async (client) => {
        await client.query(`
          INSERT INTO _simplebase_subscriptions (topic, name)
          SELECT * FROM unnest($1::text[], $2::text[]) ORDER BY 1, 2
          ON CONFLICT DO NOTHING
        `, [bindings.map((binding) => binding.topic), bindings.map((binding) => binding.name)]);
      });
    },
    async publish(client: PoolClient, topic: string, payloadJson: string): Promise<void> {
      await client.query(`
        INSERT INTO _simplebase_queue_jobs (destination_kind, topic, queue_name, payload_json, available_at_ms)
        SELECT 'subscription', topic, name, $2::jsonb, floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint
        FROM _simplebase_subscriptions WHERE topic = $1
      `, [topic, payloadJson]);
    },
    async enqueue(client: PoolClient, queue: string, payload: unknown): Promise<void> {
      await client.query(
        `INSERT INTO _simplebase_queue_jobs (queue_name, payload_json, available_at_ms)
         VALUES ($1, $2::jsonb, floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint)`,
        [queue, encodePayload(payload)],
      );
    },
    async nextJob(client: PoolClient, queues: string[], bindings: SubscriptionBinding[]): Promise<Job | undefined> {
      const { rows } = await client.query<Job>(`
        SELECT id, queue_name, destination_kind, topic, payload_json FROM _simplebase_queue_jobs
        WHERE status = 'pending'
          AND available_at_ms <= floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint
          AND ((destination_kind = 'queue' AND queue_name = ANY($1::text[]))
            OR (destination_kind = 'subscription' AND (topic, queue_name) IN (
              SELECT * FROM unnest($2::text[], $3::text[])
            )))
        ORDER BY available_at_ms, id LIMIT 1 FOR UPDATE SKIP LOCKED
      `, [queues, bindings.map((binding) => binding.topic), bindings.map((binding) => binding.name)]);
      return rows[0];
    },
    async complete(client: PoolClient, id: string): Promise<void> {
      await client.query(`
        UPDATE _simplebase_queue_jobs SET status = 'completed', attempt_count = attempt_count + 1,
        last_error = NULL, completed_at = NOW(), updated_at = NOW() WHERE id = $1
      `, [id]);
    },
    async fail(client: PoolClient, id: string, error: unknown): Promise<void> {
      const message = error instanceof Error ? error.message : String(error);
      await client.query(`
        UPDATE _simplebase_queue_jobs SET attempt_count = attempt_count + 1,
        status = CASE WHEN attempt_count + 1 >= $2 THEN 'failed' ELSE 'pending' END,
        last_error = $3,
        available_at_ms = floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint + $4,
        updated_at = NOW()
        WHERE id = $1 AND status = 'pending'
      `, [id, MAX_ATTEMPTS, message, RETRY_DELAY_MS]);
    },
    close: () => pool.end(),
  };
}
