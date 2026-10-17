import type { KvAdapter } from "../kv.js";

export function postgres(): KvAdapter {
  return async () => ({
    bind(db) {
      return {
        async get(namespace, key) {
          const [row] = await db.query<{ value: string }>(
            "SELECT value::text AS value FROM _simplebase_kv WHERE namespace = $1 AND key = $2",
            [namespace, key],
          );
          return row?.value;
        },
        async set(namespace, key, value) {
          await db.query(
            `INSERT INTO _simplebase_kv (namespace, key, value) VALUES ($1, $2, $3::jsonb)
             ON CONFLICT (namespace, key) DO UPDATE SET value = EXCLUDED.value`,
            [namespace, key, value],
          );
        },
        async delete(namespace, key) {
          const rows = await db.query(
            "DELETE FROM _simplebase_kv WHERE namespace = $1 AND key = $2 RETURNING key",
            [namespace, key],
          );
          return rows.length > 0;
        },
      };
    },
  });
}
