import { createClient } from "redis";
import { requireName } from "../core/contracts.js";
import type { KvAdapter, KvDriver } from "../kv.js";

export interface RedisOptions {
  url: string;
  prefix?: string;
}

export function redis(options: RedisOptions): KvAdapter {
  requireName(options.url, "Redis URL");
  const prefix = options.prefix ?? "simplebase:kv:";
  requireName(prefix, "Redis key prefix");
  const keyFor = (namespace: string, key: string) => prefix + JSON.stringify([namespace, key]);

  return async () => {
    let connected = false;
    const client = createClient({
      url: options.url,
      disableOfflineQueue: true,
      socket: {
        // Fail bootstrap immediately; reconnect after an established connection is lost.
        reconnectStrategy: (retries) => connected ? Math.min(100 * 2 ** retries, 2_000) : false,
      },
    });
    client.on("error", (error) => console.error("Redis KV connection error", error));
    try { await client.connect(); connected = true; }
    catch (error) { client.destroy(); throw error; }

    const driver: KvDriver = {
      async get(namespace, key) {
        return await client.get(keyFor(namespace, key)) ?? undefined;
      },
      async set(namespace, key, value) {
        await client.set(keyFor(namespace, key), value);
      },
      async delete(namespace, key) {
        return await client.del(keyFor(namespace, key)) > 0;
      },
    };
    let stopped: Promise<void> | undefined;
    return {
      bind: () => driver,
      stop: () => stopped ??= client.close(),
    };
  };
}
