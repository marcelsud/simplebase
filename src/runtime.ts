import type { PoolClient } from "pg";
import { createPostgresStore, encodePayload, POLL_INTERVAL_MS, type Job } from "./postgres.js";
import { v, type Validator } from "./validators.js";

export interface HandlerContext {
  db: { query<T = Record<string, unknown>>(sql: string, params?: readonly unknown[]): Promise<T[]> };
  pubsub: { publish(topic: string, payload: unknown): void };
  queue: { enqueue(queue: string, payload: unknown): Promise<void> };
  log: { info(message: string, fields?: Record<string, unknown>): void };
}

export interface SimplebaseOptions {
  storage: { engine: "postgres"; url: string };
}

export type SubscriptionOptions =
  | { idempotent: true; name: string }
  | { idempotent?: false; name?: string };

type Handler<Payload = unknown> = (ctx: HandlerContext, payload: Payload) => unknown | Promise<unknown>;
interface Subscription {
  handler: Handler;
  idempotent: boolean;
  name?: string;
}

// Internal helper exports let tests exercise redelivery without adding a public replay API.
export interface LocalEvent {
  topic: string;
  payload: unknown;
  delivered: Set<string>;
  delivery?: Promise<void>;
}

export function dispatchEvent(
  event: LocalEvent,
  subscriptions: readonly Subscription[],
  invoke: (handler: Handler, payload: unknown, onCommit: () => void) => Promise<unknown>,
): Promise<void> {
  const delivery = (event.delivery ?? Promise.resolve()).catch(() => {}).then(async () => {
    for (const entry of subscriptions) {
      if (entry.idempotent && event.delivered.has(entry.name!)) continue;
      await invoke(entry.handler, structuredClone(event.payload), () => {
        if (entry.idempotent) event.delivered.add(entry.name!);
      });
    }
  });
  event.delivery = delivery;
  return delivery;
}

function requireName(value: string, kind: string): void {
  if (typeof value !== "string" || !value.trim()) throw new Error(`${kind} requires a nonempty name`);
}

export async function createSimplebase(options: SimplebaseOptions) {
  if (options.storage.engine !== "postgres") throw new Error("This prototype supports PostgreSQL storage");
  const store = await createPostgresStore(options.storage.url);
  const subscriptions = new Map<string, Subscription[]>();
  const workers = new Map<string, Handler>();
  const actions = new Set<Promise<unknown>>();
  let closing = false;
  let running = false;
  let loop: Promise<void> | undefined;
  let stopPromise: Promise<void> | undefined;
  let wake: (() => void) | undefined;

  function ensureOpen(): void {
    if (closing) throw new Error("Simplebase is stopped");
  }

  async function dispatch(events: LocalEvent[]): Promise<void> {
    for (const event of events) {
      await dispatchEvent(event, [...(subscriptions.get(event.topic) ?? [])], invoke);
    }
  }

  async function inContext<T>(
    client: PoolClient,
    events: LocalEvent[],
    handler: (ctx: HandlerContext) => Promise<T>,
  ): Promise<T> {
    let active = true;
    function ensureActive(): void {
      if (!active) throw new Error("Handler context is no longer active");
    }
    const ctx: HandlerContext = {
      db: {
        async query<T>(sql: string, params: readonly unknown[] = []): Promise<T[]> {
          ensureActive();
          const result = await client.query(sql, [...params]);
          return result.rows as T[];
        },
      },
      pubsub: {
        publish(topic, payload) {
          ensureActive();
          requireName(topic, "Event topic");
          const snapshot: unknown = JSON.parse(encodePayload(payload));
          events.push({ topic, payload: snapshot, delivered: new Set() });
        },
      },
      queue: {
        async enqueue(queue, payload) {
          ensureActive();
          requireName(queue, "Queue");
          await store.enqueue(client, queue, payload);
        },
      },
      log: { info: (message, fields) => console.info(message, fields ?? {}) },
    };
    try {
      return await handler(ctx);
    } finally {
      active = false;
    }
  }

  async function invoke(handler: Handler, payload: unknown, onCommit?: () => void): Promise<unknown> {
    const events: LocalEvent[] = [];
    const result = await store.transaction((client) => inContext(client, events, async (ctx) => handler(ctx, payload)));
    onCommit?.();
    // shortcut: events live only in memory; add durable delivery when a use case requires recovery across this gap.
    await dispatch(events);
    return result;
  }

  async function processJob(): Promise<boolean> {
    if (workers.size === 0) return false;
    let job: Job | undefined;
    const events: LocalEvent[] = [];
    try {
      await store.transaction(async (client) => {
        job = await store.nextJob(client, [...workers.keys()]);
        if (!job) return;
        await inContext(client, events, async (ctx) => workers.get(job!.queue_name)!(ctx, job!.payload_json));
        await store.complete(client, job.id);
      });
    } catch (error) {
      if (!job) throw error;
      // shortcut: a crash before this separate update can add an attempt; use stronger accounting when it matters.
      await store.fail(job.id, error);
      console.error("Worker failed", { queue: job.queue_name, jobId: job.id, error });
      return true;
    }
    if (!job) return false;
    // Delivery failures here must not retry an already committed job.
    await dispatch(events);
    return true;
  }

  function waitForPoll(): Promise<void> {
    return new Promise((resolve) => {
      const timer = setTimeout(() => { wake = undefined; resolve(); }, POLL_INTERVAL_MS);
      wake = () => { clearTimeout(timer); wake = undefined; resolve(); };
    });
  }

  async function workerLoop(): Promise<void> {
    while (running) {
      try {
        if (await processJob()) continue;
      } catch (error) {
        console.error("Worker loop error", error);
      }
      if (running) await waitForPoll();
    }
  }

  return {
    v,
    action<Input, Result>(definition: {
      args: Validator<Input>;
      handler: (ctx: HandlerContext, input: Input) => Result | Promise<Result>;
    }): (input: Input) => Promise<Awaited<Result>> {
      ensureOpen();
      return async (input): Promise<Awaited<Result>> => {
        ensureOpen();
        const parsed = definition.args.parse(input, "args");
        const invocation = invoke((ctx) => definition.handler(ctx, parsed), parsed);
        actions.add(invocation);
        try {
          return await invocation as Awaited<Result>;
        } finally {
          actions.delete(invocation);
        }
      };
    },
    subscription<Payload = Record<string, unknown>>(
      topic: string,
      handler: Handler<Payload>,
      subscriptionOptions: SubscriptionOptions = {},
    ): void {
      ensureOpen();
      requireName(topic, "Event topic");
      if (subscriptionOptions.idempotent) requireName(subscriptionOptions.name, "Idempotent subscription");
      if (subscriptionOptions.name !== undefined) requireName(subscriptionOptions.name, "Subscription");
      const entries = subscriptions.get(topic) ?? [];
      if (subscriptionOptions.name && entries.some((entry) => entry.name === subscriptionOptions.name)) {
        throw new Error(`Duplicate subscription name on ${topic}: ${subscriptionOptions.name}`);
      }
      entries.push({
        handler: (ctx, payload) => handler(ctx, payload as Payload),
        idempotent: subscriptionOptions.idempotent === true,
        name: subscriptionOptions.name,
      });
      subscriptions.set(topic, entries);
    },
    worker<Payload = Record<string, unknown>>(queue: string, handler: Handler<Payload>): void {
      ensureOpen();
      requireName(queue, "Queue");
      if (workers.has(queue)) throw new Error(`Duplicate worker for queue: ${queue}`);
      workers.set(queue, (ctx, payload) => handler(ctx, payload as Payload));
    },
    async start(): Promise<void> {
      ensureOpen();
      if (running) return;
      running = true;
      loop = workerLoop();
    },
    stop(): Promise<void> {
      if (stopPromise) return stopPromise;
      closing = true;
      running = false;
      wake?.();
      stopPromise = (async () => {
        await loop;
        await Promise.allSettled([...actions]);
        await store.close();
      })();
      return stopPromise;
    },
  };
}

export type Simplebase = Awaited<ReturnType<typeof createSimplebase>>;
