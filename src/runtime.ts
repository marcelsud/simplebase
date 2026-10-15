import type { PoolClient } from "pg";
import { createPostgresStore, encodePayload, POLL_INTERVAL_MS, type SubscriptionBinding } from "./postgres.js";
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

export interface SubscriptionOptions { name: string }

export interface StartOptions {
  signals?: boolean;
  beforeStop?: () => void | Promise<void>;
}

type Handler<Payload = unknown> = (ctx: HandlerContext, payload: Payload) => unknown | Promise<unknown>;

function requireName(value: string, kind: string): void {
  if (typeof value !== "string" || !value.trim()) throw new Error(`${kind} requires a nonempty name`);
}

export async function createSimplebase(options: SimplebaseOptions) {
  if (options.storage.engine !== "postgres") throw new Error("This prototype supports PostgreSQL storage");
  const store = await createPostgresStore(options.storage.url);
  const subscriptions = new Map<string, Map<string, Handler>>();
  const bindings: SubscriptionBinding[] = [];
  const workers = new Map<string, Handler>();
  const actions = new Set<Promise<unknown>>();
  let closing = false;
  let running = false;
  let loop: Promise<void> | undefined;
  let startPromise: Promise<void> | undefined;
  let stopPromise: Promise<void> | undefined;
  let wake: (() => void) | undefined;
  let beforeStop: StartOptions["beforeStop"];
  const signals = ["SIGINT", "SIGTERM"] as const;

  function removeSignalHandlers(): void {
    for (const signal of signals) process.off(signal, onSignal);
  }

  function onSignal(): void {
    void stop().catch((error) => {
      console.error("Simplebase shutdown failed", error);
      process.exitCode = 1;
    });
  }

  function stop(): Promise<void> {
    if (stopPromise) return stopPromise;
    running = false;
    wake?.();
    removeSignalHandlers();
    if (!beforeStop) closing = true;
    stopPromise = Promise.resolve().then(async () => {
      try {
        await beforeStop?.();
      } finally {
        closing = true;
        await Promise.allSettled(startPromise ? [startPromise] : []);
        await loop;
        await Promise.allSettled([...actions]);
        await store.close();
      }
    });
    return stopPromise;
  }

  function ensureOpen(): void {
    if (closing) throw new Error("Simplebase is stopped");
  }

  function ensureDeclaring(): void {
    ensureOpen();
    if (startPromise) throw new Error("Declare consumers before calling start()");
  }

  async function inContext<T>(
    client: PoolClient,
    handler: (ctx: HandlerContext) => Promise<T>,
  ): Promise<T> {
    const events: { topic: string; payloadJson: string }[] = [];
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
          events.push({ topic, payloadJson: encodePayload(payload) });
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
      const result = await handler(ctx);
      active = false;
      for (const event of events) await store.publish(client, event.topic, event.payloadJson);
      return result;
    } finally {
      active = false;
    }
  }

  async function invoke(handler: Handler, payload: unknown): Promise<unknown> {
    return store.transaction((client) => inContext(client, async (ctx) => handler(ctx, payload)));
  }

  async function processJob(): Promise<boolean> {
    return store.transaction(async (client) => {
      if (!running) return false;
      const job = await store.nextJob(client, [...workers.keys()], bindings);
      if (!job || !running) return false;
      const handler = job.destination_kind === "queue"
        ? workers.get(job.queue_name)!
        : subscriptions.get(job.topic!)!.get(job.queue_name)!;
      // The claim predates the savepoint, so rollback keeps its lock through retry bookkeeping.
      await client.query("SAVEPOINT handler");
      try {
        await inContext(client, async (ctx) => handler(ctx, job.payload_json));
        await store.complete(client, job.id);
        await client.query("RELEASE SAVEPOINT handler");
      } catch (error) {
        await client.query("ROLLBACK TO SAVEPOINT handler");
        await store.fail(client, job.id, error);
        console.error("Delivery failed", { destination: job.destination_kind, name: job.queue_name, jobId: job.id, error });
      }
      return true;
    });
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
      subscriptionOptions: SubscriptionOptions,
    ): void {
      ensureDeclaring();
      requireName(topic, "Event topic");
      const name = subscriptionOptions?.name;
      requireName(name, "Subscription");
      const entries = subscriptions.get(topic) ?? new Map<string, Handler>();
      if (entries.has(name)) {
        throw new Error(`Duplicate subscription name on ${topic}: ${name}`);
      }
      entries.set(name, (ctx, payload) => handler(ctx, payload as Payload));
      subscriptions.set(topic, entries);
      bindings.push({ topic, name });
    },
    worker<Payload = Record<string, unknown>>(queue: string, handler: Handler<Payload>): void {
      ensureDeclaring();
      requireName(queue, "Queue");
      if (workers.has(queue)) throw new Error(`Duplicate worker for queue: ${queue}`);
      workers.set(queue, (ctx, payload) => handler(ctx, payload as Payload));
    },
    async start(startOptions: StartOptions = {}): Promise<void> {
      ensureOpen();
      if (stopPromise) throw new Error("Simplebase is stopped");
      if (!startPromise) {
        beforeStop = startOptions.beforeStop;
        if (startOptions.signals !== false) {
          for (const signal of signals) process.once(signal, onSignal);
        }
        startPromise = store.bindSubscriptions(bindings).then(() => {
          ensureOpen();
          if (stopPromise) throw new Error("Simplebase is stopped");
          running = true;
          if (workers.size || bindings.length) loop = workerLoop();
        }).catch((error) => {
          removeSignalHandlers();
          startPromise = undefined;
          throw error;
        });
      }
      await startPromise;
    },
    stop,
  };
}

export type Simplebase = Awaited<ReturnType<typeof createSimplebase>>;
