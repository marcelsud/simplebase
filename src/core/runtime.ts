import {
  requireName, ValidationError, type CoreContext, type Logger, type Parser,
  type ExecutionScope, type ExecutionState,
  type Store, type SubscriptionBinding, type SubscriptionOptions, type Transaction,
} from "./contracts.js";
import { encodePayload } from "./payload.js";

const POLL_INTERVAL_MS = 250;
type Handler<Context, Payload = unknown> = (ctx: Context, payload: Payload) => unknown | Promise<unknown>;

export function createRuntime<Context extends CoreContext>(
  store: Store,
  extendContext: (ctx: CoreContext, ensureActive: () => void) => Context,
  logger: Logger,
  executions: ExecutionScope<ExecutionState<Context>>,
) {
  const subscriptions = new Map<string, Map<string, Handler<Context>>>();
  const bindings: SubscriptionBinding[] = [];
  const workers = new Map<string, Handler<Context>>();
  const actions = new Set<Promise<unknown>>();
  let closing = false;
  let declaring = true;
  let running = false;
  let loop: Promise<void> | undefined;
  let wake: (() => void) | undefined;

  function ensureOpen(): void {
    if (closing) throw new Error("Simplebase is stopped");
  }

  function ensureDeclaring(): void {
    ensureOpen();
    if (!declaring) throw new Error("Declare consumers before calling start()");
  }

  async function inContext<T>(transaction: Transaction, handler: (ctx: Context) => Promise<T>): Promise<T> {
    const events: { topic: string; payloadJson: string }[] = [];
    let active = true;
    function ensureActive(): void {
      if (!active) throw new Error("Handler context is no longer active");
    }
    const ctx: CoreContext = {
      db: {
        async query<T>(sql: string, params: readonly unknown[] = []): Promise<T[]> {
          ensureActive();
          return transaction.query<T>(sql, params);
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
          await transaction.enqueue(queue, encodePayload(payload));
        },
      },
      log: { info: (message, fields) => logger.info(message, fields) },
    };
    try {
      const execution: ExecutionState<Context> = {
        ctx: extendContext(ctx, ensureActive), ensureActive, rollbackOnly: false,
      };
      const result = await executions.run(execution, () => handler(execution.ctx));
      if (execution.rollbackOnly) throw new Error("Transaction aborted by a nested action");
      active = false;
      for (const event of events) await transaction.publish(event.topic, event.payloadJson);
      return result;
    } finally {
      active = false;
    }
  }

  async function processJob(): Promise<boolean> {
    return store.transaction(async (transaction) => {
      if (!running) return false;
      const job = await transaction.nextJob([...workers.keys()], bindings);
      if (!job || !running) return false;
      const handler = job.destination_kind === "queue"
        ? workers.get(job.queue_name)!
        : subscriptions.get(job.topic!)!.get(job.queue_name)!;
      await transaction.attempt(async () => {
        await inContext(transaction, async (ctx) => handler(ctx, job.payload_json));
        await transaction.complete(job.id);
      }, async (error) => {
        await transaction.fail(job.id, error);
        logger.error("Delivery failed", { destination: job.destination_kind, name: job.queue_name, jobId: job.id, error });
      });
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
        logger.error("Worker loop error", { error });
      }
      if (running) await waitForPoll();
    }
  }

  return {
    ensureOpen,
    ensureDeclaring,
    prepare(): Promise<void> {
      ensureOpen();
      declaring = false;
      return store.bindSubscriptions(bindings);
    },
    resetStartup(): void { declaring = true; },
    run(): void {
      ensureOpen();
      running = true;
      if (workers.size || bindings.length) loop = workerLoop();
    },
    stopClaims(): void { running = false; wake?.(); },
    closeActions(): void { closing = true; },
    async drain(): Promise<void> {
      await loop;
      await Promise.allSettled([...actions]);
    },
    action<Input, Result>(definition: {
      args: Parser<Input>;
      handler: (ctx: Context, input: Input) => Result | Promise<Result>;
    }): (input: Input) => Promise<Awaited<Result>> {
      ensureOpen();
      return async (input): Promise<Awaited<Result>> => {
        const current = executions.get();
        if (current) current.ensureActive();
        else ensureOpen();
        try {
          let parsed: Input;
          try { parsed = definition.args.parse(input, "args"); }
          catch (error) { throw new ValidationError(error instanceof Error ? error.message : String(error)); }
          if (current) return await definition.handler(current.ctx, parsed) as Awaited<Result>;
          const invocation = store.transaction((transaction) => inContext(transaction, async (ctx) => definition.handler(ctx, parsed)));
          actions.add(invocation);
          try {
            return await invocation as Awaited<Result>;
          } finally {
            actions.delete(invocation);
          }
        } catch (error) {
          if (current) current.rollbackOnly = true;
          throw error;
        }
      };
    },
    subscription<Payload = Record<string, unknown>>(
      topic: string,
      handler: Handler<Context, Payload>,
      options: SubscriptionOptions,
    ): void {
      ensureDeclaring();
      requireName(topic, "Event topic");
      const name = options?.name;
      requireName(name, "Subscription");
      const entries = subscriptions.get(topic) ?? new Map<string, Handler<Context>>();
      if (entries.has(name)) throw new Error(`Duplicate subscription name on ${topic}: ${name}`);
      entries.set(name, (ctx, payload) => handler(ctx, payload as Payload));
      subscriptions.set(topic, entries);
      bindings.push({ topic, name });
    },
    worker<Payload = Record<string, unknown>>(queue: string, handler: Handler<Context, Payload>): void {
      ensureDeclaring();
      requireName(queue, "Queue");
      if (workers.has(queue)) throw new Error(`Duplicate worker for queue: ${queue}`);
      workers.set(queue, (ctx, payload) => handler(ctx, payload as Payload));
    },
  };
}
