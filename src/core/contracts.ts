export const MAX_ATTEMPTS = 5;
export const RETRY_DELAY_MS = 1_000;

export class ValidationError extends Error {
  override name = "ValidationError";
}

export interface Parser<T> {
  parse(value: unknown, path?: string): T;
}

export interface Database {
  query<T = Record<string, unknown>>(sql: string, params?: readonly unknown[]): Promise<T[]>;
}

export interface Logger {
  info(message: string, fields?: Record<string, unknown>): void;
  error(message: string, fields?: Record<string, unknown>): void;
}

export interface CoreContext {
  db: Database;
  pubsub: { publish(topic: string, payload: unknown): void };
  queue: { enqueue(queue: string, payload: unknown): Promise<void> };
  log: Pick<Logger, "info">;
}

export interface ExecutionState<Context extends CoreContext> {
  ctx: Context;
  ensureActive(): void;
  rollbackOnly: boolean;
}

export interface ExecutionScope<Value> {
  get(): Value | undefined;
  run<Result>(value: Value, handler: () => Promise<Result>): Promise<Result>;
}

export interface Job {
  id: string;
  queue_name: string;
  destination_kind: "queue" | "subscription";
  topic: string | null;
  payload_json: unknown;
}

export interface SubscriptionOptions { name: string }
export interface SubscriptionBinding { topic: string; name: string }

export interface Transaction extends Database {
  publish(topic: string, payloadJson: string): Promise<void>;
  enqueue(queue: string, payloadJson: string): Promise<void>;
  nextJob(queues: string[], bindings: SubscriptionBinding[]): Promise<Job | undefined>;
  // A failed attempt rolls back handler effects while retaining the claim for onError.
  attempt(handler: () => Promise<void>, onError: (error: unknown) => Promise<void>): Promise<void>;
  complete(id: string): Promise<void>;
  fail(id: string, error: unknown): Promise<void>;
}

export interface Store {
  transaction<T>(handler: (transaction: Transaction) => Promise<T>): Promise<T>;
  bindSubscriptions(bindings: SubscriptionBinding[]): Promise<void>;
}

export function requireName(value: string, kind: string): void {
  if (typeof value !== "string" || !value.trim()) throw new Error(`${kind} requires a nonempty name`);
}
