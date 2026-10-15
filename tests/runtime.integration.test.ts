import { afterAll, afterEach, beforeAll, describe, expect, test } from "bun:test";
import { createSimplebase, type Simplebase, type HandlerContext } from "@marcelsud/simplebase";
import { dispatchEvent, type LocalEvent } from "../src/runtime.ts";
import { MAX_ATTEMPTS } from "../src/postgres.ts";
import { cleanupFixtures, createFixture, startDatabase, stopDatabase, waitUntil, type Fixture } from "./database.ts";

beforeAll(startDatabase, 20_000);
afterEach(cleanupFixtures);
afterAll(stopDatabase);

function customerAction(runtime: Simplebase) {
  const { action, v } = runtime;
  return action({
    args: v.object({ email: v.string(), name: v.string(), plan: v.string() }),
    async handler(ctx, input) {
      const [customer] = await ctx.db.query<{ id: number }>(
        "INSERT INTO customers (email, name, plan) VALUES ($1, $2, $3) RETURNING id",
        [input.email, input.name, input.plan],
      );
      ctx.pubsub.publish("customer.created", { customerId: customer.id, email: input.email });
      return customer;
    },
  });
}

const customerInput = { email: "hello@example.com", name: "Hello World", plan: "starter" };

function attachSync(runtime: Simplebase): void {
  const { subscription, worker } = runtime;
  subscription("customer.created", async (ctx, event) => {
    await ctx.queue.enqueue("customer.sync", event);
  }, { idempotent: true, name: "enqueueCustomerSync" });
  worker("customer.sync", async (ctx, event) => {
    await ctx.db.query("UPDATE customers SET synced_at = NOW(), sync_count = sync_count + 1 WHERE id = $1", [event.customerId]);
  });
}

async function expectSynced(fixture: Fixture, id: number): Promise<void> {
  await waitUntil(async () => (await fixture.rows<{ synced_at: Date | null }>(
    "SELECT synced_at FROM customers WHERE id = $1", [id],
  ))[0]?.synced_at instanceof Date);
  expect((await fixture.rows("SELECT sync_count FROM customers WHERE id = $1", [id]))[0].sync_count).toBe(1);
}

function crashWorker(fixture: Fixture, scenario: "before-commit" | "after-commit") {
  let resolve!: () => void;
  let reject!: (reason: Error) => void;
  const reached = new Promise<void>((ok, fail) => { resolve = ok; reject = fail; });
  const timer = setTimeout(() => reject(new Error(`Worker never reached ${scenario}`)), 10_000);
  let observed = false;
  const child = Bun.spawn([process.execPath, import.meta.dir + "/fixtures/worker-process.ts", scenario], {
    env: { ...process.env, DATABASE_URL: fixture.url },
    stdout: "ignore",
    stderr: "pipe",
    ipc(message) {
      if (message === scenario) {
        observed = true;
        clearTimeout(timer);
        resolve();
      }
    },
  });
  void child.exited.then(async (code) => {
    clearTimeout(timer);
    if (!observed) reject(new Error(`Worker exited with ${code}: ${await new Response(child.stderr).text()}`));
  });
  return {
    reached,
    async kill() {
      clearTimeout(timer);
      child.kill("SIGKILL");
      await child.exited;
    },
  };
}

describe("core runtime acceptance", () => {
  test("A1: root import, destructured factories, and actions before and after start", async () => {
    const fixture = await createFixture();
    const runtime = await fixture.runtime();
    const { start, stop } = runtime;
    attachSync(runtime);
    const createCustomer = customerAction(runtime);
    const before = await createCustomer(customerInput);
    expect(typeof before.id).toBe("number");
    expect((await fixture.rows("SELECT status FROM _simplebase_queue_jobs"))[0].status).toBe("pending");
    await start();
    await start();
    const after = await createCustomer({ ...customerInput, email: "O'Reilly'); DROP TABLE customers;--" });
    await expectSynced(fixture, before.id);
    await expectSynced(fixture, after.id);
    expect((await fixture.rows("SELECT email FROM customers WHERE id = $1", [after.id]))[0].email).toBe("O'Reilly'); DROP TABLE customers;--");
    expect((await fixture.rows("SELECT status FROM _simplebase_queue_jobs")).every((job) => job.status === "completed")).toBe(true);
    expect((await fixture.rows("SELECT COUNT(*)::int AS count FROM information_schema.tables WHERE table_schema = current_schema() AND table_name LIKE '_simplebase_%'"))[0].count).toBe(1);
    await stop();
    await stop();
  });

  test("A2: invalid input and action rollback never dispatch subscriptions or persist jobs", async () => {
    const fixture = await createFixture();
    const runtime = await fixture.runtime();
    let deliveries = 0;
    runtime.subscription("customer.created", () => { deliveries += 1; });
    const createCustomer = customerAction(runtime);
    // @ts-expect-error Invalid input deliberately exercises the runtime boundary.
    await expect(createCustomer({ ...customerInput, email: 1 })).rejects.toThrow("args.email must be a string");
    const fail = runtime.action({
      args: runtime.v.object({}),
      async handler(ctx) {
        await ctx.db.query("INSERT INTO customers (email, name, plan) VALUES ('bad', 'bad', 'bad')");
        ctx.pubsub.publish("customer.created", { customerId: 1 });
        await ctx.queue.enqueue("customer.sync", { customerId: 1 });
        throw new Error("rollback action");
      },
    });
    await expect(fail({})).rejects.toThrow("rollback action");
    expect(deliveries).toBe(0);
    expect(await fixture.rows("SELECT * FROM customers")).toEqual([]);
    expect(await fixture.rows("SELECT * FROM _simplebase_queue_jobs")).toEqual([]);
  });

  test("A3: same-event delivery is serialized, skips committed subscriptions, and retries a rollback", async () => {
    const fixture = await createFixture();
    const runtime = await fixture.runtime();
    const invoke = async (handler: (ctx: HandlerContext, payload: unknown) => unknown, payload: unknown, committed: () => void) => {
      const deliver = runtime.action({ args: runtime.v.unknown(), handler });
      await deliver(payload);
      committed();
    };
    const event: LocalEvent = { topic: "customer.created", payload: { customerId: 1 }, delivered: new Set() };
    let attempts = 0;
    const successful = [{ name: "enqueueCustomerSync", idempotent: true, async handler(ctx: HandlerContext, payload: unknown) {
      attempts += 1;
      await ctx.queue.enqueue("customer.sync", payload);
    } }];
    await Promise.all([dispatchEvent(event, successful, invoke), dispatchEvent(event, successful, invoke)]);
    expect(attempts).toBe(1);
    expect(event.delivered.has("enqueueCustomerSync")).toBe(true);

    const retryEvent: LocalEvent = { topic: event.topic, payload: event.payload, delivered: new Set() };
    let shouldFail = true;
    const retrying = [{ name: "enqueueCustomerSync", idempotent: true, async handler(ctx: HandlerContext, payload: unknown) {
      await ctx.queue.enqueue("customer.sync", payload);
      if (shouldFail) throw new Error("subscription failed");
    } }];
    await expect(dispatchEvent(retryEvent, retrying, invoke)).rejects.toThrow("subscription failed");
    expect(retryEvent.delivered.size).toBe(0);
    expect((await fixture.rows("SELECT * FROM _simplebase_queue_jobs")).length).toBe(1);
    shouldFail = false;
    await dispatchEvent(retryEvent, retrying, invoke);
    await dispatchEvent(retryEvent, retrying, invoke);
    expect(retryEvent.delivered.has("enqueueCustomerSync")).toBe(true);
    expect((await fixture.rows("SELECT * FROM _simplebase_queue_jobs")).length).toBe(2);
    await dispatchEvent({ topic: event.topic, payload: event.payload, delivered: new Set() }, successful, invoke);
    expect((await fixture.rows("SELECT * FROM _simplebase_queue_jobs")).length).toBe(3);
  });

  test("subscription failure rejects after customer commit while its enqueue rolls back", async () => {
    const fixture = await createFixture();
    const runtime = await fixture.runtime();
    runtime.subscription("customer.created", async (ctx, event) => {
      await ctx.queue.enqueue("customer.sync", event);
      throw new Error("cannot dispatch");
    }, { idempotent: true, name: "enqueueCustomerSync" });
    await expect(customerAction(runtime)(customerInput)).rejects.toThrow("cannot dispatch");
    expect((await fixture.rows("SELECT * FROM customers")).length).toBe(1);
    expect(await fixture.rows("SELECT * FROM _simplebase_queue_jobs")).toEqual([]);
  });

  test("A4: pending jobs survive runtime replacement; queues without a worker stay pending", async () => {
    const fixture = await createFixture();
    const original = await fixture.runtime();
    original.subscription("customer.created", async (ctx, event) => {
      await ctx.queue.enqueue("unknown.queue", event);
      await ctx.queue.enqueue("customer.sync", event);
    });
    const customer = await customerAction(original)(customerInput);
    await original.stop();
    const replacement = await fixture.runtime();
    attachSync(replacement);
    await replacement.start();
    await expectSynced(fixture, customer.id);
    expect((await fixture.rows("SELECT status FROM _simplebase_queue_jobs WHERE queue_name = 'unknown.queue'"))[0].status).toBe("pending");
    expect((await fixture.rows("SELECT status FROM _simplebase_queue_jobs WHERE queue_name = 'customer.sync'"))[0].status).toBe("completed");
  });

  test("A5: worker errors roll back SQL, retry, and leave exhausted jobs failed", async () => {
    const fixture = await createFixture();
    const runtime = await fixture.runtime();
    runtime.subscription("customer.created", async (ctx, event) => { await ctx.queue.enqueue("customer.sync", event); });
    runtime.worker("customer.sync", async (ctx, event) => {
      await ctx.db.query("UPDATE customers SET synced_at = NOW(), sync_count = sync_count + 1 WHERE id = $1", [event.customerId]);
      throw new Error("sync unavailable");
    });
    await customerAction(runtime)(customerInput);
    await runtime.start();
    await waitUntil(async () => (await fixture.rows("SELECT status FROM _simplebase_queue_jobs"))[0]?.status === "failed");
    const [job] = await fixture.rows("SELECT * FROM _simplebase_queue_jobs");
    expect(job.attempt_count).toBe(MAX_ATTEMPTS);
    expect(job.last_error).toBe("sync unavailable");
    const [customer] = await fixture.rows("SELECT * FROM customers");
    expect(customer.synced_at).toBeNull();
    expect(customer.sync_count).toBe(0);
  });

  test("a worker can recover on a later attempt without repeating rolled-back SQL", async () => {
    const fixture = await createFixture();
    const runtime = await fixture.runtime();
    let calls = 0;
    runtime.subscription("customer.created", async (ctx, event) => { await ctx.queue.enqueue("customer.sync", event); });
    runtime.worker("customer.sync", async (ctx, event) => {
      calls += 1;
      await ctx.db.query("UPDATE customers SET synced_at = NOW(), sync_count = sync_count + 1 WHERE id = $1", [event.customerId]);
      if (calls === 1) throw new Error("temporary failure");
    });
    const customer = await customerAction(runtime)(customerInput);
    await runtime.start();
    await expectSynced(fixture, customer.id);
    const [job] = await fixture.rows("SELECT * FROM _simplebase_queue_jobs");
    expect(job.attempt_count).toBe(2);
    expect(job.last_error).toBeNull();
  });

  test("A5: killing a process inside the worker transaction releases its lock and rolls back effects", async () => {
    const fixture = await createFixture();
    const original = await fixture.runtime();
    original.subscription("customer.created", async (ctx, event) => { await ctx.queue.enqueue("customer.sync", event); });
    const customer = await customerAction(original)(customerInput);
    await original.stop();
    const child = crashWorker(fixture, "before-commit");
    try {
      await child.reached;
      expect((await fixture.rows("SELECT sync_count FROM customers"))[0].sync_count).toBe(0);
      expect((await fixture.rows("SELECT status FROM _simplebase_queue_jobs"))[0].status).toBe("pending");
    } finally {
      await child.kill();
    }
    const replacement = await fixture.runtime();
    attachSync(replacement);
    await replacement.start();
    await expectSynced(fixture, customer.id);
    expect((await fixture.rows("SELECT attempt_count FROM _simplebase_queue_jobs"))[0].attempt_count).toBe(1);
  });

  test("A6: killing a process after job commit preserves effects and never redelivers the completed job", async () => {
    const fixture = await createFixture();
    const original = await fixture.runtime();
    original.subscription("customer.created", async (ctx, event) => { await ctx.queue.enqueue("customer.sync", event); });
    const first = await customerAction(original)(customerInput);
    await original.stop();
    const child = crashWorker(fixture, "after-commit");
    try {
      await child.reached;
      expect((await fixture.rows("SELECT status FROM _simplebase_queue_jobs"))[0].status).toBe("completed");
      expect((await fixture.rows("SELECT sync_count FROM customers"))[0].sync_count).toBe(1);
    } finally {
      await child.kill();
    }
    const replacement = await fixture.runtime();
    attachSync(replacement);
    const second = await customerAction(replacement)({ ...customerInput, email: "second@example.com" });
    await replacement.start();
    await expectSynced(fixture, second.id);
    await expectSynced(fixture, first.id);
  });
});

describe("composition and lifecycle", () => {
  test("the standalone customer example runs through the package script", async () => {
    const fixture = await createFixture();
    const child = Bun.spawn([process.execPath, "run", "example"], {
      cwd: import.meta.dir + "/..",
      env: { ...process.env, DATABASE_URL: fixture.url },
      stdout: "pipe", stderr: "pipe",
    });
    const [output, errors, code] = await Promise.all([
      new Response(child.stdout).text(), new Response(child.stderr).text(), child.exited,
    ]);
    if (code !== 0) throw new Error(errors);
    expect(output).toContain("Customer created and synced");
    expect((await fixture.rows("SELECT synced_at FROM customers"))[0].synced_at).toBeInstanceOf(Date);
    expect((await fixture.rows("SELECT status FROM _simplebase_queue_jobs"))[0].status).toBe("completed");
  });

  test("post-commit event failures never retry a completed worker", async () => {
    const fixture = await createFixture();
    const runtime = await fixture.runtime();
    let deliveries = 0;
    let sentinelProcessed = false;
    runtime.subscription("customer.created", async (ctx, event) => { await ctx.queue.enqueue("customer.sync", event); });
    runtime.subscription("committed", () => { deliveries += 1; throw new Error("local delivery failed"); });
    runtime.worker("customer.sync", async (ctx, event) => {
      await ctx.db.query("UPDATE customers SET synced_at = NOW(), sync_count = sync_count + 1 WHERE id = $1", [event.customerId]);
      await ctx.queue.enqueue("sentinel", {});
      ctx.pubsub.publish("committed", event);
    });
    runtime.worker("sentinel", () => { sentinelProcessed = true; });
    const customer = await customerAction(runtime)(customerInput);
    await runtime.start();
    await waitUntil(async () => sentinelProcessed);
    await runtime.stop();
    await expectSynced(fixture, customer.id);
    expect(deliveries).toBe(1);
    const [job] = await fixture.rows("SELECT status, attempt_count FROM _simplebase_queue_jobs WHERE queue_name = 'customer.sync'");
    expect(job).toEqual({ status: "completed", attempt_count: 1 });
  });

  test("events are snapshots, handlers run in creation order, and subscription events dispatch after commit", async () => {
    const fixture = await createFixture();
    const runtime = await fixture.runtime();
    const deliveries: string[] = [];
    runtime.subscription("parent", async (ctx, event) => {
      deliveries.push("first");
      expect(event.value).toBe("original");
      event.value = "mutated";
      await ctx.db.query("INSERT INTO customers (email, name, plan) VALUES ('committed', 'Sam', 'pro')");
      ctx.pubsub.publish("child", { value: "child" });
    });
    runtime.subscription("child", async () => {
      expect((await fixture.rows("SELECT * FROM customers")).length).toBe(1);
      deliveries.push("child");
    });
    runtime.subscription("parent", (_ctx, event) => {
      expect(event.value).toBe("original");
      deliveries.push("second");
    });
    const publish = runtime.action({ args: runtime.v.object({}), handler(ctx) {
      const payload = { value: "original" };
      ctx.pubsub.publish("parent", payload);
      payload.value = "changed after publishing";
    } });
    await publish({});
    expect(deliveries).toEqual(["first", "child", "second"]);
  });

  test("worker SQL and enqueue roll back together and failed worker events do not dispatch", async () => {
    const fixture = await createFixture();
    const runtime = await fixture.runtime();
    let delivered = false;
    runtime.subscription("worker.event", () => { delivered = true; });
    runtime.worker("customer.sync", async (ctx, event) => {
      await ctx.queue.enqueue("another.queue", event);
      ctx.pubsub.publish("worker.event", event);
      await ctx.db.query("INSERT INTO table_that_does_not_exist VALUES (1)");
    });
    const enqueue = runtime.action({ args: runtime.v.object({}), async handler(ctx) { await ctx.queue.enqueue("customer.sync", {}); } });
    await enqueue({});
    await runtime.start();
    await waitUntil(async () => (await fixture.rows("SELECT attempt_count FROM _simplebase_queue_jobs WHERE queue_name = 'customer.sync'"))[0]?.attempt_count === 1);
    await runtime.stop();
    expect(delivered).toBe(false);
    expect(await fixture.rows("SELECT * FROM _simplebase_queue_jobs WHERE queue_name = 'another.queue'")).toEqual([]);
  });

  test("a caught SQL error cannot report a successful commit or dispatch its events", async () => {
    const fixture = await createFixture();
    const runtime = await fixture.runtime();
    let delivered = false;
    runtime.subscription("event", () => { delivered = true; });
    const invoke = runtime.action({ args: runtime.v.object({}), async handler(ctx) {
      try { await ctx.db.query("INSERT INTO missing_table VALUES (1)"); } catch {}
      ctx.pubsub.publish("event", {});
      return "success";
    } });
    await expect(invoke({})).rejects.toThrow("Transaction rolled back");
    expect(delivered).toBe(false);
  });

  test("stop waits for in-flight actions, closes storage, and rejects further work", async () => {
    const fixture = await createFixture();
    const runtime = await fixture.runtime();
    let proceed!: () => void;
    let entered!: () => void;
    const gate = new Promise<void>((resolve) => { proceed = resolve; });
    const ready = new Promise<void>((resolve) => { entered = resolve; });
    const invoke = runtime.action({ args: runtime.v.object({}), async handler(ctx) {
      entered();
      await gate;
      await ctx.queue.enqueue("later", {});
      return "done";
    } });
    const pending = invoke({});
    await ready;
    const stopped = runtime.stop();
    try {
      await expect(invoke({})).rejects.toThrow("stopped");
      await expect(runtime.start()).rejects.toThrow("stopped");
    } finally {
      proceed();
    }
    expect(await pending).toBe("done");
    await stopped;
    expect((await fixture.rows("SELECT * FROM _simplebase_queue_jobs")).length).toBe(1);
  });

  test("contexts expire and runtime definitions are isolated between instances", async () => {
    const fixture = await createFixture();
    const first = await fixture.runtime();
    const second = await fixture.runtime();
    let captured!: HandlerContext;
    let calls = 0;
    first.subscription("event", () => { calls += 1; });
    const firstAction = first.action({ args: first.v.object({}), handler(ctx) { captured = ctx; ctx.pubsub.publish("event", {}); } });
    const secondAction = second.action({ args: second.v.object({}), handler(ctx) { ctx.pubsub.publish("event", {}); } });
    await secondAction({});
    expect(calls).toBe(0);
    await firstAction({});
    expect(calls).toBe(1);
    await expect(captured.db.query("SELECT 1")).rejects.toThrow("no longer active");
    await expect(captured.queue.enqueue("job", {})).rejects.toThrow("no longer active");
    expect(() => captured.pubsub.publish("event", {})).toThrow("no longer active");
    expect(first.v).toBe(second.v);
  });

  test("invalid or duplicate handler names and non-JSON payloads fail before committing", async () => {
    const fixture = await createFixture();
    const runtime = await fixture.runtime();
    expect(() => runtime.worker("", async () => {})).toThrow("nonempty name");
    runtime.worker("one", async () => {});
    expect(() => runtime.worker("one", async () => {})).toThrow("Duplicate worker");
    runtime.subscription("event", async () => {}, { idempotent: true, name: "one" });
    expect(() => runtime.subscription("event", async () => {}, { idempotent: true, name: "one" })).toThrow("Duplicate subscription");
    // @ts-expect-error Runtime validation also protects JavaScript callers.
    expect(() => runtime.subscription("event", async () => {}, { idempotent: true })).toThrow("nonempty name");
    const bad = runtime.action({ args: runtime.v.object({}), async handler(ctx) {
      await ctx.db.query("INSERT INTO customers (email, name, plan) VALUES ('bad', 'bad', 'bad')");
      await ctx.queue.enqueue("one", { id: 1n });
    } });
    await expect(bad({})).rejects.toThrow("JSON values");
    expect(await fixture.rows("SELECT * FROM customers")).toEqual([]);
    await expect(createSimplebase({ storage: { engine: "postgres", url: "" } })).rejects.toThrow("requires a URL");
  });
});
