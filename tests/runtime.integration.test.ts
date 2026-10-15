import { afterAll, afterEach, beforeAll, expect, test } from "bun:test";
import { createSimplebase, type Simplebase, type HandlerContext } from "@marcelsud/simplebase";
import { MAX_ATTEMPTS } from "../src/postgres.ts";
import { cleanupFixtures, createFixture, startDatabase, stopDatabase, waitUntil, type Fixture } from "./database.ts";

beforeAll(startDatabase, 20_000);
afterEach(cleanupFixtures);
afterAll(stopDatabase);

function customerAction(runtime: Simplebase) {
  return runtime.action({
    args: runtime.schema.object({ email: runtime.schema.string(), name: runtime.schema.string(), plan: runtime.schema.string() }),
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
  runtime.subscription("customer.created", async (ctx, event) => {
    await ctx.queue.enqueue("customer.sync", event);
  }, { name: "enqueueCustomerSync" });
  runtime.worker("customer.sync", async (ctx, event) => {
    await ctx.db.query("UPDATE customers SET synced_at = NOW(), sync_count = sync_count + 1 WHERE id = $1", [event.customerId]);
  });
}

async function expectSynced(fixture: Fixture, id: number): Promise<void> {
  await waitUntil(async () => (await fixture.rows("SELECT sync_count FROM customers WHERE id = $1", [id]))[0]?.sync_count === 1);
}

test("root import, destructured factories, and actions before and after start", async () => {
  const fixture = await createFixture();
  const runtime = await fixture.runtime();
  const { start, stop } = runtime;
  expect(Object.hasOwn(runtime, "v")).toBe(false);
  attachSync(runtime);
  const createCustomer = customerAction(runtime);
  const before = await createCustomer(customerInput);
  expect(typeof before.id).toBe("number");
  expect(await fixture.rows("SELECT * FROM _simplebase_queue_jobs")).toEqual([]);
  await Promise.all([start(), start()]);
  const after = await createCustomer({ ...customerInput, email: "O'Reilly'); DROP TABLE customers;--" });
  await expectSynced(fixture, after.id);
  expect((await fixture.rows("SELECT sync_count FROM customers WHERE id = $1", [before.id]))[0].sync_count).toBe(0);
  expect((await fixture.rows("SELECT email FROM customers WHERE id = $1", [after.id]))[0].email).toBe("O'Reilly'); DROP TABLE customers;--");
  expect((await fixture.rows("SELECT status FROM _simplebase_queue_jobs")).every((job) => job.status === "completed")).toBe(true);
  expect((await fixture.rows("SELECT COUNT(*)::int AS count FROM information_schema.tables WHERE table_schema = current_schema() AND table_name LIKE '_simplebase_%'"))[0].count).toBe(3);
  await Promise.all([stop(), stop()]);
});

test("invalid input and action rollback persist neither application writes nor deliveries", async () => {
  const fixture = await createFixture();
  const runtime = await fixture.runtime();
  runtime.subscription("customer.created", () => {}, { name: "known" });
  await runtime.start();
  // @ts-expect-error Deliberately exercise runtime validation.
  await expect(customerAction(runtime)({ ...customerInput, email: 1 })).rejects.toThrow("args.email must be a string");
  const fail = runtime.action({
    args: runtime.schema.object({}),
    async handler(ctx) {
      await ctx.db.query("INSERT INTO customers (email, name, plan) VALUES ('bad', 'bad', 'bad')");
      ctx.pubsub.publish("customer.created", { customerId: 1 });
      await ctx.queue.enqueue("customer.sync", {});
      throw new Error("rollback action");
    },
  });
  await expect(fail({})).rejects.toThrow("rollback action");
  expect(await fixture.rows("SELECT * FROM customers")).toEqual([]);
  expect(await fixture.rows("SELECT * FROM _simplebase_queue_jobs")).toEqual([]);
});

test("subscription failures retry independently after the action commits", async () => {
  const fixture = await createFixture();
  const runtime = await fixture.runtime();
  runtime.subscription("customer.created", async (ctx, event) => {
    await ctx.queue.enqueue("customer.sync", event);
    throw new Error("cannot dispatch");
  }, { name: "enqueueCustomerSync" });
  await runtime.start();
  const customer = await customerAction(runtime)(customerInput);
  expect(customer.id).toBe(1);
  await waitUntil(async () => (await fixture.rows("SELECT status FROM _simplebase_queue_jobs"))[0]?.status === "failed");
  const [job] = await fixture.rows("SELECT * FROM _simplebase_queue_jobs");
  expect(job.destination_kind).toBe("subscription");
  expect(job.attempt_count).toBe(MAX_ATTEMPTS);
  expect(job.last_error).toBe("cannot dispatch");
  expect(await fixture.rows("SELECT * FROM _simplebase_queue_jobs WHERE destination_kind = 'queue'")).toEqual([]);
});

test("pending jobs survive replacement and undeclared queues remain pending", async () => {
  const fixture = await createFixture();
  const original = await fixture.runtime();
  const customer = await customerAction(original)(customerInput);
  await original.action({ args: original.schema.object({}), async handler(ctx) {
    await ctx.queue.enqueue("unknown.queue", customer);
    await ctx.queue.enqueue("customer.sync", { customerId: customer.id });
  } })({});
  await original.stop();
  const replacement = await fixture.runtime();
  attachSync(replacement);
  await replacement.start();
  await expectSynced(fixture, customer.id);
  expect((await fixture.rows("SELECT status FROM _simplebase_queue_jobs WHERE queue_name = 'unknown.queue'"))[0].status).toBe("pending");
});

test("worker failures roll back writes, jobs and publications, and exhaust retries", async () => {
  const fixture = await createFixture();
  const runtime = await fixture.runtime();
  runtime.subscription("customer.created", async (ctx, event) => { await ctx.queue.enqueue("customer.sync", event); }, { name: "enqueue" });
  runtime.subscription("worker.event", () => { throw new Error("must not run"); }, { name: "observe" });
  runtime.worker("customer.sync", async (ctx, event) => {
    await ctx.db.query("UPDATE customers SET sync_count = sync_count + 1 WHERE id = $1", [event.customerId]);
    await ctx.queue.enqueue("another.queue", event);
    ctx.pubsub.publish("worker.event", event);
    throw new Error("sync unavailable");
  });
  await runtime.start();
  await customerAction(runtime)(customerInput);
  await waitUntil(async () => (await fixture.rows("SELECT status FROM _simplebase_queue_jobs WHERE destination_kind = 'queue'"))[0]?.status === "failed");
  const [job] = await fixture.rows("SELECT * FROM _simplebase_queue_jobs WHERE destination_kind = 'queue'");
  expect(job.attempt_count).toBe(MAX_ATTEMPTS);
  expect(job.last_error).toBe("sync unavailable");
  expect((await fixture.rows("SELECT sync_count FROM customers"))[0].sync_count).toBe(0);
  expect((await fixture.rows("SELECT * FROM _simplebase_queue_jobs")).length).toBe(2);
});

test("a subscription can recover without repeating rolled-back SQL, jobs or publications", async () => {
  const fixture = await createFixture();
  const runtime = await fixture.runtime();
  let calls = 0;
  runtime.subscription("customer.created", async (ctx, event) => {
    calls += 1;
    await ctx.db.query("UPDATE customers SET sync_count = sync_count + 1 WHERE id = $1", [event.customerId]);
    await ctx.queue.enqueue("later", event);
    ctx.pubsub.publish("committed", event);
    if (calls === 1) throw new Error("temporary failure");
  }, { name: "retrying" });
  runtime.subscription("committed", () => {}, { name: "observe" });
  await runtime.start();
  const customer = await customerAction(runtime)(customerInput);
  await waitUntil(async () => (await fixture.rows("SELECT status FROM _simplebase_queue_jobs WHERE topic = 'committed'"))[0]?.status === "completed");
  await expectSynced(fixture, customer.id);
  const [job] = await fixture.rows("SELECT * FROM _simplebase_queue_jobs WHERE topic = 'customer.created'");
  expect(job.attempt_count).toBe(2);
  expect(job.last_error).toBeNull();
  expect((await fixture.rows("SELECT * FROM _simplebase_queue_jobs WHERE queue_name = 'later'")).length).toBe(1);
});

test("subscriber failure never retries a completed publishing worker", async () => {
  const fixture = await createFixture();
  const runtime = await fixture.runtime();
  attachSync(runtime);
  runtime.subscription("worker.done", () => { throw new Error("subscriber unavailable"); }, { name: "observe" });
  runtime.worker("publisher", (ctx) => { ctx.pubsub.publish("worker.done", {}); });
  await runtime.start();
  await runtime.action({ args: runtime.schema.object({}), async handler(ctx) { await ctx.queue.enqueue("publisher", {}); } })({});
  await waitUntil(async () => (await fixture.rows("SELECT attempt_count FROM _simplebase_queue_jobs WHERE topic = 'worker.done'"))[0]?.attempt_count >= 1);
  await runtime.stop();
  expect((await fixture.rows("SELECT status, attempt_count FROM _simplebase_queue_jobs WHERE destination_kind = 'queue'"))[0]).toEqual({ status: "completed", attempt_count: 1 });
});

test("publications are snapshots and chained subscriptions observe committed effects", async () => {
  const fixture = await createFixture();
  const runtime = await fixture.runtime();
  const deliveries: string[] = [];
  runtime.subscription("parent", async (ctx, event) => {
    expect(event.value).toBe("original");
    event.value = "mutated";
    await ctx.db.query("INSERT INTO customers (email, name, plan) VALUES ('committed', 'Sam', 'pro')");
    ctx.pubsub.publish("child", {});
    deliveries.push("first");
  }, { name: "first" });
  runtime.subscription("child", async () => {
    expect((await fixture.rows("SELECT * FROM customers")).length).toBe(1);
    deliveries.push("child");
  }, { name: "child" });
  runtime.subscription("parent", (_ctx, event) => {
    expect(event.value).toBe("original");
    deliveries.push("second");
  }, { name: "second" });
  await runtime.start();
  await runtime.action({ args: runtime.schema.object({}), handler(ctx) {
    const payload = { value: "original" };
    ctx.pubsub.publish("parent", payload);
    payload.value = "changed after publishing";
  } })({});
  await waitUntil(async () => deliveries.length === 3);
  expect(deliveries.sort()).toEqual(["child", "first", "second"]);
});

test("a caught SQL error cannot report a successful action commit", async () => {
  const fixture = await createFixture();
  const runtime = await fixture.runtime();
  runtime.subscription("event", () => {}, { name: "observe" });
  await runtime.start();
  const invoke = runtime.action({ args: runtime.schema.object({}), async handler(ctx) {
    try { await ctx.db.query("INSERT INTO missing_table VALUES (1)"); } catch {}
    ctx.pubsub.publish("event", {});
    return "success";
  } });
  await expect(invoke({})).rejects.toThrow();
  expect(await fixture.rows("SELECT * FROM _simplebase_queue_jobs")).toEqual([]);
});

test("stop drains in-flight actions and deliveries and refuses new work", async () => {
  const fixture = await createFixture();
  const runtime = await fixture.runtime();
  let proceed!: () => void;
  let entered!: () => void;
  const gate = new Promise<void>((resolve) => { proceed = resolve; });
  const ready = new Promise<void>((resolve) => { entered = resolve; });
  runtime.worker("later", () => { throw new Error("must not claim during shutdown"); });
  runtime.worker("drain", async (ctx) => {
    entered();
    await gate;
    await ctx.queue.enqueue("later", {});
  });
  await runtime.start();
  const enqueue = runtime.action({ args: runtime.schema.object({}), async handler(ctx) { await ctx.queue.enqueue("drain", {}); } });
  await enqueue({});
  await ready;
  const stopped = runtime.stop();
  try {
    await expect(enqueue({})).rejects.toThrow("stopped");
    await expect(runtime.start()).rejects.toThrow("stopped");
  } finally { proceed(); }
  await stopped;
  expect((await fixture.rows("SELECT status FROM _simplebase_queue_jobs WHERE queue_name = 'drain'"))[0].status).toBe("completed");
  expect((await fixture.rows("SELECT status FROM _simplebase_queue_jobs WHERE queue_name = 'later'"))[0].status).toBe("pending");

  const other = await fixture.runtime();
  let release!: () => void;
  const actionGate = new Promise<void>((resolve) => { release = resolve; });
  const invoke = other.action({ args: other.schema.object({}), async handler(ctx) {
    await actionGate;
    await ctx.queue.enqueue("after-stop", {});
  } });
  const pending = invoke({});
  const closed = other.stop();
  release();
  await Promise.all([pending, closed]);
  expect((await fixture.rows("SELECT * FROM _simplebase_queue_jobs WHERE queue_name = 'after-stop'")).length).toBe(1);
});

test("contexts expire; publishers use durable bindings from another runtime", async () => {
  const fixture = await createFixture();
  const consumer = await fixture.runtime();
  const publisher = await fixture.runtime();
  let captured!: HandlerContext;
  let calls = 0;
  consumer.subscription("event", () => { calls += 1; }, { name: "observe" });
  await consumer.start();
  await publisher.action({ args: publisher.schema.object({}), handler(ctx) { captured = ctx; ctx.pubsub.publish("event", {}); } })({});
  await waitUntil(async () => calls === 1);
  await expect(captured.db.query("SELECT 1")).rejects.toThrow("no longer active");
  await expect(captured.queue.enqueue("job", {})).rejects.toThrow("no longer active");
  expect(() => captured.pubsub.publish("event", {})).toThrow("no longer active");
  expect(consumer.schema).toBe(publisher.schema);
});

test("start installs signal handlers once, supports opt-out and preserves host listeners", async () => {
  const fixture = await createFixture();
  const hostListener = () => {};
  process.on("SIGTERM", hostListener);
  const before = [process.listenerCount("SIGINT"), process.listenerCount("SIGTERM")];
  try {
    const managed = await fixture.runtime();
    const unmanaged = await fixture.runtime();
    await unmanaged.start({ signals: false });
    expect([process.listenerCount("SIGINT"), process.listenerCount("SIGTERM")]).toEqual(before);
    let calls = 0;
    await managed.start({ beforeStop: () => { calls += 1; } });
    await managed.start();
    expect([process.listenerCount("SIGINT"), process.listenerCount("SIGTERM")]).toEqual(before.map((count) => count + 1));
    const stopped = managed.stop();
    expect(managed.stop()).toBe(stopped);
    await stopped;
    expect(calls).toBe(1);
    expect([process.listenerCount("SIGINT"), process.listenerCount("SIGTERM")]).toEqual(before);
    expect(process.listeners("SIGTERM")).toContain(hostListener);
  } finally { process.off("SIGTERM", hostListener); }
});

test("beforeStop lets an HTTP request invoke its action while the server drains", async () => {
  const fixture = await createFixture();
  const runtime = await fixture.runtime();
  let release!: () => void;
  let entered!: () => void;
  let stopping!: () => void;
  const gate = new Promise<void>((resolve) => { release = resolve; });
  const ready = new Promise<void>((resolve) => { entered = resolve; });
  const shutdown = new Promise<void>((resolve) => { stopping = resolve; });
  const action = runtime.action({ args: runtime.schema.object({}), async handler(ctx) {
    await ctx.queue.enqueue("http.request", {});
    return "committed";
  } });
  const server = Bun.serve({ port: 0, async fetch() {
    entered();
    await gate;
    return new Response(await action({}));
  } });
  try {
    await runtime.start({ beforeStop: async () => { stopping(); await server.stop(); } });
    const response = fetch(server.url);
    await ready;
    const stopped = runtime.stop();
    await shutdown;
    await expect(runtime.start()).rejects.toThrow("stopped");
    release();
    expect(await (await response).text()).toBe("committed");
    await stopped;
    await expect(action({})).rejects.toThrow("stopped");
    expect((await fixture.rows("SELECT queue_name FROM _simplebase_queue_jobs"))[0].queue_name).toBe("http.request");
  } finally {
    release();
    await server.stop(true);
  }
});

test("a failing beforeStop still drains and closes storage", async () => {
  const fixture = await createFixture();
  const runtime = await createSimplebase({ storage: { engine: "postgres", url: fixture.url } });
  const before = [process.listenerCount("SIGINT"), process.listenerCount("SIGTERM")];
  const pid = runtime.action({ args: runtime.schema.object({}), async handler(ctx) {
    return (await ctx.db.query<{ pid: number }>("SELECT pg_backend_pid() AS pid"))[0].pid;
  } });
  await runtime.start({ beforeStop: () => { throw new Error("cleanup failed"); } });
  const backend = await pid({});
  await expect(runtime.stop()).rejects.toThrow("cleanup failed");
  await expect(pid({})).rejects.toThrow("stopped");
  await waitUntil(async () => (await fixture.rows("SELECT pid FROM pg_stat_activity WHERE pid = $1", [backend])).length === 0);
  expect([process.listenerCount("SIGINT"), process.listenerCount("SIGTERM")]).toEqual(before);
});

test("startup failure and stop during startup remove signal handlers", async () => {
  const fixture = await createFixture();
  const runtime = await fixture.runtime();
  const before = [process.listenerCount("SIGINT"), process.listenerCount("SIGTERM")];
  await fixture.rows("DROP TABLE _simplebase_subscriptions");
  await expect(runtime.start()).rejects.toThrow("does not exist");
  expect([process.listenerCount("SIGINT"), process.listenerCount("SIGTERM")]).toEqual(before);
  await fixture.rows("CREATE TABLE _simplebase_subscriptions (topic TEXT NOT NULL, name TEXT NOT NULL, PRIMARY KEY (topic, name))");
  const starting = runtime.start();
  const stopped = runtime.stop();
  await expect(starting).rejects.toThrow("stopped");
  await stopped;
  expect([process.listenerCount("SIGINT"), process.listenerCount("SIGTERM")]).toEqual(before);
});

test("consumer declarations require unique names before start; non-JSON payloads roll back", async () => {
  const fixture = await createFixture();
  const runtime = await fixture.runtime();
  expect(() => runtime.worker("", () => {})).toThrow("nonempty name");
  runtime.worker("one", () => {});
  expect(() => runtime.worker("one", () => {})).toThrow("Duplicate worker");
  runtime.subscription("event", () => {}, { name: "one" });
  runtime.subscription("other-event", () => {}, { name: "one" });
  expect(() => runtime.subscription("event", () => {}, { name: "one" })).toThrow("Duplicate subscription");
  // @ts-expect-error JavaScript callers must also supply options.
  expect(() => runtime.subscription("event", () => {})).toThrow("nonempty name");
  expect(() => runtime.subscription("event", () => {}, { name: " " })).toThrow("nonempty name");
  const bad = runtime.action({ args: runtime.schema.object({}), async handler(ctx) {
    await ctx.db.query("INSERT INTO customers (email, name, plan) VALUES ('bad', 'bad', 'bad')");
    await ctx.queue.enqueue("one", { id: 1n });
  } });
  await expect(bad({})).rejects.toThrow("JSON values");
  const badEvent = runtime.action({ args: runtime.schema.object({}), handler(ctx) { ctx.pubsub.publish("event", { value: undefined }); } });
  await expect(badEvent({})).rejects.toThrow("JSON values");
  expect(await fixture.rows("SELECT * FROM customers")).toEqual([]);
  const starting = runtime.start();
  expect(() => runtime.worker("late", () => {})).toThrow("before calling start");
  expect(() => runtime.subscription("late", () => {}, { name: "late" })).toThrow("before calling start");
  await starting;
  await expect(createSimplebase({ storage: { engine: "postgres", url: "" } })).rejects.toThrow("requires a URL");
});

test("the combined customer example runs through the package script", async () => {
  const fixture = await createFixture();
  const child = Bun.spawn([process.execPath, "run", "example"], {
    cwd: import.meta.dir + "/..", env: { ...process.env, DATABASE_URL: fixture.url }, stdout: "pipe", stderr: "pipe",
  });
  const [output, errors, code] = await Promise.all([new Response(child.stdout).text(), new Response(child.stderr).text(), child.exited]);
  if (code !== 0) throw new Error(errors);
  expect(output).toContain("Customer created and synced");
  expect((await fixture.rows("SELECT synced_at FROM customers"))[0].synced_at).toBeInstanceOf(Date);
  expect((await fixture.rows("SELECT status FROM _simplebase_queue_jobs")).every((job) => job.status === "completed")).toBe(true);
});
