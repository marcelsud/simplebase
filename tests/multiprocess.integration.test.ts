import { afterAll, afterEach, beforeAll, expect, test } from "bun:test";
import { cleanupFixtures, createFixture, startDatabase, stopDatabase, waitUntil, type Fixture } from "./database.ts";
import { MAX_ATTEMPTS } from "../src/postgres.ts";
import { Pool } from "pg";

beforeAll(startDatabase, 20_000);
const processes: { kill(): Promise<void> }[] = [];
afterEach(async () => {
  await Promise.all(processes.splice(0).map((child) => child.kill()));
  await cleanupFixtures();
});
afterAll(stopDatabase);

interface Message { type: string; requestId?: number; customerId?: number; message?: string }

function spawnRuntime(fixture: Fixture, role: string, scenario = "normal", name = "enqueueCustomerSync") {
  const messages: Message[] = [];
  let exited = false;
  const child = Bun.spawn([process.execPath, import.meta.dir + "/fixtures/worker-process.ts", role, scenario, name], {
    env: { ...process.env, DATABASE_URL: fixture.url }, stdout: "ignore", stderr: "pipe",
    ipc(message: Message) { messages.push(message); },
  });
  const errors = new Response(child.stderr).text();
  void child.exited.then(() => { exited = true; });
  let requestId = 0;
  const processHandle = {
    send(message: Message & { email?: string }) { child.send(message); },
    async wait(type: string, id?: number): Promise<Message> {
      let found: Message | undefined;
      await waitUntil(async () => {
        found = messages.find((message) => message.type === type && (id === undefined || message.requestId === id));
        if (!found && exited) throw new Error(`Process exited before ${type}: ${await errors}`);
        return found !== undefined;
      });
      return found!;
    },
    async start() {
      await this.wait("booted");
      child.send({ type: "initialize" });
      await this.wait("ready");
      return this;
    },
    async publish(email = "test@example.com"): Promise<number> {
      const id = ++requestId;
      child.send({ type: "publish", requestId: id, email });
      return (await this.wait("published", id)).customerId!;
    },
    async kill() {
      if (!exited) child.kill("SIGKILL");
      await child.exited;
      await errors;
    },
    signal(signal: "SIGINT" | "SIGTERM") { child.kill(signal); },
    async exit(): Promise<number> { return child.exited; },
    async stop() {
      child.send({ type: "stop" });
      await this.wait("stopped");
      await child.exited;
    },
  };
  processes.push(processHandle);
  return processHandle;
}

async function setup(): Promise<Fixture> {
  const fixture = await createFixture();
  await fixture.rows("CREATE TABLE delivery_effects (kind TEXT NOT NULL, customer_id INTEGER NOT NULL)");
  return fixture;
}

async function synced(fixture: Fixture, id: number, count = 1): Promise<void> {
  await waitUntil(async () => (await fixture.rows("SELECT sync_count FROM customers WHERE id = $1", [id]))[0]?.sync_count === count);
}

async function completed(fixture: Fixture, kind: "queue" | "subscription", count: number): Promise<void> {
  await waitUntil(async () => (await fixture.rows(`
    SELECT count(*)::int AS count FROM _simplebase_queue_jobs
    WHERE destination_kind = $1 AND status = 'completed'
  `, [kind]))[0].count === count);
}

test("three processes run the flow, then replicas share work and distinct subscription names fan out", async () => {
  const fixture = await setup();
  const subscription = await spawnRuntime(fixture, "subscription").start();
  await spawnRuntime(fixture, "worker").start();
  const publisher = await spawnRuntime(fixture, "application").start();
  const first = await publisher.publish();
  await completed(fixture, "queue", 1);
  await synced(fixture, first);
  expect((await fixture.rows("SELECT count(*)::int AS count FROM delivery_effects"))[0].count).toBe(2);

  await Promise.all([spawnRuntime(fixture, "subscription").start(), spawnRuntime(fixture, "worker").start()]);
  const publisherReplica = await spawnRuntime(fixture, "application").start();
  const ids = await Promise.all(Array.from({ length: 8 }, (_, index) =>
    (index % 2 ? publisher : publisherReplica).publish(`replica${index}@example.com`)));
  await completed(fixture, "queue", 9);
  await Promise.all(ids.map((id) => synced(fixture, id)));
  expect(await fixture.rows("SELECT kind, customer_id, count(*) FROM delivery_effects GROUP BY kind, customer_id HAVING count(*) <> 1")).toEqual([]);

  await spawnRuntime(fixture, "subscription", "normal", "auditSync").start();
  const fanout = await publisher.publish("fanout@example.com");
  await completed(fixture, "queue", 11);
  await synced(fixture, fanout, 2);
  const bindings = await fixture.rows("SELECT * FROM _simplebase_subscriptions ORDER BY name");
  expect(bindings.map((binding) => binding.name)).toEqual(["auditSync", "enqueueCustomerSync"]);
  await subscription.stop();
});

test.each(["subscription", "worker"])("%s replicas skip a locked delivery and process another concurrently", async (role) => {
  const fixture = await setup();
  const first = spawnRuntime(fixture, role, "before-commit");
  const second = spawnRuntime(fixture, role, "before-commit");
  await Promise.all([first.start(), second.start()]);
  const publisher = await spawnRuntime(fixture, "application", role === "worker" ? "direct-job" : "normal").start();
  const ids = [await publisher.publish(), await publisher.publish()];
  const entered = await Promise.all([first.wait("entered"), second.wait("entered")]);
  expect(entered.map((message) => message.customerId).sort()).toEqual(ids.sort());
  expect(await fixture.rows("SELECT * FROM delivery_effects")).toEqual([]);
  first.send({ type: "release", customerId: entered[0].customerId });
  second.send({ type: "release", customerId: entered[1].customerId });
  await completed(fixture, role === "worker" ? "queue" : "subscription", 2);
  expect((await fixture.rows("SELECT * FROM delivery_effects")).length).toBe(2);
});

test("publisher rollback and death before commit leave no writes, jobs or deliveries", async () => {
  const fixture = await setup();
  const subscriber = await spawnRuntime(fixture, "subscription").start();
  await subscriber.stop();
  const rollback = await spawnRuntime(fixture, "application", "rollback").start();
  rollback.send({ type: "publish", requestId: 1 });
  expect((await rollback.wait("error", 1)).message).toContain("publisher rollback");
  const crashed = await spawnRuntime(fixture, "application", "before-commit").start();
  crashed.send({ type: "publish", requestId: 1 });
  await crashed.wait("entered");
  await crashed.kill();
  expect(await fixture.rows("SELECT * FROM customers")).toEqual([]);
  expect(await fixture.rows("SELECT * FROM _simplebase_queue_jobs")).toEqual([]);
});

test("publisher death after commit preserves deliveries across subscriber downtime and restart", async () => {
  const fixture = await setup();
  const original = await spawnRuntime(fixture, "subscription").start();
  await original.stop();
  const publisher = await spawnRuntime(fixture, "application").start();
  const id = await publisher.publish();
  await publisher.kill();
  expect((await fixture.rows("SELECT status FROM _simplebase_queue_jobs"))[0].status).toBe("pending");
  await spawnRuntime(fixture, "subscription").start();
  await spawnRuntime(fixture, "worker").start();
  await completed(fixture, "queue", 1);
  await synced(fixture, id);
  expect((await fixture.rows("SELECT * FROM _simplebase_subscriptions")).length).toBe(1);
});

test("a new subscription receives only future publications", async () => {
  const fixture = await setup();
  const publisher = await spawnRuntime(fixture, "application").start();
  const historical = await publisher.publish();
  expect(await fixture.rows("SELECT * FROM _simplebase_queue_jobs")).toEqual([]);
  await spawnRuntime(fixture, "subscription").start();
  await spawnRuntime(fixture, "worker").start();
  const future = await publisher.publish();
  await completed(fixture, "queue", 1);
  await synced(fixture, future);
  expect((await fixture.rows("SELECT sync_count FROM customers WHERE id = $1", [historical]))[0].sync_count).toBe(0);
  expect(await fixture.rows("SELECT * FROM delivery_effects WHERE customer_id = $1", [historical])).toEqual([]);
});

test.each(["subscription", "worker"])("killing a %s before commit rolls back effects and a replica retries", async (role) => {
  const fixture = await setup();
  const crashing = await spawnRuntime(fixture, role, "before-commit").start();
  const publisher = await spawnRuntime(fixture, "application", role === "worker" ? "direct-job" : "normal").start();
  const id = await publisher.publish();
  await crashing.wait("entered");
  expect(await fixture.rows("SELECT * FROM delivery_effects")).toEqual([]);
  expect(await fixture.rows("SELECT * FROM _simplebase_collections")).toEqual([]);
  await crashing.kill();
  expect(await fixture.rows("SELECT * FROM _simplebase_collections")).toEqual([]);
  expect((await fixture.rows("SELECT * FROM _simplebase_queue_jobs")).length).toBe(1);
  await spawnRuntime(fixture, role).start();
  await completed(fixture, role === "worker" ? "queue" : "subscription", 1);
  const [delivery] = await fixture.rows("SELECT attempt_count FROM _simplebase_queue_jobs ORDER BY id LIMIT 1");
  expect(delivery.attempt_count).toBe(1);
  expect((await fixture.rows("SELECT * FROM delivery_effects")).length).toBe(1);
  expect((await fixture.rows("SELECT data FROM _simplebase_collections"))[0].data).toEqual({
    customerId: id, kind: role === "worker" ? "worker" : "subscription:enqueueCustomerSync",
  });
  if (role === "worker") await synced(fixture, id);
});

test.each(["subscription", "worker"])("killing a %s after completion never repeats the completed effects", async (role) => {
  const fixture = await setup();
  const original = await spawnRuntime(fixture, role).start();
  const publisher = await spawnRuntime(fixture, "application", role === "worker" ? "direct-job" : "normal").start();
  const first = await publisher.publish();
  const kind = role === "worker" ? "queue" : "subscription";
  await completed(fixture, kind, 1);
  expect((await fixture.rows("SELECT * FROM _simplebase_collections")).length).toBe(1);
  await original.kill();
  await spawnRuntime(fixture, role).start();
  await publisher.publish();
  await completed(fixture, kind, 2);
  expect((await fixture.rows("SELECT * FROM delivery_effects WHERE customer_id = $1", [first])).length).toBe(1);
  expect((await fixture.rows("SELECT * FROM _simplebase_collections WHERE data->>'customerId' = $1", [String(first)])).length).toBe(1);
  expect((await fixture.rows("SELECT attempt_count FROM _simplebase_queue_jobs WHERE destination_kind = $1 AND status = 'completed'", [kind])).map((row) => row.attempt_count)).toEqual([1, 1]);
});

test.each(["subscription", "worker"])("%s failure bookkeeping and PostgreSQL retry time hold under replica contention", async (role) => {
  const fixture = await setup();
  await Promise.all([
    spawnRuntime(fixture, role, "caught-sql").start(),
    spawnRuntime(fixture, role, "caught-sql").start(),
  ]);
  const publisher = await spawnRuntime(fixture, "application", role === "worker" ? "direct-job" : "normal").start();
  await publisher.publish();
  await waitUntil(async () => (await fixture.rows("SELECT attempt_count FROM _simplebase_queue_jobs"))[0]?.attempt_count === 1);
  const [retry] = await fixture.rows(`SELECT attempt_count, status, last_error,
    available_at_ms - floor(extract(epoch FROM updated_at) * 1000)::bigint AS delay
    FROM _simplebase_queue_jobs`);
  expect(retry.status).toBe("pending");
  expect(retry.last_error).toContain("transaction is aborted");
  expect(Number(retry.delay)).toBeGreaterThanOrEqual(1000);
  await waitUntil(async () => (await fixture.rows("SELECT status FROM _simplebase_queue_jobs"))[0]?.status === "failed");
  const [failed] = await fixture.rows("SELECT attempt_count, last_error FROM _simplebase_queue_jobs");
  expect(failed.attempt_count).toBe(MAX_ATTEMPTS);
  expect(failed.last_error).toContain("transaction is aborted");
  expect(await fixture.rows("SELECT * FROM delivery_effects")).toEqual([]);
  expect(await fixture.rows("SELECT * FROM _simplebase_collections")).toEqual([]);
  expect((await fixture.rows("SELECT sync_count FROM customers"))[0].sync_count).toBe(0);
  expect((await fixture.rows("SELECT * FROM _simplebase_queue_jobs")).length).toBe(1);
});

test("concurrent initialization on an empty schema and repeat startup preserve pending work", async () => {
  const fixture = await setup();
  const children = Array.from({ length: 4 }, () => spawnRuntime(fixture, "subscription"));
  await Promise.all(children.map((child) => child.wait("booted")));
  children.forEach((child) => child.send({ type: "initialize" }));
  await Promise.all(children.map((child) => child.wait("ready")));
  expect((await fixture.rows("SELECT * FROM _simplebase_subscriptions")).length).toBe(1);
  await Promise.all(children.map((child) => child.stop()));
  const publisher = await spawnRuntime(fixture, "application").start();
  await publisher.publish();
  const before = await fixture.rows("SELECT * FROM _simplebase_queue_jobs");
  const idle = await spawnRuntime(fixture, "application").start();
  await idle.stop();
  expect(await fixture.rows("SELECT * FROM _simplebase_queue_jobs")).toEqual(before);
  await spawnRuntime(fixture, "subscription").start();
  await completed(fixture, "subscription", 1);
});

test("migration retains legacy worker jobs, states and metadata", async () => {
  const fixture = await setup();
  await fixture.rows("INSERT INTO customers (email, name, plan) VALUES ('legacy', 'Test', 'starter')");
  await fixture.rows(`CREATE TABLE _simplebase_queue_jobs (
    id BIGSERIAL PRIMARY KEY, queue_name TEXT NOT NULL, payload_json JSONB NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'completed', 'failed')),
    available_at_ms BIGINT NOT NULL, attempt_count INTEGER NOT NULL DEFAULT 0, last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), completed_at TIMESTAMPTZ
  )`);
  await fixture.rows(`INSERT INTO _simplebase_queue_jobs (queue_name, payload_json, status, available_at_ms, attempt_count, last_error)
    VALUES ('customer.sync', '{"customerId":1,"a":1}', 'pending', 0, 2, 'retry'),
      ('legacy', '{}', 'completed', 0, 1, NULL), ('legacy', '{}', 'failed', 0, 5, 'exhausted')`);
  const before = await fixture.rows("SELECT * FROM _simplebase_queue_jobs ORDER BY id");
  await Promise.all([spawnRuntime(fixture, "application").start(), spawnRuntime(fixture, "application").start()]);
  const after = await fixture.rows("SELECT * FROM _simplebase_queue_jobs ORDER BY id");
  expect(after.map(({ destination_kind, topic, ...job }) => job)).toEqual(before);
  expect(after.every((job) => job.destination_kind === "queue" && job.topic === null)).toBe(true);
  await spawnRuntime(fixture, "worker").start();
  await completed(fixture, "queue", 2);
  await synced(fixture, 1);
  expect((await fixture.rows("SELECT attempt_count FROM _simplebase_queue_jobs WHERE id = 1"))[0].attempt_count).toBe(3);
  expect((await fixture.rows("SELECT * FROM _simplebase_queue_jobs WHERE id IN (2, 3) ORDER BY id"))
    .map(({ destination_kind, topic, ...job }) => job)).toEqual(before.slice(1));
});

test("combined deployment uses the same durable path and queue/subscription names stay distinct", async () => {
  const fixture = await setup();
  const combined = await spawnRuntime(fixture, "combined", "normal", "customer.sync").start();
  const id = await combined.publish();
  await completed(fixture, "subscription", 1);
  await completed(fixture, "queue", 1);
  await synced(fixture, id);
  expect((await fixture.rows("SELECT destination_kind FROM _simplebase_queue_jobs WHERE queue_name = 'customer.sync' ORDER BY destination_kind")).map((row) => row.destination_kind)).toEqual(["queue", "subscription"]);
});

test("a failed handler retains its claim while failure bookkeeping is blocked", async () => {
  const fixture = await setup();
  await Promise.all([
    spawnRuntime(fixture, "worker", "caught-sql").start(),
    spawnRuntime(fixture, "worker", "caught-sql").start(),
  ]);
  const publisher = await spawnRuntime(fixture, "application", "direct-job").start();
  const pool = new Pool({ connectionString: fixture.url });
  const gate = await pool.connect();
  try {
    const { rows: [lock] } = await gate.query("SELECT pg_backend_pid() AS pid, hashtextextended(current_schema() || ':retry-test', 0)::text AS key");
    await gate.query("SELECT pg_advisory_lock($1::bigint)", [lock.key]);
    await fixture.rows(`CREATE FUNCTION block_failure() RETURNS trigger LANGUAGE plpgsql AS $$
      BEGIN
        IF NEW.last_error IS NOT NULL THEN
          PERFORM pg_advisory_xact_lock(hashtextextended(current_schema() || ':retry-test', 0));
        END IF;
        RETURN NEW;
      END
    $$`);
    await fixture.rows(`CREATE TRIGGER block_failure BEFORE UPDATE ON _simplebase_queue_jobs
      FOR EACH ROW EXECUTE FUNCTION block_failure()`);
    await publisher.publish();
    await waitUntil(async () => (await fixture.rows(`SELECT EXISTS (
      SELECT 1 FROM pg_stat_activity WHERE $1::int = ANY(pg_blocking_pids(pid))
    ) AS blocked`, [lock.pid]))[0].blocked);
    // Handler SQL has rolled back and its failure UPDATE is waiting at the trigger.
    expect(await fixture.rows("SELECT id FROM _simplebase_queue_jobs WHERE status = 'pending' FOR UPDATE SKIP LOCKED")).toEqual([]);
    expect((await fixture.rows("SELECT attempt_count FROM _simplebase_queue_jobs"))[0].attempt_count).toBe(0);
    expect(await fixture.rows("SELECT * FROM delivery_effects")).toEqual([]);
    await gate.query("SELECT pg_advisory_unlock($1::bigint)", [lock.key]);
    await waitUntil(async () => (await fixture.rows("SELECT attempt_count FROM _simplebase_queue_jobs"))[0]?.attempt_count === 1);
  } finally {
    await gate.query("SELECT pg_advisory_unlock_all()");
    gate.release();
    await pool.end();
  }
});

test("the three example entrypoints serve HTTP and share work with replicas", async () => {
  const fixture = await setup();
  const examples: { kill(): Promise<void> }[] = [];
  async function launch(file: string): Promise<string> {
    let exited = false;
    let output = "";
    const child = Bun.spawn([process.execPath, import.meta.dir + "/../examples/" + file + ".ts"], {
      env: { ...process.env, DATABASE_URL: fixture.url, PORT: "0" }, stdout: "pipe", stderr: "pipe",
    });
    const errors = new Response(child.stderr).text();
    const readOutput = (async () => {
      for await (const chunk of child.stdout) output += new TextDecoder().decode(chunk);
    })();
    void child.exited.then(() => { exited = true; });
    examples.push({ async kill() {
      if (!exited) child.kill("SIGTERM");
      await child.exited;
      await readOutput;
      await errors;
    } });
    await waitUntil(async () => {
      if (exited) throw new Error(`Example exited before readiness: ${await errors}`);
      return output.toLowerCase().includes("ready");
    });
    return output;
  }
  try {
    await launch("subscription");
    await launch("worker");
    const ready = await launch("application");
    const url = ready.match(/http:\/\/[^\s]+/)?.[0];
    if (!url) throw new Error("Application did not report its URL");
    const endpoint = new URL("customers", url);
    const response = await fetch(endpoint, { method: "POST", body: JSON.stringify({ email: "example@test.com", name: "Test", plan: "starter" }) });
    expect(response.status).toBe(201);
    await completed(fixture, "queue", 1);
    expect((await fixture.rows("SELECT synced_at FROM customers"))[0].synced_at).toBeInstanceOf(Date);
    await Promise.all([launch("subscription"), launch("worker"), launch("application")]);
    const next = await fetch(endpoint, { method: "POST", body: JSON.stringify({ email: "replica@test.com", name: "Test", plan: "starter" }) });
    expect(next.status).toBe(201);
    await completed(fixture, "queue", 2);
    const customers = await (await fetch(endpoint)).json() as { synced_at: string | null }[];
    expect(customers.length).toBe(2);
    expect(customers.every((customer) => customer.synced_at !== null)).toBe(true);
    expect((await fetch(endpoint, { method: "POST", body: "{}" })).status).toBe(400);
  } finally {
    await Promise.all(examples.map((child) => child.kill()));
  }
});

test.each(["SIGINT", "SIGTERM"] as const)("%s automatically drains the active delivery and exits without claiming more", async (signal) => {
  const fixture = await setup();
  const worker = await spawnRuntime(fixture, "signal-worker", "before-commit").start();
  const publisher = await spawnRuntime(fixture, "application", "direct-job").start();
  const first = await publisher.publish();
  await worker.wait("entered");
  worker.signal(signal);
  await worker.wait("stopping");
  const second = await publisher.publish();
  worker.send({ type: "release", customerId: first });
  expect(await worker.exit()).toBe(0);
  await synced(fixture, first);
  expect((await fixture.rows("SELECT sync_count FROM customers WHERE id = $1", [second]))[0].sync_count).toBe(0);
  expect((await fixture.rows("SELECT status FROM _simplebase_queue_jobs WHERE queue_name = 'customer.sync' ORDER BY id")).map((job) => job.status)).toEqual(["completed", "pending"]);
});
