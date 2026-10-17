import { afterAll, afterEach, beforeAll, expect, test } from "bun:test";
import { type HandlerContext } from "@marcelsud/simplebase";
import { resolve } from "node:path";
import { pathToFileURL } from "node:url";
import { cleanupFixtures, createFixture, startDatabase, stopDatabase, waitUntil } from "./database.ts";

beforeAll(startDatabase, 20_000);
afterEach(cleanupFixtures);
afterAll(stopDatabase);

async function writeEffects(ctx: HandlerContext, name: string): Promise<void> {
  await ctx.db.query("INSERT INTO customers (email, name, plan) VALUES ($1, $1, 'starter')", [name]);
  await ctx.collection.insert("nested", { name });
  await ctx.queue.enqueue("nested.jobs", { name });
  ctx.pubsub.publish("nested.event", { name });
}

test("nested actions reuse the context and connection and commit all effects together", async () => {
  const fixture = await createFixture();
  const app = await fixture.runtime();
  app.subscription("nested.event", () => {}, { name: "observe" });
  await app.start({ signals: false });
  let parent!: HandlerContext;
  let parentPid: number;
  const inner = app.action({ args: app.schema.object({}), async handler(ctx) {
    expect(ctx).toBe(parent);
    expect((await ctx.db.query<{ pid: number }>("SELECT pg_backend_pid() AS pid"))[0].pid).toBe(parentPid);
    expect((await ctx.db.query("SELECT * FROM customers")).length).toBe(1);
    await writeEffects(ctx, "inner");
    expect(await fixture.rows("SELECT * FROM customers")).toEqual([]);
    return "nested result";
  } });
  const outer = app.action({ args: app.schema.object({}), async handler(ctx) {
    parent = ctx;
    parentPid = (await ctx.db.query<{ pid: number }>("SELECT pg_backend_pid() AS pid"))[0].pid;
    await writeEffects(ctx, "outer");
    return inner({});
  } });
  expect(await outer({})).toBe("nested result");
  await app.stop();
  expect((await fixture.rows("SELECT * FROM customers")).length).toBe(2);
  expect((await fixture.rows("SELECT * FROM _simplebase_collections")).length).toBe(2);
  expect((await fixture.rows("SELECT * FROM _simplebase_queue_jobs WHERE destination_kind = 'queue'")).length).toBe(2);
  expect((await fixture.rows("SELECT * FROM _simplebase_queue_jobs WHERE destination_kind = 'subscription'")).length).toBe(2);
});

for (const caught of [false, true]) {
  test(`nested failure ${caught ? "caught by the parent" : "propagated"} rolls back writes, jobs and events`, async () => {
    const fixture = await createFixture();
    const app = await fixture.runtime();
    app.subscription("nested.event", () => {}, { name: "observe" });
    await app.start({ signals: false });
    const inner = app.action({ args: app.schema.object({}), async handler(ctx) {
      await writeEffects(ctx, "inner");
      throw new Error("nested failure");
    } });
    const outer = app.action({ args: app.schema.object({}), async handler(ctx) {
      await writeEffects(ctx, "outer");
      if (caught) {
        try { await inner({}); } catch { /* The enclosing transaction must still roll back. */ }
        await ctx.collection.insert("nested", { afterCatch: true });
      } else await inner({});
    } });
    await expect(outer({})).rejects.toThrow(caught ? "Transaction aborted by a nested action" : "nested failure");
    expect(await fixture.rows("SELECT * FROM customers")).toEqual([]);
    expect(await fixture.rows("SELECT * FROM _simplebase_collections")).toEqual([]);
    expect(await fixture.rows("SELECT * FROM _simplebase_queue_jobs")).toEqual([]);
  });
}

test("caught nested validation failures also abort the enclosing transaction", async () => {
  const fixture = await createFixture();
  const app = await fixture.runtime();
  const inner = app.action({ args: app.schema.object({ name: app.schema.string() }), handler: () => {
    throw new Error("Invalid input must not reach the handler");
  } });
  const outer = app.action({ args: app.schema.object({}), async handler(ctx) {
    await writeEffects(ctx, "outer");
    // @ts-expect-error Deliberately exercise nested input validation.
    await expect(inner({ name: 42 })).rejects.toThrow("args.name must be a string");
  } });
  await expect(outer({})).rejects.toThrow("Transaction aborted by a nested action");
  expect(await fixture.rows("SELECT * FROM customers")).toEqual([]);
  expect(await fixture.rows("SELECT * FROM _simplebase_collections")).toEqual([]);
  expect(await fixture.rows("SELECT * FROM _simplebase_queue_jobs")).toEqual([]);
});

test("a successful nested action rolls back when its parent later fails", async () => {
  const fixture = await createFixture();
  const app = await fixture.runtime();
  const inner = app.action({ args: app.schema.object({}), handler: (ctx) => writeEffects(ctx, "inner") });
  const outer = app.action({ args: app.schema.object({}), async handler() {
    await inner({});
    throw new Error("outer failure");
  } });
  await expect(outer({})).rejects.toThrow("outer failure");
  expect(await fixture.rows("SELECT * FROM customers")).toEqual([]);
  expect(await fixture.rows("SELECT * FROM _simplebase_collections")).toEqual([]);
  expect(await fixture.rows("SELECT * FROM _simplebase_queue_jobs")).toEqual([]);
});

for (const kind of ["queue", "subscription"] as const) {
  test(`nested actions in a ${kind} share its context and preserve retry bookkeeping on failure`, async () => {
    const fixture = await createFixture();
    const app = await fixture.runtime();
    let parent!: HandlerContext;
    const inner = app.action({ args: app.schema.object({}), async handler(ctx) {
      expect(ctx).toBe(parent);
      await writeEffects(ctx, "inner");
      throw new Error("nested delivery failure");
    } });
    const consumer = async (ctx: HandlerContext) => {
      parent = ctx;
      await writeEffects(ctx, "consumer");
      await expect(inner({})).rejects.toThrow("nested delivery failure");
    };
    if (kind === "queue") app.worker("incoming", consumer);
    else app.subscription("incoming", consumer, { name: "consumer" });
    await app.start({ signals: false });
    await app.action({ args: app.schema.object({}), async handler(ctx) {
      if (kind === "queue") await ctx.queue.enqueue("incoming", {});
      else ctx.pubsub.publish("incoming", {});
    } })({});
    await waitUntil(async () => (await fixture.rows("SELECT attempt_count FROM _simplebase_queue_jobs"))[0]?.attempt_count === 1);
    await app.stop();
    expect(await fixture.rows("SELECT * FROM customers")).toEqual([]);
    expect(await fixture.rows("SELECT * FROM _simplebase_collections")).toEqual([]);
    const jobs = await fixture.rows("SELECT * FROM _simplebase_queue_jobs");
    expect(jobs.length).toBe(1);
    expect(jobs[0].last_error).toBe("Transaction aborted by a nested action");
    expect(jobs[0].attempt_count).toBe(1);
  });
}

test("ten simultaneous action chains finish without exhausting the connection pool", async () => {
  const fixture = await createFixture();
  const entrypoint = pathToFileURL(resolve(import.meta.dir, "../src/index.ts")).href;
  const source = `
    import assert from "node:assert/strict";
    import { createSimplebase } from ${JSON.stringify(entrypoint)};
    import { postgres as postgresStorage } from ${JSON.stringify(new URL("./storage/postgres.ts", entrypoint).href)};
    const app = await createSimplebase({ storage: postgresStorage({ url: process.env.SIMPLEBASE_TEST_PG_URL }) });
    let ready = 0;
    let release;
    const gate = new Promise(resolve => { release = resolve; });
    const inner = app.action({ args: app.schema.object({ pid: app.schema.number() }), async handler(ctx, input) {
      const [row] = await ctx.db.query("SELECT pg_backend_pid() AS pid");
      assert.equal(row.pid, input.pid);
      await ctx.collection.insert("concurrent", input);
    } });
    const outer = app.action({ args: app.schema.object({}), async handler(ctx) {
      const [row] = await ctx.db.query("SELECT pg_backend_pid() AS pid");
      if (++ready === 10) release();
      await gate;
      await inner(row);
    } });
    try { await Promise.all(Array.from({ length: 10 }, () => outer({}))); }
    finally { await app.stop(); }
    console.log("All ten action chains committed");
  `;
  const child = Bun.spawn([process.execPath, "--eval", source], {
    env: { ...process.env, SIMPLEBASE_TEST_PG_URL: fixture.url }, stdout: "pipe", stderr: "pipe",
  });
  const timeout = setTimeout(() => child.kill(), 5_000);
  try {
    const [output, errors, code] = await Promise.all([
      new Response(child.stdout).text(), new Response(child.stderr).text(), child.exited,
    ]);
    if (code !== 0) throw new Error(`Concurrent nested actions failed: ${output}\n${errors}`);
    expect(output).toContain("All ten action chains committed");
    expect((await fixture.rows("SELECT * FROM _simplebase_collections")).length).toBe(10);
  } finally { clearTimeout(timeout); child.kill(); }
}, 10_000);

test("separate app instances have independent transactions", async () => {
  const first = await createFixture();
  const second = await createFixture();
  const app = await first.runtime();
  const other = await second.runtime();
  const inner = other.action({ args: other.schema.object({}), handler: (ctx) => ctx.collection.insert("independent", {}) });
  const outer = app.action({ args: app.schema.object({}), async handler(ctx) {
    await ctx.collection.insert("independent", {});
    await inner({});
    throw new Error("outer only");
  } });
  await expect(outer({})).rejects.toThrow("outer only");
  expect(await first.rows("SELECT * FROM _simplebase_collections")).toEqual([]);
  expect((await second.rows("SELECT * FROM _simplebase_collections")).length).toBe(1);
});

test("detached calls cannot reuse an expired handler context", async () => {
  const fixture = await createFixture();
  const app = await fixture.runtime();
  let release!: () => void;
  const gate = new Promise<void>((resolve) => { release = resolve; });
  let detached!: Promise<unknown>;
  const inner = app.action({ args: app.schema.object({}), handler: (ctx) => ctx.collection.insert("detached", {}) });
  const outer = app.action({ args: app.schema.object({}), handler() {
    detached = gate.then(() => inner({}));
  } });
  await outer({});
  release();
  await expect(detached).rejects.toThrow("Handler context is no longer active");
  expect(await fixture.rows("SELECT * FROM _simplebase_collections")).toEqual([]);
});

test("shutdown drains nested calls while rejecting new root actions", async () => {
  const fixture = await createFixture();
  const app = await fixture.runtime();
  let entered!: () => void;
  let release!: () => void;
  const ready = new Promise<void>((resolve) => { entered = resolve; });
  const gate = new Promise<void>((resolve) => { release = resolve; });
  const inner = app.action({ args: app.schema.object({}), handler: (ctx) => ctx.collection.insert("drain", {}) });
  const outer = app.action({ args: app.schema.object({}), async handler() {
    entered();
    await gate;
    return inner({});
  } });
  const pending = outer({});
  await ready;
  const stopped = app.stop();
  try {
    await expect(inner({})).rejects.toThrow("stopped");
    release();
    expect(typeof await pending).toBe("string");
    await stopped;
    expect((await fixture.rows("SELECT * FROM _simplebase_collections")).length).toBe(1);
  } finally { release(); await stopped; }
});
