import { afterAll, afterEach, beforeAll, describe, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import Database from "libsql";
import { createSimplebase, schema, type HandlerContext, type KvNamespaces, type Simplebase, type SimplebaseOptions, type StorageAdapter } from "@marcelsud/simplebase";
import { sqlite } from "@marcelsud/simplebase/storage/sqlite";
import { cleanupFixtures, createFixture, startDatabase, stopDatabase, waitUntil, type Fixture } from "./database.ts";

const cleanups: (() => Promise<void>)[] = [];
afterEach(async () => {
  for (const cleanup of cleanups.splice(0).reverse()) await cleanup();
});

async function sqliteFixture(): Promise<Pick<Fixture, "runtime" | "rows"> & { filename: string }> {
  const directory = await mkdtemp(join(tmpdir(), "simplebase-sqlite-"));
  const filename = join(directory, "app.sqlite");
  const inspector = new Database(filename, {});
  const apps: { stop(): Promise<void> }[] = [];
  cleanups.push(async () => {
    await Promise.all(apps.map((app) => app.stop()));
    inspector.close();
    await rm(directory, { recursive: true, force: true });
  });
  return {
    filename,
    async runtime<Namespaces extends KvNamespaces = {}>(options?: Pick<SimplebaseOptions<Namespaces>, "http" | "kv">) {
      const app = await createSimplebase({ ...options, storage: sqlite({ filename }) });
      apps.push(app);
      return app;
    },
    async rows<T>(sql: string, params: unknown[] = []): Promise<T[]> {
      return inspector.prepare(sql).all(params) as T[];
    },
  };
}

async function run<T>(app: Simplebase, handler: (ctx: HandlerContext) => T | Promise<T>): Promise<T> {
  return app.action({ args: schema.object({}), handler })({});
}

for (const engine of ["SQLite", "PostgreSQL"] as const) describe(`${engine} storage contract`, () => {
  if (engine === "PostgreSQL") {
    beforeAll(startDatabase, 20_000);
    afterEach(cleanupFixtures);
    afterAll(stopDatabase);
  }
  const fixtureFor = () => engine === "SQLite" ? sqliteFixture() : createFixture();

  test("collections and KV preserve JSON, namespaces, shallow patches, and validation", async () => {
    const fixture = await fixtureFor();
    const app = await fixture.runtime();
    await run(app, async (ctx) => {
      const name = "notes'); DROP TABLE _simplebase_kv;--";
      const id = await ctx.collection.insert(name, { nested: { left: 1, right: 2 }, nullable: true, untouched: true });
      expect(await ctx.collection.update(name, id, JSON.parse('{"nested":{"left":3},"nullable":null,"__proto__":{"safe":true}}'))).toBe(true);
      const doc = await ctx.collection.get(name, id);
      expect(doc).toEqual({ id, nested: { left: 3 }, nullable: null, untouched: true, ...JSON.parse('{"__proto__":{"safe":true}}') });
      expect(Object.getPrototypeOf(doc)).toBe(Object.prototype);
      expect(await ctx.collection.update(name, id, {})).toBe(true);
      expect(await ctx.collection.get("other", id)).toBeNull();
      expect(await ctx.collection.update(name, crypto.randomUUID(), {})).toBe(false);
      for (const value of [null, [], { id }, { missing: undefined }, { nested: Infinity }]) {
        await expect(ctx.collection.insert(name, value as any)).rejects.toThrow();
      }
      for (const value of [null, false, 0, "", [1, null], { nested: { value: true } }]) {
        await ctx.kv.set("raw", value);
        expect(await ctx.kv.get<unknown>("raw")).toEqual(value);
      }
      const counts = ctx.kv.namespace("counts", schema.integer());
      await counts.set("raw", 1);
      expect(await counts.get("raw")).toBe(1);
      expect(await ctx.kv.get<unknown>("raw")).toEqual({ nested: { value: true } });
      await expect(counts.set("bad", "value" as any)).rejects.toThrow();
      expect(await counts.delete("raw")).toBe(true);
      expect(await counts.delete("raw")).toBe(false);
      expect(await counts.get("raw")).toBeUndefined();
      expect(await ctx.collection.delete(name, id)).toBe(true);
      expect(await ctx.collection.delete(name, id)).toBe(false);
    });
  });

  test("SQL, collections, default KV, jobs and events commit or roll back together", async () => {
    const fixture = await fixtureFor();
    const app = await fixture.runtime();
    await run(app, (ctx) => ctx.db.query("CREATE TABLE effects (name TEXT NOT NULL)"));
    app.subscription("changed", async (ctx) => { await ctx.kv.set("observed", true); }, { name: "observe" });
    await app.start({ signals: false });
    const change = app.action({ args: schema.object({ fail: schema.boolean() }), async handler(ctx, { fail }) {
      await ctx.db.query("INSERT INTO effects VALUES ('action')");
      const id = await ctx.collection.insert("notes", { value: true });
      await ctx.kv.set("key", null);
      await ctx.queue.enqueue("unclaimed", { id });
      ctx.pubsub.publish("changed", { id });
      if (fail) throw new Error("rollback everything");
      return id;
    } });
    await expect(change({ fail: true })).rejects.toThrow("rollback everything");
    for (const table of ["effects", "_simplebase_collections", "_simplebase_kv", "_simplebase_queue_jobs"]) {
      expect(await fixture.rows(`SELECT * FROM ${table}`)).toEqual([]);
    }
    const id = await change({ fail: false });
    await waitUntil(async () => (await fixture.rows("SELECT status FROM _simplebase_queue_jobs WHERE destination_kind = 'subscription'"))[0]?.status === "completed");
    await run(app, async (ctx) => {
      expect(await ctx.collection.get<{ value: boolean }>("notes", id)).toEqual({ id, value: true });
      expect(await ctx.kv.get("key")).toBeNull();
      expect(await ctx.kv.get<boolean>("observed")).toBe(true);
    });
    expect(await fixture.rows("SELECT name FROM effects")).toEqual([{ name: "action" }]);
    expect((await fixture.rows("SELECT * FROM _simplebase_queue_jobs WHERE status = 'pending'")).length).toBe(1);
  });

  test("parallel patches in one transaction retain all fields and JSON types", async () => {
    const fixture = await fixtureFor();
    const app = await fixture.runtime();
    await run(app, async (ctx) => {
      const id = await ctx.collection.insert("notes", { kept: true, empty: {}, nested: { old: true }, list: [1, false] });
      const patches = [
        { left: true, nested: { replaced: true } },
        { right: false, nullable: null },
        { text: 'quotes " and dots .', number: 1.5, list: [null, true, { value: 1 }] },
        JSON.parse('{"__proto__":{"safe":true},"a.b\\\"c":false}'),
      ];
      expect(await Promise.all(patches.map((patch) => ctx.collection.update("notes", id, patch)))).toEqual([true, true, true, true]);
      expect(await ctx.collection.get<Record<string, unknown>>("notes", id)).toEqual({
        id, kept: true, empty: {}, ...patches.reduce((merged, patch) => ({ ...merged, ...patch }), {}),
      });
      const empty = await ctx.collection.insert("notes", {});
      expect(await ctx.collection.update("notes", empty, {})).toBe(true);
      expect(await ctx.collection.get("notes", empty)).toEqual({ id: empty });
    });
  });

  test("nested actions share a transaction and caught failures abort the parent", async () => {
    const fixture = await fixtureFor();
    const app = await fixture.runtime();
    const inner = app.action({ args: schema.object({ fail: schema.boolean() }), async handler(ctx, { fail }) {
      expect(await ctx.kv.get<boolean>("outer")).toBe(true);
      await ctx.collection.insert("notes", { inner: true });
      if (fail) throw new Error("inner failed");
    } });
    const outer = app.action({ args: schema.object({ fail: schema.boolean() }), async handler(ctx, input) {
      await ctx.kv.set("outer", true);
      try { await inner(input); } catch {}
    } });
    await expect(outer({ fail: true })).rejects.toThrow("nested action");
    expect(await fixture.rows("SELECT * FROM _simplebase_kv")).toEqual([]);
    expect(await fixture.rows("SELECT * FROM _simplebase_collections")).toEqual([]);
    await outer({ fail: false });
    expect((await fixture.rows("SELECT * FROM _simplebase_collections")).length).toBe(1);
  });

  test("durable bindings fan out and pending work survives application replacement", async () => {
    const fixture = await fixtureFor();
    const original = await fixture.runtime();
    for (const name of ["first", "second"]) original.subscription("event", () => {}, { name });
    await original.start({ signals: false });
    await original.stop();
    const publisher = await fixture.runtime();
    const id = await run(publisher, async (ctx) => {
      const id = await ctx.collection.insert("notes", { durable: true });
      await ctx.kv.set("durable", id);
      ctx.pubsub.publish("event", { id });
      await ctx.queue.enqueue("work", { id });
      await ctx.queue.enqueue("unknown", {});
      return id;
    });
    await publisher.stop();
    const replacement = await fixture.runtime();
    const observe = async (ctx: HandlerContext, name: string) => {
      expect(await ctx.kv.get<string>("durable")).toBe(id);
      expect(await ctx.collection.get<{ durable: boolean }>("notes", id)).toEqual({ id, durable: true });
      await ctx.collection.insert("deliveries", { name });
    };
    for (const name of ["first", "second", "new"]) replacement.subscription("event", (ctx) => observe(ctx, name), { name });
    replacement.worker("work", (ctx) => observe(ctx, "worker"));
    await replacement.start({ signals: false });
    await waitUntil(async () => (await fixture.rows("SELECT * FROM _simplebase_queue_jobs WHERE status = 'completed'")).length === 3);
    expect((await fixture.rows("SELECT * FROM _simplebase_collections WHERE collection_name = 'deliveries'")).length).toBe(3);
    expect((await fixture.rows("SELECT queue_name FROM _simplebase_queue_jobs WHERE status = 'pending'"))).toEqual([{ queue_name: "unknown" }]);
    await replacement.stop();
    const last = await fixture.runtime();
    last.worker("work", () => { throw new Error("completed work repeated"); });
    await last.start({ signals: false });
    await run(last, (ctx) => ctx.kv.get("durable"));
    expect((await fixture.rows("SELECT * FROM _simplebase_queue_jobs WHERE status = 'completed'")).length).toBe(3);
  });

  test("failed delivery effects roll back before retrying and completion is atomic", async () => {
    const fixture = await fixtureFor();
    const app = await fixture.runtime();
    let attempts = 0;
    app.worker("retry", async (ctx) => {
      attempts++;
      await ctx.collection.insert("effects", { attempt: attempts });
      await ctx.kv.set("attempt", attempts);
      await ctx.queue.enqueue("followup", {});
      if (attempts === 1) throw new Error("retry once");
    });
    await run(app, (ctx) => ctx.queue.enqueue("retry", {}));
    await app.start({ signals: false });
    await waitUntil(async () => (await fixture.rows("SELECT attempt_count FROM _simplebase_queue_jobs WHERE queue_name = 'retry'"))[0]?.attempt_count === 1);
    expect(await fixture.rows("SELECT * FROM _simplebase_collections")).toEqual([]);
    expect(await fixture.rows("SELECT * FROM _simplebase_kv")).toEqual([]);
    expect(await fixture.rows("SELECT * FROM _simplebase_queue_jobs WHERE queue_name = 'followup'")).toEqual([]);
    await waitUntil(async () => (await fixture.rows("SELECT status FROM _simplebase_queue_jobs WHERE queue_name = 'retry'"))[0]?.status === "completed");
    expect(attempts).toBe(2);
    expect((await fixture.rows("SELECT attempt_count FROM _simplebase_queue_jobs WHERE queue_name = 'retry'"))[0].attempt_count).toBe(2);
    expect((await fixture.rows("SELECT * FROM _simplebase_collections")).length).toBe(1);
    expect((await fixture.rows("SELECT * FROM _simplebase_queue_jobs WHERE queue_name = 'followup'")).length).toBe(1);
  });

  test("caught SQL errors abort actions and roll back delivery effects through all retries", async () => {
    const fixture = await fixtureFor();
    const app = await fixture.runtime();
    async function fail(ctx: HandlerContext) {
      await ctx.collection.insert("effects", { rollback: true });
      try { await ctx.db.query("INSERT INTO missing_table VALUES (1)"); } catch {}
    }
    await expect(run(app, fail)).rejects.toThrow();
    app.worker("fail", fail);
    await run(app, (ctx) => ctx.queue.enqueue("fail", {}));
    await app.start({ signals: false });
    await waitUntil(async () => (await fixture.rows("SELECT status FROM _simplebase_queue_jobs"))[0]?.status === "failed");
    expect((await fixture.rows("SELECT attempt_count FROM _simplebase_queue_jobs"))[0].attempt_count).toBe(5);
    expect(await fixture.rows("SELECT * FROM _simplebase_collections")).toEqual([]);
  });

  test("stop drains async actions and their contexts expire", async () => {
    const fixture = await fixtureFor();
    const app = await fixture.runtime();
    let release!: () => void;
    let entered!: () => void;
    const ready = new Promise<void>((resolve) => { entered = resolve; });
    const gate = new Promise<void>((resolve) => { release = resolve; });
    let captured!: HandlerContext;
    const action = run(app, async (ctx) => {
      captured = ctx;
      await ctx.kv.set("before", true);
      entered();
      await gate;
      await ctx.kv.set("after", true);
    });
    await ready;
    const stopped = app.stop();
    try { await expect(run(app, () => {})).rejects.toThrow("stopped"); }
    finally { release(); }
    await Promise.all([action, stopped]);
    expect((await fixture.rows("SELECT * FROM _simplebase_kv")).length).toBe(2);
    await expect(captured.kv.get("after")).rejects.toThrow("no longer active");
    await expect(captured.collection.insert("notes", {})).rejects.toThrow("no longer active");
  });
});

test("SQLite: memory databases, required configuration, and connection pragmas", async () => {
  const app = await createSimplebase({ storage: sqlite({ filename: ":memory:" }) });
  try {
    await run(app, async (ctx) => {
      await ctx.kv.set("memory", true);
      expect(await ctx.kv.get<boolean>("memory")).toBe(true);
    });
  } finally { await app.stop(); }
  for (const filename of ["", "  "]) await expect(createSimplebase({ storage: sqlite({ filename }) })).rejects.toThrow("filename");
  for (const busyTimeoutMs of [-1, NaN, Infinity, 0.5]) {
    await expect(createSimplebase({ storage: sqlite({ filename: ":memory:", busyTimeoutMs }) })).rejects.toThrow("nonnegative integer");
  }
  const fixture = await sqliteFixture();
  const fileApp = await fixture.runtime();
  await run(fileApp, async (ctx) => {
    expect(await ctx.db.query("PRAGMA journal_mode")).toEqual([{ journal_mode: "wal" }]);
    expect(await ctx.db.query("PRAGMA foreign_keys")).toEqual([{ foreign_keys: 1 }]);
    expect(await ctx.db.query("PRAGMA synchronous")).toEqual([{ synchronous: 2 }]);
  });
});

test("SQLite: concurrent bootstrap and writes serialize without blocking the event loop", async () => {
  const fixture = await sqliteFixture();
  const apps = await Promise.all(Array.from({ length: 4 }, () => fixture.runtime()));
  const id = await run(apps[0], (ctx) => ctx.collection.insert("notes", {}));
  await Promise.all(apps.flatMap((app, index) => Array.from({ length: 3 }, (_, call) => run(app, async (ctx) => {
    await ctx.collection.update("notes", id, { [`field${index}_${call}`]: true });
    await new Promise((resolve) => setTimeout(resolve, 5));
  }))));
  const note = await run(apps[0], (ctx) => ctx.collection.get("notes", id));
  expect(Object.keys(note!).length).toBe(13);
});

test("SQLite: lock timeout never invokes or replays a blocked handler", async () => {
  const fixture = await sqliteFixture();
  const owner = await fixture.runtime();
  const contender = await createSimplebase({ storage: sqlite({ filename: fixture.filename, busyTimeoutMs: 30 }) });
  let release!: () => void;
  let entered!: () => void;
  const ready = new Promise<void>((resolve) => { entered = resolve; });
  const gate = new Promise<void>((resolve) => { release = resolve; });
  const held = run(owner, async (ctx) => { await ctx.kv.set("owner", true); entered(); await gate; });
  await ready;
  let calls = 0;
  try {
    await expect(run(contender, () => { calls++; })).rejects.toThrow();
    expect(calls).toBe(0);
  } finally {
    release();
    await held;
    await contender.stop();
  }
});

test("SQLite: replicas claim each job once and retain unclaimed queues", async () => {
  const fixture = await sqliteFixture();
  const apps = await Promise.all([fixture.runtime(), fixture.runtime()]);
  for (const app of apps) app.worker<{ index: number }>("work", async (ctx, job) => {
    await ctx.collection.insert("effects", job);
    await new Promise((resolve) => setTimeout(resolve, 5));
  });
  await run(apps[0], async (ctx) => {
    for (let index = 0; index < 15; index++) await ctx.queue.enqueue("work", { index });
    await ctx.queue.enqueue("unknown", {});
  });
  await Promise.all(apps.map((app) => app.start({ signals: false })));
  await waitUntil(async () => (await fixture.rows("SELECT * FROM _simplebase_queue_jobs WHERE status = 'completed'")).length === 15);
  const docs = await fixture.rows("SELECT data FROM _simplebase_collections");
  expect(new Set(docs.map((row) => JSON.parse(row.data).index)).size).toBe(15);
  expect(docs.length).toBe(15);
  expect(await fixture.rows("SELECT queue_name FROM _simplebase_queue_jobs WHERE status = 'pending'")).toEqual([{ queue_name: "unknown" }]);
});

test("SQLite: process death rolls back effects and a replacement reclaims the durable job", async () => {
  const fixture = await sqliteFixture();
  const app = await fixture.runtime();
  await run(app, (ctx) => ctx.queue.enqueue("crash", { value: true }));
  let output = "";
  const child = Bun.spawn([process.execPath, "--eval", `
    import { createSimplebase } from "./src/index.ts";
    import { sqlite } from "./src/storage/sqlite.ts";
    setInterval(() => {}, 1000);
    const app = await createSimplebase({ storage: sqlite({ filename: process.env.SQLITE_FILE }) });
    app.worker("crash", async (ctx, job) => {
      await ctx.kv.set("effect", job);
      await ctx.collection.insert("effects", job);
      console.log("entered");
      await new Promise(() => {});
    });
    await app.start({ signals: false });
  `], {
    cwd: resolve(import.meta.dir, ".."), env: { ...process.env, SQLITE_FILE: fixture.filename },
    stdout: "pipe", stderr: "pipe",
  });
  const errors = new Response(child.stderr).text();
  const readOutput = (async () => {
    for await (const chunk of child.stdout) output += new TextDecoder().decode(chunk);
  })();
  cleanups.push(async () => { child.kill("SIGKILL"); await child.exited; await readOutput; await errors; });
  await waitUntil(async () => {
    if (child.exitCode !== null) throw new Error(await errors);
    return output.includes("entered");
  });
  expect(await fixture.rows("SELECT * FROM _simplebase_collections")).toEqual([]);
  expect(await fixture.rows("SELECT * FROM _simplebase_kv")).toEqual([]);
  child.kill("SIGKILL");
  await child.exited;
  const replacement = await fixture.runtime();
  replacement.worker("crash", async (ctx, job) => {
    await ctx.kv.set("effect", job);
    await ctx.collection.insert("effects", job);
  });
  await replacement.start({ signals: false });
  await waitUntil(async () => (await fixture.rows("SELECT status FROM _simplebase_queue_jobs"))[0]?.status === "completed");
  expect(await fixture.rows("SELECT attempt_count FROM _simplebase_queue_jobs")).toEqual([{ attempt_count: 1 }]);
  expect((await fixture.rows("SELECT * FROM _simplebase_collections")).length).toBe(1);
  expect((await fixture.rows("SELECT * FROM _simplebase_kv")).length).toBe(1);
});

test("storage factories initialize once, drain external KV, and close after failed bootstrap", async () => {
  let opened = 0;
  let closed = 0;
  let kvStopped = false;
  const storage: StorageAdapter = async () => {
    opened++;
    const backend = await sqlite({ filename: ":memory:" })();
    return { ...backend, async close() { closed++; await backend.close(); } };
  };
  const values = new Map<string, string>();
  const app = await createSimplebase({ storage, kv: {
    namespaces: { sessions: { schema: schema.object({ userId: schema.string() }) } },
    adapter: async () => ({
      bind: () => ({
        async get(namespace, key) { return values.get(JSON.stringify([namespace, key])); },
        async set(namespace, key, value) { values.set(JSON.stringify([namespace, key]), value); },
        async delete(namespace, key) { return values.delete(JSON.stringify([namespace, key])); },
      }),
      async stop() { kvStopped = true; },
    }),
  } });
  expect(opened).toBe(1);
  try {
    const fail = app.action({ args: schema.object({}), async handler(ctx) {
      await ctx.kv.namespace("sessions").set("key", { userId: "external" });
      await ctx.collection.insert("notes", {});
      throw new Error("rollback local writes");
    } });
    await expect(fail({})).rejects.toThrow("rollback local writes");
    await app.action({ args: schema.object({}), async handler(ctx) {
      expect((await ctx.kv.namespace("sessions").get("key"))?.userId).toBe("external");
      expect(await ctx.db.query("SELECT * FROM _simplebase_kv")).toEqual([]);
      expect(await ctx.db.query("SELECT * FROM _simplebase_collections")).toEqual([]);
    } })({});
  } finally { await Promise.all([app.stop(), app.stop()]); }
  expect(kvStopped).toBe(true);
  expect(closed).toBe(1);
  await expect(createSimplebase({ storage, kv: { adapter: async () => { throw new Error("KV initialization failed"); } } })).rejects.toThrow("KV initialization failed");
  expect(opened).toBe(2);
  expect(closed).toBe(2);
});
