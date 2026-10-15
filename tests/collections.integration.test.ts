import { afterAll, afterEach, beforeAll, expect, test } from "bun:test";
import type { HandlerContext, Simplebase } from "@marcelsud/simplebase";
import { cleanupFixtures, createFixture, startDatabase, stopDatabase, waitUntil } from "./database.ts";

beforeAll(startDatabase, 20_000);
afterEach(cleanupFixtures);
afterAll(stopDatabase);

function run<T>(runtime: Simplebase, handler: (ctx: HandlerContext) => T | Promise<T>) {
  return runtime.action({ args: runtime.schema.object({}), handler })({});
}

test("insert returns a UUID and get returns the document with its immutable ID", async () => {
  const fixture = await createFixture();
  const runtime = await fixture.runtime();
  const createNote = runtime.action({
    args: runtime.schema.object({ body: runtime.schema.string() }),
    async handler(ctx, input) {
      const id = await ctx.collection.insert("notes", input);
      const note = await ctx.collection.get<{ body: string }>("notes", id);
      return { id, note };
    },
  });
  const { id, note } = await createNote({ body: "First note" });
  expect(id).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/);
  expect(note).toEqual({ id, body: "First note" });
  expect((await fixture.rows("SELECT data FROM _simplebase_collections"))[0].data).toEqual({ body: "First note" });
});

test("CRUD scopes names and IDs and binds names, IDs and data as parameters", async () => {
  const fixture = await createFixture();
  const runtime = await fixture.runtime();
  const name = "notes'); DROP TABLE _simplebase_collections;--";
  const body = "O'Reilly'); DELETE FROM customers;--";
  const id = await run(runtime, (ctx) => ctx.collection.insert(name, { body }));
  const missing = crypto.randomUUID();
  await run(runtime, async (ctx) => {
    expect(await ctx.collection.get(name, missing)).toBeNull();
    expect(await ctx.collection.update(name, missing, {})).toBe(false);
    expect(await ctx.collection.delete(name, missing)).toBe(false);
    expect(await ctx.collection.get("other", id)).toBeNull();
    expect(await ctx.collection.update("other", id, { body: "wrong" })).toBe(false);
    expect(await ctx.collection.delete("other", id)).toBe(false);
    const original = await ctx.collection.get(name, id);
    expect(original).toEqual({ id, body });
    const patch = JSON.parse('{"__proto__":{"safe":true}}') as Record<string, unknown>;
    expect(await ctx.collection.update(name, id, patch)).toBe(true);
    const note = await ctx.collection.get(name, id);
    expect(Object.hasOwn(note!, "__proto__")).toBe(true);
    expect(Object.getPrototypeOf(note)).toBe(Object.prototype);
    expect(note!.body).toBe(body);
  });
  await expect(run(runtime, (ctx) => ctx.collection.get(name, id + "' OR TRUE--"))).rejects.toThrow("invalid input syntax");
  await run(runtime, async (ctx) => {
    expect(await ctx.collection.delete(name, id)).toBe(true);
    expect(await ctx.collection.delete(name, id)).toBe(false);
    expect(await ctx.collection.get(name, id)).toBeNull();
  });
  expect(await fixture.rows("SELECT * FROM _simplebase_collections")).toEqual([]);
});

test("documents and patches reject non-JSON data, arrays, supplied IDs and empty names", async () => {
  const fixture = await createFixture();
  const runtime = await fixture.runtime();
  const id = await run(runtime, (ctx) => ctx.collection.insert("notes", { body: "original" }));
  const circular: Record<string, unknown> = {};
  circular.self = circular;
  const hiddenId: Record<string, unknown> = { id };
  hiddenId.toJSON = () => { delete hiddenId.id; return {}; };
  const invalid: unknown[] = [
    null, undefined, false, "text", 1, [], [1], NaN, Infinity, 1n, Symbol(), () => {},
    { missing: undefined }, { nested: { invalid: NaN } }, { values: [undefined] },
    { callback: () => {} }, { symbol: Symbol() }, { bigint: 1n }, circular,
    { id }, { id: null }, { id: undefined },
    { toJSON: () => ({ id }) }, { toJSON: () => [] }, hiddenId,
  ];
  for (const input of invalid) {
    const value = input as Record<string, unknown>;
    await expect(run(runtime, (ctx) => ctx.collection.insert("notes", value))).rejects.toThrow();
    await expect(run(runtime, (ctx) => ctx.collection.update("notes", id, value))).rejects.toThrow();
  }
  await run(runtime, async (ctx) => {
    for (const name of ["", "  ", 1 as unknown as string]) {
      await expect(ctx.collection.insert(name, {})).rejects.toThrow("nonempty name");
      await expect(ctx.collection.get(name, id)).rejects.toThrow("nonempty name");
      await expect(ctx.collection.update(name, id, {})).rejects.toThrow("nonempty name");
      await expect(ctx.collection.delete(name, id)).rejects.toThrow("nonempty name");
    }
    await expect(ctx.collection.get("notes", "")).rejects.toThrow("nonempty name");
    await expect(ctx.collection.update("notes", "", {})).rejects.toThrow("nonempty name");
    await expect(ctx.collection.delete("notes", "")).rejects.toThrow("nonempty name");
    const note = await ctx.collection.get("notes", id);
    expect(note).toEqual({ id, body: "original" });
  });
  expect((await fixture.rows("SELECT * FROM _simplebase_collections")).length).toBe(1);
});

test("patches replace top-level fields, preserve others, store null and accept an empty patch", async () => {
  const fixture = await createFixture();
  const runtime = await fixture.runtime();
  await run(runtime, async (ctx) => {
    const id = await ctx.collection.insert("notes", {
      body: "original", untouched: true, nested: { left: 1, right: 2 }, values: [1, 2], nullable: "value",
    });
    expect(await ctx.collection.update("notes", id, {
      body: "revised", nested: { left: 3 }, values: [4], nullable: null,
    })).toBe(true);
    const expected = { id, body: "revised", untouched: true, nested: { left: 3 }, values: [4], nullable: null };
    expect(await ctx.collection.get("notes", id)).toEqual(expected);
    expect(await ctx.collection.update("notes", id, {})).toBe(true);
    expect(await ctx.collection.get("notes", id)).toEqual(expected);
  });
  expect(await fixture.rows("SELECT * FROM _simplebase_queue_jobs")).toEqual([]);
});

test("collections, SQL, jobs and explicit events share action and subscription transactions", async () => {
  const fixture = await createFixture();
  const runtime = await fixture.runtime();
  const [kept, removed] = await run(runtime, async (ctx) => [
    await ctx.collection.insert("notes", { body: "original" }),
    await ctx.collection.insert("notes", { body: "remove later" }),
  ]);
  runtime.subscription<{ id: string }>("note.committed", async (ctx, event) => {
    const note = await ctx.collection.get<{ body: string }>("notes", event.id);
    expect(note?.body).toBe("created");
    await ctx.collection.update("notes", event.id, { observed: true });
  }, { name: "observeNote" });
  await runtime.start();
  const change = runtime.action({ args: runtime.schema.object({ fail: runtime.schema.boolean() }), async handler(ctx, input) {
    await ctx.collection.update("notes", kept, { body: "updated" });
    await ctx.collection.delete("notes", removed);
    const id = await ctx.collection.insert("notes", { body: "created" });
    await ctx.db.query("INSERT INTO customers (email, name, plan) VALUES ('note@test.com', 'Test', 'starter')");
    await ctx.queue.enqueue("notes.saved", { id });
    ctx.pubsub.publish("note.committed", { id });
    if (input.fail) throw new Error("rollback all changes");
    return id;
  } });
  await expect(change({ fail: true })).rejects.toThrow("rollback all changes");
  expect((await fixture.rows("SELECT data FROM _simplebase_collections WHERE id = $1", [kept]))[0].data).toEqual({ body: "original" });
  expect((await fixture.rows("SELECT data FROM _simplebase_collections WHERE id = $1", [removed]))[0].data).toEqual({ body: "remove later" });
  expect((await fixture.rows("SELECT * FROM _simplebase_collections")).length).toBe(2);
  expect(await fixture.rows("SELECT * FROM customers")).toEqual([]);
  expect(await fixture.rows("SELECT * FROM _simplebase_queue_jobs")).toEqual([]);
  const id = await change({ fail: false });
  await waitUntil(async () => (await fixture.rows("SELECT status FROM _simplebase_queue_jobs WHERE destination_kind = 'subscription'"))[0]?.status === "completed");
  const committed = await run(runtime, (ctx) => ctx.collection.get("notes", id));
  expect(committed).toEqual({ id, body: "created", observed: true });
  expect(await run(runtime, (ctx) => ctx.collection.get("notes", removed))).toBeNull();
  expect((await fixture.rows("SELECT * FROM customers")).length).toBe(1);
  expect((await fixture.rows("SELECT * FROM _simplebase_queue_jobs WHERE destination_kind = 'queue'")).length).toBe(1);
});

test("collection operations reject an expired handler context", async () => {
  const fixture = await createFixture();
  const runtime = await fixture.runtime();
  let captured!: HandlerContext;
  const id = await run(runtime, async (ctx) => {
    captured = ctx;
    return ctx.collection.insert("notes", {});
  });
  await expect(captured.collection.insert("notes", {})).rejects.toThrow("no longer active");
  await expect(captured.collection.get("notes", id)).rejects.toThrow("no longer active");
  await expect(captured.collection.update("notes", id, {})).rejects.toThrow("no longer active");
  await expect(captured.collection.delete("notes", id)).rejects.toThrow("no longer active");
});

test("concurrent patches preserve different fields and apply shared fields in database write order", async () => {
  const fixture = await createFixture();
  const firstRuntime = await fixture.runtime();
  const secondRuntime = await fixture.runtime();
  const id = await run(firstRuntime, (ctx) => ctx.collection.insert("notes", { untouched: true }));
  let release!: () => void;
  let firstEntered!: () => void;
  let secondEntered!: () => void;
  const gate = new Promise<void>((resolve) => { release = resolve; });
  const firstReady = new Promise<void>((resolve) => { firstEntered = resolve; });
  const secondReady = new Promise<void>((resolve) => { secondEntered = resolve; });
  let firstPid = 0;
  let secondPid = 0;
  const first = run(firstRuntime, async (ctx) => {
    firstPid = (await ctx.db.query<{ pid: number }>("SELECT pg_backend_pid() AS pid"))[0].pid;
    await ctx.collection.update("notes", id, { left: "first", shared: "first" });
    firstEntered();
    await gate;
  });
  let second: Promise<unknown> | undefined;
  try {
    await firstReady;
    second = run(secondRuntime, async (ctx) => {
      secondPid = (await ctx.db.query<{ pid: number }>("SELECT pg_backend_pid() AS pid"))[0].pid;
      secondEntered();
      await ctx.collection.update("notes", id, { right: "second", shared: "second" });
    });
    await secondReady;
    await waitUntil(async () => (await fixture.rows("SELECT $1::int = ANY(pg_blocking_pids($2::int)) AS blocked", [firstPid, secondPid]))[0].blocked);
  } finally {
    release();
    await Promise.all([first, second]);
  }
  const patched = await run(firstRuntime, (ctx) => ctx.collection.get("notes", id));
  expect(patched).toEqual({
    id, untouched: true, left: "first", right: "second", shared: "second",
  });
});

test("documents survive runtime replacement", async () => {
  const fixture = await createFixture();
  const original = await fixture.runtime();
  const id = await run(original, (ctx) => ctx.collection.insert("notes", { body: "durable" }));
  await original.stop();
  const replacement = await fixture.runtime();
  const note = await run(replacement, (ctx) => ctx.collection.get("notes", id));
  expect(note).toEqual({ id, body: "durable" });
});

test("the note example stores a document using the package script", async () => {
  const fixture = await createFixture();
  const child = Bun.spawn([process.execPath, "run", "example:collections"], {
    cwd: import.meta.dir + "/..", env: { ...process.env, DATABASE_URL: fixture.url }, stdout: "pipe", stderr: "pipe",
  });
  const [output, errors, code] = await Promise.all([
    new Response(child.stdout).text(), new Response(child.stderr).text(), child.exited,
  ]);
  if (code !== 0) throw new Error(errors);
  expect(output).toContain("Note created");
  const [note] = await fixture.rows("SELECT id, collection_name, data FROM _simplebase_collections");
  expect(output).toContain(note.id);
  expect(note.collection_name).toBe("notes");
  expect(note.data).toEqual({ body: "First note" });
});
