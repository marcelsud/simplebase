import { afterAll, afterEach, beforeAll, expect, test } from "bun:test";
import { Get, Post, type HttpContext } from "@marcelsud/simplebase";
import { node } from "@marcelsud/simplebase/http/node";
import { bun } from "@marcelsud/simplebase/http/bun";
import { hono } from "@marcelsud/simplebase/http/hono";
import { express } from "@marcelsud/simplebase/http/express";
import ts from "typescript";
import { pathToFileURL } from "node:url";
import { resolve } from "node:path";
import { cleanupFixtures, createFixture, startDatabase, stopDatabase } from "./database.ts";

beforeAll(startDatabase, 20_000);
afterEach(cleanupFixtures);
afterAll(stopDatabase);

test("HTTP bindings retain action validation, rollback and output after commit", async () => {
  const fixture = await createFixture();
  const app = await fixture.runtime();
  const input = app.schema.object({ title: app.schema.string() });
  const create = app.action({ args: input, async handler(ctx, document) {
    const id = await ctx.collection.insert("notes", document);
    await ctx.queue.enqueue("index", { id });
    return { id, ...document };
  } });
  app.route("POST", "/notes", {
    action: create,
    input: async ({ request }) => await request.json(),
    output: async (note) => {
      expect((await fixture.rows("SELECT * FROM _simplebase_collections WHERE id = $1", [note.id])).length).toBe(1);
      expect((await fixture.rows("SELECT * FROM _simplebase_queue_jobs")).length).toBe(1);
      return { data: note };
    },
    status: 201,
  });
  const response = await app.fetch(new Request("http://localhost/notes", { method: "POST", body: '{"title":"hello"}' }));
  expect(response.status).toBe(201);
  expect((await response.json()).data.title).toBe("hello");
  expect((await app.fetch(new Request("http://localhost/notes", { method: "POST", body: '{"title":1}' }))).status).toBe(400);
  expect((await app.fetch(new Request("http://localhost/notes", { method: "POST", body: "{" }))).status).toBe(400);
  const fail = app.action({ args: input, async handler(ctx, document) {
    await ctx.collection.insert("notes", document);
    throw new Error("rollback");
  } });
  app.route("POST", "/fail", { action: fail });
  expect((await app.fetch(new Request("http://localhost/fail", { method: "POST", body: '{"title":"bad"}' }))).status).toBe(500);
  app.route("POST", "/bad-output", { action: create, output: () => { throw new Error("output failed"); } });
  expect((await app.fetch(new Request("http://localhost/bad-output", { method: "POST", body: '{"title":"committed"}' }))).status).toBe(500);
  expect((await fixture.rows("SELECT data->>'title' AS title FROM _simplebase_collections ORDER BY data->>'title'")).map((row) => row.title)).toEqual(["committed", "hello"]);
});

for (const [name, adapter] of [["bun", bun], ["node", node], ["hono", hono], ["express", express]] as const) {
  test(`${name}: start/stop owns HTTP, drains requests and keeps actions open until they finish`, async () => {
    const fixture = await createFixture();
    const app = await fixture.runtime();
    let release!: () => void;
    let entered!: () => void;
    const gate = new Promise<void>((resolve) => { release = resolve; });
    const ready = new Promise<void>((resolve) => { entered = resolve; });
    const create = app.action({ args: app.schema.object({}), async handler(ctx) {
      return ctx.collection.insert("drain", { complete: true });
    } });
    class Controller {
      @Get("/health") health() { return { ok: true }; }
      @Post("/slow", { status: 201 })
      async slow(_http: HttpContext) { entered(); await gate; return { id: await create({}) }; }
    }
    app.controller(new Controller());
    await app.start({ http: adapter({ port: 0 }), signals: false });
    const url = app.http!.url;
    expect(await (await fetch(new URL("/health", url))).json()).toEqual({ ok: true });
    const pending = fetch(new URL("/slow", url), { method: "POST" });
    await ready;
    const stopping = app.stop();
    try {
      expect(app.stop()).toBe(stopping);
      expect((await app.fetch(new Request("http://localhost/health"))).status).toBe(503);
      expect(() => app.route("GET", "/late", () => null)).toThrow();
      release();
      expect((await pending).status).toBe(201);
      await stopping;
      expect((await fixture.rows("SELECT * FROM _simplebase_collections")).length).toBe(1);
      await expect(create({})).rejects.toThrow("stopped");
    } finally { release(); await stopping; }
  });
}

test("an adapter startup failure removes signals and allows retry", async () => {
  const fixture = await createFixture();
  const app = await fixture.runtime();
  app.route("GET", "/health", () => ({ ok: true }));
  const occupied = await node({ port: 0 })(async () => new Response("occupied"));
  const before = process.listenerCount("SIGTERM");
  try {
    await expect(app.start({ http: node({ port: Number(occupied.url.port) }) })).rejects.toThrow();
    expect(process.listenerCount("SIGTERM")).toBe(before);
    expect(app.http).toBeUndefined();
    await app.start({ http: node({ port: 0 }), signals: false });
    expect(await (await fetch(new URL("/health", app.http!.url))).json()).toEqual({ ok: true });
  } finally { await occupied.stop(); }
});

test("stop during HTTP startup closes the eventual server", async () => {
  const fixture = await createFixture();
  const app = await fixture.runtime();
  let release!: () => void;
  let entered!: () => void;
  const gate = new Promise<void>((resolve) => { release = resolve; });
  const ready = new Promise<void>((resolve) => { entered = resolve; });
  let closed = 0;
  const started = app.start({ signals: false, http: async () => {
    entered();
    await gate;
    return { url: new URL("http://localhost"), async stop() { closed++; } };
  } }).then(() => undefined, (error: unknown) => error);
  await ready;
  const stopped = app.stop();
  release();
  const failure = await started;
  expect(failure).toBeInstanceOf(Error);
  expect((failure as Error).message).toContain("stopped");
  await stopped;
  expect(closed).toBe(1);
});

test("compiled decorators, actions and the Node/Hono/Express adapters run in real Node", async () => {
  const fixture = await createFixture();
  const root = resolve(import.meta.dir, "..");
  const build = Bun.spawn([process.execPath, "run", "build"], { cwd: root, stdout: "pipe", stderr: "pipe" });
  const [buildOutput, buildErrors, buildCode] = await Promise.all([
    new Response(build.stdout).text(), new Response(build.stderr).text(), build.exited,
  ]);
  if (buildCode !== 0) throw new Error(buildOutput + buildErrors);
  const entrypoint = pathToFileURL(resolve(root, "dist/index.js")).href;
  const adapters = pathToFileURL(resolve(root, "dist/http")).href + "/";
  const source = `
    import assert from "node:assert/strict";
    import { createSimplebase, Get, Post, type HttpContext } from ${JSON.stringify(entrypoint)};
    for (const name of ["node", "hono", "express"]) {
      const adapter = (await import(${JSON.stringify(adapters)} + name + ".js"))[name];
      const app = await createSimplebase({ storage: { engine: "postgres", url: process.env.SIMPLEBASE_TEST_PG_URL! } });
      try {
        const input = app.schema.object({ title: app.schema.string() });
        const create = app.action({ args: input, async handler(ctx, data) {
          const id = await ctx.collection.insert("node-http", data);
          await ctx.queue.enqueue("node-http", { id });
          return { id, title: data.title };
        } });
        app.route("POST", "/notes", {
          body: app.schema.object({ data: input }), action: create,
          input: ({ body }) => body.data,
          output: (data) => ({ data }), status: 201,
        });
        class Controller {
          prefix = name;
          @Get("/health")
          health(http: HttpContext) { return { adapter: this.prefix, native: !!http.native }; }
          @Post("/decorated", { body: input, status: 201 })
          create(http: HttpContext<{ title: string }>) { return create(http.body); }
        }
        app.controller(new Controller());
        await app.start({ signals: false, http: adapter({ port: 0 }) });
        const base = app.http!.url;
        assert.deepEqual(await (await fetch(new URL("/health", base))).json(), { adapter: name, native: true });
        const response = await fetch(new URL("/notes", base), { method: "POST", body: JSON.stringify({ data: { title: name } }) });
        assert.equal(response.status, 201);
        assert.equal((await response.json()).data.title, name);
        const decorated = await fetch(new URL("/decorated", base), { method: "POST", body: JSON.stringify({ title: name + "-decorated" }) });
        assert.equal(decorated.status, 201);
        assert.equal((await decorated.json()).title, name + "-decorated");
        assert.equal((await fetch(new URL("/notes", base), { method: "POST", body: "{" })).status, 400);
      } finally { await app.stop(); }
    }
    console.log("Node adapters and decorators verified");
  `;
  const compiled = ts.transpileModule(source, {
    compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.ESNext },
  }).outputText;
  const child = Bun.spawn(["node", "--input-type=module", "--eval", compiled], {
    cwd: root, env: { ...process.env, SIMPLEBASE_TEST_PG_URL: fixture.url }, stdout: "pipe", stderr: "pipe",
  });
  const timeout = setTimeout(() => child.kill(), 15_000);
  try {
    const [output, errors, code] = await Promise.all([
      new Response(child.stdout).text(), new Response(child.stderr).text(), child.exited,
    ]);
    if (code !== 0) throw new Error(`Node HTTP integration failed: ${output}\n${errors}`);
    expect(output).toContain("Node adapters and decorators verified");
    expect((await fixture.rows("SELECT * FROM _simplebase_collections WHERE collection_name = 'node-http'")).length).toBe(6);
    expect((await fixture.rows("SELECT * FROM _simplebase_queue_jobs WHERE queue_name = 'node-http'")).length).toBe(6);
  } finally { clearTimeout(timeout); child.kill(); }
}, 20_000);
