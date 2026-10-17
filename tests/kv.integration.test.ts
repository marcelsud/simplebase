import { afterAll, afterEach, beforeAll, expect, test } from "bun:test";
import { createClient } from "redis";
import { createSimplebase, schema, ValidationError, type HandlerContext, type KvAdapter, type RawKvNamespace, type Simplebase } from "@marcelsud/simplebase";
import { redis } from "@marcelsud/simplebase/kv/redis";
import { postgres } from "@marcelsud/simplebase/kv/postgres";
import { cleanupFixtures, createFixture, startDatabase, stopDatabase, waitUntil } from "./database.ts";

let redisUrl: string;
let redisContainer: string | undefined;
let admin: ReturnType<typeof createClient>;
const prefixes: string[] = [];

function docker(...args: string[]): string {
  const result = Bun.spawnSync(["docker", ...args], { stdout: "pipe", stderr: "pipe" });
  if (result.exitCode !== 0) throw new Error(result.stderr.toString());
  return result.stdout.toString().trim();
}

beforeAll(async () => {
  await startDatabase();
  redisUrl = process.env.SIMPLEBASE_TEST_REDIS_URL ?? "";
  if (!redisUrl) {
    redisContainer = "simplebase-redis-test-" + crypto.randomUUID();
    docker("run", "--rm", "--detach", "--name", redisContainer,
      "--publish", "127.0.0.1::6379", "redis:7-alpine");
    const port = docker("port", redisContainer, "6379/tcp").match(/127\.0\.0\.1:(\d+)/)?.[1];
    if (!port) throw new Error("Could not determine the Redis container port");
    redisUrl = `redis://127.0.0.1:${port}`;
  }
  admin = createClient({ url: redisUrl, socket: { reconnectStrategy: false } });
  admin.on("error", () => {});
  await waitUntil(async () => {
    try { await admin.connect(); return true; } catch { return false; }
  });
}, 20_000);

afterEach(async () => {
  await cleanupFixtures();
  if (admin?.isReady) {
    for (const prefix of prefixes.splice(0)) {
      for await (const keys of admin.scanIterator({ MATCH: prefix + "*" })) {
        if (keys.length) await admin.del(keys);
      }
    }
  }
});

afterAll(async () => {
  try { await stopDatabase(); }
  finally {
    admin?.destroy();
    if (redisContainer) docker("rm", "--force", redisContainer);
  }
});

function redisAdapter(): KvAdapter {
  const prefix = "simplebase-test:" + crypto.randomUUID() + ":";
  prefixes.push(prefix);
  return redis({ url: redisUrl, prefix });
}

for (const backend of ["postgres", "redis"] as const) {
  const adapterFor = () => backend === "redis" ? redisAdapter() : undefined;

  test(`${backend}: raw and named KV support JSON, replacement, deletion and isolated keys`, async () => {
    const fixture = await createFixture();
    const adapter = adapterFor();
    const app = await fixture.runtime({ kv: { adapter } });
    await app.action({ args: schema.object({}), async handler(ctx) {
      const values = [null, false, 0, "", [1, "two", null], { nested: { active: true } }];
      for (const [index, value] of values.entries()) {
        await ctx.kv.set(String(index), value);
        expect(await ctx.kv.get<unknown>(String(index))).toEqual(value);
      }
      expect(await ctx.kv.get("missing")).toBeUndefined();
      expect(await ctx.kv.delete("missing")).toBe(false);
      await ctx.kv.set("shared", "raw");
      await ctx.kv.namespace("settings").set("shared", "named");
      expect(await ctx.kv.get<string>("shared")).toBe("raw");
      expect(await ctx.kv.namespace("settings").get<string>("shared")).toBe("named");
      expect(await ctx.kv.namespace("other").get("shared")).toBeUndefined();
      await ctx.kv.set("shared", { replaced: true });
      expect(await ctx.kv.get<unknown>("shared")).toEqual({ replaced: true });
      expect(await ctx.kv.delete("shared")).toBe(true);
      expect(await ctx.kv.delete("shared")).toBe(false);
      expect(await ctx.kv.namespace("settings").get<string>("shared")).toBe("named");
      await ctx.kv.namespace("a:b").set("c", 1);
      await ctx.kv.namespace("a").set("b:c", 2);
      expect(await ctx.kv.namespace("a:b").get<number>("c")).toBe(1);
      expect(await ctx.kv.namespace("a").get<number>("b:c")).toBe(2);
      const name = "設定'); DROP TABLE _simplebase_kv;--";
      const key = "café\"\\' OR TRUE--";
      await ctx.kv.namespace(name).set(key, "O'Reilly");
      expect(await ctx.kv.namespace(name).get<string>(key)).toBe("O'Reilly");
    } })({});
    await app.stop();
    const replacement = await fixture.runtime({ kv: { adapter } });
    expect(await replacement.action({ args: schema.object({}), handler: (ctx) =>
      ctx.kv.namespace("settings").get("shared"),
    })({})).toBe("named");
  });

  test(`${backend}: central and local schemas validate writes and existing records`, async () => {
    const fixture = await createFixture();
    const adapter = adapterFor();
    const writer = await fixture.runtime({ kv: { adapter } });
    await writer.action({ args: schema.object({}), handler: (ctx) =>
      ctx.kv.namespace("sessions").set("old", { userId: 123 }),
    })({});
    const app = await fixture.runtime({ kv: { adapter, namespaces: {
      sessions: { schema: schema.object({ userId: schema.string(), locale: schema.string().optional() }) },
      settings: {},
      nullable: { schema: schema.string().nullable() },
    } } });
    await app.action({ args: schema.object({}), async handler(ctx) {
      const sessions = ctx.kv.namespace("sessions");
      expect(await sessions.get("missing")).toBeUndefined();
      await expect(sessions.get("old")).rejects.toBeInstanceOf(ValidationError);
      await sessions.set("valid", { userId: "user-1" });
      expect(await sessions.get("valid")).toEqual({ userId: "user-1" });
      // @ts-expect-error Deliberately exercise runtime validation.
      await expect(sessions.set("valid", { userId: 42 })).rejects.toThrow("value.userId must be a string");
      expect(await sessions.get("valid")).toEqual({ userId: "user-1" });
      const dynamicName: string = "sessions";
      await expect(ctx.kv.namespace(dynamicName).set("invalid", {})).rejects.toThrow("required");
      expect(() => ctx.kv.namespace(dynamicName, schema.unknown())).toThrow("registered KV namespaces");
      await ctx.kv.namespace("settings").set("theme", "dark");
      const local = ctx.kv.namespace("local", schema.integer());
      await local.set("count", 2);
      expect(await local.get("count")).toBe(2);
      // @ts-expect-error Deliberately exercise runtime validation.
      await expect(local.set("count", "two")).rejects.toBeInstanceOf(ValidationError);
      await ctx.kv.namespace("nullable").set("key", null);
      expect(await ctx.kv.namespace("nullable").get("key")).toBeNull();
    } })({});
  });

  test(`${backend}: KV rejects invalid JSON, empty names and expired contexts`, async () => {
    const fixture = await createFixture();
    const app = await fixture.runtime({ kv: { adapter: adapterFor() } });
    let captured!: HandlerContext;
    let capturedNamespace!: RawKvNamespace;
    await app.action({ args: schema.object({}), async handler(ctx) {
      captured = ctx;
      capturedNamespace = ctx.kv.namespace("saved");
      const circular: Record<string, unknown> = {};
      circular.self = circular;
      for (const value of [undefined, NaN, Infinity, 1n, Symbol(), () => {}, { nested: undefined }, [undefined], circular]) {
        await expect(ctx.kv.set("invalid", value)).rejects.toThrow();
      }
      for (const name of ["", "  ", 1 as unknown as string]) {
        expect(() => ctx.kv.namespace(name)).toThrow("nonempty name");
        await expect(ctx.kv.get(name)).rejects.toThrow("nonempty name");
        await expect(ctx.kv.set(name, true)).rejects.toThrow("nonempty name");
        await expect(ctx.kv.delete(name)).rejects.toThrow("nonempty name");
      }
      await capturedNamespace.set("key", true);
    } })({});
    expect(() => captured.kv.namespace("late")).toThrow("no longer active");
    for (const kv of [captured.kv, capturedNamespace]) {
      await expect(kv.get("key")).rejects.toThrow("no longer active");
      await expect(kv.set("key", false)).rejects.toThrow("no longer active");
      await expect(kv.delete("key")).rejects.toThrow("no longer active");
    }
  });

  test(`${backend}: handler rollback ${backend === "postgres" ? "includes" : "does not include"} KV writes`, async () => {
    const fixture = await createFixture();
    const app = await fixture.runtime({ kv: { adapter: adapterFor() } });
    app.subscription("saved", () => {}, { name: "observer" });
    await app.start({ signals: false });
    const fail = app.action({ args: schema.object({}), async handler(ctx) {
      await ctx.kv.set("written", true);
      await ctx.collection.insert("notes", { body: "rolled back" });
      await ctx.queue.enqueue("work", {});
      ctx.pubsub.publish("saved", {});
      throw new Error("rollback");
    } });
    await expect(fail({})).rejects.toThrow("rollback");
    const value = await app.action({ args: schema.object({}), handler: (ctx) => ctx.kv.get("written") })({});
    expect(value).toBe(backend === "postgres" ? undefined : true);
    expect(await fixture.rows("SELECT * FROM _simplebase_collections")).toEqual([]);
    expect(await fixture.rows("SELECT * FROM _simplebase_queue_jobs")).toEqual([]);
  });

  test(`${backend}: subscriptions and workers receive KV and shutdown drains active writes`, async () => {
    const fixture = await createFixture();
    const app = await fixture.runtime({ kv: { adapter: adapterFor() } });
    app.subscription("saved", async (ctx) => {
      await ctx.kv.set("subscription", true);
      await ctx.queue.enqueue("work", {});
    }, { name: "observer" });
    app.worker("work", (ctx) => ctx.kv.set("worker", true));
    await app.start({ signals: false });
    await app.action({ args: schema.object({}), handler(ctx) { ctx.pubsub.publish("saved", {}); } })({});
    const read = app.action({ args: schema.object({}), async handler(ctx) {
      return [await ctx.kv.get("subscription"), await ctx.kv.get("worker")];
    } });
    await waitUntil(async () => (await read({})).every((value) => value === true));
    let release!: () => void;
    let entered!: () => void;
    const gate = new Promise<void>((resolve) => { release = resolve; });
    const ready = new Promise<void>((resolve) => { entered = resolve; });
    const pending = app.action({ args: schema.object({}), async handler(ctx) {
      entered();
      await gate;
      await ctx.kv.set("last", true);
      expect(await ctx.kv.get<boolean>("last")).toBe(true);
    } })({});
    await ready;
    const stopped = app.stop();
    expect(app.stop()).toBe(stopped);
    release();
    await Promise.all([pending, stopped]);
  });
}

test("PostgreSQL nested actions share KV changes and caught failures abort them", async () => {
  const fixture = await createFixture();
  const app = await fixture.runtime();
  const nested = app.action({ args: schema.object({}), async handler(ctx) {
    await ctx.kv.set("nested", true);
    throw new Error("nested failure");
  } });
  const outer = app.action({ args: schema.object({}), async handler(ctx) {
    await ctx.kv.set("outer", true);
    try { await nested({}); } catch {}
    expect(await ctx.kv.get<boolean>("nested")).toBe(true);
  } });
  await expect(outer({})).rejects.toThrow("Transaction aborted by a nested action");
  expect(await fixture.rows("SELECT * FROM _simplebase_kv")).toEqual([]);
});

test("Redis prefixes isolate applications and adapter stop is idempotent", async () => {
  const fixture = await createFixture();
  const first = await fixture.runtime({ kv: { adapter: redisAdapter() } });
  const second = await fixture.runtime({ kv: { adapter: redisAdapter() } });
  await first.action({ args: schema.object({}), handler: (ctx) => ctx.kv.set("key", "first") })({});
  expect(await second.action({ args: schema.object({}), handler: (ctx) => ctx.kv.get("key") })({})).toBeUndefined();
  const backend = await redisAdapter()();
  const stopped = backend.stop!();
  expect(backend.stop!()).toBe(stopped);
  await stopped;
});

test("compiled KV namespaces and the Redis adapter run in Node.js", async () => {
  const fixture = await createFixture();
  const prefix = "simplebase-node-test:" + crypto.randomUUID() + ":";
  prefixes.push(prefix);
  const build = Bun.spawn([process.execPath, "run", "build"], {
    cwd: import.meta.dir + "/..", stdout: "pipe", stderr: "pipe",
  });
  const [buildOutput, buildErrors, buildCode] = await Promise.all([
    new Response(build.stdout).text(), new Response(build.stderr).text(), build.exited,
  ]);
  if (buildCode !== 0) throw new Error(buildOutput + buildErrors);
  const child = Bun.spawn(["node", "--input-type=module", "--eval", `
    import { createSimplebase, schema } from "./dist/index.js";
    import { redis } from "./dist/kv/redis.js";
    const app = await createSimplebase({
      storage: { engine: "postgres", url: process.env.DATABASE_URL },
      kv: {
        adapter: redis({ url: process.env.REDIS_URL, prefix: process.env.REDIS_PREFIX }),
        namespaces: { sessions: { schema: schema.object({ userId: schema.string() }) } },
      },
    });
    try {
      await app.action({ args: schema.object({}), async handler(ctx) {
        const sessions = ctx.kv.namespace("sessions");
        await sessions.set("id", { userId: "node-user" });
        if ((await sessions.get("id"))?.userId !== "node-user") throw new Error("Node Redis roundtrip failed");
        if (!await sessions.delete("id")) throw new Error("Node Redis delete failed");
        if (await sessions.get("id") !== undefined) throw new Error("Node Redis missing key failed");
      } })({});
    } finally { await app.stop(); }
  `], {
    cwd: import.meta.dir + "/..",
    env: { ...process.env, DATABASE_URL: fixture.url, REDIS_URL: redisUrl, REDIS_PREFIX: prefix },
    stdout: "pipe", stderr: "pipe",
  });
  const [output, errors, code] = await Promise.all([
    new Response(child.stdout).text(), new Response(child.stderr).text(), child.exited,
  ]);
  if (code !== 0) throw new Error(output + errors);
});

test("bootstrap rejects invalid namespaces and closes PostgreSQL when Redis initialization fails", async () => {
  const fixture = await createFixture();
  await expect(fixture.runtime({ kv: { namespaces: { " ": {} } } })).rejects.toThrow("nonempty name");
  const url = new URL(fixture.url);
  const applicationName = "simplebase-kv-failed-" + crypto.randomUUID();
  url.searchParams.set("application_name", applicationName);
  const invalidRedisUrl = new URL(redisUrl);
  invalidRedisUrl.username = "missing-user-" + crypto.randomUUID();
  invalidRedisUrl.password = crypto.randomUUID();
  let unexpected: Simplebase | undefined;
  try {
    await expect(createSimplebase({
      storage: { engine: "postgres", url: url.toString() },
      kv: { adapter: redis({ url: invalidRedisUrl.toString() }) },
    }).then((app) => { unexpected = app; return app; })).rejects.toThrow();
  } finally { await unexpected?.stop(); }
  await waitUntil(async () => (await fixture.rows(
    "SELECT 1 FROM pg_stat_activity WHERE application_name = $1", [applicationName],
  )).length === 0);
  const backend = await postgres()();
  expect(backend.stop).toBeUndefined();
});
