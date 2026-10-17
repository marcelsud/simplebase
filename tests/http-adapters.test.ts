import { expect, test } from "bun:test";
import { Hono } from "hono";
import createExpress from "express";
import { createServer } from "node:http";
import { createConnection } from "node:net";
import { createHttpRouter, type HttpAdapter } from "../src/http.ts";
import { bun, getBunContext } from "@marcelsud/simplebase/http/bun";
import { node, getNodeContext } from "@marcelsud/simplebase/http/node";
import { hono, getHonoContext } from "@marcelsud/simplebase/http/hono";
import { express, toExpress, getExpressContext } from "@marcelsud/simplebase/http/express";
import { listen } from "../src/http/node.ts";

function rawGet(url: URL, path: string): Promise<number> {
  return new Promise((resolve, reject) => {
    const socket = createConnection({ host: url.hostname, port: Number(url.port) }, () => {
      socket.write(`GET ${path} HTTP/1.1\r\nHost: ${url.host}\r\nConnection: close\r\n\r\n`);
    });
    let response = "";
    socket.setEncoding("utf8");
    socket.setTimeout(2_000, () => socket.destroy(new Error("Raw request timed out")));
    socket.on("data", (chunk) => { response += chunk; });
    socket.on("end", () => {
      const status = /^HTTP\/1\.[01] (\d{3})/.exec(response)?.[1];
      if (status) resolve(Number(status));
      else reject(new Error(`Invalid HTTP response: ${response}`));
    });
    socket.on("error", reject);
  });
}

const adapters: [string, () => HttpAdapter][] = [
  ["bun", () => bun({ port: 0 })], ["node", () => node({ port: 0 })],
  ["hono", () => hono({ port: 0 })], ["express", () => express({ port: 0 })],
];

for (const [name, adapter] of adapters) {
  test(`${name}: stop waits for an active streaming response`, async () => {
    let release!: () => void;
    const gate = new Promise<void>((resolve) => { release = resolve; });
    const server = await adapter()(async () => new Response(new ReadableStream({
      async start(controller) {
        controller.enqueue(new TextEncoder().encode("first"));
        await gate;
        controller.enqueue(new TextEncoder().encode("last"));
        controller.close();
      },
    })));
    let complete = false;
    try {
      const response = await fetch(server.url);
      const reader = response.body!.getReader();
      await reader.read();
      const stopped = server.stop().then(() => { complete = true; });
      await Bun.sleep(20);
      expect(complete).toBe(false);
      release();
      await stopped;
      expect(new TextDecoder().decode((await reader.read()).value)).toBe("last");
    } finally { release(); await server.stop(); }
  });

  test(`${name}: JSON, raw uploads, streaming, cookies, method handling and native context`, async () => {
    const router = createHttpRouter();
    router.route("POST", "/echo", { action: async (input: unknown) => input, status: 201 });
    router.route("POST", "/raw", async ({ request }) => new Response(await request.arrayBuffer()));
    router.route("GET", "/stream", () => {
      const headers = new Headers({ "X-Stream": "yes" });
      headers.append("Set-Cookie", "one=1; Path=/");
      headers.append("Set-Cookie", "two=2; Path=/");
      return new Response(new ReadableStream({
        async start(controller) {
          controller.enqueue(new TextEncoder().encode("first"));
          await Bun.sleep(10);
          controller.enqueue(new TextEncoder().encode("second"));
          controller.close();
        },
      }), { headers });
    });
    router.route("GET", "/native", (context) => ({
      bun: !!getBunContext(context), node: !!getNodeContext(context),
      hono: !!getHonoContext(context), express: !!getExpressContext(context),
    }));
    const originalRequest = globalThis.Request;
    const originalResponse = globalThis.Response;
    const server = await adapter()(router.fetch);
    try {
      const post = await fetch(new URL("/echo", server.url), { method: "POST", body: '{"hello":"world"}' });
      expect(post.status).toBe(201);
      expect(await post.json()).toEqual({ hello: "world" });
      const empty = await fetch(new URL("/echo", server.url), { method: "POST" });
      expect(await empty.json()).toEqual({});
      const raw = await fetch(new URL("/raw", server.url), { method: "POST", body: new Uint8Array([0, 1, 128, 255]) });
      expect(new Uint8Array(await raw.arrayBuffer())).toEqual(new Uint8Array([0, 1, 128, 255]));
      const streamed = await fetch(new URL("/stream", server.url));
      expect(streamed.headers.get("X-Stream")).toBe("yes");
      expect(streamed.headers.getSetCookie()).toEqual(["one=1; Path=/", "two=2; Path=/"]);
      expect(await streamed.text()).toBe("firstsecond");
      const head = await fetch(new URL("/stream", server.url), { method: "HEAD" });
      expect(await head.text()).toBe("");
      const native = await (await fetch(new URL("/native", server.url))).json();
      expect(native).toEqual(Object.fromEntries(adapters.map(([key]) => [key, key === name])));
      const wrongMethod = await fetch(new URL("/native", server.url), { method: "PUT" });
      expect(wrongMethod.status).toBe(405);
      expect(wrongMethod.headers.get("Allow")).toBe("GET, HEAD, OPTIONS");
      expect(globalThis.Request).toBe(originalRequest);
      expect(globalThis.Response).toBe(originalResponse);
    } finally { await Promise.all([server.stop(), server.stop()]); }
  });

  test(`${name}: disconnect aborts the request and cancels response streaming`, async () => {
    const router = createHttpRouter();
    let aborted!: () => void;
    let cancelled!: () => void;
    const abortReady = new Promise<void>((resolve) => { aborted = resolve; });
    const cancelReady = new Promise<void>((resolve) => { cancelled = resolve; });
    router.route("GET", "/stream", ({ request }) => {
      request.signal.addEventListener("abort", aborted, { once: true });
      return new Response(new ReadableStream({
        start(controller) { controller.enqueue(new TextEncoder().encode("first")); },
        cancel() { cancelled(); },
      }));
    });
    const server = await adapter()(router.fetch);
    try {
      const abort = new AbortController();
      const response = await fetch(new URL("/stream", server.url), { signal: abort.signal });
      const reader = response.body!.getReader();
      await reader.read();
      abort.abort();
      await Promise.all([abortReady, cancelReady]);
    } finally { await server.stop(); }
  }, 5_000);
}

test("Hono middleware and existing routes coexist with Simplebase routes", async () => {
  type Environment = { Variables: { identity: string } };
  const api = new Hono<Environment>();
  api.use("*", async (context, next) => { context.set("identity", "hono-user"); await next(); context.header("X-Middleware", "hono"); });
  api.get("/existing", (context) => context.text("existing"));
  const router = createHttpRouter();
  router.route("GET", "/identity", (context) => getHonoContext<Environment>(context)!.get("identity"));
  const server = await hono({ app: api, port: 0 })(router.fetch);
  try {
    const response = await fetch(new URL("/identity", server.url));
    expect(await response.json()).toBe("hono-user");
    expect(response.headers.get("X-Middleware")).toBe("hono");
    expect(await (await fetch(new URL("/existing", server.url))).text()).toBe("existing");
  } finally { await server.stop(); }
});

test("Express middleware and existing routes coexist with Simplebase routes", async () => {
  const api = createExpress();
  api.use((_req, res, next) => { res.locals.identity = "express-user"; res.setHeader("X-Middleware", "express"); next(); });
  api.get("/existing", (_req, res) => res.send("existing"));
  const router = createHttpRouter();
  router.route("GET", "/identity", (context) => getExpressContext(context)!.res.locals.identity);
  const server = await express({ app: api, port: 0 })(router.fetch);
  try {
    const response = await fetch(new URL("/identity", server.url));
    expect(await response.json()).toBe("express-user");
    expect(response.headers.get("X-Middleware")).toBe("express");
    expect(await (await fetch(new URL("/existing", server.url))).text()).toBe("existing");
  } finally { await server.stop(); }
});

for (const mounted of [false, true]) {
  test(`Express ${mounted ? "mounted bridge" : "adapter"}: path aliases cannot bypass scoped authentication`, async () => {
    const api = createExpress();
    const prefix = mounted ? "/api" : "";
    api.use(`${prefix}/admin`, (req, res, next) => {
      if (req.headers.authorization === "Bearer test") next();
      else res.status(401).send("authentication required");
    });
    const router = createHttpRouter();
    let privateCalls = 0;
    router.route("GET", "/admin", () => { privateCalls++; return { private: true }; });
    router.route("GET", "/notes/:id", ({ params, query }) => ({ params, query }));
    if (mounted) api.use(prefix, toExpress(router.fetch));
    const server = mounted
      ? await listen(createServer(api), { port: 0 })
      : await express({ app: api, port: 0 })(router.fetch);
    try {
      expect(await rawGet(server.url, `${prefix}/admin`)).toBe(401);
      for (const path of ["//unused/admin", "/public/../admin", "/public/%2e%2e/admin", "/public\\..\\admin", "/public//../admin"]) {
        expect({ path, status: await rawGet(server.url, prefix + path) }).toEqual({ path, status: 400 });
      }
      expect(await rawGet(server.url, `${prefix}/%61dmin`)).toBe(404);
      expect(privateCalls).toBe(0);
      const allowed = await fetch(new URL(`${prefix}/admin`, server.url), { headers: { authorization: "Bearer test" } });
      expect(allowed.status).toBe(200);
      expect(await allowed.json()).toEqual({ private: true });
      expect(privateCalls).toBe(1);
      const valid = await fetch(new URL(`${prefix}/notes/caf%C3%A9%20a%2Fb%252F?next=/public/../admin&other=//unused/admin`, server.url));
      expect(valid.status).toBe(200);
      expect(await valid.json()).toEqual({
        params: { id: "café a/b%2F" }, query: { next: "/public/../admin", other: "//unused/admin" },
      });
    } finally { await server.stop(); }
  });
}
