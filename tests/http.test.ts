import { expect, test } from "bun:test";
import { Get, Post, ValidationError, type HttpContext } from "@marcelsud/simplebase";
import { createHttpRouter } from "../src/http.ts";
import { schema } from "../src/validators.ts";

function request(path: string, init?: RequestInit): Request { return new Request(`http://localhost${path}`, init); }

test("routes match literal paths before parameters and preserve decoded parameters and repeated query values", async () => {
  const router = createHttpRouter();
  router.route("GET", "/notes/:id", ({ params, query }) => ({ params, query }));
  router.route("GET", "/notes/new", () => "literal");
  expect(await (await router.fetch(request("/notes/new"))).json()).toBe("literal");
  expect(await (await router.fetch(request("/notes/hello%20world?tag=a&tag=b&__proto__=safe"))).json()).toEqual({
    params: { id: "hello world" }, query: { tag: ["a", "b"], ["__proto__"]: "safe" },
  });
  expect((await router.fetch(request("/notes/%ZZ"))).status).toBe(400);
  expect((await router.fetch(request("/notes/a/b"))).status).toBe(404);
});

test("HTTP method handling includes HEAD, OPTIONS, Allow and explicit overrides", async () => {
  const router = createHttpRouter();
  router.route("GET", "/notes", () => new Response("notes", { headers: { "X-Test": "retained" } }));
  router.route("POST", "/notes", () => undefined);
  const head = await router.fetch(request("/notes", { method: "HEAD" }));
  expect(await head.text()).toBe("");
  expect(head.headers.get("X-Test")).toBe("retained");
  const options = await router.fetch(request("/notes", { method: "OPTIONS" }));
  expect(options.status).toBe(204);
  expect(options.headers.get("Allow")).toBe("GET, HEAD, POST, OPTIONS");
  const unsupported = await router.fetch(request("/notes", { method: "DELETE" }));
  expect(unsupported.status).toBe(405);
  expect(unsupported.headers.get("Allow")).toBe(options.headers.get("Allow"));
  expect((await router.fetch(request("/missing"))).status).toBe(404);
  const missingHead = await router.fetch(request("/missing", { method: "HEAD" }));
  expect(missingHead.status).toBe(404);
  expect(await missingHead.text()).toBe("");
  router.route("HEAD", "/notes", () => new Response(null, { status: 202 }));
  router.route("OPTIONS", "/notes", () => new Response(null, { status: 200 }));
  expect((await router.fetch(request("/notes", { method: "HEAD" }))).status).toBe(202);
  expect((await router.fetch(request("/notes", { method: "OPTIONS" }))).status).toBe(200);
});

test("declarative routes validate body, params and query, and retain Request for raw handlers", async () => {
  const router = createHttpRouter();
  router.route("POST", "/notes/:id", {
    body: schema.object({ title: schema.string() }),
    params: schema.object({ id: schema.string() }),
    query: schema.object({ mode: schema.string() }),
    status: 201,
    handler: ({ body, params, query }) => ({ title: body.title, id: params.id, mode: query.mode }),
  });
  const valid = await router.fetch(request("/notes/abc?mode=edit", { method: "POST", body: '{"title":"hello"}' }));
  expect(valid.status).toBe(201);
  expect(await valid.json()).toEqual({ title: "hello", id: "abc", mode: "edit" });
  expect((await router.fetch(request("/notes/abc?mode=edit", { method: "POST", body: '{"title":1}' }))).status).toBe(400);
  expect((await router.fetch(request("/notes/abc", { method: "POST", body: '{"title":"hello"}' }))).status).toBe(400);
  expect((await router.fetch(request("/notes/abc?mode=edit", { method: "POST", body: "{" }))).status).toBe(400);
  router.route("POST", "/raw", async ({ request }) => new Response(await request.text(), { status: 202 }));
  expect(await (await router.fetch(request("/raw", { method: "POST", body: "unparsed" }))).text()).toBe("unparsed");
});

test("actions support async input/output, default JSON input and Response precedence", async () => {
  const router = createHttpRouter();
  const calls: string[] = [];
  router.route("POST", "/customers/:id", {
    body: schema.object({ data: schema.string() }),
    action: async (input: { id: string; name: string }) => { calls.push("action"); return { ...input, saved: true }; },
    input: async ({ body, params }) => { calls.push("input"); return { id: params.id, name: body.data }; },
    output: async (result, { request }) => {
      calls.push("output");
      return Response.json({ data: result }, { status: 202, headers: { "X-Method": request.method } });
    },
    status: 201,
  });
  const response = await router.fetch(request("/customers/abc", { method: "POST", body: '{"data":"Ada"}' }));
  expect(calls).toEqual(["input", "action", "output"]);
  expect(response.status).toBe(202);
  expect(response.headers.get("X-Method")).toBe("POST");
  expect(await response.json()).toEqual({ data: { id: "abc", name: "Ada", saved: true } });
  router.route("POST", "/echo", { action: async (input: unknown) => input });
  expect(await (await router.fetch(request("/echo", { method: "POST", body: "[1,2]" }))).json()).toEqual([1, 2]);
  expect((await router.fetch(request("/echo", { method: "POST", body: "{" }))).status).toBe(400);
  router.route("GET", "/empty", { action: async (input: unknown) => input });
  expect(await (await router.fetch(request("/empty"))).json()).toEqual({});
});

test("typed validation errors become 400 while unexpected failures remain private", async () => {
  const router = createHttpRouter();
  router.route("GET", "/invalid", () => { throw new ValidationError("Invalid customer"); });
  router.route("GET", "/failure", () => { throw new Error("secret database credentials"); });
  router.route("GET", "/bad-json", () => ({ invalid: BigInt(1) }));
  router.route("GET", "/bad-output", { handler: () => null, output: () => { throw new ValidationError("Invalid output"); } });
  expect(await (await router.fetch(request("/invalid"))).json()).toEqual({ error: "Invalid customer" });
  const response = await router.fetch(request("/failure"));
  expect(response.status).toBe(500);
  expect(await response.json()).toEqual({ error: "Internal server error" });
  expect((await router.fetch(request("/bad-json"))).status).toBe(500);
  expect((await router.fetch(request("/bad-output"))).status).toBe(500);
});

test("decorators share dispatch, support inherited methods, preserve this and allow overrides", async () => {
  const router = createHttpRouter();
  class Base {
    @Get("/base") base() { return "base"; }
    @Get("/overridden") overridden() { return "original"; }
  }
  class Controller extends Base {
    prefix = "saved";
    @Post("/notes", { body: schema.object({ title: schema.string() }), status: 201 })
    create({ body }: HttpContext<{ title: string }>) { return { title: `${this.prefix}: ${body.title}` }; }
    @Get("/one") @Get("/two") both() { return this.prefix; }
    override overridden() { return "replacement"; }
  }
  router.controller(new Controller());
  const response = await router.fetch(request("/notes", { method: "POST", body: '{"title":"hello"}' }));
  expect(response.status).toBe(201);
  expect(await response.json()).toEqual({ title: "saved: hello" });
  expect(await (await router.fetch(request("/base"))).json()).toBe("base");
  expect(await (await router.fetch(request("/one"))).json()).toBe("saved");
  expect(await (await router.fetch(request("/two"))).json()).toBe("saved");
  expect((await router.fetch(request("/overridden"))).status).toBe(404);
});

test("registration rejects ambiguous definitions and controller registration is atomic", async () => {
  const router = createHttpRouter();
  router.route("GET", "/notes/:id", () => null);
  expect(() => router.route("GET", "/notes/:other", () => null)).toThrow("Duplicate route");
  expect(() => router.route("GET", "relative", () => null)).toThrow("absolute path");
  expect(() => router.route("GET", "/:id/:id", () => null)).toThrow("repeated");
  // @ts-expect-error A route must select one executor.
  expect(() => router.route("GET", "/invalid", { handler: () => null, action: () => null })).toThrow("exactly one");
  class Duplicate {
    @Get("/new") first() { return null; }
    @Get("/notes/:id") duplicate() { return null; }
  }
  expect(() => router.controller(new Duplicate())).toThrow("Duplicate route");
  expect((await router.fetch(request("/new"))).status).toBe(404);
});
