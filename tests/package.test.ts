import { expect, test } from "bun:test";
import { mkdtemp, mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

const root = resolve(import.meta.dir, "..");

async function run(cmd: string[], cwd: string): Promise<string> {
  const child = Bun.spawn(cmd, { cwd, stdout: "pipe", stderr: "pipe" });
  const [output, errors, code] = await Promise.all([
    new Response(child.stdout).text(), new Response(child.stderr).text(), child.exited,
  ]);
  if (code !== 0) throw new Error(`${cmd.join(" ")} failed: ${errors}`);
  return output;
}

test("the npm tarball installs, imports, and typechecks in an independent application", async () => {
  const directory = await mkdtemp(join(tmpdir(), "simplebase-package-"));
  try {
    const project = JSON.parse(await readFile(join(root, "package.json"), "utf8")) as {
      name: string; version: string; devDependencies: Record<string, string>;
    };
    const packed = JSON.parse(await run(["npm", "pack", "--json", "--pack-destination", directory], root)) as {
      filename: string;
      files: { path: string }[];
    }[];
    const paths = packed[0].files.map((file) => file.path);
    expect(paths).toContain("dist/index.js");
    expect(paths).toContain("dist/index.d.ts");
    for (const adapter of ["bun", "node", "hono", "express"]) {
      expect(paths).toContain(`dist/http/${adapter}.js`);
      expect(paths).toContain(`dist/http/${adapter}.d.ts`);
    }
    expect(paths).toContain("README.md");
    expect(paths.every((path) => path.startsWith("dist/") || ["package.json", "README.md", "LICENSE", "NOTICE"].includes(path))).toBe(true);

    const consumer = join(directory, "consumer");
    await mkdir(consumer);
    const consumerManifest = {
      name: "simplebase-consumer", private: true, type: "module",
      dependencies: { [project.name]: "file:" + join(directory, packed[0].filename) },
      devDependencies: { "@types/node": "26.6.5" } as Record<string, string>,
    };
    await writeFile(join(consumer, "package.json"), JSON.stringify(consumerManifest));
    await run([process.execPath, "install", "--ignore-scripts"], consumer);
    const manifest = JSON.parse(await readFile(join(consumer, "node_modules", project.name, "package.json"), "utf8")) as { name: string; version: string };
    expect(manifest.name).toBe(project.name);
    expect(manifest.version).toBe(project.version);

    await writeFile(join(consumer, "main.ts"), `
import { createSimplebase, Get, type Simplebase, type HandlerContext, type StartOptions, type HttpContext } from ${JSON.stringify(project.name)};
import { node } from ${JSON.stringify(project.name + "/http/node")};

class Controller {
  @Get("/health")
  health(_http: HttpContext) { return { ok: true }; }
}

async function check(): Promise<void> {
  const runtime: Simplebase = await createSimplebase({ storage: { engine: "postgres", url: "" } });
  // @ts-expect-error The published API exposes schema without the old v shortcut.
  runtime.v;
  const greet = runtime.action({
    args: runtime.schema.object({ name: runtime.schema.string() }),
    async handler(ctx: HandlerContext, input) {
      const id: string = await ctx.collection.insert("notes", { body: input.name });
      const note = await ctx.collection.get<{ body: string }>("notes", id);
      if (note) {
        const noteId: string = note.id;
        const body: string = note.body;
        // @ts-expect-error Published types preserve the requested document shape.
        const invalid: number = note.body;
        void [noteId, body, invalid];
      }
      const updated: boolean = await ctx.collection.update("notes", id, { body: "revised" });
      const deleted: boolean = await ctx.collection.delete("notes", id);
      void [updated, deleted];
      return { message: input.name };
    },
  });
  const result: { message: string } = await greet({ name: "world" });
  runtime.controller(new Controller());
  runtime.route("POST", "/greet", {
    body: runtime.schema.object({ name: runtime.schema.string() }),
    action: greet,
    input: ({ body }) => body,
    output: (result) => {
      const message: string = result.message;
      // @ts-expect-error Published HTTP types preserve action results.
      const wrong: number = result.message;
      return { message, wrong };
    },
  });
  // @ts-expect-error The published declarations must reject the wrong action input.
  await greet({ name: 123 });
  runtime.subscription<{ id: number }>("created", async (ctx, event) => {
    await ctx.queue.enqueue("sync", event);
  }, { name: "enqueueSync" });
  // @ts-expect-error Published subscriptions require a stable name.
  runtime.subscription("created", () => {});
  // @ts-expect-error The old idempotent option is no longer accepted.
  runtime.subscription("created", () => {}, { name: "old", idempotent: true });
  const options: StartOptions = { signals: false, http: node({ port: 0 }), beforeStop: async () => {} };
  await runtime.start(options);
  await runtime.stop();
  void result;
}

void check;
`);
    await run([
      join(root, "node_modules/.bin/tsc"), "--noEmit", "--strict", "--target", "ES2022",
      "--module", "ESNext", "--moduleResolution", "Bundler", "main.ts",
    ], consumer);
    const imported = await run(["node", "--input-type=module", "--eval", `
      import { createSimplebase } from ${JSON.stringify(project.name)};
      import { node } from ${JSON.stringify(project.name + "/http/node")};
      try { await createSimplebase({ storage: { engine: "postgres", url: "" } }); }
      catch (error) {
        if (!(error instanceof Error) || !error.message.includes("requires a URL")) throw error;
        console.log("Published entrypoint works");
      }
      const server = await node({ port: 0 })(async (request) => Response.json({ method: request.method }));
      try {
        const response = await fetch(server.url);
        if ((await response.json()).method !== "GET") throw new Error("Published Node adapter failed");
        console.log("Published Node adapter works");
      } finally { await server.stop(); }
      for (const framework of ["hono", "express"]) {
        try { await import(framework); throw new Error("Optional framework installed unexpectedly"); }
        catch (error) { if (error.code !== "ERR_MODULE_NOT_FOUND") throw error; }
      }
    `], consumer);
    expect(imported).toContain("Published entrypoint works");
    expect(imported).toContain("Published Node adapter works");
    for (const dependency of ["hono", "express", "@types/express", "bun-types"]) {
      consumerManifest.devDependencies[dependency] = project.devDependencies[dependency];
    }
    await writeFile(join(consumer, "package.json"), JSON.stringify(consumerManifest));
    await run([process.execPath, "install", "--ignore-scripts"], consumer);
    await writeFile(join(consumer, "adapters.ts"), `
import { type HttpAdapter, type HttpContext } from ${JSON.stringify(project.name)};
import { bun, getBunContext } from ${JSON.stringify(project.name + "/http/bun")};
import { node, getNodeContext } from ${JSON.stringify(project.name + "/http/node")};
import { hono, getHonoContext, toHono } from ${JSON.stringify(project.name + "/http/hono")};
import { express, getExpressContext, toExpress } from ${JSON.stringify(project.name + "/http/express")};
import { Hono } from "hono";
import makeExpress from "express";
const adapters: HttpAdapter[] = [bun(), node(), hono({ app: new Hono() }), express({ app: makeExpress() })];
function checkNative(http: HttpContext) {
  const bunPort: number | undefined = getBunContext(http)?.server.port;
  const nodeUrl: string | undefined = getNodeContext(http)?.req.url;
  const honoMethod: string | undefined = getHonoContext(http)?.req.method;
  const expressMethod: string | undefined = getExpressContext(http)?.req.method;
  void [bunPort, nodeUrl, honoMethod, expressMethod];
}
void [adapters, checkNative, toHono, toExpress];
`);
    await run([
      join(root, "node_modules/.bin/tsc"), "--noEmit", "--strict", "--target", "ES2022",
      "--module", "ESNext", "--moduleResolution", "Bundler", "--types", "bun-types", "adapters.ts",
    ], consumer);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
}, 60_000);
