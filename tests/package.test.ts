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
    const project = JSON.parse(await readFile(join(root, "package.json"), "utf8")) as { name: string; version: string };
    const packed = JSON.parse(await run(["npm", "pack", "--json", "--pack-destination", directory], root)) as {
      filename: string;
      files: { path: string }[];
    }[];
    const paths = packed[0].files.map((file) => file.path);
    expect(paths).toContain("dist/index.js");
    expect(paths).toContain("dist/index.d.ts");
    expect(paths).toContain("README.md");
    expect(paths.every((path) => path.startsWith("dist/") || ["package.json", "README.md", "LICENSE", "NOTICE"].includes(path))).toBe(true);

    const consumer = join(directory, "consumer");
    await mkdir(consumer);
    await writeFile(join(consumer, "package.json"), JSON.stringify({
      name: "simplebase-consumer", private: true, type: "module",
      dependencies: { [project.name]: "file:" + join(directory, packed[0].filename) },
    }));
    await run([process.execPath, "install", "--ignore-scripts"], consumer);
    const manifest = JSON.parse(await readFile(join(consumer, "node_modules", project.name, "package.json"), "utf8")) as { name: string; version: string };
    expect(manifest.name).toBe(project.name);
    expect(manifest.version).toBe(project.version);

    await writeFile(join(consumer, "main.ts"), `
import { createSimplebase, type Simplebase, type HandlerContext, type StartOptions } from ${JSON.stringify(project.name)};

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
  // @ts-expect-error The published declarations must reject the wrong action input.
  await greet({ name: 123 });
  runtime.subscription<{ id: number }>("created", async (ctx, event) => {
    await ctx.queue.enqueue("sync", event);
  }, { name: "enqueueSync" });
  // @ts-expect-error Published subscriptions require a stable name.
  runtime.subscription("created", () => {});
  // @ts-expect-error The old idempotent option is no longer accepted.
  runtime.subscription("created", () => {}, { name: "old", idempotent: true });
  const options: StartOptions = { signals: false, beforeStop: async () => {} };
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
    const imported = await run([process.execPath, "--eval", `
      import { createSimplebase } from ${JSON.stringify(project.name)};
      try { await createSimplebase({ storage: { engine: "postgres", url: "" } }); }
      catch (error) {
        if (!(error instanceof Error) || !error.message.includes("requires a URL")) throw error;
        console.log("Published entrypoint works");
      }
    `], consumer);
    expect(imported).toContain("Published entrypoint works");
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
}, 60_000);
