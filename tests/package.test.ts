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
      dependencies: { simplebase: "file:" + join(directory, packed[0].filename) },
    }));
    await run([process.execPath, "install", "--ignore-scripts"], consumer);
    const manifest = JSON.parse(await readFile(join(consumer, "node_modules/simplebase/package.json"), "utf8")) as { name: string; version: string };
    const project = JSON.parse(await readFile(join(root, "package.json"), "utf8")) as { name: string; version: string };
    expect(manifest.name).toBe(project.name);
    expect(manifest.version).toBe(project.version);

    await writeFile(join(consumer, "main.ts"), `
import { createSimplebase, type Simplebase, type HandlerContext } from "simplebase";

async function check(): Promise<void> {
  const runtime: Simplebase = await createSimplebase({ storage: { engine: "postgres", url: "" } });
  const greet = runtime.action({
    args: runtime.v.object({ name: runtime.v.string() }),
    handler(_ctx: HandlerContext, input) { return { message: input.name }; },
  });
  const result: { message: string } = await greet({ name: "world" });
  // @ts-expect-error The published declarations must reject the wrong action input.
  await greet({ name: 123 });
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
      import { createSimplebase } from "simplebase";
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
