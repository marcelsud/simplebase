import { expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { isBuiltin } from "node:module";
import { relative, resolve } from "node:path";
import ts from "typescript";

const root = resolve(import.meta.dir, "..");
const sourceRoot = resolve(root, "src");
const config = ts.readConfigFile(resolve(root, "tsconfig.json"), ts.sys.readFile);
const { options } = ts.parseJsonConfigFileContent(config.config, ts.sys, root);

function layer(file: string): number {
  if (file.startsWith("core/")) return 0;
  if (file === "http.ts") return 2;
  if (file.startsWith("http/")) return 3;
  if (file.startsWith("storage/") || file.startsWith("kv/")) return 3;
  if (file === "simplebase.ts") return 4;
  if (file === "index.ts") return 5;
  return 1;
}

test("source imports, including types and re-exports, point inward", () => {
  const violations: string[] = [];
  for (const file of new Bun.Glob("**/*.ts").scanSync({ cwd: sourceRoot })) {
    const path = resolve(sourceRoot, file);
    const { importedFiles } = ts.preProcessFile(readFileSync(path, "utf8"), true, true);
    for (const { fileName } of importedFiles) {
      if (isBuiltin(fileName)) {
        if (file.startsWith("core/")) violations.push(`${file}: core imports ${fileName}`);
        continue;
      }
      const target = ts.resolveModuleName(fileName, path, options, ts.sys).resolvedModule;
      if (!target) {
        violations.push(`${file}: unresolved import ${fileName}`);
        continue;
      }
      const dependency = relative(sourceRoot, target.resolvedFileName);
      if (file === "simplebase.ts" && (dependency.startsWith("storage/") || dependency.startsWith("kv/"))) {
        violations.push(`${file}: bootstrap imports concrete adapter ${fileName}`);
      }
      if (["simplebase.ts", "collections.ts", "kv.ts", "storage.ts"].includes(file) && target.isExternalLibraryImport) {
        violations.push(`${file}: service imports external driver ${fileName}`);
      }
      if (file.startsWith("core/") && !dependency.startsWith("core/")) {
        violations.push(`${file}: core imports ${fileName}`);
      } else if (!dependency.startsWith("../") && layer(dependency) > layer(file)) {
        violations.push(`${file}: outward dependency on ${fileName}`);
      }
    }
  }
  expect(violations).toEqual([]);
});
