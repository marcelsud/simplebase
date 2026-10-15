import { Pool, type QueryResultRow } from "pg";
import { createSimplebase, type Simplebase } from "simplebase";

let databaseUrl: string;
let admin: Pool;
let container: string | undefined;
const fixtures: Fixture[] = [];

function docker(...args: string[]): string {
  const result = Bun.spawnSync(["docker", ...args], { stdout: "pipe", stderr: "pipe" });
  if (result.exitCode !== 0) throw new Error(result.stderr.toString());
  return result.stdout.toString().trim();
}

export async function waitUntil(check: () => Promise<boolean>, timeout = 10_000): Promise<void> {
  const deadline = Date.now() + timeout;
  while (Date.now() < deadline) {
    if (await check()) return;
    await Bun.sleep(25);
  }
  throw new Error("Timed out waiting for the expected database state");
}

export async function startDatabase(): Promise<void> {
  const supplied = process.env.SIMPLEBASE_TEST_PG_URL;
  if (supplied) {
    databaseUrl = supplied;
  } else {
    container = "simplebase-test-" + crypto.randomUUID();
    docker("run", "--rm", "--detach", "--name", container,
      "--env", "POSTGRES_USER=simplebase", "--env", "POSTGRES_PASSWORD=simplebase",
      "--env", "POSTGRES_DB=simplebase", "--publish", "127.0.0.1::5432", "postgres:16-alpine");
    const port = docker("port", container, "5432/tcp").match(/127\.0\.0\.1:(\d+)/)?.[1];
    if (!port) throw new Error("Could not determine the PostgreSQL container port");
    databaseUrl = `postgresql://simplebase:simplebase@127.0.0.1:${port}/simplebase`;
  }
  admin = new Pool({ connectionString: databaseUrl, connectionTimeoutMillis: 1_000 });
  await waitUntil(async () => {
    try { await admin.query("SELECT 1"); return true; } catch { return false; }
  }, 15_000);
}

export async function stopDatabase(): Promise<void> {
  await cleanupFixtures();
  await admin?.end();
  if (container) docker("rm", "--force", container);
}

export async function cleanupFixtures(): Promise<void> {
  for (const fixture of fixtures.splice(0)) await fixture.close();
}

export interface Fixture {
  url: string;
  runtime(): Promise<Simplebase>;
  rows<T extends QueryResultRow = QueryResultRow>(sql: string, params?: unknown[]): Promise<T[]>;
  close(): Promise<void>;
}

export async function createFixture(): Promise<Fixture> {
  const schema = "simplebase_test_" + crypto.randomUUID().replaceAll("-", "");
  await admin.query(`CREATE SCHEMA ${schema}`);
  const scopedUrl = new URL(databaseUrl);
  scopedUrl.searchParams.set("options", (scopedUrl.searchParams.get("options") ?? "") + ` -csearch_path=${schema}`);
  const url = scopedUrl.toString();
  const pool = new Pool({ connectionString: url });
  const runtimes: Simplebase[] = [];
  const fixture: Fixture = {
    url,
    async runtime() {
      const runtime = await createSimplebase({ storage: { engine: "postgres", url } });
      runtimes.push(runtime);
      return runtime;
    },
    async rows<T extends QueryResultRow>(sql: string, params?: unknown[]): Promise<T[]> {
      return (await pool.query<T>(sql, params)).rows;
    },
    async close() {
      for (const runtime of runtimes) await runtime.stop();
      await pool.end();
      await admin.query(`DROP SCHEMA ${schema} CASCADE`);
    },
  };
  fixtures.push(fixture);
  await pool.query(`CREATE TABLE customers (
    id SERIAL PRIMARY KEY, email TEXT NOT NULL, name TEXT NOT NULL, plan TEXT NOT NULL,
    synced_at TIMESTAMPTZ, sync_count INTEGER NOT NULL DEFAULT 0
  )`);
  return fixture;
}
