import { createSimplebase } from "@marcelsud/simplebase";

const app = await createSimplebase({ storage: {
  engine: "postgres",
  url: process.env.DATABASE_URL ?? "postgresql://simplebase:simplebase@127.0.0.1:55432/simplebase",
} });

await app.action({ args: app.schema.object({}), async handler(ctx) {
  await ctx.db.query(`CREATE TABLE IF NOT EXISTS customers (
    id SERIAL PRIMARY KEY, email TEXT NOT NULL, name TEXT NOT NULL, plan TEXT NOT NULL, synced_at TIMESTAMPTZ
  )`);
} })({});

const input = app.schema.object({ email: app.schema.string(), name: app.schema.string(), plan: app.schema.string() });
const createCustomer = app.action({ args: input, async handler(ctx, customer) {
  const [row] = await ctx.db.query<{ id: number }>(
    "INSERT INTO customers (email, name, plan) VALUES ($1, $2, $3) RETURNING id",
    [customer.email, customer.name, customer.plan],
  );
  ctx.pubsub.publish("customer.created", { customerId: row.id });
  return row;
} });
const listCustomers = app.action({ args: app.schema.object({}), handler(ctx) {
  return ctx.db.query("SELECT id, email, name, plan, synced_at FROM customers ORDER BY id");
} });

const server = Bun.serve({
  port: Number(process.env.PORT ?? 3000),
  async fetch(request) {
    if (new URL(request.url).pathname !== "/customers") return new Response("Not found", { status: 404 });
    if (request.method === "GET") return Response.json(await listCustomers({}));
    if (request.method !== "POST") return new Response("Method not allowed", { status: 405, headers: { Allow: "GET, POST" } });
    let customer;
    try { customer = input.parse(await request.json(), "body"); }
    catch { return Response.json({ error: "Expected email, name and plan strings" }, { status: 400 }); }
    return Response.json(await createCustomer(customer), { status: 201 });
  },
});
await app.start({ beforeStop: () => server.stop() });
console.info(`Application ready at ${server.url}`);
