import { createSimplebase, Get } from "@marcelsud/simplebase";
import { bun } from "@marcelsud/simplebase/http/bun";

const app = await createSimplebase({
  storage: {
    engine: "postgres",
    url: process.env.DATABASE_URL ?? "postgresql://simplebase:simplebase@127.0.0.1:55432/simplebase",
  },
  http: bun({ port: Number(process.env.PORT ?? 3000) }),
});

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

app.route("GET", "/health", () => ({ ok: true }));
app.route("POST", "/customers", { action: createCustomer, status: 201 });

class Customers {
  @Get("/customers")
  list() { return listCustomers({}); }
}
app.controller(new Customers());

await app.start();
console.info(`Application ready at ${app.http!.url}`);
