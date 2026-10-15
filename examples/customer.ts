import { createSimplebase } from "@marcelsud/simplebase";

const simplebase = await createSimplebase({
  storage: {
    engine: "postgres",
    url: process.env.DATABASE_URL ?? "postgresql://simplebase:simplebase@127.0.0.1:55432/simplebase",
  },
});

const { action, subscription, v, worker, start, stop } = simplebase;

const initializeCustomers = action({
  args: v.object({}),
  async handler(ctx) {
    await ctx.db.query(`CREATE TABLE IF NOT EXISTS customers (
      id SERIAL PRIMARY KEY,
      email TEXT NOT NULL,
      name TEXT NOT NULL,
      plan TEXT NOT NULL,
      synced_at TIMESTAMPTZ
    )`);
  },
});

const createCustomer = action({
  args: v.object({ email: v.string(), name: v.string(), plan: v.string() }),
  async handler(ctx, input) {
    const [customer] = await ctx.db.query<{ id: number }>(
      "insert into customers (email, name, plan) values ($1, $2, $3) returning id",
      [input.email, input.name, input.plan],
    );
    ctx.pubsub.publish("customer.created", { customerId: customer.id, email: input.email });
    return customer;
  },
});

subscription("customer.created", async (ctx, event) => {
  await ctx.queue.enqueue("customer.sync", event);
}, { name: "enqueueCustomerSync" });

worker("customer.sync", async (ctx, event) => {
  ctx.log.info("syncing customer", { customerId: event.customerId });
  await ctx.db.query("update customers set synced_at = now() where id = $1", [event.customerId]);
});

const getCustomer = action({
  args: v.object({ id: v.integer() }),
  async handler(ctx, input) {
    const [customer] = await ctx.db.query<{ id: number; email: string; synced_at: Date | null }>(
      "select id, email, synced_at from customers where id = $1", [input.id],
    );
    return customer;
  },
});

try {
  await initializeCustomers({});
  await start();
  const customer = await createCustomer({ email: "hello@example.com", name: "Hello World", plan: "starter" });
  const deadline = Date.now() + 10_000;
  for (;;) {
    const stored = await getCustomer(customer);
    if (stored.synced_at) {
      console.info("Customer created and synced", stored);
      break;
    }
    if (Date.now() >= deadline) throw new Error("Timed out waiting for customer sync");
    await Bun.sleep(50);
  }
} finally {
  await stop();
}
