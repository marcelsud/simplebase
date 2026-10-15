import { createSimplebase, type HandlerContext } from "@marcelsud/simplebase";

const [role, scenario = "normal", name = "enqueueCustomerSync"] = Bun.argv.slice(2);
// Deliberately skew failing replicas to prove retries do not use the process clock.
if (scenario === "caught-sql") Date.now = () => 0;
await new Promise<void>((resolve) => {
  process.once("message", () => resolve());
  process.send?.({ type: "booted" });
});
const runtime = await createSimplebase({ storage: { engine: "postgres", url: process.env.DATABASE_URL! } });
const releases = new Map<number, () => void>();

async function barrier(customerId: number): Promise<void> {
  await new Promise<void>((resolve) => {
    releases.set(customerId, resolve);
    process.send?.({ type: "entered", customerId });
  });
}

async function effect(ctx: HandlerContext, kind: string, customerId: number): Promise<void> {
  await ctx.db.query("INSERT INTO delivery_effects (kind, customer_id) VALUES ($1, $2)", [kind, customerId]);
  await ctx.collection.insert("delivery.effects", { kind, customerId });
  if (scenario === "before-commit") await barrier(customerId);
  if (scenario === "fail") throw new Error("consumer unavailable");
  if (scenario === "caught-sql") {
    try { await ctx.db.query("INSERT INTO missing_table VALUES (1)"); } catch {}
  }
}

if (role === "subscription" || role === "combined") {
  runtime.subscription<{ customerId: number }>("customer.created", async (ctx, event) => {
    await ctx.queue.enqueue("customer.sync", event);
    ctx.pubsub.publish("subscription.done", event);
    await effect(ctx, "subscription:" + name, event.customerId);
  }, { name });
}
if (role === "worker" || role === "combined" || role === "signal-worker") {
  runtime.worker<{ customerId: number }>("customer.sync", async (ctx, event) => {
    await ctx.db.query("UPDATE customers SET synced_at = NOW(), sync_count = sync_count + 1 WHERE id = $1", [event.customerId]);
    await ctx.queue.enqueue("worker.followup", event);
    ctx.pubsub.publish("worker.done", event);
    await effect(ctx, "worker", event.customerId);
  });
}

const publish = runtime.action({
  args: runtime.schema.object({ email: runtime.schema.string() }),
  async handler(ctx, input) {
    const [customer] = await ctx.db.query<{ id: number }>(
      "INSERT INTO customers (email, name, plan) VALUES ($1, 'Test', 'starter') RETURNING id", [input.email],
    );
    ctx.pubsub.publish("customer.created", { customerId: customer.id });
    if (scenario === "direct-job") await ctx.queue.enqueue("customer.sync", { customerId: customer.id });
    if (scenario === "before-commit") await barrier(customer.id);
    if (scenario === "rollback") {
      await ctx.queue.enqueue("customer.sync", { customerId: customer.id });
      throw new Error("publisher rollback");
    }
    return customer;
  },
});

process.on("message", async (message: { type: string; requestId?: number; customerId?: number; email?: string }) => {
  try {
    if (message.type === "release") releases.get(message.customerId!)?.();
    if (message.type === "publish") {
      const customer = await publish({ email: message.email ?? "test@example.com" });
      process.send?.({ type: "published", requestId: message.requestId, customerId: customer.id });
    }
    if (message.type === "stop") {
      await runtime.stop();
      process.send?.({ type: "stopped" });
      process.exit(0);
    }
  } catch (error) {
    process.send?.({ type: "error", requestId: message.requestId, message: String(error) });
  }
});

if (role === "signal-worker") {
  process.channel?.unref();
  await runtime.start({ beforeStop: () => { process.send?.({ type: "stopping" }); } });
} else {
  await runtime.start();
}
process.send?.({ type: "ready" });
