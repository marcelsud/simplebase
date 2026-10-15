import type { Simplebase, Infer } from "simplebase";

function checkContract(runtime: Simplebase): void {
  const { action, subscription, worker, v, start, stop } = runtime;
  const customer = v.object({ email: v.string(), name: v.string(), plan: v.string(), age: v.integer().optional() });
  const input: Infer<typeof customer> = { email: "a@b.com", name: "Sam", plan: "starter" };
  const createCustomer = action({
    args: customer,
    async handler(ctx, args) {
      const email: string = args.email;
      const age: number | undefined = args.age;
      const [row] = await ctx.db.query<{ id: number }>("SELECT 1 AS id");
      ctx.pubsub.publish("customer.created", { customerId: row.id, email, age: age ?? null });
      return row;
    },
  });
  const result: Promise<{ id: number }> = createCustomer(input);
  // @ts-expect-error Action inputs are inferred from the validator.
  createCustomer({ email: 1, name: "Sam", plan: "starter" });
  subscription("customer.created", async (ctx, event) => {
    await ctx.queue.enqueue("customer.sync", event);
  }, { idempotent: true, name: "enqueueCustomerSync" });
  // @ts-expect-error Idempotent subscriptions require a name.
  subscription("bad", async () => {}, { idempotent: true });
  worker("customer.sync", async (ctx, event) => {
    ctx.log.info("syncing", { customerId: event.customerId });
    await ctx.db.query("UPDATE customers SET synced_at = NOW() WHERE id = $1", [event.customerId]);
  });
  worker<{ customerId: number }>("typed.sync", (_ctx, event) => event.customerId + 1);
  const started: Promise<void> = start();
  const stopped: Promise<void> = stop();
  void [result, started, stopped];
}

void checkContract;
