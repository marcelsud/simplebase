import { createSimplebase } from "@marcelsud/simplebase";

const scenario = Bun.argv[2];
const runtime = await createSimplebase({ storage: { engine: "postgres", url: process.env.DATABASE_URL! } });

runtime.worker("customer.sync", async (ctx, event) => {
  await ctx.db.query("UPDATE customers SET synced_at = NOW(), sync_count = sync_count + 1 WHERE id = $1", [event.customerId]);
  if (scenario === "before-commit") {
    process.send?.("before-commit");
    await new Promise(() => {});
  } else {
    ctx.pubsub.publish("sync.committed", event);
  }
});

runtime.subscription("sync.committed", async () => {
  process.send?.("after-commit");
  await new Promise(() => {});
});

await runtime.start();
