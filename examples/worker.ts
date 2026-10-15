import { createSimplebase } from "@marcelsud/simplebase";

const app = await createSimplebase({ storage: {
  engine: "postgres",
  url: process.env.DATABASE_URL ?? "postgresql://simplebase:simplebase@127.0.0.1:55432/simplebase",
} });

app.worker<{ customerId: number }>("customer.sync", async (ctx, event) => {
  await ctx.db.query("UPDATE customers SET synced_at = NOW() WHERE id = $1", [event.customerId]);
  ctx.log.info("Customer synced", { customerId: event.customerId });
});

await app.start();
console.info("Worker ready: customer.sync");
