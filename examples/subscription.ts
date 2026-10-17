import { createSimplebase } from "@marcelsud/simplebase";
import { postgres as postgresStorage } from "@marcelsud/simplebase/storage/postgres";

const app = await createSimplebase({ storage: postgresStorage({ url: process.env.DATABASE_URL ?? "postgresql://simplebase:simplebase@127.0.0.1:55432/simplebase" }) });

app.subscription<{ customerId: number }>("customer.created", async (ctx, event) => {
  await ctx.queue.enqueue("customer.sync", event);
}, { name: "enqueueCustomerSync" });

await app.start();
console.info("Subscription ready: customer.created / enqueueCustomerSync");
