import type { Simplebase, Infer, StartOptions } from "@marcelsud/simplebase";

function checkContract(runtime: Simplebase): void {
  const { action, subscription, worker, schema, start, stop } = runtime;
  // @ts-expect-error The v shortcut was removed from the public API.
  runtime.v;
  const customer = schema.object({ email: schema.string(), name: schema.string(), plan: schema.string(), age: schema.integer().optional() });
  const input: Infer<typeof customer> = { email: "a@b.com", name: "Sam", plan: "starter" };
  const createCustomer = action({
    args: customer,
    async handler(ctx, args) {
      const email: string = args.email;
      const age: number | undefined = args.age;
      const [row] = await ctx.db.query<{ id: number }>("SELECT 1 AS id");
      const id: string = await ctx.collection.insert("notes", { body: email });
      const note: { id: string; body: string } | null = await ctx.collection.get<{ body: string }>("notes", id);
      if (note) {
        const body: string = note.body;
        // @ts-expect-error The supplied document shape determines the field type.
        const invalid: number = note.body;
        void [body, invalid];
      }
      const updated: boolean = await ctx.collection.update("notes", id, { body: "revised" });
      const deleted: boolean = await ctx.collection.delete("notes", id);
      const unknownNote = await ctx.collection.get("notes", id);
      if (unknownNote) {
        const documentId: string = unknownNote.id;
        const value: unknown = unknownNote.body;
        void [documentId, value];
      }
      // @ts-expect-error Collections accept object documents.
      await ctx.collection.insert("notes", "text");
      // @ts-expect-error Patches must be objects.
      await ctx.collection.update("notes", id, null);
      // @ts-expect-error Document IDs are strings.
      await ctx.collection.get("notes", 123);
      // @ts-expect-error A document row type must be an object.
      await ctx.collection.get<string>("notes", id);
      void [updated, deleted];
      ctx.pubsub.publish("customer.created", { customerId: row.id, email, age: age ?? null });
      return row;
    },
  });
  const result: Promise<{ id: number }> = createCustomer(input);
  // @ts-expect-error Action inputs are inferred from the validator.
  createCustomer({ email: 1, name: "Sam", plan: "starter" });
  subscription("customer.created", async (ctx, event) => {
    await ctx.queue.enqueue("customer.sync", event);
  }, { name: "enqueueCustomerSync" });
  // @ts-expect-error All subscriptions require a stable name.
  subscription("bad", async () => {});
  // @ts-expect-error The idempotent option has been removed.
  subscription("bad", async () => {}, { name: "old", idempotent: true });
  subscription<{ id: number }>("typed", (_ctx, event) => event.id, { name: "typed" });
  worker("customer.sync", async (ctx, event) => {
    ctx.log.info("syncing", { customerId: event.customerId });
    await ctx.db.query("UPDATE customers SET synced_at = NOW() WHERE id = $1", [event.customerId]);
  });
  worker<{ customerId: number }>("typed.sync", (_ctx, event) => event.customerId + 1);
  const options: StartOptions = { signals: false, beforeStop: async () => {} };
  const started: Promise<void> = start(options);
  const stopped: Promise<void> = stop();
  void [result, started, stopped];
}

void checkContract;
