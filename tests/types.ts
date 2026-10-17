import { createSimplebase, schema, type Simplebase, type Infer, type StartOptions, type HttpContext, type KvAdapter, type StorageAdapter } from "@marcelsud/simplebase";
import { sqlite } from "@marcelsud/simplebase/storage/sqlite";
import { postgres as postgresStorage } from "@marcelsud/simplebase/storage/postgres";
import { redis } from "@marcelsud/simplebase/kv/redis";
import { postgres } from "@marcelsud/simplebase/kv/postgres";

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
  runtime.route("POST", "/customers", {
    body: schema.object({ data: customer }),
    action: createCustomer,
    input: ({ body }) => body.data,
    output: (created, context) => {
      const id: number = created.id;
      const email: string = context.body.data.email;
      // @ts-expect-error Action results retain their inferred type in output.
      const invalid: string = created.id;
      return { id, email, invalid };
    },
    status: 201,
  });
  runtime.route("POST", "/preview", {
    body: customer,
    params: schema.object({ id: schema.string() }),
    query: schema.object({ mode: schema.string() }),
    handler: ({ body, params, query }) => {
      const email: string = body.email;
      const id: string = params.id;
      const mode: string = query.mode;
      // @ts-expect-error HTTP schemas determine the handler input types.
      const invalid: number = body.email;
      return { email, id, mode, invalid };
    },
  });
  // @ts-expect-error An input mapping must satisfy the bound action's input.
  runtime.route("POST", "/wrong-input", { action: createCustomer, input: () => ({ email: 123 }) });
  // @ts-expect-error A route cannot select both handler and action.
  runtime.route("POST", "/ambiguous", { handler: () => null, action: createCustomer });
  runtime.route("GET", "/raw", async (http: HttpContext) => new Response(await http.request.text()));
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

async function checkKv(): Promise<void> {
  const adapters: KvAdapter[] = [postgres(), redis({ url: "redis://localhost:6379" })];
  const runtime = await createSimplebase({
    storage: postgresStorage({ url: "" }),
    kv: { namespaces: {
      settings: {},
      sessions: { schema: schema.object({ userId: schema.string(), locale: schema.string().optional() }) },
      nullable: { schema: schema.string().nullable() },
    } },
  });
  const action = runtime.action({ args: schema.object({}), async handler(ctx) {
    const raw: unknown = await ctx.kv.get("raw");
    const asserted: string | undefined = await ctx.kv.get<string>("raw");
    await ctx.kv.set("raw", { anything: true });
    const settings = ctx.kv.namespace("settings");
    const theme: string | undefined = await settings.get<string>("theme");
    await settings.set("theme", "dark");
    const sessions = ctx.kv.namespace("sessions");
    await sessions.set("id", { userId: "user-1" });
    const session = await sessions.get("id");
    if (session) {
      const userId: string = session.userId;
      const locale: string | undefined = session.locale;
      // @ts-expect-error Bootstrap namespace schemas determine their value types.
      const wrong: number = session.userId;
      void [userId, locale, wrong];
    }
    // @ts-expect-error A registered namespace cannot bypass its schema using get<T>.
    await sessions.get<number>("id");
    // @ts-expect-error The namespace requires a string userId.
    await sessions.set("id", { userId: 123 });
    // @ts-expect-error Required fields cannot be omitted.
    await sessions.set("id", {});
    // @ts-expect-error A locally supplied schema cannot replace a registered schema.
    ctx.kv.namespace("sessions", schema.string());
    const local = ctx.kv.namespace("local", schema.integer());
    const count: number | undefined = await local.get("count");
    await local.set("count", 1);
    // @ts-expect-error Local schemas also infer and check write types.
    await local.set("count", "1");
    const nullable: string | null | undefined = await ctx.kv.namespace("nullable").get("key");
    const deleted: boolean = await sessions.delete("id");
    void [raw, asserted, theme, count, nullable, deleted];
    return session;
  } });
  const result: { userId: string; locale?: string } | undefined = await action({});
  runtime.subscription("typed", async (ctx) => {
    const session = await ctx.kv.namespace("sessions").get("id");
    const userId: string | undefined = session?.userId;
    void userId;
  }, { name: "typed" });
  runtime.worker("typed", async (ctx) => {
    // @ts-expect-error Workers retain the bootstrap namespace types.
    await ctx.kv.namespace("sessions").set("id", 123);
  });
  // @ts-expect-error KV is always available; configuration does not accept an enablement flag.
  await createSimplebase({ storage: postgresStorage({ url: "" }), kv: true });
  void [adapters, result];
}

void checkKv;

function checkStorage(): void {
  const adapters: StorageAdapter[] = [postgresStorage({ url: "postgresql://localhost/app" }), sqlite({ filename: ":memory:" })];
  // @ts-expect-error Storage selection uses an adapter factory.
  void createSimplebase({ storage: { engine: "postgres", url: "" } });
  // @ts-expect-error SQLite requires an explicit filename.
  sqlite({});
  // @ts-expect-error PostgreSQL requires a URL.
  postgresStorage({ filename: "app.sqlite" });
  void adapters;
}

void checkStorage;
