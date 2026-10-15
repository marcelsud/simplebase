# Simplebase

**Build complex apps with simple tools.**

Signup flows, order processing, imports, notifications—applications grow into workflows. Simplebase gives you a few tools that work together, so you can build those workflows in TypeScript.

Start with a function. Add SQL, events, and background work as you need them.

- **Functions** handle application logic with validated inputs.
- **SQL** gives you direct access to your data.
- **Events** let different features react to the same change.
- **Jobs** move work into the background and survive restarts.

Simplebase runs on Bun and PostgreSQL.

## Get started

```bash
bun add @marcelsud/simplebase
```

Add your PostgreSQL URL to `.env`:

```dotenv
DATABASE_URL=postgresql://user:password@localhost:5432/my_app
```

Save this as `app.ts`:

```ts
import { createSimplebase } from "@marcelsud/simplebase";

const app = await createSimplebase({
  storage: {
    engine: "postgres",
    url: process.env.DATABASE_URL!,
  },
});

const welcome = app.action({
  args: app.v.object({ name: app.v.string() }),

  async handler(ctx, input) {
    await ctx.queue.enqueue("welcome", input);
  },
});

app.worker<{ name: string }>("welcome", (ctx, job) => {
  ctx.log.info(`Welcome, ${job.name}!`);
});

await welcome({ name: "world" });
await app.start();
```

Run it:

```bash
bun run app.ts
```

The function saves a job. The worker picks it up and logs `Welcome, world!`. Press **Ctrl+C** to exit.

## Grow the workflow

The same tools can handle a larger flow:

1. Create a customer with SQL.
2. Publish `customer.created`.
3. Let subscriptions enqueue welcome messages, CRM syncs, or other work.
4. Process those jobs with workers.

Your application defines what happens. Simplebase provides the transactions, durable event delivery, and job processing.

Give every subscription a stable name and declare consumers before `start()`:

```ts
app.subscription<{ customerId: number }>("customer.created", async (ctx, event) => {
  await ctx.queue.enqueue("customer.sync", event);
}, { name: "enqueueCustomerSync" });

await app.start();
```

`start()` saves the subscription bindings before reporting readiness. Start a subscription once before sending publications it must receive. A new subscription receives future publications, without historical replay. Its binding survives shutdown, so publications made while all its processes are offline remain pending until a consumer returns.

See the [customer example](https://github.com/marcelsud/simplebase/blob/main/examples/customer.ts) for a working flow.

## Try the full example

Requires Bun and Docker with Compose.

```bash
git clone https://github.com/marcelsud/simplebase.git
cd simplebase
bun install --frozen-lockfile
bun run db:up
bun run example
```

## Run separate processes

Use the same `DATABASE_URL` and application schema for every process. These are application replicas connecting to one writable PostgreSQL database.

Run these commands in separate terminals, waiting for each readiness message. Start the subscription before allowing publisher traffic:

```bash
bun run examples/subscription.ts
```

```bash
bun run examples/worker.ts
```

```bash
bun run examples/application.ts
```

Create a customer and inspect the eventual result:

```bash
curl http://localhost:3000/customers \
  -H 'Content-Type: application/json' \
  -d '{"email":"hello@example.com","name":"Hello World","plan":"starter"}'
curl http://localhost:3000/customers
```

The application commits the customer and a subscription delivery. The subscription enqueues a job; the worker updates `synced_at`. HTTP routes belong to the application. Simplebase consumes only destinations with handlers loaded in that process.

After verifying the three-process flow, run additional copies of the subscription and worker commands to share their work. Additional application replicas need different ports, for example `PORT=3001 bun run examples/application.ts`.

The pair `(topic, name)` identifies a subscription. Replicas with the same pair compete for deliveries; different names each get a delivery. A worker queue and subscription with the same name are separate destinations. A combined runtime, such as `examples/customer.ts`, uses the same durable delivery path.

## Transactions and retries

Application writes, directly enqueued jobs, and deliveries to known subscriptions commit or roll back together. `ctx.pubsub.publish()` validates and snapshots the JSON payload synchronously; the runtime materializes deliveries using the handler's database connection before commit.

Actions return after their own commit. Subscriptions execute asynchronously, and their failures never reject the publishing action. Each subscriber or worker commits its database changes, enqueued jobs, publications, and completion marker in one transaction. Failed attempts roll back those changes and record retry state while retaining the delivery lock. Retries wait one second according to PostgreSQL time, up to five attempts. Exhaustion retains a `failed` row and its last error in `_simplebase_queue_jobs`; completed records are retained too.

Committed deliveries survive process failure. A process killed before commit leaves its delivery available for another attempt; a completed delivery is no longer claimed. External effects can repeat when an attempt fails: use application-level idempotency for external services. Separately published events are distinct deliveries, even when their payloads match.

There is no exactly-once execution guarantee, global event order, creation-order handler dispatch, or ordering between subscriptions. Express dependencies by publishing or enqueueing the next step explicitly.

`stop()` stops new claims, drains the current delivery and in-flight actions, then closes the pool. It keeps subscription bindings and pending work. Consumers cannot be declared after `start()` is called.

`start()` handles `SIGINT` and `SIGTERM` automatically by calling `stop()`. Disable this when the host application manages signals:

```ts
await app.start({ signals: false });
```

For application-owned resources such as an HTTP server, register their cleanup with `beforeStop`:

```ts
const server = Bun.serve({ fetch: () => new Response("OK") });
await app.start({ beforeStop: () => server.stop() });
```

On a signal or explicit `stop()`, the runtime stops claiming deliveries and runs `beforeStop` once. Actions remain available while that callback drains existing HTTP requests. After it finishes, the runtime rejects new actions, drains in-flight work, and closes the pool. Signal listeners are removed on shutdown or startup failure. A failing callback still triggers runtime cleanup; signal-triggered cleanup errors are logged and set the process exit code to 1. Simplebase lets the process exit naturally without forcing termination.

## Upgrade from 0.1.1

1. Pause publisher traffic and stop all old consumers, draining their work. Running old and new consumer versions together is unsupported.
2. Add `{ name: "stableName" }` to every subscription and remove `idempotent`. Account for asynchronous subscriber execution and the loss of creation-order dispatch. Awaiting an action confirms only its own commit; observe application state when a workflow must finish.
3. Initialize the new runtime against the existing database. Initialization serializes concurrent migrations and adds destination metadata to existing worker jobs without discarding their payloads, states, attempts, errors, or timestamps. Events held only in memory by 0.1.1 cannot be recovered.
4. Start the new subscription and worker processes. Wait for their `start()` calls to finish before resuming publishers.

Keep stable names across deploys. Renaming creates a new subscription with no historical replay; the old binding continues receiving deliveries until explicitly retired. Either finish the old backlog with its old handler or deliberately discard it during retirement.

To retire a subscription, pause publishers, stop all consumers declaring that binding, and drain in-flight transactions. In the application's schema, remove the binding and any pending or failed deliveries in one transaction:

```sql
BEGIN;
DELETE FROM _simplebase_subscriptions
WHERE topic = 'customer.created' AND name = 'enqueueCustomerSync';
DELETE FROM _simplebase_queue_jobs
WHERE destination_kind = 'subscription'
  AND topic = 'customer.created' AND queue_name = 'enqueueCustomerSync'
  AND status IN ('pending', 'failed');
COMMIT;
```

Completed records remain for inspection. Remove the declaration from future deployments, or the next `start()` will recreate the binding. To rename, start the replacement binding before resuming publishers. Ordinary process shutdown never retires a binding.

## Current scope

Simplebase is an early prototype supporting combined and split deployments with replicated actions, subscriptions, and workers. Polling processes handle one delivery at a time, using `FOR UPDATE SKIP LOCKED` to share work. Scale concurrency by adding processes.

Each running handler holds a PostgreSQL transaction and connection for its duration. Long handlers therefore occupy connections and retain locks; size pools and keep handlers short for your workload. There is no separate broker, event history, leader election, lease system, or process discovery.

You provide your application tables and HTTP routes. External operations should tolerate job retries.

## License

[MIT](LICENSE).
