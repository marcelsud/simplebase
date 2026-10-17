# Simplebase

**Build complex apps with simple tools.**

Signup flows, order processing, imports, notifications—applications grow into workflows. Simplebase gives you a few tools that work together, so you can build those workflows in TypeScript.

Start with a function. Add SQL, events, and background work as you need them.

- **Functions** handle application logic with validated inputs.
- **SQL** gives you direct access to your data.
- **Collections** store flexible JSON documents by ID.
- **Events** let different features react to the same change.
- **Jobs** move work into the background and survive restarts.
- **HTTP routes** expose handlers and actions through Bun, Node, Hono, or Express.

Simplebase runs on Bun or Node.js 20+ with PostgreSQL.

## Get started

```bash
bun add @marcelsud/simplebase
```

You can also install it with `npm install @marcelsud/simplebase`. Node applications use ES modules; compile TypeScript with `tsc` before running the generated JavaScript.

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
  args: app.schema.object({ name: app.schema.string() }),

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

## Collections

Store JSON documents without creating an application table for each shape. Collections are available in actions, subscriptions, and workers through `ctx.collection`:

```ts
const createNote = app.action({
  args: app.schema.object({ body: app.schema.string() }),
  handler(ctx, input) {
    return ctx.collection.insert("notes", input);
  },
});

const id = await createNote({ body: "First note" });
```

Inside a handler, use the returned UUID to read or mutate the document:

```ts
const note = await ctx.collection.get<{ body: string }>("notes", id);
await ctx.collection.update("notes", id, { body: "Revised note" });
await ctx.collection.delete("notes", id);
```

`insert` returns a generated UUID string. `get<T>` returns the document with its `id`, or `null` for a missing document. `update` and `delete` return booleans indicating whether the document existed. Names scope every operation and require no per-collection setup. A missing UUID returns `null` or `false`; malformed UUIDs are rejected by PostgreSQL.

Documents and patches must be JSON objects using the same JSON encoder as events and jobs. Top-level arrays, non-JSON values, and a supplied top-level `id` are rejected. IDs are immutable. `get<T>` supplies a compile-time row type; validate application data with `schema` when needed.

Patches replace the supplied top-level fields and preserve all other fields. Nested objects and arrays are replaced as whole fields, and `null` is stored as a value. An empty patch succeeds for an existing document. A single JSONB update applies each patch atomically: concurrent patches to different fields survive, while overlapping fields follow PostgreSQL's write order.

Documents live in `_simplebase_collections` (`collection_name`, `id`, `data`), keyed by collection name and UUID. Operations use the handler's existing guarded database query and transaction. Document changes, SQL writes, enqueued jobs, explicit publications, and delivery completion commit or roll back together. Collection operations publish no implicit events. Use `ctx.db.query()` for document queries beyond retrieval by ID.

After starting PostgreSQL, run the note example:

```bash
bun run example:collections
```

## HTTP routes

Declare routes before `start()`. A function is shorthand for `{ handler }`:

```ts
app.route("GET", "/health", () => Response.json({ ok: true }));

app.route("POST", "/preview", {
  body: app.schema.object({ name: app.schema.string() }),
  handler: ({ body }) => ({ greeting: `Hello, ${body.name}` }),
});
```

Handlers receive `request` (the standard `Request`), `params`, `query`, `body`, and optional `native` context. Routes support literal paths and `:parameters`; literal segments take precedence. A trailing slash is optional. `params` contains decoded strings, and repeated query keys produce arrays. Optional `body`, `params`, and `query` schemas validate those values and infer the handler's TypeScript types. Query strings are not automatically converted to numbers or booleans.

Bind an existing action with optional asynchronous transformations:

```ts
app.route("POST", "/notes", {
  body: app.schema.object({ text: app.schema.string() }),
  action: createNote,
  input: ({ body }) => ({ body: body.text }),
  output: (id) => ({ data: { id } }),
  status: 201,
});
```

Each route selects exactly one `handler` or `action`. Without `input`, an action receives the JSON body, or `{}` when there is no body. Without `output`, its result becomes JSON. Both transformations can return promises; `output` receives the result and HTTP context. Handlers can also use `output`.

Actions keep their own validation and transactions. `input` runs before action validation; `output` runs after the action commits. An output failure returns `500` and does not undo a committed action. HTTP adds no enclosing transaction and does not retry actions.

Return a `Response` to control the entire response, including status, headers, cookies, and streaming. JSON values use the route's `status`, defaulting to `200`; an undefined result produces an empty response, defaulting to `204`. Raw handlers can read text, form data, or streams directly through `request`. The core reads JSON only when a body schema or the default action input needs it. When a body schema is declared, use the already parsed `body` instead of reading `request` again.

Missing paths return `404`; unsupported methods return `405` with `Allow`. `HEAD` falls back to `GET` with no response body, and `OPTIONS` returns the allowed methods unless explicitly registered. Invalid JSON and input validation failures return `400`; unexpected errors return a generic `500` and are logged server-side. Throw the exported `ValidationError` for an invalid custom input mapping.

### Decorators

Decorators register the same routes and preserve the controller instance's `this`:

```ts
import { Get, Post, type HttpContext, type Infer } from "@marcelsud/simplebase";

const noteInput = app.schema.object({ body: app.schema.string() });

class Notes {
  @Get("/health")
  health() { return { ok: true }; }

  @Post("/notes", { body: noteInput, status: 201 })
  create({ body }: HttpContext<Infer<typeof noteInput>>) {
    return createNote(body);
  }
}

app.controller(new Notes());
```

`@Get`, `@Post`, `@Put`, `@Patch`, `@Delete`, `@Head`, and `@Options` accept the path and handler options. Use standard TypeScript decorators (TypeScript 5+, without `experimentalDecorators` or `emitDecoratorMetadata`). Bun can run the TypeScript directly; Node runs the JavaScript emitted by `tsc`. Decorators do not infer a method parameter's type: annotate it with `HttpContext<Infer<typeof bodySchema>>` when validating a body. Public inherited methods are supported; overriding a method replaces its inherited registrations. Controllers are registered explicitly, without dependency injection or automatic scanning.

### Adapters

Choose the adapter when creating the application:

```ts
import { createSimplebase } from "@marcelsud/simplebase";
import { node } from "@marcelsud/simplebase/http/node";

const app = await createSimplebase({
  storage: { engine: "postgres", url: process.env.DATABASE_URL! },
  http: node({ port: 3000 }),
});

app.route("GET", "/health", () => ({ ok: true }));
await app.start();
console.info(app.http!.url);
```

The adapter starts only when `start()` is called. You can also select it at startup with `app.start({ http: node({ port: 3000 }) })`. Configure `http` in one place: supplying it to both `createSimplebase()` and `start()` rejects startup, even if it is the same adapter.

| Adapter | Import | Runtime |
| --- | --- | --- |
| `bun()` | `@marcelsud/simplebase/http/bun` | Bun |
| `node()` | `@marcelsud/simplebase/http/node` | Node or Bun |
| `hono()` | `@marcelsud/simplebase/http/hono` | Node or Bun, using the Node HTTP transport |
| `express()` | `@marcelsud/simplebase/http/express` | Node or Bun, using the Node HTTP transport |

All adapters accept `port` and `hostname` (defaults: `3000` and `127.0.0.1`). Port `0` selects an available port, reported through `app.http.url`. The Bun adapter requires Bun's TypeScript types in a TypeScript project. Hono and Express are optional peer dependencies: install `hono` or `express` when choosing their adapter, and `@types/express` for Express TypeScript applications. Bun and Node adapters need neither framework.

Supply an existing framework application to use its middleware and routes before the Simplebase routes:

```ts
import { Hono } from "hono";
import { hono, getHonoContext } from "@marcelsud/simplebase/http/hono";

const api = new Hono();
api.use("*", async (context, next) => {
  await next();
  context.header("X-Service", "notes");
});

app.route("GET", "/method", (http) => ({
  method: getHonoContext(http)?.req.method ?? http.request.method,
}));

await app.start({ http: hono({ app: api, port: 3000 }) });
```

Express accepts the same `{ app, port, hostname }` options. `getExpressContext(http)` returns `{ req, res }`; `getNodeContext(http)` returns the native Node request and response; `getBunContext(http)` returns `{ server }`. Each helper returns `undefined` when another adapter is in use. Native access opts that route into the chosen framework; return the final response from the handler rather than sending it through the native response object.

Middleware before Simplebase routes should leave the request body unread. The core owns JSON parsing and validation; do not put `express.json()` before the Simplebase bridge. Existing framework routes can use their own parsers. Native framework paths remain available alongside Simplebase paths.

`app.fetch(request)` also works without a server. For an application-owned server, mount `toHono(app.fetch)` or `toExpress(app.fetch)` from the respective adapter module, then call `app.start()` with no `http` configured in either `createSimplebase()` or `start()`. The host application owns server shutdown and can drain it with `beforeStop`.

An adapter is an asynchronous function that receives `app.fetch` and returns `{ url, stop }`. It can pass native context as the second argument to `fetch`. New adapters reuse the same route dispatcher, validation, transformations, and error handling.

With an HTTP adapter configured, `stop()` closes the listener, drains HTTP requests and actions, and then closes PostgreSQL. Signal handling and `signals: false` work with every adapter. A failed startup removes its signal handlers and allows retry. Without `http`, the runtime continues to support processes dedicated to workers and subscriptions.

### HTTP examples

The runnable examples in [examples/http](examples/http) share a [notes application](examples/http/app.ts) and select the adapter in separate entrypoints. Start PostgreSQL with `docker compose up -d --wait`, then run one example:

| Entrypoint | Command | Runtime |
| --- | --- | --- |
| [bun.ts](examples/http/bun.ts) | `bun run example:http:bun` | Bun |
| [node.ts](examples/http/node.ts) | `npm run example:http:node` | Node |
| [hono.ts](examples/http/hono.ts) | `npm run example:http:hono` | Node with Hono |
| [express.ts](examples/http/express.ts) | `npm run example:http:express` | Node with Express |

The Node commands compile the library and examples with `tsc`, including decorators, before starting the server. You can also run the Hono and Express TypeScript entrypoints directly with Bun. Set `DATABASE_URL` to use another PostgreSQL database or `PORT` to choose another port; defaults match the local Docker setup and port `3000`. Press Ctrl+C to drain and stop the application.

| Route | Demonstrates |
| --- | --- |
| `GET /health` | A direct handler returning a `Response` |
| `POST /preview` | A declarative handler validating `{ "body": "text" }` |
| `POST /notes` | An action binding transforming `{ "text": "text" }`, with a response envelope and `Location` header |
| `POST /notes/decorated` | `@Post` with a validated `{ "body": "text" }` input |
| `GET /notes/:id` | `@Get`, typed path parameters, and a collection read |
| `GET /context` | The selected adapter's native context |
| `GET /framework` | An existing framework route, in Hono and Express |

Try the same requests with any adapter:

```bash
curl http://localhost:3000/health
curl http://localhost:3000/preview \
  -H 'Content-Type: application/json' -d '{"body":"  First note  "}'
curl -i http://localhost:3000/notes \
  -H 'Content-Type: application/json' -d '{"text":"  First note  "}'
curl http://localhost:3000/notes/decorated \
  -H 'Content-Type: application/json' -d '{"body":"Created through a decorator"}'
curl http://localhost:3000/context
```

Follow the URL in the `Location` header to retrieve a created note. The Hono and Express examples also add an `X-Request-ID` header in middleware and expose that same ID through `/context`.

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

The application commits the customer and a subscription delivery. The subscription enqueues a job; the worker updates `synced_at`. Simplebase consumes only destinations with handlers loaded in that process.

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

## Subscription lifecycle

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

Simplebase is an early prototype and its API may change between releases. It supports combined and split deployments with replicated actions, subscriptions, and workers. Polling processes handle one delivery at a time, using `FOR UPDATE SKIP LOCKED` to share work. Scale concurrency by adding processes.

Each running handler holds a PostgreSQL transaction and connection for its duration. Long handlers therefore occupy connections and retain locks; size pools and keep handlers short for your workload. There is no separate broker, event history, leader election, lease system, or process discovery.

You provide your application tables and HTTP routes. External operations should tolerate job retries.

## License

[MIT](LICENSE).
