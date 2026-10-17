# Simplebase

**Build complex apps with simple tools.**

Validated TypeScript actions, SQL, JSON collections, durable events, background jobs, and HTTP routes, backed by PostgreSQL.

Runs on Bun or Node.js 20+. Simplebase is an early prototype; its API may change.

## Get started

Install with Bun or npm:

```bash
bun add @marcelsud/simplebase
# or: npm install @marcelsud/simplebase
```

With PostgreSQL running, save this as `app.ts`:

```ts
import { createSimplebase } from "@marcelsud/simplebase";
import { node } from "@marcelsud/simplebase/http/node";

const app = await createSimplebase({
  storage: { engine: "postgres", url: process.env.DATABASE_URL! },
  http: node({ port: 3000 }),
});

const createNote = app.action({
  args: app.schema.object({ body: app.schema.string() }),
  async handler(ctx, input) {
    const id = await ctx.collection.insert("notes", input);
    ctx.pubsub.publish("note.created", { id });
    return { id, ...input };
  },
});

app.subscription<{ id: string }>("note.created", async (ctx, event) => {
  await ctx.queue.enqueue("note.notify", event);
}, { name: "notifyOnNoteCreated" });

app.worker<{ id: string }>("note.notify", (ctx, job) => {
  ctx.log.info("Note created", job);
});

app.route("GET", "/health", () => ({ ok: true }));
app.route("POST", "/notes", { action: createNote, status: 201 });

await app.start();
```

Run it with your database URL:

```bash
DATABASE_URL=postgresql://user:password@localhost:5432/my_app bun run app.ts
```

Then, in another terminal:

```bash
curl http://localhost:3000/notes \
  -H 'Content-Type: application/json' -d '{"body":"First note"}'
```

This saves a note, publishes an event, and processes a notification job. Press Ctrl+C to stop.

For Node.js, use ES modules and compile TypeScript with `tsc` before running the generated JavaScript.

## Inside a handler

Actions, subscriptions, and workers receive the same context:

| API | Purpose |
| --- | --- |
| `ctx.db.query(sql, params)` | Execute parameterized SQL against your application tables. |
| `ctx.collection` | Store JSON documents by collection name and UUID. |
| `ctx.pubsub.publish(topic, payload)` | Publish an event to named subscriptions. |
| `ctx.queue.enqueue(queue, payload)` | Save a job for a worker. |
| `ctx.log.info(message, fields)` | Write a log message. |

Collections need no setup. `insert(name, data)` returns a UUID; `get<T>(name, id)` returns the document with its `id`, or `null`. `update(name, id, patch)` replaces supplied top-level fields; `delete(name, id)` removes the document. Both return whether the document existed. Documents and patches must be JSON objects without a supplied `id`.

## HTTP

Routes accept a handler or an action. Action routes receive the JSON body by default; `input` and `output` can transform requests and results. Return a `Response` for custom status, headers, or streaming.

Choose an adapter when creating the app or in `app.start({ http })`:

| Adapter | Import | Runtime |
| --- | --- | --- |
| `bun()` | `@marcelsud/simplebase/http/bun` | Bun |
| `node()` | `@marcelsud/simplebase/http/node` | Node or Bun |
| `hono()` | `@marcelsud/simplebase/http/hono` | Node or Bun |
| `express()` | `@marcelsud/simplebase/http/express` | Node or Bun |

Install `hono` or `express` when using its adapter. Express TypeScript apps also need `@types/express`. Existing middleware and routes can be passed through the adapter's `app` option; leave the request body unread before Simplebase handles it.

Literal paths match their URL spelling: encode spaces and Unicode in route declarations. `:parameters` are decoded once. Without an adapter, use `app.fetch(request)` or run only background consumers.

See the [HTTP examples](examples/http) for controllers, decorators, validation, and framework integration.

## Transactions and delivery

- A handler's SQL writes, documents, enqueued jobs, and event deliveries commit or roll back together.
- Nested actions on the same app share the transaction. Await them; a nested failure aborts the transaction even if caught.
- Subscriptions run after the publisher commits. Subscription and worker failures roll back their changes and retry, up to five attempts with a one-second delay.
- Jobs survive restarts. External effects can repeat, and delivery order is not guaranteed; make external operations idempotent.
- Declare routes and consumers before `start()`. Start subscriptions before publishing events they must receive. Keep subscription names stable: bindings survive shutdown, and new subscriptions receive only future events.
- `stop()` drains active work and closes HTTP and PostgreSQL. `start()` handles SIGINT and SIGTERM automatically; use `{ signals: false }` when the host manages shutdown.

## Examples

- [Customer workflow](examples/customer.ts): SQL, events, subscriptions, and jobs.
- [Collections](examples/notes.ts): store a JSON document.
- [Separate processes](examples/application.ts): run with [subscriptions](examples/subscription.ts) and [workers](examples/worker.ts) against the same database.

To run the customer example from this repository:

```bash
bun install --frozen-lockfile
bun run db:up
bun run example
```

## License

[MIT](LICENSE).
