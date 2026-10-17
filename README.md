# Simplebase

**Build complex apps with simple tools.**

Validated TypeScript actions, SQL, JSON collections, key-value storage, durable events, background jobs, and HTTP routes, backed by PostgreSQL or SQLite.

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
import { postgres } from "@marcelsud/simplebase/storage/postgres";
import { node } from "@marcelsud/simplebase/http/node";

const app = await createSimplebase({
  storage: postgres({ url: process.env.DATABASE_URL! }),
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

## Storage

Storage uses an explicit adapter factory. PostgreSQL uses `postgres({ url })` from `@marcelsud/simplebase/storage/postgres`; this replaces the earlier `storage: { engine: "postgres", url }` configuration.

For SQLite, install the optional driver with `bun add libsql` or `npm install libsql`, then configure a filename:

```ts
import { createSimplebase } from "@marcelsud/simplebase";
import { sqlite } from "@marcelsud/simplebase/storage/sqlite";

const app = await createSimplebase({
  storage: sqlite({ filename: "./app.sqlite", busyTimeoutMs: 5_000 }),
});
```

Both adapters support Bun and Node.js 20+ and provide SQL, collections, default KV, durable events, and jobs. SQLite uses the same `libsql` driver on both runtimes; it needs no database server. Relative filenames resolve against the working directory. The file is created if absent; its parent directory must already exist. Use `filename: ":memory:"` for an isolated, temporary database per app instance.

SQLite enables WAL, foreign keys, and full commit synchronization. It serializes handler transactions, including awaited work, and allows one writer per database file across processes. `busyTimeoutMs` bounds lock acquisition (default 5,000 ms); waiting yields to the event loop, and lock contention never replays a handler. Long handlers delay other writes. Processes sharing a database must use the same local file on the same host; PostgreSQL supports deployments across hosts.

`ctx.db.query()` executes the chosen engine's native SQL: PostgreSQL uses `$1` parameters and SQLite uses `?`. Application SQL and row types follow that engine; changing adapters does not translate queries or migrate application tables. Collections, KV, events, and jobs keep the same public APIs. Simplebase initializes its internal tables when the storage factory runs during `createSimplebase()` and closes storage after active handlers drain.

Custom factories implement the exported `StorageAdapter` contract and return a `StorageBackend`: the core store operations, `bind(db)` returning a `CollectionDriver` and default `KvDriver`, and `close()`. Bound drivers must use the supplied transaction database. Validation and JSON encoding stay in the collection/KV services; SQL, locking, and initialization belong to adapters. The core runtime and bootstrap import no concrete storage adapters.

## Inside a handler

Actions, subscriptions, and workers receive the same context:

| API | Purpose |
| --- | --- |
| `ctx.db.query(sql, params)` | Execute parameterized SQL against your application tables. |
| `ctx.collection` | Store JSON documents by collection name and UUID. |
| `ctx.kv` | Store JSON values by key, with optional namespaces and schemas. |
| `ctx.pubsub.publish(topic, payload)` | Publish an event to named subscriptions. |
| `ctx.queue.enqueue(queue, payload)` | Save a job for a worker. |
| `ctx.log.info(message, fields)` | Write a log message. |

Collections need no setup. `insert(name, data)` returns a UUID; `get<T>(name, id)` returns the document with its `id`, or `null`. `update(name, id, patch)` replaces supplied top-level fields; `delete(name, id)` removes the document. Both return whether the document existed. Documents and patches must be JSON objects without a supplied `id`.

## Key-value storage

KV is always available and uses the application's storage adapter by default. Within a handler:

```ts
await ctx.kv.set("theme", "dark");
const theme = await ctx.kv.get<string>("theme");
const removed = await ctx.kv.delete("theme");

const settings = ctx.kv.namespace("settings");
await settings.set("theme", "light");
```

`set` inserts or replaces the complete value. `get` returns `undefined` for missing keys, preserving stored JSON `null`; `delete` returns whether the key existed. Keys and namespace names must be nonempty strings. Raw KV has its own scope, separate from named namespaces. Without a schema, `get<T>` provides a TypeScript assertion without runtime shape validation.

Declare namespaces centrally to infer their types and validate reads and writes:

```ts
import { createSimplebase, schema } from "@marcelsud/simplebase";
import { postgres } from "@marcelsud/simplebase/storage/postgres";

const app = await createSimplebase({
  storage: postgres({ url: process.env.DATABASE_URL! }),
  kv: {
    namespaces: {
      settings: {},
      sessions: {
        schema: schema.object({
          userId: schema.string(),
          locale: schema.string().optional(),
        }),
      },
    },
  },
});
```

In this app's handlers, `ctx.kv.namespace("sessions").get(key)` returns `{ userId: string; locale?: string } | undefined`. Invalid values throw `ValidationError`. Existing values are validated when read; schema changes do not migrate them. Bootstrap schemas apply to all handles of that namespace and cannot be replaced locally. Undeclared namespaces remain available, and can optionally supply a local schema with `ctx.kv.namespace("counts", app.schema.integer())`. All values, including schema outputs, must be JSON serializable. KV handles expire with their handler context.

To use Redis, install its optional client with `bun add redis` or `npm install redis`, then provide an adapter:

```ts
import { createSimplebase } from "@marcelsud/simplebase";
import { postgres } from "@marcelsud/simplebase/storage/postgres";
import { redis } from "@marcelsud/simplebase/kv/redis";

const app = await createSimplebase({
  storage: postgres({ url: process.env.DATABASE_URL! }),
  kv: {
    adapter: redis({ url: process.env.REDIS_URL!, prefix: "my-app:kv:" }),
  },
});
```

Namespaces and schemas work with either backend. Redis connects during `createSimplebase`, so actions can use it before `start()`, and disconnects after handlers drain during `stop()`. Its default key prefix is `simplebase:kv:`; use a separate prefix for applications sharing a Redis database, and the same prefix across an application's processes. SQL, collections, events, and jobs continue to use the configured storage adapter.

Redis writes take effect independently of the storage transaction: a handler failure does not roll them back, and retries can repeat them. Default KV shares the handler transaction with either storage engine. Explicit PostgreSQL selection is available as `postgres()` from `@marcelsud/simplebase/kv/postgres`. Custom adapter factories implement the exported `KvAdapter` contract: return a backend with `bind(db)` and optional `stop()`. Its driver receives `(namespace, key)` and stores JSON strings through `get`, `set`, and `delete`; `get` returns `undefined` for missing keys. KV handles namespace schemas and JSON encoding independently of the adapter.

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

- A handler's SQL writes, documents, default KV writes, enqueued jobs, and event deliveries commit or roll back together. External KV adapters such as Redis have independent persistence semantics.
- Nested actions on the same app share the transaction. Await them; a nested failure aborts the transaction even if caught.
- Subscriptions run after the publisher commits. Subscription and worker failures roll back their changes and retry, up to five attempts with a one-second delay.
- Jobs survive restarts. External effects can repeat, and delivery order is not guaranteed; make external operations idempotent.
- Declare routes and consumers before `start()`. Start subscriptions before publishing events they must receive. Keep subscription names stable: bindings survive shutdown, and new subscriptions receive only future events.
- `stop()` drains active work and closes HTTP and storage connections. `start()` handles SIGINT and SIGTERM automatically; use `{ signals: false }` when the host manages shutdown.

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
