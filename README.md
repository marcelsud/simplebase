# Simplebase

Simplebase is a Bun and PostgreSQL backend prototype for building complex applications from a small set of composable primitives.

Wave one provides validated, callable actions; local subscriptions; durable queued jobs; and workers. One runtime process uses one PostgreSQL store and processes one job at a time.

## Install the SDK

Install with either package manager:

```bash
bun add simplebase
```

```bash
npm install simplebase
```

The package provides JavaScript modules and TypeScript declarations. Run your application with Bun and provide a PostgreSQL connection URL.

## Run the customer example

Requires Bun 1.3.14 and Docker with Compose.

```bash
git clone https://github.com/marcelsud/simplebase.git
cd simplebase
bun install --frozen-lockfile
cp .env.example .env
bun run db:up
bun run example
```

The [customer example](https://github.com/marcelsud/simplebase/blob/main/examples/customer.ts) creates its application table, inserts a customer, publishes an event, enqueues a sync job from a subscription, and updates the customer's `synced_at` in a worker. It prints `Customer created and synced` and closes the runtime.

The example defaults to the Compose database at `127.0.0.1:55432`. Set `DATABASE_URL` to use another PostgreSQL instance; [.env.example](https://github.com/marcelsud/simplebase/blob/main/.env.example) shows the local connection format. The application owns its SQL tables. Simplebase initializes its internal `_simplebase_queue_jobs` table, with JSONB job payloads.

Stop the local database with:

```bash
bun run db:down
```

Database data remains in the Docker volume.

## Define an action

Set `DATABASE_URL`, import the factory from the package root, and use its bound functions. The example checkout loads its local URL from `.env`:

```ts
import { createSimplebase } from "simplebase";

const simplebase = await createSimplebase({
  storage: { engine: "postgres", url: process.env.DATABASE_URL! },
});
const { action, v, stop } = simplebase;

const greet = action({
  args: v.object({ name: v.string() }),
  handler(_ctx, input) {
    return { message: `Hello, ${input.name}!` };
  },
});

try {
  console.log(await greet({ name: "world" }));
} finally {
  await stop();
}
```

`action()` returns a callable immediately. Calling it validates input, runs the handler in a PostgreSQL transaction, and returns its result. Actions work before or after `start()`. The functions keep their runtime binding when destructured.

## Handler context

Actions, subscriptions, and workers receive the same capability groups:

| Operation | Behavior |
| --- | --- |
| `ctx.db.query<T>(sql, params)` | Runs parameterized SQL in the current transaction and returns rows |
| `ctx.pubsub.publish(topic, payload)` | Buffers a local event for dispatch after the handler's transaction commits |
| `ctx.queue.enqueue(queue, payload)` | Inserts a durable job in the current transaction |
| `ctx.log.info(message, fields)` | Writes a log; rollback does not undo its output |

Use PostgreSQL's native `$1`, `$2`, and `$3` placeholders and pass values separately. SQL and directly enqueued jobs commit or roll back together. `query<T>` provides a compile-time row type; it does not validate database rows.

Event and job payloads must be JSON-compatible. Await database queries and enqueue operations inside the handler; their context expires when the handler finishes.

## Subscriptions and workers

`subscription(topic, handler, options?)` attaches a handler to a local event topic. Subscriptions run sequentially in creation order, each in its own transaction, after the publisher commits.

`worker(queue, handler)` attaches a handler to a queue. Call `await simplebase.start()` to start the worker loop. A worker's database changes and job completion commit together. Pending jobs survive restart; queues without a worker remain pending.

The [customer example](https://github.com/marcelsud/simplebase/blob/main/examples/customer.ts) shows both operations. A subscription can use `{ idempotent: true, name: "enqueueCustomerSync" }`; a worker can supply its payload type as `worker<{ customerId: number }>(...)`. Payload type arguments declare application contracts without runtime schema validation.

The worker polls every 250 ms. Failed jobs retry after 1 second and become `failed` after five recorded failures, retaining their last error. A process crash before failure bookkeeping can allow an extra attempt.

Call `await simplebase.stop()` at shutdown. It waits for in-flight actions and worker work, closes the pool, and rejects further calls. Repeated `start()` calls share one loop; repeated `stop()` calls share shutdown. Create a new runtime to restart after stopping.

## Transaction limits

Local events are held in memory. If the process dies after an action commits but before its subscription enqueues a job, the action's SQL remains committed and the job may never be created. Direct enqueue inside the action shares its transaction when the job must commit with its SQL.

A subscription failure rejects the action call after the publishing transaction has committed. Retrying the entire action can repeat its writes.

Subscription idempotency tracks successful delivery of a named subscription for the same live event object. It does not persist across restart or deduplicate separately published events with identical payloads.

Worker SQL and job completion are atomic; external effects can repeat after a crash. Run one runtime process against the prototype's queues. Competing consumers and parallel worker execution are outside wave one.

## Checks

```bash
bun run check
```

This runs TypeScript checks and the tests. Integration tests start and remove a temporary `postgres:16-alpine` container, use isolated schemas, and exercise rollback, retries, runtime replacement, process crashes, and the runnable customer example. The packaging test uses the npm CLI to pack the SDK, installs it in an independent application, and checks its imports and TypeScript declarations.

To use an existing test database, provide a role that can create schemas:

```bash
SIMPLEBASE_TEST_PG_URL=postgresql://user:password@localhost/test_db bun run check
```

The harness creates and drops only its generated test schemas. Use a dedicated test database. Run `bun run typecheck` or `bun run test` separately when needed.

Collections are planned for a later wave and are not part of this implementation.

## Build the npm package

```bash
bun run build
npm pack
```

Packing runs the build automatically. The tarball includes `dist`, this README, and the license notices. Development examples, tests, environment files, and drafts stay in the repository. Once npm login is configured, publish with `npm publish --access public`.

## Code and license

The core lives in [src/runtime.ts](https://github.com/marcelsud/simplebase/blob/main/src/runtime.ts) and [src/postgres.ts](https://github.com/marcelsud/simplebase/blob/main/src/postgres.ts), with reused validators in [src/validators.ts](https://github.com/marcelsud/simplebase/blob/main/src/validators.ts).

Licensed under [Apache 2.0](LICENSE). See [NOTICE](NOTICE) for Chimpbase attribution.
