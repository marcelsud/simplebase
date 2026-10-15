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

Your application defines what happens. Simplebase provides the transactions, event dispatch, and job processing.

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

## Current scope

Simplebase is an early prototype: one runtime process, one job at a time. Jobs are stored in PostgreSQL; events stay within the running process.

You provide your application tables and HTTP routes. External operations should tolerate job retries.

## License

[MIT](LICENSE).
