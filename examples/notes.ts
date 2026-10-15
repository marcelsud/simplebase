import { createSimplebase } from "@marcelsud/simplebase";

const app = await createSimplebase({ storage: {
  engine: "postgres",
  url: process.env.DATABASE_URL ?? "postgresql://simplebase:simplebase@127.0.0.1:55432/simplebase",
} });

const createNote = app.action({
  args: app.schema.object({ body: app.schema.string() }),
  handler(ctx, input) {
    return ctx.collection.insert("notes", input);
  },
});

try {
  const id = await createNote({ body: "First note" });
  console.info("Note created", { id });
} finally {
  await app.stop();
}
