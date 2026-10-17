import { createSimplebase, Get, Post, ValidationError, type HttpContext, type Infer } from "@marcelsud/simplebase";
import { postgres as postgresStorage } from "@marcelsud/simplebase/storage/postgres";

export async function createHttpApp() {
  const app = await createSimplebase({ storage: postgresStorage({ url: process.env.DATABASE_URL ?? "postgresql://simplebase:simplebase@127.0.0.1:55432/simplebase" }) });

  const noteInput = app.schema.object({ body: app.schema.string() });
  const noteId = app.schema.object({ id: app.schema.string() });

  const createNote = app.action({
    args: noteInput,
    async handler(ctx, input) {
      const id = await ctx.collection.insert("notes", input);
      return { id, ...input };
    },
  });

  const getNote = app.action({
    args: noteId,
    handler(ctx, { id }) {
      if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(id)) {
        throw new ValidationError("Note id must be a UUID");
      }
      return ctx.collection.get<{ body: string }>("notes", id);
    },
  });

  app.route("GET", "/health", () => Response.json({ ok: true }));

  app.route("POST", "/preview", {
    body: noteInput,
    handler: ({ body }) => ({ text: body.body.trim(), length: body.body.trim().length }),
  });

  app.route("POST", "/notes", {
    body: app.schema.object({ text: app.schema.string() }),
    action: createNote,
    input: ({ body }) => ({ body: body.text.trim() }),
    output: (note, { request }) => Response.json({ data: note }, {
      status: 201,
      headers: { Location: new URL(`/notes/${note.id}`, request.url).href },
    }),
  });

  class Notes {
    @Post("/notes/decorated", { body: noteInput, status: 201 })
    async create({ body }: HttpContext<Infer<typeof noteInput>>) {
      return { data: await createNote(body) };
    }

    @Get("/notes/:id", { params: noteId })
    async get({ params }: HttpContext<unknown, Infer<typeof noteId>>) {
      const note = await getNote(params);
      return note ? { data: note } : Response.json({ error: "Note not found" }, { status: 404 });
    }
  }
  app.controller(new Notes());

  return app;
}
