import { Hono } from "hono";
import { hono, getHonoContext } from "@marcelsud/simplebase/http/hono";
import { createHttpApp } from "./app.js";

type Environment = { Variables: { requestId: string } };
const api = new Hono<Environment>();
api.use("*", async (context, next) => {
  context.set("requestId", crypto.randomUUID());
  await next();
  context.header("X-Request-ID", context.get("requestId"));
});
api.get("/framework", (context) => context.json({ framework: "hono" }));

const app = await createHttpApp();
app.route("GET", "/context", (http) => ({
  adapter: "hono",
  requestId: getHonoContext<Environment>(http)?.get("requestId") ?? null,
}));

await app.start({ http: hono({ app: api, port: Number(process.env.PORT ?? 3000) }) });
console.info(`HTTP example (hono) at ${app.http!.url}`);
