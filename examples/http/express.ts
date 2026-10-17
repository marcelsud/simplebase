import createExpress from "express";
import { express, getExpressContext } from "@marcelsud/simplebase/http/express";
import { createHttpApp } from "./app.js";

const api = createExpress();
api.use((_req, res, next) => {
  res.locals.requestId = crypto.randomUUID();
  res.setHeader("X-Request-ID", res.locals.requestId);
  next();
});
api.get("/framework", (_req, res) => res.json({ framework: "express" }));

const app = await createHttpApp();
app.route("GET", "/context", (http) => ({
  adapter: "express",
  requestId: getExpressContext(http)?.res.locals.requestId ?? null,
}));

await app.start({ http: express({ app: api, port: Number(process.env.PORT ?? 3000) }) });
console.info(`HTTP example (express) at ${app.http!.url}`);
