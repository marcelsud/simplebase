import { bun, getBunContext } from "@marcelsud/simplebase/http/bun";
import { createHttpApp } from "./app.js";

const app = await createHttpApp();

app.route("GET", "/context", (http) => ({
  adapter: "bun",
  address: getBunContext(http)?.server.requestIP(http.request)?.address ?? null,
}));

await app.start({ http: bun({ port: Number(process.env.PORT ?? 3000) }) });
console.info(`HTTP example (bun) at ${app.http!.url}`);
