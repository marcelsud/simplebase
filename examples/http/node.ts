import { node, getNodeContext } from "@marcelsud/simplebase/http/node";
import { createHttpApp } from "./app.js";

const app = await createHttpApp();

app.route("GET", "/context", (http) => ({
  adapter: "node",
  address: getNodeContext(http)?.req.socket.remoteAddress ?? null,
}));

await app.start({ http: node({ port: Number(process.env.PORT ?? 3000) }) });
console.info(`HTTP example (node) at ${app.http!.url}`);
