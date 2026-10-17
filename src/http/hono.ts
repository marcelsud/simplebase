import { Hono, type Context, type Env } from "hono";
import type { HttpAdapter, HttpContext, HttpHandler, ListenOptions } from "../http.js";
import { node } from "./node.js";

const honoContext = Symbol("simplebase.hono");

export function getHonoContext<E extends Env = Env>(context: Pick<HttpContext, "native">): Context<E> | undefined {
  const native = context.native as { type?: symbol; context?: Context<E> } | undefined;
  return native?.type === honoContext ? native.context : undefined;
}

export function toHono(handle: HttpHandler) {
  return (context: Context) => handle(context.req.raw, { native: { type: honoContext, context } });
}

export function hono<E extends Env = Env>(options: ListenOptions & { app?: Hono<E> } = {}): HttpAdapter {
  return async (handle) => {
    const app = new Hono<E>();
    if (options.app) app.route("/", options.app);
    app.all("*", toHono(handle));
    return node(options)(async (request) => app.fetch(request));
  };
}
