import createExpress, { type Express, type Request as ExpressRequest, type Response as ExpressResponse, type RequestHandler } from "express";
import { createServer } from "node:http";
import type { HttpAdapter, HttpContext, HttpHandler, ListenOptions } from "../http.js";
import { listen, requestListener } from "./node.js";

const expressContext = Symbol("simplebase.express");
export interface ExpressContext { req: ExpressRequest; res: ExpressResponse }

export function getExpressContext(context: Pick<HttpContext, "native">): ExpressContext | undefined {
  const native = context.native as { type?: symbol; context?: ExpressContext } | undefined;
  return native?.type === expressContext ? native.context : undefined;
}

export function toExpress(handle: HttpHandler): RequestHandler {
  return requestListener(handle, (req, res) => ({ type: expressContext, context: { req, res } }));
}

export function express(options: ListenOptions & { app?: Express } = {}): HttpAdapter {
  return async (handle) => {
    const app = createExpress();
    if (options.app) app.use(options.app);
    app.use(toExpress(handle));
    return listen(createServer(app), options);
  };
}
