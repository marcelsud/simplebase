import type { HttpAdapter, HttpContext, ListenOptions } from "../http.js";

const bunContext = Symbol("simplebase.bun");
export interface BunContext { server: Bun.Server<undefined> }

export function getBunContext(context: Pick<HttpContext, "native">): BunContext | undefined {
  const native = context.native as { type?: symbol; context?: BunContext } | undefined;
  return native?.type === bunContext ? native.context : undefined;
}

export function bun(options: ListenOptions = {}): HttpAdapter {
  return async (handle) => {
    if (typeof Bun === "undefined") throw new Error("The Bun adapter requires the Bun runtime");
    const server = Bun.serve({
      port: options.port ?? 3000,
      hostname: options.hostname ?? "127.0.0.1",
      fetch: (request, server) => handle(request, { native: { type: bunContext, context: { server } } }),
    });
    let stopped: Promise<void> | undefined;
    return { url: server.url, stop: () => stopped ??= Promise.resolve(server.stop()) };
  };
}
