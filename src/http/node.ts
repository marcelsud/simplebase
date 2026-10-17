import { createServer, type IncomingMessage, type Server, type ServerResponse, type RequestListener } from "node:http";
import type { AddressInfo } from "node:net";
import { Readable } from "node:stream";
import { pipeline } from "node:stream/promises";
import type { ReadableStream as NodeReadableStream } from "node:stream/web";
import type { HttpAdapter, HttpContext, HttpHandler, HttpServer, ListenOptions } from "../http.js";

const nodeContext = Symbol("simplebase.node");
export interface NodeContext { req: IncomingMessage; res: ServerResponse }

export function getNodeContext(context: Pick<HttpContext, "native">): NodeContext | undefined {
  const native = context.native as { type?: symbol; context?: NodeContext } | undefined;
  return native?.type === nodeContext ? native.context : undefined;
}

export function requestListener(
  handle: HttpHandler,
  native: (req: IncomingMessage, res: ServerResponse) => unknown = (req, res) => ({ type: nodeContext, context: { req, res } }),
): RequestListener {
  return (req, res) => {
    const abort = new AbortController();
    const onAbort = () => abort.abort();
    const onClose = () => { if (!res.writableFinished) abort.abort(); };
    res.once("close", onClose);
    req.once("aborted", onAbort);
    req.socket.once("close", onClose);
    void (async () => {
      try {
        const headers = new Headers();
        for (let index = 0; index < req.rawHeaders.length; index += 2) headers.append(req.rawHeaders[index], req.rawHeaders[index + 1]);
        const origin = `http://${headers.get("host") ?? "localhost"}`;
        const init: RequestInit & { duplex?: "half" } = { method: req.method, headers, signal: abort.signal };
        if (req.method !== "GET" && req.method !== "HEAD" && (Number(headers.get("content-length")) > 0 || headers.has("transfer-encoding"))) {
          init.body = Readable.toWeb(req) as unknown as BodyInit;
          init.duplex = "half";
        }
        const request = new Request(new URL(req.url ?? "/", origin), init);
        const response = await handle(request, { native: native(req, res) });
        if (res.destroyed) { await response.body?.cancel(); return; }
        res.statusCode = response.status;
        response.headers.forEach((value, name) => { if (name !== "set-cookie") res.setHeader(name, value); });
        const cookies = response.headers.getSetCookie();
        if (cookies.length) res.setHeader("set-cookie", cookies);
        if (req.method === "HEAD" || !response.body) {
          await response.body?.cancel();
          res.end();
        } else await pipeline(Readable.fromWeb(response.body as unknown as NodeReadableStream<Uint8Array>), res, { signal: abort.signal });
      } catch (error) {
        if (!res.destroyed) {
          if (res.headersSent) res.destroy(error instanceof Error ? error : undefined);
          else { res.statusCode = 500; res.end("Internal server error"); }
        }
      } finally {
        res.off("close", onClose);
        req.off("aborted", onAbort);
        req.socket.off("close", onClose);
        if (!req.destroyed && !req.readableEnded) req.resume();
      }
    })();
  };
}

export async function listen(server: Server, options: ListenOptions): Promise<HttpServer> {
  await new Promise<void>((resolve, reject) => {
    const onError = (error: Error) => { server.off("listening", onListening); reject(error); };
    const onListening = () => { server.off("error", onError); resolve(); };
    server.once("error", onError);
    server.once("listening", onListening);
    server.listen(options.port ?? 3000, options.hostname ?? "127.0.0.1");
  });
  const address = server.address() as AddressInfo;
  const hostname = address.address.includes(":") ? `[${address.address}]` : address.address;
  let stopped: Promise<void> | undefined;
  return {
    url: new URL(`http://${hostname}:${address.port}`),
    stop() {
      stopped ??= new Promise<void>((resolve, reject) => {
        server.close((error) => error ? reject(error) : resolve());
        server.closeIdleConnections();
      });
      return stopped;
    },
  };
}

export function node(options: ListenOptions = {}): HttpAdapter {
  return (handle) => listen(createServer(requestListener(handle)), options);
}
