import { createPostgresStore } from "./postgres.js";
import { schema, type Validator } from "./validators.js";
import { createHttpRouter, type HttpAdapter, type HttpHandler, type HttpServer, type HttpMethod } from "./http.js";
import { createCollections, type Collections } from "./collections.js";
import { createRuntime } from "./core/runtime.js";
import type { CoreContext } from "./core/contracts.js";
export type { SubscriptionOptions } from "./core/contracts.js";

export interface HandlerContext extends CoreContext {
  collection: Collections;
}

export interface SimplebaseOptions {
  storage: { engine: "postgres"; url: string };
  http?: HttpAdapter;
}

export interface StartOptions {
  http?: HttpAdapter;
  signals?: boolean;
  beforeStop?: () => void | Promise<void>;
}

export async function createSimplebase(options: SimplebaseOptions) {
  if (options.storage.engine !== "postgres") throw new Error("This prototype supports PostgreSQL storage");
  const store = await createPostgresStore(options.storage.url);
  const runtime = createRuntime<HandlerContext>(store,
    (ctx, ensureActive) => ({ ...ctx, collection: createCollections(ctx.db, ensureActive) }),
    {
      info: (message, fields) => console.info(message, fields ?? {}),
      error: (message, fields) => console.error(message, fields),
    },
  );
  const router = createHttpRouter();
  const requests = new Set<Promise<Response>>();
  let httpServer: HttpServer | undefined;
  let httpStarting = false;
  let acceptingRequests = true;
  let startPromise: Promise<void> | undefined;
  let stopPromise: Promise<void> | undefined;
  let beforeStop: StartOptions["beforeStop"];
  const signals = ["SIGINT", "SIGTERM"] as const;

  function removeSignalHandlers(): void {
    for (const signal of signals) process.off(signal, onSignal);
  }

  function onSignal(): void {
    void stop().catch((error) => {
      console.error("Simplebase shutdown failed", error);
      process.exitCode = 1;
    });
  }

  function stop(): Promise<void> {
    if (stopPromise) return stopPromise;
    runtime.stopClaims();
    acceptingRequests = false;
    removeSignalHandlers();
    if (!beforeStop && !httpServer && !httpStarting && !requests.size) runtime.closeActions();
    stopPromise = Promise.resolve().then(async () => {
      try {
        await Promise.allSettled(startPromise ? [startPromise] : []);
        const cleanups = await Promise.allSettled([
          Promise.resolve().then(() => httpServer?.stop()),
          Promise.resolve().then(() => beforeStop?.()),
        ]);
        await Promise.allSettled([...requests]);
        const failure = cleanups.find((result) => result.status === "rejected");
        if (failure?.status === "rejected") throw failure.reason;
      } finally {
        runtime.closeActions();
        await runtime.drain();
        await store.close();
      }
    });
    return stopPromise;
  }

  const route: typeof router.route = (method: HttpMethod, path: string, definition: any) => {
    runtime.ensureDeclaring();
    router.route(method, path, definition);
  };

  const fetch: HttpHandler = (request, context) => {
    if (!acceptingRequests) return Promise.resolve(new Response("Service unavailable", { status: 503 }));
    const pending = router.fetch(request, context);
    requests.add(pending);
    return pending.finally(() => requests.delete(pending));
  };

  return {
    schema,
    route,
    controller(instance: object): void { runtime.ensureDeclaring(); router.controller(instance); },
    fetch,
    get http(): HttpServer | undefined { return httpServer; },
    action<Input, Result>(definition: {
      args: Validator<Input>;
      handler: (ctx: HandlerContext, input: Input) => Result | Promise<Result>;
    }): (input: Input) => Promise<Awaited<Result>> {
      return runtime.action(definition);
    },
    subscription: runtime.subscription,
    worker: runtime.worker,
    async start(startOptions: StartOptions = {}): Promise<void> {
      runtime.ensureOpen();
      if (stopPromise) throw new Error("Simplebase is stopped");
      if (options.http && startOptions.http) {
        throw new Error("Configure the HTTP adapter in either createSimplebase() or start(), not both");
      }
      if (!startPromise) {
        const http = startOptions.http ?? options.http;
        beforeStop = startOptions.beforeStop;
        if (startOptions.signals !== false) {
          for (const signal of signals) process.once(signal, onSignal);
        }
        startPromise = runtime.prepare().then(async () => {
          runtime.ensureOpen();
          if (stopPromise) throw new Error("Simplebase is stopped");
          if (http) {
            httpStarting = true;
            try { httpServer = await http(fetch); }
            finally { httpStarting = false; }
          }
          if (stopPromise) throw new Error("Simplebase is stopped");
          runtime.run();
        }).catch(async (error) => {
          removeSignalHandlers();
          await httpServer?.stop();
          httpServer = undefined;
          runtime.resetStartup();
          startPromise = undefined;
          throw error;
        });
      }
      await startPromise;
    },
    stop,
  };
}

export type Simplebase = Awaited<ReturnType<typeof createSimplebase>>;
