import { requireName, ValidationError, type Database } from "./core/contracts.js";
import { encodePayload } from "./core/payload.js";
import type { Validator } from "./validators.js";

export interface KvDriver {
  get(namespace: string, key: string): Promise<string | undefined>;
  set(namespace: string, key: string, value: string): Promise<void>;
  delete(namespace: string, key: string): Promise<boolean>;
}

export interface KvBackend {
  bind(db: Database): KvDriver;
  stop?(): Promise<void>;
}

export type KvAdapter = () => Promise<KvBackend>;
export interface KvNamespaceDefinition { schema?: Validator<unknown> }
export type KvNamespaces = Record<string, KvNamespaceDefinition>;
export interface KvOptions<Namespaces extends KvNamespaces = KvNamespaces> {
  adapter?: KvAdapter;
  namespaces?: Namespaces;
}

export interface KvNamespace<Value> {
  get(key: string): Promise<Value | undefined>;
  set(key: string, value: Value): Promise<void>;
  delete(key: string): Promise<boolean>;
}

export interface RawKvNamespace {
  get<Value = unknown>(key: string): Promise<Value | undefined>;
  set(key: string, value: unknown): Promise<void>;
  delete(key: string): Promise<boolean>;
}

type NamespaceFor<Definition> = Definition extends { schema: Validator<infer Value> }
  ? KvNamespace<Value> : RawKvNamespace;
type ValidatedNames<Namespaces> = {
  [Name in keyof Namespaces]: Namespaces[Name] extends { schema: Validator<unknown> } ? Name : never;
}[keyof Namespaces];

export interface Kv<Namespaces extends KvNamespaces = {}> extends RawKvNamespace {
  namespace<Name extends string>(name: Name): Name extends keyof Namespaces
    ? NamespaceFor<Namespaces[Name]> : RawKvNamespace;
  namespace<Name extends string, Value>(
    name: Name extends ValidatedNames<Namespaces> ? never : Name,
    schema: Validator<Value>,
  ): KvNamespace<Value>;
}

export function createKv<Namespaces extends KvNamespaces>(definitions?: Namespaces) {
  const schemas = new Map<string, Validator<unknown> | undefined>();
  for (const [name, definition] of Object.entries(definitions ?? {})) {
    requireName(name, "KV namespace");
    if (!definition || typeof definition !== "object" || Array.isArray(definition)) {
      throw new TypeError("KV namespace definitions must be objects");
    }
    if (definition.schema !== undefined && typeof definition.schema?.parse !== "function") {
      throw new TypeError("KV namespace schemas must be validators");
    }
    schemas.set(name, definition.schema);
  }

  return (driver: KvDriver, ensureActive: () => void): Kv<Namespaces> => {
    function scope(namespace: string, schema?: Validator<unknown>): RawKvNamespace {
      function parse(value: unknown): unknown {
        if (!schema) return value;
        try { return schema.parse(value, "value"); }
        catch (error) { throw new ValidationError(error instanceof Error ? error.message : String(error)); }
      }

      return {
        async get<Value>(key: string): Promise<Value | undefined> {
          ensureActive();
          requireName(key, "KV key");
          const value = await driver.get(namespace, key);
          return value === undefined ? undefined : parse(JSON.parse(value)) as Value;
        },
        async set(key: string, value: unknown): Promise<void> {
          ensureActive();
          requireName(key, "KV key");
          // Validate the JSON representation as well as the value supplied by the caller.
          let json = encodePayload(parse(value));
          if (schema) json = encodePayload(parse(JSON.parse(json)));
          await driver.set(namespace, key, json);
        },
        async delete(key: string): Promise<boolean> {
          ensureActive();
          requireName(key, "KV key");
          return driver.delete(namespace, key);
        },
      };
    }

    function namespace(name: string, schema?: Validator<unknown>): RawKvNamespace {
      ensureActive();
      requireName(name, "KV namespace");
      const registered = schemas.get(name);
      if (registered && schema && registered !== schema) {
        throw new Error("Configure schemas for registered KV namespaces in createSimplebase()");
      }
      return scope(name, registered ?? schema);
    }

    return { ...scope(""), namespace } as Kv<Namespaces>;
  };
}
