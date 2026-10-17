import type { Database, Store } from "./core/contracts.js";
import type { CollectionDriver } from "./collections.js";
import type { KvDriver } from "./kv.js";

export interface StorageBackend extends Store {
  bind(db: Database): { collection: CollectionDriver; kv: KvDriver };
  close(): Promise<void>;
}

export type StorageAdapter = () => Promise<StorageBackend>;
