import { requireName } from "./core/contracts.js";
import { encodePayload } from "./core/payload.js";

export interface CollectionDriver {
  insert(name: string, id: string, documentJson: string): Promise<void>;
  get(name: string, id: string): Promise<string | undefined>;
  update(name: string, id: string, patchJson: string): Promise<boolean>;
  delete(name: string, id: string): Promise<boolean>;
}

function encodeDocument(document: Record<string, unknown>): string {
  function validate(value: unknown): void {
    if (value === null || typeof value !== "object" || Array.isArray(value)) {
      throw new TypeError("Collection documents and patches must be JSON objects");
    }
    if (Object.hasOwn(value, "id")) throw new TypeError("Collection documents and patches cannot supply id");
  }
  validate(document);
  const json = encodePayload(document);
  validate(JSON.parse(json));
  return json;
}

export function createCollections(driver: CollectionDriver, ensureActive: () => void) {
  return {
    async insert(name: string, document: Record<string, unknown>): Promise<string> {
      ensureActive();
      requireName(name, "Collection");
      const json = encodeDocument(document);
      const id = crypto.randomUUID();
      await driver.insert(name, id, json);
      return id;
    },
    async get<T extends object = Record<string, unknown>>(name: string, id: string): Promise<(T & { id: string }) | null> {
      ensureActive();
      requireName(name, "Collection");
      requireName(id, "Document ID");
      const json = await driver.get(name, id);
      return json === undefined ? null : { ...JSON.parse(json), id } as T & { id: string };
    },
    async update(name: string, id: string, patch: Record<string, unknown>): Promise<boolean> {
      ensureActive();
      requireName(name, "Collection");
      requireName(id, "Document ID");
      return driver.update(name, id, encodeDocument(patch));
    },
    async delete(name: string, id: string): Promise<boolean> {
      ensureActive();
      requireName(name, "Collection");
      requireName(id, "Document ID");
      return driver.delete(name, id);
    },
  };
}

export type Collections = ReturnType<typeof createCollections>;
