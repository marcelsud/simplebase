import { requireName, type Database } from "./core/contracts.js";
import { encodePayload } from "./core/payload.js";

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

export function createCollections(db: Database, ensureActive: () => void) {
  return {
    async insert(name: string, document: Record<string, unknown>): Promise<string> {
      ensureActive();
      requireName(name, "Collection");
      const json = encodeDocument(document);
      const id = crypto.randomUUID();
      await db.query(
        "INSERT INTO _simplebase_collections (collection_name, id, data) VALUES ($1, $2::uuid, $3::jsonb)",
        [name, id, json],
      );
      return id;
    },
    async get<T extends object = Record<string, unknown>>(name: string, id: string): Promise<(T & { id: string }) | null> {
      ensureActive();
      requireName(name, "Collection");
      requireName(id, "Document ID");
      const [row] = await db.query<{ document: T & { id: string } }>(
        "SELECT data || jsonb_build_object('id', id) AS document FROM _simplebase_collections WHERE collection_name = $1 AND id = $2::uuid",
        [name, id],
      );
      return row?.document ?? null;
    },
    async update(name: string, id: string, patch: Record<string, unknown>): Promise<boolean> {
      ensureActive();
      requireName(name, "Collection");
      requireName(id, "Document ID");
      const rows = await db.query(
        "UPDATE _simplebase_collections SET data = data || $3::jsonb WHERE collection_name = $1 AND id = $2::uuid RETURNING id",
        [name, id, encodeDocument(patch)],
      );
      return rows.length > 0;
    },
    async delete(name: string, id: string): Promise<boolean> {
      ensureActive();
      requireName(name, "Collection");
      requireName(id, "Document ID");
      const rows = await db.query(
        "DELETE FROM _simplebase_collections WHERE collection_name = $1 AND id = $2::uuid RETURNING id",
        [name, id],
      );
      return rows.length > 0;
    },
  };
}

export type Collections = ReturnType<typeof createCollections>;
