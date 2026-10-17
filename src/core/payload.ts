export function encodePayload(payload: unknown): string {
  const json = JSON.stringify(payload, (_key, value: unknown) => {
    if (
      value === undefined || typeof value === "function" ||
      typeof value === "symbol" || typeof value === "bigint" ||
      (typeof value === "number" && !Number.isFinite(value))
    ) {
      throw new TypeError("Event and job payloads must contain JSON values");
    }
    return value;
  });
  if (json === undefined) throw new TypeError("A payload is required");
  return json;
}
