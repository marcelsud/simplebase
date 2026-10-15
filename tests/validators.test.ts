import { expect, test } from "bun:test";
import { v } from "../src/validators.ts";
import { encodePayload } from "../src/postgres.ts";

test("object validation checks required keys, validates nested values, and strips extra keys", () => {
  const args = v.object({ email: v.string(), settings: v.object({ active: v.boolean() }), tags: v.string().array() });
  expect(args.parse({ email: "a@b.com", settings: { active: true }, tags: ["new"], extra: 1 })).toEqual({
    email: "a@b.com", settings: { active: true }, tags: ["new"],
  });
  expect(() => args.parse({})).toThrow("value.email is required");
  expect(() => args.parse({ email: "a", settings: { active: 1 }, tags: [] })).toThrow("value.settings.active must be a boolean");
  expect(() => v.string().array().parse(["ok", 1])).toThrow("value[1] must be a string");
  expect(() => v.object({}).parse(new Date())).toThrow("must be an object");
});

test("optional, nullable, union, enum, literal, and record validators compose", () => {
  const args = v.object({ name: v.string(), age: v.integer().optional(), address: v.string().nullable() });
  expect(args.parse({ name: "Sam", address: null })).toEqual({ name: "Sam", address: null });
  expect(() => args.parse({ name: "Sam", age: 1.5, address: "here" })).toThrow("safe integer");
  expect(v.union(v.string(), v.number()).parse(3)).toBe(3);
  expect(v.enum(["starter", "pro"]).parse("pro")).toBe("pro");
  expect(() => v.enum(["starter", "pro"]).parse("other")).toThrow("one of");
  expect(v.literal("yes").parse("yes")).toBe("yes");
  expect(() => v.literal("yes").parse("no")).toThrow('must be "yes"');
  expect(v.record(v.boolean()).parse({ first: true })).toEqual({ first: true });
  expect(() => v.number().parse(NaN)).toThrow("finite number");
  expect(() => v.integer().parse(Number.MAX_SAFE_INTEGER + 1)).toThrow("safe integer");
});

test("object and record validators preserve __proto__ as a data key", () => {
  const shape = JSON.parse('{"__proto__":null}') as Record<string, ReturnType<typeof v.string>>;
  Object.defineProperty(shape, "__proto__", { value: v.string(), enumerable: true });
  const input: unknown = JSON.parse('{"__proto__":"safe"}');
  for (const parsed of [v.object(shape).parse(input), v.record(v.string()).parse(input)]) {
    expect(Object.getPrototypeOf(parsed)).toBe(Object.prototype);
    expect(Object.hasOwn(parsed, "__proto__")).toBe(true);
    expect(parsed["__proto__"]).toBe("safe");
  }
});

test("payload encoding rejects values JSON would lose or cannot represent", () => {
  expect(encodePayload({ customerId: 1, email: "a@b.com" })).toBe('{"customerId":1,"email":"a@b.com"}');
  expect(encodePayload(null)).toBe("null");
  for (const value of [undefined, NaN, Infinity, 1n, Symbol(), () => {}, { missing: undefined }, [undefined]]) {
    expect(() => encodePayload(value)).toThrow();
  }
  const circular: Record<string, unknown> = {};
  circular.self = circular;
  expect(() => encodePayload(circular)).toThrow();
});
