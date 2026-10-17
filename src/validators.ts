// Adapted from Chimpbase's existing validators; see LICENSE and NOTICE.
export class ValidationError extends Error {
  override name = "ValidationError";
}

export interface Validator<T = unknown> {
  readonly isOptional?: true;
  readonly schema: unknown;
  array(): Validator<T[]>;
  nullable(): Validator<T | null>;
  optional(): OptionalValidator<T | undefined>;
  parse(value: unknown, path?: string): T;
}
export interface OptionalValidator<T = unknown> extends Validator<T> {
  readonly isOptional: true;
}
export type Infer<V extends Validator<unknown>> = V extends Validator<infer T> ? T : never;
type Simplify<T> = { [K in keyof T]: T[K] };
function isArrayValue(value: unknown): value is unknown[] { return Array.isArray(value); }
function isPlainObject(value: unknown): value is Record<string, unknown> {
  if (typeof value !== "object" || value === null) return false;
  const prototype = Object.getPrototypeOf(value);
  return prototype === Object.prototype || prototype === null;
}

type ValidatorShape = Record<string, Validator<unknown>>;
type OptionalValidatorKeys<TShape extends ValidatorShape> = Extract<{
  [TKey in keyof TShape]: TShape[TKey]["isOptional"] extends true ? TKey : never;
}[keyof TShape], string>;
type RequiredValidatorKeys<TShape extends ValidatorShape> = Exclude<
  Extract<keyof TShape, string>,
  OptionalValidatorKeys<TShape>
>;
type ObjectValidatorResult<TShape extends ValidatorShape> = Simplify<
  { [TKey in RequiredValidatorKeys<TShape>]: Infer<TShape[TKey]> } &
  { [TKey in OptionalValidatorKeys<TShape>]?: Infer<TShape[TKey]> }
>;

interface ValidatorFactoryOptions<TValue> {
  isOptional?: true;
  parser: (value: unknown, path: string) => TValue;
  schema: unknown;
}

function createValidator<TValue>(
  options: ValidatorFactoryOptions<TValue>,
): Validator<TValue> {
  const validator: Validator<TValue> = {
    ...((options.isOptional === true) ? { isOptional: true as const } : {}),
    schema: options.schema,
    array() {
      return createValidator<TValue[]>({
        parser(value, path) {
          if (!isArrayValue(value)) {
            throw new Error(`${path} must be an array`);
          }

          return value.map((entry, index) => validator.parse(entry, `${path}[${index}]`));
        },
        schema: {
          items: validator.schema,
          type: "array",
        },
      });
    },
    nullable() {
      return createValidator<TValue | null>({
        parser(value, path) {
          if (value === null) {
            return null;
          }

          return validator.parse(value, path);
        },
        schema: {
          anyOf: [validator.schema, { type: "null" }],
        },
      });
    },
    optional() {
      return createOptionalValidator<TValue | undefined>({
        parser(value, path) {
          if (value === undefined) {
            return undefined;
          }

          return validator.parse(value, path);
        },
        schema: validator.schema,
      });
    },
    parse(value, path = "value") {
      return options.parser(value, path);
    },
  };

  return validator;
}

function createOptionalValidator<TValue>(
  options: Omit<ValidatorFactoryOptions<TValue>, "isOptional">,
): OptionalValidator<TValue> {
  return {
    ...createValidator<TValue>({ ...options, isOptional: true }),
    isOptional: true,
  };
}

function isObjectValidatorResult<TShape extends ValidatorShape>(
  value: unknown,
  shape: TShape,
): value is ObjectValidatorResult<TShape> {
  if (!isPlainObject(value)) {
    return false;
  }

  for (const [key, validator] of Object.entries(shape)) {
    if (
      validator.isOptional !== true
      && !Object.prototype.hasOwnProperty.call(value, key)
    ) {
      return false;
    }
  }

  return true;
}

function createObjectValidator<TShape extends ValidatorShape>(
  shape: TShape,
): Validator<ObjectValidatorResult<TShape>> {
  const properties = Object.fromEntries(
    Object.entries(shape).map(([key, validator]) => [key, validator.schema]),
  );
  const required = Object.entries(shape)
    .filter(([, validator]) => validator.isOptional !== true)
    .map(([key]) => key);

  return createValidator<ObjectValidatorResult<TShape>>({
    parser(value, path) {
      if (!isPlainObject(value)) {
        throw new Error(`${path} must be an object`);
      }

      const output: Record<string, unknown> = {};
      for (const [key, validator] of Object.entries(shape)) {
        const hasKey = Object.prototype.hasOwnProperty.call(value, key);
        if (!hasKey) {
          if (validator.isOptional === true) {
            continue;
          }

          throw new Error(`${path}.${key} is required`);
        }

        const parsed = validator.parse(value[key], `${path}.${key}`);
        if (parsed !== undefined || hasKey) {
          Object.defineProperty(output, key, { value: parsed, enumerable: true, writable: true, configurable: true });
        }
      }

      if (!isObjectValidatorResult(output, shape)) {
        throw new Error(`${path} failed object validation`);
      }
      return output;
    },
    schema: {
      properties,
      required,
      type: "object",
    },
  });
}

function createUnionValidator<TValue>(
  validators: readonly Validator<TValue>[],
): Validator<TValue> {
  return createValidator<TValue>({
    parser(value, path) {
      const messages: string[] = [];
      for (const validator of validators) {
        try {
          return validator.parse(value, path);
        } catch (error) {
          messages.push(error instanceof Error ? error.message : String(error));
        }
      }

      throw new Error(messages[0] ?? `${path} did not match any allowed shape`);
    },
    schema: {
      anyOf: validators.map((validator) => validator.schema),
    },
  });
}
function unionValidator<
  TValidators extends readonly [Validator<unknown>, ...Validator<unknown>[]],
>(
  ...validators: TValidators
): Validator<Infer<TValidators[number]>>;
function unionValidator(
  ...validators: Validator<unknown>[]
): Validator<unknown> {
  return createUnionValidator(validators);
}


export const schema = {
  any(): Validator<unknown> {
    return this.unknown();
  },
  array<TValue>(validator: Validator<TValue>): Validator<TValue[]> {
    return validator.array();
  },
  boolean(): Validator<boolean> {
    return createValidator<boolean>({
      parser(value, path) {
        if (typeof value !== "boolean") {
          throw new Error(`${path} must be a boolean`);
        }

        return value;
      },
      schema: { type: "boolean" },
    });
  },
  enum<TValues extends readonly [string, ...string[]]>(
    values: TValues,
  ): Validator<TValues[number]> {
    return createValidator<TValues[number]>({
      parser(value, path) {
        if (typeof value !== "string" || !values.includes(value as TValues[number])) {
          throw new Error(`${path} must be one of ${values.join(", ")}`);
        }

        return value as TValues[number];
      },
      schema: { enum: [...values] },
    });
  },
  literal<TValue extends string | number | boolean | null>(
    expected: TValue,
  ): Validator<TValue> {
    return createValidator<TValue>({
      parser(value, path) {
        if (value !== expected) {
          throw new Error(`${path} must be ${JSON.stringify(expected)}`);
        }

        return expected;
      },
      schema: { const: expected },
    });
  },
  null(): Validator<null> {
    return createValidator<null>({
      parser(value, path) {
        if (value !== null) {
          throw new Error(`${path} must be null`);
        }

        return null;
      },
      schema: { type: "null" },
    });
  },
  integer(): Validator<number> {
    return createValidator<number>({
      parser(value, path) {
        if (typeof value !== "number" || !Number.isSafeInteger(value)) {
          throw new Error(`${path} must be a safe integer`);
        }
        return value;
      },
      schema: { type: "integer" },
    });
  },
  number(): Validator<number> {
    return createValidator<number>({
      parser(value, path) {
        if (typeof value !== "number" || !Number.isFinite(value)) {
          throw new Error(`${path} must be a finite number`);
        }

        return value;
      },
      schema: { type: "number" },
    });
  },
  object<TShape extends ValidatorShape>(
    shape: TShape,
  ): Validator<ObjectValidatorResult<TShape>> {
    return createObjectValidator(shape);
  },
  optional<TValue>(validator: Validator<TValue>): OptionalValidator<TValue | undefined> {
    return validator.optional();
  },
  record<TValue>(validator: Validator<TValue>): Validator<Record<string, TValue>> {
    return createValidator<Record<string, TValue>>({
      parser(value, path) {
        if (!isPlainObject(value)) {
          throw new Error(`${path} must be an object`);
        }

        const output: Record<string, TValue> = {};
        for (const [key, entry] of Object.entries(value)) {
          Object.defineProperty(output, key, {
            configurable: true,
            enumerable: true,
            value: validator.parse(entry, `${path}.${key}`),
            writable: true,
          });
        }
        return output;
      },
      schema: { additionalProperties: validator.schema, type: "object" },
    });
  },
  string(): Validator<string> {
    return createValidator<string>({
      parser(value, path) {
        if (typeof value !== "string") {
          throw new Error(`${path} must be a string`);
        }

        return value;
      },
      schema: { type: "string" },
    });
  },
  union: unionValidator,
  unknown(): Validator<unknown> {
    return createValidator<unknown>({
      parser(value) {
        return value;
      },
      schema: {},
    });
  },
};
