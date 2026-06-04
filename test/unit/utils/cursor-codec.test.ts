import {
  CURSOR_VERSION,
  CursorExpectation,
  CursorPayload,
  computeContextHash,
  computeSortHash,
  decodeCursor,
  encodeCursor
} from '../../../src/utils/cursor-codec';
import { BadRequestException } from '../../../src/exceptions/bad-request.exception';

const baseExpected: CursorExpectation = {
  queryStoreId: 'qs-1',
  revisionId: 'rev-1',
  language: 'en-GB',
  sortHash: computeSortHash([{ columnName: 'Year', direction: 'asc' }], 'en-GB'),
  keyArity: 2
};

const baseContext = computeContextHash(
  baseExpected.queryStoreId,
  baseExpected.revisionId,
  baseExpected.language,
  baseExpected.sortHash
);

function basePayload(overrides: Partial<CursorPayload> = {}): CursorPayload {
  return {
    v: CURSOR_VERSION,
    c: baseContext,
    d: 'f',
    k: [2022, 'W06000001'],
    ...overrides
  };
}

// Build an encoded token straight from a positional [v, c, d, k] wire array,
// bypassing the typed encodeCursor so malformed shapes can be exercised.
function encodeWire(wire: unknown): string {
  return Buffer.from(JSON.stringify(wire), 'utf8').toString('base64url');
}

describe('computeSortHash', () => {
  it('produces a stable hash for the same spec + language', () => {
    const a = computeSortHash([{ columnName: 'Year', direction: 'asc' }], 'en-GB');
    const b = computeSortHash([{ columnName: 'Year', direction: 'asc' }], 'en-GB');
    expect(a).toBe(b);
  });

  it('differs when the direction changes', () => {
    const a = computeSortHash([{ columnName: 'Year', direction: 'asc' }], 'en-GB');
    const b = computeSortHash([{ columnName: 'Year', direction: 'desc' }], 'en-GB');
    expect(a).not.toBe(b);
  });

  it('differs when the column order changes', () => {
    const a = computeSortHash(
      [
        { columnName: 'Year', direction: 'asc' },
        { columnName: 'Area', direction: 'asc' }
      ],
      'en-GB'
    );
    const b = computeSortHash(
      [
        { columnName: 'Area', direction: 'asc' },
        { columnName: 'Year', direction: 'asc' }
      ],
      'en-GB'
    );
    expect(a).not.toBe(b);
  });

  it('differs across languages', () => {
    const a = computeSortHash([{ columnName: 'Year', direction: 'asc' }], 'en-GB');
    const b = computeSortHash([{ columnName: 'Year', direction: 'asc' }], 'cy-GB');
    expect(a).not.toBe(b);
  });
});

describe('computeContextHash', () => {
  it('produces a stable hash for the same inputs', () => {
    const a = computeContextHash('qs-1', 'rev-1', 'en-GB', baseExpected.sortHash);
    const b = computeContextHash('qs-1', 'rev-1', 'en-GB', baseExpected.sortHash);
    expect(a).toBe(b);
  });

  it('differs when the query store changes', () => {
    const a = computeContextHash('qs-1', 'rev-1', 'en-GB', baseExpected.sortHash);
    const b = computeContextHash('qs-2', 'rev-1', 'en-GB', baseExpected.sortHash);
    expect(a).not.toBe(b);
  });

  it('differs when the revision changes', () => {
    const a = computeContextHash('qs-1', 'rev-1', 'en-GB', baseExpected.sortHash);
    const b = computeContextHash('qs-1', 'rev-2', 'en-GB', baseExpected.sortHash);
    expect(a).not.toBe(b);
  });

  it('differs when the language changes', () => {
    const a = computeContextHash('qs-1', 'rev-1', 'en-GB', baseExpected.sortHash);
    const b = computeContextHash('qs-1', 'rev-1', 'cy-GB', baseExpected.sortHash);
    expect(a).not.toBe(b);
  });

  it('differs when the sort hash changes', () => {
    const a = computeContextHash('qs-1', 'rev-1', 'en-GB', baseExpected.sortHash);
    const b = computeContextHash('qs-1', 'rev-1', 'en-GB', 'other-sort-hash');
    expect(a).not.toBe(b);
  });
});

describe('encode / decode round-trip', () => {
  it('round-trips a forward cursor', () => {
    const encoded = encodeCursor(basePayload());
    const decoded = decodeCursor(encoded, baseExpected);
    expect(decoded).toEqual(basePayload());
  });

  it('round-trips a backward cursor', () => {
    const encoded = encodeCursor(basePayload({ d: 'b' }));
    const decoded = decodeCursor(encoded, baseExpected);
    expect(decoded.d).toBe('b');
  });

  it('preserves null key values', () => {
    const encoded = encodeCursor(basePayload({ k: [null, 'x'] }));
    const decoded = decodeCursor(encoded, baseExpected);
    expect(decoded.k[0]).toBeNull();
    expect(decoded.k[1]).toBe('x');
  });

  it('preserves numeric key values without coercion to string', () => {
    const encoded = encodeCursor(basePayload({ k: [2022, 'x'] }));
    const decoded = decodeCursor(encoded, baseExpected);
    expect(decoded.k[0]).toBe(2022);
    expect(typeof decoded.k[0]).toBe('number');
  });
});

describe('decodeCursor rejections', () => {
  it('rejects an empty string', () => {
    expect(() => decodeCursor('', baseExpected)).toThrow(BadRequestException);
  });

  it('rejects a non-string input', () => {
    expect(() => decodeCursor(undefined as unknown as string, baseExpected)).toThrow(BadRequestException);
  });

  it('rejects a cursor longer than the maximum', () => {
    const huge = 'a'.repeat(3000);
    expect(() => decodeCursor(huge, baseExpected)).toThrow(BadRequestException);
  });

  it('rejects malformed base64url that does not parse as JSON', () => {
    const garbage = Buffer.from('not json at all', 'utf8').toString('base64url');
    expect(() => decodeCursor(garbage, baseExpected)).toThrow(BadRequestException);
  });

  it('rejects a payload that is not a positional array', () => {
    const wrong = encodeWire({ foo: 'bar' });
    expect(() => decodeCursor(wrong, baseExpected)).toThrow(BadRequestException);
  });

  it('rejects a positional array of the wrong length', () => {
    const wrong = encodeWire([CURSOR_VERSION, baseContext, 'f']);
    expect(() => decodeCursor(wrong, baseExpected)).toThrow(BadRequestException);
  });

  it('rejects a version mismatch', () => {
    const encoded = encodeCursor(basePayload({ v: 999 }));
    expect(() => decodeCursor(encoded, baseExpected)).toThrow(BadRequestException);
  });

  it('rejects a different queryStoreId', () => {
    const encoded = encodeCursor(basePayload());
    expect(() => decodeCursor(encoded, { ...baseExpected, queryStoreId: 'qs-other' })).toThrow(BadRequestException);
  });

  it('rejects a different revisionId', () => {
    const encoded = encodeCursor(basePayload());
    expect(() => decodeCursor(encoded, { ...baseExpected, revisionId: 'rev-other' })).toThrow(BadRequestException);
  });

  it('rejects a different language', () => {
    const encoded = encodeCursor(basePayload());
    expect(() => decodeCursor(encoded, { ...baseExpected, language: 'cy-GB' })).toThrow(BadRequestException);
  });

  it('rejects a different sortHash', () => {
    const encoded = encodeCursor(basePayload());
    expect(() => decodeCursor(encoded, { ...baseExpected, sortHash: 'something-else' })).toThrow(BadRequestException);
  });

  it('rejects a key tuple with the wrong arity', () => {
    const encoded = encodeCursor(basePayload({ k: [2022] }));
    expect(() => decodeCursor(encoded, baseExpected)).toThrow(BadRequestException);
  });

  it('rejects a key tuple containing an object', () => {
    const encoded = encodeWire([CURSOR_VERSION, baseContext, 'f', [{ nested: true }, 'x']]);
    expect(() => decodeCursor(encoded, baseExpected)).toThrow(BadRequestException);
  });

  it('rejects an invalid direction', () => {
    const encoded = encodeWire([CURSOR_VERSION, baseContext, 'x', [2022, 'x']]);
    expect(() => decodeCursor(encoded, baseExpected)).toThrow(BadRequestException);
  });
});
