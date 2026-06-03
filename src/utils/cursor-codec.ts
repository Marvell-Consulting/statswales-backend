import { createHash } from 'node:crypto';

import { BadRequestException } from '../exceptions/bad-request.exception';

// Bump whenever the wire format or binding changes in a shipped release: old
// cursors then fail the version check and 400, and the client restarts from
// page 1. Still 1 — this format has never been released.
export const CURSOR_VERSION = 1;
export const MAX_CURSOR_LENGTH = 2048;

export type CursorDirection = 'f' | 'b';

// Values that can appear in a key tuple. NULL handling matters for keyset
// pagination, so the codec must preserve a JSON null through encode/decode.
export type CursorKeyValue = string | number | boolean | null;

// Decoded, in-memory shape. On the wire it is serialised positionally as
// [v, c, d, k] (see encodeCursor) so the token doesn't carry JSON key names
// — that keeps the URL parameter short.
export interface CursorPayload {
  v: number;
  c: string; // context hash — binds the cursor to its query/revision/lang/sort
  d: CursorDirection;
  k: CursorKeyValue[];
}

export interface CursorExpectation {
  queryStoreId: string;
  revisionId: string;
  language: string;
  sortHash: string;
  keyArity: number;
}

interface CanonicalSortColumn {
  columnName: string;
  direction: 'asc' | 'desc';
}

// Stable hash over the resolved sort spec + language. Two requests that
// produce the same hash see compatible cursors; anything else is a 400.
export function computeSortHash(spec: CanonicalSortColumn[], language: string): string {
  const canonical = JSON.stringify({
    spec: spec.map((s) => ({ c: s.columnName, d: s.direction.toLowerCase() })),
    l: language.toLowerCase()
  });
  return createHash('sha256').update(canonical).digest('base64url').slice(0, 22);
}

// Single short hash binding a cursor to the query store, revision, language
// and sort it was issued for. It is embedded in the token in place of the raw
// ids; on decode we recompute it from the server-known values and compare.
// A mismatch (cursor replayed against different filters/revision/lang/sort,
// or tampered) produces a 400 rather than silently mis-paginating. None of
// the inputs are transmitted — only this 22-char digest — so the binding is
// free in URL terms.
export function computeContextHash(
  queryStoreId: string,
  revisionId: string,
  language: string,
  sortHash: string
): string {
  const canonical = JSON.stringify([queryStoreId, revisionId, language.toLowerCase(), sortHash]);
  return createHash('sha256').update(canonical).digest('base64url').slice(0, 22);
}

export function encodeCursor(payload: CursorPayload): string {
  // Serialise positionally — [v, c, d, k] — rather than as an object so the
  // token carries no JSON key names.
  const wire = [payload.v, payload.c, payload.d, payload.k];
  return Buffer.from(JSON.stringify(wire), 'utf8').toString('base64url');
}

export function decodeCursor(raw: string, expected: CursorExpectation): CursorPayload {
  if (typeof raw !== 'string' || raw.length === 0 || raw.length > MAX_CURSOR_LENGTH) {
    throw new BadRequestException('errors.invalid_cursor');
  }

  let decoded: string;
  try {
    decoded = Buffer.from(raw, 'base64url').toString('utf8');
  } catch {
    throw new BadRequestException('errors.invalid_cursor');
  }

  let parsed: unknown;
  try {
    parsed = JSON.parse(decoded);
  } catch {
    throw new BadRequestException('errors.invalid_cursor');
  }

  const payload = parseWire(parsed);
  if (!payload) {
    throw new BadRequestException('errors.invalid_cursor');
  }

  const expectedContext = computeContextHash(
    expected.queryStoreId,
    expected.revisionId,
    expected.language,
    expected.sortHash
  );

  if (payload.v !== CURSOR_VERSION) throw new BadRequestException('errors.invalid_cursor');
  if (payload.c !== expectedContext) throw new BadRequestException('errors.invalid_cursor');
  if (payload.k.length !== expected.keyArity) throw new BadRequestException('errors.invalid_cursor');

  return payload;
}

// Validate and lift the positional wire array [v, c, d, k] into a typed
// payload. Returns null on any shape mismatch.
function parseWire(x: unknown): CursorPayload | null {
  if (!Array.isArray(x) || x.length !== 4) return null;
  const [v, c, d, k] = x;
  if (typeof v !== 'number') return null;
  if (typeof c !== 'string') return null;
  if (d !== 'f' && d !== 'b') return null;
  if (!Array.isArray(k)) return null;
  for (const val of k) {
    if (val === null) continue;
    const t = typeof val;
    if (t !== 'string' && t !== 'number' && t !== 'boolean') return null;
  }
  return { v, c, d, k };
}
