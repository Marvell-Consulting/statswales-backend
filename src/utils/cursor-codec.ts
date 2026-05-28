import { createHash } from 'node:crypto';

import { BadRequestException } from '../exceptions/bad-request.exception';

export const CURSOR_VERSION = 1;
export const MAX_CURSOR_LENGTH = 2048;

export type CursorDirection = 'f' | 'b';

// Values that can appear in a key tuple. NULL handling matters for keyset
// pagination, so the codec must preserve a JSON null through encode/decode.
export type CursorKeyValue = string | number | boolean | null;

export interface CursorPayload {
  v: number;
  q: string; // queryStoreId
  r: string; // revisionId
  l: string; // language
  h: string; // sortHash
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

export function encodeCursor(payload: CursorPayload): string {
  const json = JSON.stringify(payload);
  return Buffer.from(json, 'utf8').toString('base64url');
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

  if (!isCursorPayload(parsed)) {
    throw new BadRequestException('errors.invalid_cursor');
  }

  if (parsed.v !== CURSOR_VERSION) throw new BadRequestException('errors.invalid_cursor');
  if (parsed.q !== expected.queryStoreId) throw new BadRequestException('errors.invalid_cursor');
  if (parsed.r !== expected.revisionId) throw new BadRequestException('errors.invalid_cursor');
  if (parsed.l !== expected.language) throw new BadRequestException('errors.invalid_cursor');
  if (parsed.h !== expected.sortHash) throw new BadRequestException('errors.invalid_cursor');
  if (parsed.k.length !== expected.keyArity) throw new BadRequestException('errors.invalid_cursor');

  return parsed;
}

function isCursorPayload(x: unknown): x is CursorPayload {
  if (!x || typeof x !== 'object') return false;
  const o = x as Record<string, unknown>;
  if (typeof o.v !== 'number') return false;
  if (typeof o.q !== 'string') return false;
  if (typeof o.r !== 'string') return false;
  if (typeof o.l !== 'string') return false;
  if (typeof o.h !== 'string') return false;
  if (o.d !== 'f' && o.d !== 'b') return false;
  if (!Array.isArray(o.k)) return false;
  for (const v of o.k) {
    if (v === null) continue;
    const t = typeof v;
    if (t !== 'string' && t !== 'number' && t !== 'boolean') return false;
  }
  return true;
}
