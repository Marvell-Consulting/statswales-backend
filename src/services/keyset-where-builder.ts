import { format as pgformat } from '@scaleleap/pg-format/lib/pg-format';

import { CursorKeyValue } from '../utils/cursor-codec';

export interface KeysetSortColumn {
  // The column identifier as it appears in the outer SELECT — already
  // translated and postfixed where applicable. Will be passed through
  // pgformat's %I.
  sqlIdent: string;
  direction: 'asc' | 'desc';
}

export type KeysetDirection = 'f' | 'b';

// Build the WHERE clause for a keyset paginated query. Returns a single SQL
// fragment of the form `(... OR ... OR ...)` that, AND-ed onto a query
// emitting rows in `ORDER BY columns`, yields the rows strictly after (or
// before, for backward) the supplied key tuple.
//
// Assumes Postgres default NULL placement: NULLS LAST for ASC, NULLS FIRST
// for DESC. That matches the existing ORDER BY clauses elsewhere in the
// service, which don't override NULLS placement.
export function buildKeysetWhere(
  columns: KeysetSortColumn[],
  key: CursorKeyValue[],
  direction: KeysetDirection
): string {
  if (columns.length === 0) {
    throw new Error('buildKeysetWhere: at least one sort column is required');
  }
  if (columns.length !== key.length) {
    throw new Error('buildKeysetWhere: key tuple length must match sort column count');
  }

  const rungs: string[] = [];
  for (let i = 0; i < columns.length; i++) {
    const equalityPrefix = columns.slice(0, i).map((col, j) => equalityPredicate(col.sqlIdent, key[j]));
    const step = stepPredicate(columns[i], key[i], direction);
    const parts = [...equalityPrefix, step];
    rungs.push(parts.length === 1 ? parts[0] : `(${parts.join(' AND ')})`);
  }

  return `(${rungs.join(' OR ')})`;
}

function equalityPredicate(sqlIdent: string, value: CursorKeyValue): string {
  // IS NOT DISTINCT FROM treats NULL = NULL as true, which is what we want
  // for the equality rungs that chain into the next column.
  return pgformat('%I IS NOT DISTINCT FROM %L', sqlIdent, value);
}

function stepPredicate(column: KeysetSortColumn, value: CursorKeyValue, direction: KeysetDirection): string {
  // Effective direction: backward traversal flips comparators.
  const effective = direction === 'f' ? column.direction : column.direction === 'asc' ? 'desc' : 'asc';

  if (effective === 'asc') {
    // ASC, NULLS LAST: NULLs come after all non-NULL values.
    //  - value NOT NULL: rows with c > value, OR rows where c IS NULL.
    //  - value NULL: nothing comes after NULL → both halves are false.
    return pgformat('(%I > %L OR (%I IS NULL AND %L IS NOT NULL))', column.sqlIdent, value, column.sqlIdent, value);
  }

  // DESC, NULLS FIRST: NULLs come before all non-NULL values.
  //  - value NOT NULL: rows with c < value (NULLs are already behind us).
  //  - value NULL: rows where c IS NOT NULL.
  return pgformat('(%I < %L OR (%I IS NOT NULL AND %L IS NULL))', column.sqlIdent, value, column.sqlIdent, value);
}
