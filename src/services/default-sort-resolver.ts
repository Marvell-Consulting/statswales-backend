import { Dimension } from '../entities/dataset/dimension';
import { FactTableColumn } from '../entities/dataset/fact-table-column';
import { DimensionType } from '../enums/dimension-type';
import { FactTableColumnType } from '../enums/fact-table-column-type';
import { FactTableToDimensionName } from '../interfaces/fact-table-column-to-dimension-name';

export interface ResolvedSortColumn {
  // Display name as it appears in the core view (i.e. the translated
  // dimension_name from queryStore.columnMapping for the requested language).
  // This matches the `colName` format that buildDataQuery already expects
  // for user-supplied sort_by.
  columnName: string;
  direction: 'asc' | 'desc';
}

// Resolve a deterministic default sort + tie-breakers for keyset pagination.
//
// The order follows the cube view's own ORDER BY, so the paged table lists
// rows in the same order as downloads:
//   1. The measure column first.
//   2. Then the other composite-PK columns (Dimension/Time) in columnIndex
//      order, which is the order the cube builder adds dimensions in.
//   3. Date columns sort DESC (newest first), everything else ASC. A date
//      column is a Time column, or one whose dimension is a Date or
//      DatePeriod, the same test the cube builder uses.
//
// Columns that have no entry in queryStore.columnMapping for the requested
// language are skipped — they can't be referenced in the core view's translated
// SELECT list, so emitting them would produce invalid SQL.
//
// Returns an empty array when no usable sort key can be resolved (no fact-table
// columns of the expected types, or no columnMapping entries for the language).
// The caller decides what to do with that — usually fall back to OFFSET-only.
export function resolveDefaultSort(
  factTable: FactTableColumn[] | null | undefined,
  columnMapping: FactTableToDimensionName[],
  language: string,
  dimensions: Dimension[] = []
): ResolvedSortColumn[] {
  if (!factTable || factTable.length === 0) return [];

  const lang = language.toLowerCase();
  const nameByFactColumn = new Map<string, string>();
  for (const m of columnMapping) {
    if (m.language.toLowerCase() === lang) {
      nameByFactColumn.set(m.fact_table_column, m.dimension_name);
    }
  }
  if (nameByFactColumn.size === 0) return [];

  const sorted = [...factTable].sort((a, b) => a.columnIndex - b.columnIndex);

  const isPkType = (t: FactTableColumnType): boolean =>
    t === FactTableColumnType.Time || t === FactTableColumnType.Dimension || t === FactTableColumnType.Measure;

  const dateColumns = new Set(
    dimensions
      .filter((dim) => dim.type === DimensionType.Date || dim.type === DimensionType.DatePeriod)
      .map((dim) => dim.factTableColumn)
  );
  const isDate = (c: FactTableColumn): boolean =>
    c.columnType === FactTableColumnType.Time || dateColumns.has(c.columnName);

  const result: ResolvedSortColumn[] = [];
  const seenDisplayNames = new Set<string>();

  const append = (col: FactTableColumn): void => {
    const displayName = nameByFactColumn.get(col.columnName);
    if (!displayName) return;
    if (seenDisplayNames.has(displayName)) return;
    seenDisplayNames.add(displayName);
    result.push({ columnName: displayName, direction: isDate(col) ? 'desc' : 'asc' });
  };

  for (const col of sorted) {
    if (col.columnType === FactTableColumnType.Measure) append(col);
  }

  for (const col of sorted) {
    if (!isPkType(col.columnType)) continue;
    append(col);
  }

  return result;
}
