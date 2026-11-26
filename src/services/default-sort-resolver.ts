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
// Strategy:
//   1. Primary sort = first date column on the fact table (lowest columnIndex)
//      if one exists; otherwise first Dimension column. A date column is a
//      Time column, or a column whose dimension is a Date or DatePeriod.
//   2. Tie-breakers = remaining composite-PK columns (Dimension/Measure/Time)
//      in columnIndex order, with duplicates of the primary removed.
//   3. Date columns sort DESC (newest first), matching the order the cube
//      view and downloads use. Everything else sorts ASC.
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

  const primary =
    sorted.find((c) => isDate(c) && nameByFactColumn.has(c.columnName)) ||
    sorted.find((c) => c.columnType === FactTableColumnType.Dimension && nameByFactColumn.has(c.columnName));

  if (!primary) return [];

  const result: ResolvedSortColumn[] = [];
  const seenDisplayNames = new Set<string>();

  const append = (col: FactTableColumn): void => {
    const displayName = nameByFactColumn.get(col.columnName);
    if (!displayName) return;
    if (seenDisplayNames.has(displayName)) return;
    seenDisplayNames.add(displayName);
    result.push({ columnName: displayName, direction: isDate(col) ? 'desc' : 'asc' });
  };

  append(primary);

  for (const col of sorted) {
    if (!isPkType(col.columnType)) continue;
    append(col);
  }

  return result;
}
