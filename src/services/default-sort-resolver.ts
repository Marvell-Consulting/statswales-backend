import { FactTableColumn } from '../entities/dataset/fact-table-column';
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
//   1. Primary sort = first Time column on the fact table (lowest columnIndex)
//      if one exists; otherwise first Dimension column.
//   2. Tie-breakers = remaining composite-PK columns (Dimension/Measure/Time)
//      in columnIndex order, all ASC, with duplicates of the primary removed.
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
  language: string
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

  const primary =
    sorted.find((c) => c.columnType === FactTableColumnType.Time && nameByFactColumn.has(c.columnName)) ||
    sorted.find((c) => c.columnType === FactTableColumnType.Dimension && nameByFactColumn.has(c.columnName));

  if (!primary) return [];

  const result: ResolvedSortColumn[] = [];
  const seenDisplayNames = new Set<string>();

  const append = (factColName: string): void => {
    const displayName = nameByFactColumn.get(factColName);
    if (!displayName) return;
    if (seenDisplayNames.has(displayName)) return;
    seenDisplayNames.add(displayName);
    result.push({ columnName: displayName, direction: 'asc' });
  };

  append(primary.columnName);

  for (const col of sorted) {
    if (!isPkType(col.columnType)) continue;
    append(col.columnName);
  }

  return result;
}
