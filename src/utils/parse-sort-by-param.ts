import { BadRequestException } from '../exceptions/bad-request.exception';
import { SortByInterface } from '../interfaces/sort-by-interface';

// fact_table_column values are SQL-style identifiers (e.g. YearCode, area_code).
// Reject anything that isn't, so we 400 at the edge rather than 500 inside the cube.
const IDENTIFIER_RE = /^[A-Za-z_][A-Za-z0-9_]*$/;

export function parseSortByToObjects(raw: string | undefined): SortByInterface[] | undefined {
  const strings = parseSortByParam(raw);
  if (!strings.length) return undefined;
  return strings.map((s) => {
    const [columnName, direction] = s.split('|');
    return { columnName, direction: direction.toUpperCase() as 'ASC' | 'DESC' };
  });
}

export function parseSortByParam(raw: string | undefined): string[] {
  if (!raw) return [];

  const trimmed = raw.trim();

  if (trimmed.startsWith('[')) {
    try {
      const parsed = JSON.parse(trimmed) as SortByInterface[];
      if (!Array.isArray(parsed)) throw new Error('sort_by JSON must be an array');
      return parsed.map((s) => {
        const columnName = s.columnName?.trim();
        if (!columnName || !IDENTIFIER_RE.test(columnName)) {
          throw new Error(`invalid columnName: ${columnName}`);
        }
        const dir = (s.direction || 'asc').toLowerCase();
        if (dir !== 'asc' && dir !== 'desc') throw new Error(`invalid direction: ${dir}`);
        return `${columnName}|${dir}`;
      });
    } catch {
      throw new BadRequestException('errors.invalid_sort_by');
    }
  }

  try {
    return trimmed.split(',').map((segment) => {
      const parts = segment.trim().split(':');
      if (parts.length > 2) throw new Error('too many colons');
      const [column, direction] = parts;
      if (!column || !IDENTIFIER_RE.test(column)) throw new Error(`invalid column name: ${column}`);
      const dir = (direction || 'asc').toLowerCase();
      if (dir !== 'asc' && dir !== 'desc') throw new Error(`invalid direction: ${dir}`);
      return `${column}|${dir}`;
    });
  } catch {
    throw new BadRequestException('errors.invalid_sort_by');
  }
}
