import { BadRequestException } from '../exceptions/bad-request.exception';
import { SortByInterface } from '../interfaces/sort-by-interface';

// Column names here are translated dimension display names ("Financial year",
// "Data values", etc.) resolved downstream against a known column set, and
// substituted into SQL via pgformat %I. We don't constrain the character set —
// only structural problems (empty name, bad direction) are rejected.

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
        if (!columnName) throw new Error('missing columnName');
        // `|` is the internal delimiter; rejecting it stops a column with an
        // embedded `|` from injecting attacker-controlled text into the
        // direction slot when downstream callers re-split.
        if (columnName.includes('|')) throw new Error(`invalid columnName: ${columnName}`);
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
      const column = parts[0]?.trim();
      const direction = parts[1]?.trim();
      if (!column) throw new Error('empty column name');
      if (column.includes('|')) throw new Error(`invalid column name: ${column}`);
      const dir = (direction || 'asc').toLowerCase();
      if (dir !== 'asc' && dir !== 'desc') throw new Error(`invalid direction: ${dir}`);
      return `${column}|${dir}`;
    });
  } catch {
    throw new BadRequestException('errors.invalid_sort_by');
  }
}
