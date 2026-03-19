import { BadRequestException } from '../exceptions/bad-request.exception';
import { SortByInterface } from '../interfaces/sort-by-interface';

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

  if (raw.startsWith('[')) {
    try {
      const parsed = JSON.parse(raw) as SortByInterface[];
      return parsed.map((s) => {
        if (!s.columnName) throw new Error('missing columnName');
        return `${s.columnName}|${s.direction ? s.direction.toLowerCase() : 'asc'}`;
      });
    } catch {
      throw new BadRequestException('errors.invalid_sort_by');
    }
  }

  try {
    return raw.split(',').map((segment) => {
      const [column, direction] = segment.trim().split(':');
      if (!column) throw new Error('empty column name');
      const dir = (direction || 'asc').toLowerCase();
      if (dir !== 'asc' && dir !== 'desc') throw new Error(`invalid direction: ${dir}`);
      return `${column}|${dir}`;
    });
  } catch {
    throw new BadRequestException('errors.invalid_sort_by');
  }
}
