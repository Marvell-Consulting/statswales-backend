import { SortByInterface } from '../interfaces/sort-by-interface';

export function sortObjToString(sort: SortByInterface[]): string[] {
  return sort.map((s) => `${s.columnName}|${s.direction?.toLowerCase()}`);
}
