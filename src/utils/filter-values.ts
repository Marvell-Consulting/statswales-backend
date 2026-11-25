import { FilterValues } from '../interfaces/filter-table';

const hasNoData = (value: FilterValues): boolean => value.count != null && Number(value.count) === 0;

function pruneValuesWithNoData(values: FilterValues[]): FilterValues[] {
  return values.flatMap((value) => {
    const children = value.children ? pruneValuesWithNoData(value.children) : [];
    if (hasNoData(value) && children.length === 0) return [];
    const { children: _children, ...rest } = value;
    return [children.length > 0 ? { ...rest, children } : rest];
  });
}

/**
 * Removes filter values that have no rows in the data, such as lookup table values the dataset
 * doesn't use. A parent with no rows of its own is kept if any of its children have data, so the
 * grouping stays. Values with no count (filter tables built before counts were added) are kept.
 * If no value has data the counts can't be trusted, so the values are returned unchanged.
 */
export function removeValuesWithNoData(values: FilterValues[]): FilterValues[] {
  const pruned = pruneValuesWithNoData(values);
  return pruned.length > 0 ? pruned : values;
}
