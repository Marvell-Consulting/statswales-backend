import { removeValuesWithNoData } from '../../../src/utils/filter-values';
import { FilterValues } from '../../../src/interfaces/filter-table';

describe('removeValuesWithNoData', () => {
  const value = (reference: string, count: string | null | undefined, children?: FilterValues[]): FilterValues => ({
    reference,
    description: reference,
    count,
    ...(children ? { children } : {})
  });
  const refs = (values: FilterValues[]): unknown[] =>
    values.map((v) => (v.children ? { [v.reference]: refs(v.children) } : v.reference));

  it('removes values that have no rows in the data', () => {
    const result = removeValuesWithNoData([value('a', '5'), value('b', '0'), value('c', '2')]);
    expect(refs(result)).toEqual(['a', 'c']);
  });

  it('keeps values with no count, as filter tables built before counts were added have none', () => {
    const result = removeValuesWithNoData([value('a', undefined), value('b', null), value('c', '0')]);
    expect(refs(result)).toEqual(['a', 'b']);
  });

  it('removes a parent with no data when none of its children have data', () => {
    const result = removeValuesWithNoData([
      value('y2025', '8', [value('q1y2025', '4'), value('q2y2025', '4')]),
      value('y2026', '0', [value('q1y2026', '0'), value('q2y2026', '0')])
    ]);
    expect(refs(result)).toEqual([{ y2025: ['q1y2025', 'q2y2025'] }]);
  });

  it('keeps a parent with no data when a child has data, without its empty children', () => {
    const result = removeValuesWithNoData([value('all', '0', [value('x', '3'), value('y', '0')])]);
    expect(refs(result)).toEqual([{ all: ['x'] }]);
  });

  it('removes empty children at every level', () => {
    const result = removeValuesWithNoData([
      value('total', '9', [
        value('oneToFive', '9', [value('1', '9'), value('2', '0')]),
        value('sixToTen', '0', [value('6', '0')])
      ])
    ]);
    expect(refs(result)).toEqual([{ total: [{ oneToFive: ['1'] }] }]);
  });

  it('leaves no empty list of children on a parent whose children all have no data', () => {
    const [parent] = removeValuesWithNoData([value('p', '4', [value('c', '0')])]);
    expect(parent).not.toHaveProperty('children');
  });

  it('returns the values unchanged if none of them have data, rather than an empty filter', () => {
    const values = [value('a', '0'), value('b', '0', [value('c', '0')])];
    expect(removeValuesWithNoData(values)).toEqual(values);
  });

  it('treats a numeric count of 0 as no data', () => {
    const result = removeValuesWithNoData([value('a', 0 as unknown as string), value('b', '1')]);
    expect(refs(result)).toEqual(['b']);
  });
});
