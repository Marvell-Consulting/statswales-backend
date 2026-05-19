jest.mock('../../../src/utils/logger', () => ({
  logger: { debug: jest.fn(), info: jest.fn(), error: jest.fn(), warn: jest.fn(), trace: jest.fn() }
}));

jest.mock('../../../src/db/database-manager', () => ({ dbManager: { getCubeDataSource: jest.fn() } }));

jest.mock('../../../src/services/cube-builder', () => ({
  CORE_VIEW_NAME: 'core_view',
  FILTER_TABLE_NAME: 'filter_table',
  METADATA_TABLE_NAME: 'metadata',
  FACT_TABLE_NAME: 'fact_table',
  VALIDATION_TABLE_NAME: 'validation_table',
  setupValidationTableFromDataset: jest.fn()
}));

jest.mock('i18next', () => ({ t: jest.fn((key: string) => key) }));

import { flattenHierarchy, transformHierarchy } from '../../../src/utils/consumer';
import { FilterRow } from '../../../src/interfaces/filter-row';

const baseRow = (overrides: Partial<FilterRow>): FilterRow => ({
  reference: '',
  language: 'en-GB',
  fact_table_column: 'col',
  dimension_name: 'dim',
  description: '',
  hierarchy: null,
  ...overrides
});

describe('transformHierarchy', () => {
  describe('with string references', () => {
    it('builds a two-level hierarchy', () => {
      const rows: FilterRow[] = [
        baseRow({ reference: 'parent', description: 'Parent', hierarchy: null }),
        baseRow({ reference: 'child', description: 'Child', hierarchy: 'parent' })
      ];

      const result = transformHierarchy('col', 'dim', rows);

      expect(result.values).toHaveLength(1);
      expect(result.values[0].reference).toBe('parent');
      expect(result.values[0].children).toHaveLength(1);
      expect(result.values[0].children![0].reference).toBe('child');
    });
  });

  describe('with numeric (double) references at runtime', () => {
    it('correctly links children to parents when reference and hierarchy are numbers', () => {
      // Simulate what PostgreSQL returns for a DOUBLE column — numbers at runtime.
      const rows: FilterRow[] = [
        baseRow({ reference: 1.0, description: 'Parent', hierarchy: null }),
        baseRow({ reference: 2.0, description: 'Child', hierarchy: 1.0 })
      ];

      const result = transformHierarchy('col', 'dim', rows);

      expect(result.values).toHaveLength(1);
      expect(result.values[0].reference).toBe('1');
      expect(result.values[0].children).toHaveLength(1);
      expect(result.values[0].children![0].reference).toBe('2');
    });

    it('does not lose children from the output when references are doubles', () => {
      const rows: FilterRow[] = [
        baseRow({ reference: 1.0, description: 'Root', hierarchy: null }),
        baseRow({ reference: 2.0, description: 'Child A', hierarchy: 1.0 }),
        baseRow({ reference: 3.0, description: 'Child B', hierarchy: 1.0 })
      ];

      const result = transformHierarchy('col', 'dim', rows);
      const flat = flattenHierarchy(result.values);

      expect(flat).toHaveLength(3);
    });

    it('builds a three-level hierarchy with double references', () => {
      const rows: FilterRow[] = [
        baseRow({ reference: 1.0, description: 'Root', hierarchy: null }),
        baseRow({ reference: 2.0, description: 'Mid', hierarchy: 1.0 }),
        baseRow({ reference: 3.0, description: 'Leaf', hierarchy: 2.0 })
      ];

      const result = transformHierarchy('col', 'dim', rows);

      expect(result.values).toHaveLength(1);
      expect(result.values[0].children).toHaveLength(1);
      expect(result.values[0].children![0].children).toHaveLength(1);
      expect(result.values[0].children![0].children![0].reference).toBe('3');
    });
  });
});

describe('flattenHierarchy', () => {
  it('returns a flat list of all nodes', () => {
    const tree = [
      {
        reference: 'a',
        description: 'A',
        children: [
          { reference: 'b', description: 'B' },
          { reference: 'c', description: 'C' }
        ]
      }
    ];

    const result = flattenHierarchy(tree);

    expect(result.map((n) => n.reference)).toEqual(['a', 'b', 'c']);
  });

  it('returns an empty array for empty input', () => {
    expect(flattenHierarchy([])).toEqual([]);
  });
});
