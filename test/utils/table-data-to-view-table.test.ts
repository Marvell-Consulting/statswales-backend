import { FactTableColumnType } from '../../src/enums/fact-table-column-type';
import { tableDataToViewTable } from '../../src/utils/table-data-to-view-table';

describe('table-data-to-view-table', () => {
  describe('tableDataToViewTable', () => {
    it('should create headers from object keys', () => {
      const data = [{ col_a: 'val1' as unknown as JSON, col_b: 'val2' as unknown as JSON }];
      const result = tableDataToViewTable(data);

      expect(result.headers.map((h) => h.name)).toEqual(['col_a', 'col_b']);
    });

    it('should create data as array-of-arrays from object values', () => {
      const data = [
        { col_a: 'r1a' as unknown as JSON, col_b: 'r1b' as unknown as JSON },
        { col_a: 'r2a' as unknown as JSON, col_b: 'r2b' as unknown as JSON }
      ];
      const result = tableDataToViewTable(data);

      expect(result.data).toEqual([
        ['r1a', 'r1b'],
        ['r2a', 'r2b']
      ]);
    });

    it('should set LineNumber type for line_number column', () => {
      const data = [{ line_number: 1 as unknown as JSON, value: 'a' as unknown as JSON }];
      const result = tableDataToViewTable(data);

      const lineNumberHeader = result.headers.find((h) => h.name === 'line_number');
      expect(lineNumberHeader?.sourceType).toBe(FactTableColumnType.LineNumber);
    });

    it('should set Unknown type for non-line_number columns', () => {
      const data = [{ col_a: 'val' as unknown as JSON }];
      const result = tableDataToViewTable(data);

      expect(result.headers[0].sourceType).toBe(FactTableColumnType.Unknown);
    });

    it('should assign correct index values to headers', () => {
      const data = [{ first: '1' as unknown as JSON, second: '2' as unknown as JSON, third: '3' as unknown as JSON }];
      const result = tableDataToViewTable(data);

      expect(result.headers.map((h) => h.index)).toEqual([0, 1, 2]);
    });

    it('should handle single-row data', () => {
      const data = [{ only: 'row' as unknown as JSON }];
      const result = tableDataToViewTable(data);

      expect(result.headers).toHaveLength(1);
      expect(result.data).toEqual([['row']]);
    });
  });
});
