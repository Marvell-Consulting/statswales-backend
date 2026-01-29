import { sortObjToString } from '../../src/utils/sort-obj-to-string';

describe('sort-obj-to-string', () => {
  describe('sortObjToString', () => {
    it('should format ASC direction to lowercase', () => {
      const result = sortObjToString([{ columnName: 'name', direction: 'ASC' }]);
      expect(result).toEqual(['name|asc']);
    });

    it('should format DESC direction to lowercase', () => {
      const result = sortObjToString([{ columnName: 'age', direction: 'DESC' }]);
      expect(result).toEqual(['age|desc']);
    });

    it('should default to asc when direction is undefined', () => {
      const result = sortObjToString([{ columnName: 'name' }]);
      expect(result).toEqual(['name|asc']);
    });

    it('should handle multiple sort objects', () => {
      const result = sortObjToString([
        { columnName: 'name', direction: 'ASC' },
        { columnName: 'age', direction: 'DESC' },
        { columnName: 'city' }
      ]);

      expect(result).toEqual(['name|asc', 'age|desc', 'city|asc']);
    });

    it('should return an empty array for empty input', () => {
      const result = sortObjToString([]);
      expect(result).toEqual([]);
    });
  });
});
