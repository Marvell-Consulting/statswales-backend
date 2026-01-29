import { isObject, walkObject, WalkObjectCallbackArgs } from '../../src/utils/walk-object';

describe('walk-object', () => {
  describe('isObject', () => {
    it('should return true for a plain object', () => {
      expect(isObject({ a: 1 })).toBe(true);
    });

    it('should return false for null', () => {
      expect(isObject(null)).toBe(false);
    });

    it('should return false for an array', () => {
      expect(isObject([1, 2, 3])).toBe(false);
    });

    it('should return false for a string', () => {
      expect(isObject('hello')).toBe(false);
    });

    it('should return false for a number', () => {
      expect(isObject(42)).toBe(false);
    });

    it('should return false for undefined', () => {
      expect(isObject(undefined)).toBe(false);
    });

    it('should return false for a boolean', () => {
      expect(isObject(true)).toBe(false);
    });

    it('should return true for an empty object', () => {
      expect(isObject({})).toBe(true);
    });
  });

  describe('walkObject', () => {
    it('should visit all leaf nodes of a flat object', () => {
      const visited: WalkObjectCallbackArgs[] = [];
      walkObject({ a: 1, b: 'two' }, (args) => visited.push({ ...args }));

      expect(visited).toEqual([
        { value: 1, key: 'a', location: ['a'], isLeaf: true },
        { value: 'two', key: 'b', location: ['b'], isLeaf: true }
      ]);
    });

    it('should visit nested objects with correct locations', () => {
      const visited: WalkObjectCallbackArgs[] = [];
      walkObject({ top: { nested: 'val' } }, (args) => visited.push({ ...args }));

      expect(visited).toHaveLength(2);
      expect(visited[0]).toEqual({
        value: { nested: 'val' },
        key: 'top',
        location: ['top'],
        isLeaf: false
      });
      expect(visited[1]).toEqual({
        value: 'val',
        key: 'nested',
        location: ['top', 'nested'],
        isLeaf: true
      });
    });

    it('should visit arrays with indexed keys', () => {
      const visited: WalkObjectCallbackArgs[] = [];
      walkObject({ items: [{ id: 1 }, { id: 2 }] }, (args) => visited.push({ ...args }));

      const arrayEntries = visited.filter((v) => v.key.startsWith('items:'));
      expect(arrayEntries).toHaveLength(2);
      expect(arrayEntries[0].key).toBe('items:0');
      expect(arrayEntries[0].location).toEqual(['items', 0]);
      expect(arrayEntries[1].key).toBe('items:1');
      expect(arrayEntries[1].location).toEqual(['items', 1]);
    });

    it('should handle deep nesting', () => {
      const visited: WalkObjectCallbackArgs[] = [];
      walkObject({ a: { b: { c: 'deep' } } }, (args) => visited.push({ ...args }));

      const leaf = visited.find((v) => v.isLeaf);
      expect(leaf).toEqual({
        value: 'deep',
        key: 'c',
        location: ['a', 'b', 'c'],
        isLeaf: true
      });
    });

    it('should not call callback for empty objects', () => {
      const visited: WalkObjectCallbackArgs[] = [];
      walkObject({}, (args) => visited.push({ ...args }));

      expect(visited).toEqual([]);
    });

    it('should handle null leaf values', () => {
      const visited: WalkObjectCallbackArgs[] = [];
      walkObject({ x: null }, (args) => visited.push({ ...args }));

      expect(visited).toEqual([{ value: null, key: 'x', location: ['x'], isLeaf: true }]);
    });
  });
});
