import { KeysetSortColumn, buildKeysetWhere } from '../../../src/services/keyset-where-builder';

describe('buildKeysetWhere', () => {
  describe('single column', () => {
    it('emits a forward ASC predicate with NULLS-LAST semantics', () => {
      const sql = buildKeysetWhere([{ sqlIdent: 'year_sort', direction: 'asc' }], [2022], 'f');
      expect(sql).toBe(`((year_sort > 2022 OR (year_sort IS NULL AND 2022 IS NOT NULL)))`);
    });

    it('emits a forward DESC predicate with NULLS-FIRST semantics', () => {
      const sql = buildKeysetWhere([{ sqlIdent: 'year_sort', direction: 'desc' }], [2022], 'f');
      expect(sql).toBe(`((year_sort < 2022 OR (year_sort IS NOT NULL AND 2022 IS NULL)))`);
    });

    it('flips comparators for backward ASC', () => {
      const sql = buildKeysetWhere([{ sqlIdent: 'year_sort', direction: 'asc' }], [2022], 'b');
      // backward over ASC = step uses DESC rules
      expect(sql).toContain('year_sort < 2022');
      expect(sql).toContain('year_sort IS NOT NULL');
    });

    it('flips comparators for backward DESC', () => {
      const sql = buildKeysetWhere([{ sqlIdent: 'year_sort', direction: 'desc' }], [2022], 'b');
      expect(sql).toContain('year_sort > 2022');
      expect(sql).toContain('year_sort IS NULL');
    });
  });

  describe('multi-column mixed direction', () => {
    it('builds an OR-ladder with equality prefixes for ASC then DESC', () => {
      const cols: KeysetSortColumn[] = [
        { sqlIdent: 'year_sort', direction: 'asc' },
        { sqlIdent: 'area_sort', direction: 'desc' }
      ];
      const sql = buildKeysetWhere(cols, [2022, 'Wales'], 'f');

      expect(sql).toContain('year_sort > 2022');
      expect(sql).toContain('year_sort IS NOT DISTINCT FROM 2022');
      expect(sql).toContain(`area_sort < 'Wales'`);
    });

    it('produces N rungs joined by top-level OR for N sort columns', () => {
      const cols: KeysetSortColumn[] = [
        { sqlIdent: 'a', direction: 'asc' },
        { sqlIdent: 'b', direction: 'asc' },
        { sqlIdent: 'c', direction: 'asc' }
      ];
      const sql = buildKeysetWhere(cols, [1, 2, 3], 'f');
      // Each rung after the first should mention the equality of all preceding
      // columns; 3 columns → 3 rungs → 2 IS NOT DISTINCT FROM equality clauses
      // appearing in the prefixes for rungs 2 and 3.
      expect(sql).toContain('a IS NOT DISTINCT FROM 1');
      expect(sql).toContain('b IS NOT DISTINCT FROM 2');
      expect(sql).toContain('c > 3');
    });
  });

  describe('NULL handling', () => {
    it('serialises a NULL key value as the literal NULL', () => {
      const sql = buildKeysetWhere([{ sqlIdent: 'col', direction: 'asc' }], [null], 'f');
      expect(sql).toContain('col IS NULL AND NULL IS NOT NULL');
    });

    it('uses IS NOT DISTINCT FROM for equality, so NULL = NULL holds', () => {
      const sql = buildKeysetWhere(
        [
          { sqlIdent: 'a', direction: 'asc' },
          { sqlIdent: 'b', direction: 'asc' }
        ],
        [null, 5],
        'f'
      );
      expect(sql).toContain('a IS NOT DISTINCT FROM NULL');
    });
  });

  describe('identifier escaping', () => {
    it('escapes identifiers that need quoting via pgformat %I', () => {
      // mixed-case forces pgformat to wrap in double quotes
      const sql = buildKeysetWhere([{ sqlIdent: 'Weird Col', direction: 'asc' }], ['x'], 'f');
      expect(sql).toContain(`"Weird Col"`);
    });

    it('escapes string literals with embedded quotes via pgformat %L', () => {
      const sql = buildKeysetWhere([{ sqlIdent: 'col', direction: 'asc' }], [`O'Hara`], 'f');
      expect(sql).toContain(`'O''Hara'`);
    });
  });

  describe('error cases', () => {
    it('throws when no columns supplied', () => {
      expect(() => buildKeysetWhere([], [], 'f')).toThrow(/at least one sort column/);
    });

    it('throws when key arity does not match column count', () => {
      expect(() =>
        buildKeysetWhere(
          [
            { sqlIdent: 'a', direction: 'asc' },
            { sqlIdent: 'b', direction: 'asc' }
          ],
          [1],
          'f'
        )
      ).toThrow(/length must match/);
    });
  });
});
