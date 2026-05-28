import { FactTableColumn } from '../../../src/entities/dataset/fact-table-column';
import { FactTableColumnType } from '../../../src/enums/fact-table-column-type';
import { FactTableToDimensionName } from '../../../src/interfaces/fact-table-column-to-dimension-name';
import { resolveDefaultSort } from '../../../src/services/default-sort-resolver';

function col(columnName: string, columnType: FactTableColumnType, columnIndex: number): FactTableColumn {
  return { columnName, columnType, columnIndex } as FactTableColumn;
}

function mapping(fact: string, name: string, language: string): FactTableToDimensionName {
  return { fact_table_column: fact, dimension_name: name, language };
}

describe('resolveDefaultSort', () => {
  it('returns [] when factTable is null or empty', () => {
    expect(resolveDefaultSort(null, [], 'en-GB')).toEqual([]);
    expect(resolveDefaultSort([], [], 'en-GB')).toEqual([]);
  });

  it('returns [] when there is no columnMapping for the requested language', () => {
    const ft = [col('year_code', FactTableColumnType.Time, 0)];
    const cm = [mapping('year_code', 'Year', 'cy-gb')];
    expect(resolveDefaultSort(ft, cm, 'en-GB')).toEqual([]);
  });

  it('picks the first Time column as primary sort', () => {
    const ft = [
      col('area_code', FactTableColumnType.Dimension, 0),
      col('year_code', FactTableColumnType.Time, 1),
      col('measure_code', FactTableColumnType.Measure, 2),
      col('data', FactTableColumnType.DataValues, 3)
    ];
    const cm = [
      mapping('area_code', 'Area', 'en-gb'),
      mapping('year_code', 'Year', 'en-gb'),
      mapping('measure_code', 'Measure', 'en-gb')
    ];
    const result = resolveDefaultSort(ft, cm, 'en-GB');
    expect(result[0]).toEqual({ columnName: 'Year', direction: 'asc' });
  });

  it('falls back to the first Dimension when no Time column exists', () => {
    const ft = [col('area_code', FactTableColumnType.Dimension, 0), col('country', FactTableColumnType.Dimension, 1)];
    const cm = [mapping('area_code', 'Area', 'en-gb'), mapping('country', 'Country', 'en-gb')];
    const result = resolveDefaultSort(ft, cm, 'en-GB');
    expect(result[0]).toEqual({ columnName: 'Area', direction: 'asc' });
  });

  it('appends remaining PK columns as ASC tie-breakers, de-duping the primary', () => {
    const ft = [
      col('area_code', FactTableColumnType.Dimension, 0),
      col('year_code', FactTableColumnType.Time, 1),
      col('measure_code', FactTableColumnType.Measure, 2)
    ];
    const cm = [
      mapping('area_code', 'Area', 'en-gb'),
      mapping('year_code', 'Year', 'en-gb'),
      mapping('measure_code', 'Measure', 'en-gb')
    ];
    const result = resolveDefaultSort(ft, cm, 'en-GB');
    expect(result).toEqual([
      { columnName: 'Year', direction: 'asc' },
      { columnName: 'Area', direction: 'asc' },
      { columnName: 'Measure', direction: 'asc' }
    ]);
  });

  it('skips non-PK column types (DataValues, NoteCodes, Ignore, etc.)', () => {
    const ft = [
      col('year_code', FactTableColumnType.Time, 0),
      col('data', FactTableColumnType.DataValues, 1),
      col('notes', FactTableColumnType.NoteCodes, 2),
      col('ignored', FactTableColumnType.Ignore, 3),
      col('area_code', FactTableColumnType.Dimension, 4)
    ];
    const cm = [
      mapping('year_code', 'Year', 'en-gb'),
      mapping('data', 'Data', 'en-gb'),
      mapping('notes', 'Notes', 'en-gb'),
      mapping('area_code', 'Area', 'en-gb')
    ];
    const result = resolveDefaultSort(ft, cm, 'en-GB');
    expect(result.map((r) => r.columnName)).toEqual(['Year', 'Area']);
  });

  it('uses the translated names for Welsh', () => {
    const ft = [col('area_code', FactTableColumnType.Dimension, 0), col('year_code', FactTableColumnType.Time, 1)];
    const cm = [
      mapping('area_code', 'Area', 'en-gb'),
      mapping('area_code', 'Ardal', 'cy-gb'),
      mapping('year_code', 'Year', 'en-gb'),
      mapping('year_code', 'Blwyddyn', 'cy-gb')
    ];
    const result = resolveDefaultSort(ft, cm, 'cy-GB');
    expect(result.map((r) => r.columnName)).toEqual(['Blwyddyn', 'Ardal']);
  });

  it('skips PK columns that lack a mapping for the requested language', () => {
    const ft = [
      col('year_code', FactTableColumnType.Time, 0),
      col('area_code', FactTableColumnType.Dimension, 1),
      col('measure_code', FactTableColumnType.Measure, 2)
    ];
    // measure_code has no English mapping → must be omitted
    const cm = [mapping('year_code', 'Year', 'en-gb'), mapping('area_code', 'Area', 'en-gb')];
    const result = resolveDefaultSort(ft, cm, 'en-GB');
    expect(result.map((r) => r.columnName)).toEqual(['Year', 'Area']);
  });

  it('respects columnIndex order when picking the primary', () => {
    const ft = [col('area_code', FactTableColumnType.Dimension, 1), col('country', FactTableColumnType.Dimension, 0)];
    const cm = [mapping('area_code', 'Area', 'en-gb'), mapping('country', 'Country', 'en-gb')];
    const result = resolveDefaultSort(ft, cm, 'en-GB');
    expect(result[0].columnName).toBe('Country');
  });
});
