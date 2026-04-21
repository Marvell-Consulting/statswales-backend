import { Duration, add, parseISO, sub } from 'date-fns';
import { TZDate } from '@date-fns/tz';

import { YearType } from '../../../src/enums/year-type';
import { createDatePeriodTableQuery, dateDimensionReferenceTableCreator } from '../../../src/services/date-matching';
import { DateExtractor } from '../../../src/extractors/date-extractor';

// Helper to create a UTC TZDate, matching production code behaviour.
function utc(iso: string): TZDate {
  return new TZDate(parseISO(iso), 'UTC');
}
import { FactTableColumn } from '../../../src/entities/dataset/fact-table-column';

jest.mock('../../../src/utils/logger', () => ({
  logger: { debug: jest.fn(), error: jest.fn(), info: jest.fn(), warn: jest.fn() }
}));

describe('date-matching', () => {
  describe('createDatePeriodTableQuery', () => {
    const col = { columnName: 'date_code', columnDatatype: 'VARCHAR' } as unknown as FactTableColumn;

    test('generates CREATE TABLE SQL with the correct schema and table', () => {
      const sql = createDatePeriodTableQuery(col, 'public', 'date_lookup');
      expect(sql).toContain('CREATE TABLE');
      expect(sql).toContain('public');
      expect(sql).toContain('date_lookup');
    });

    test('includes the fact table column name and datatype', () => {
      const sql = createDatePeriodTableQuery(col, 'public', 'date_lookup');
      expect(sql).toContain('date_code');
      expect(sql).toContain('VARCHAR');
    });

    test('includes required fixed columns with correct types', () => {
      const sql = createDatePeriodTableQuery(col, 'public', 'date_lookup');
      expect(sql).toContain('language VARCHAR(5)');
      expect(sql).toContain('start_date DATE');
      expect(sql).toContain('end_date DATE');
      expect(sql).toContain('sort_order BIGINT');
    });

    test('hierarchy column uses the same datatype as the fact table column', () => {
      const sql = createDatePeriodTableQuery(col, 'public', 'date_lookup');
      expect(sql).toContain('hierarchy VARCHAR');
    });

    test('uses a different datatype for column and hierarchy when specified', () => {
      const bigintCol = {
        columnName: 'period_id',
        columnDatatype: 'BIGINT'
      } as unknown as FactTableColumn;
      const sql = createDatePeriodTableQuery(bigintCol, 'myschema', 'lookup');
      expect(sql).toContain('period_id');
      expect(sql).toContain('myschema');
      expect(sql).toContain('lookup');
      expect(sql).toContain('hierarchy BIGINT');
    });
  });

  describe('dateDimensionReferenceTableCreator - year format variants', () => {
    // YYYYYY format is already covered in dimension-processor.test.ts

    test('YYYYYYYY format produces correct date codes', () => {
      const extractor: DateExtractor = { type: YearType.Financial, yearFormat: 'YYYYYYYY' };
      const result = dateDimensionReferenceTableCreator(extractor, [{ dateCode: '20232024' }]);
      expect(result.length).toBe(2);
      expect(result[0].dateCode).toBe('20232024');
    });

    test('YYYY/YYYY format produces correct date codes', () => {
      const extractor: DateExtractor = { type: YearType.Financial, yearFormat: 'YYYY/YYYY' };
      const result = dateDimensionReferenceTableCreator(extractor, [{ dateCode: '2023/2024' }]);
      expect(result.length).toBe(2);
      expect(result[0].dateCode).toBe('2023/2024');
    });

    test('YYYY-YYYY format produces correct date codes', () => {
      const extractor: DateExtractor = { type: YearType.Financial, yearFormat: 'YYYY-YYYY' };
      const result = dateDimensionReferenceTableCreator(extractor, [{ dateCode: '2023-2024' }]);
      expect(result.length).toBe(2);
      expect(result[0].dateCode).toBe('2023-2024');
    });

    test('YYYY/YY format produces correct date codes', () => {
      const extractor: DateExtractor = { type: YearType.Financial, yearFormat: 'YYYY/YY' };
      const result = dateDimensionReferenceTableCreator(extractor, [{ dateCode: '2023/24' }]);
      expect(result.length).toBe(2);
      expect(result[0].dateCode).toBe('2023/24');
    });

    test('YYYY-YY format produces correct date codes', () => {
      const extractor: DateExtractor = { type: YearType.Financial, yearFormat: 'YYYY-YY' };
      const result = dateDimensionReferenceTableCreator(extractor, [{ dateCode: '2023-24' }]);
      expect(result.length).toBe(2);
      expect(result[0].dateCode).toBe('2023-24');
    });

    test('YYYY format (Calendar) produces correct date codes', () => {
      const extractor: DateExtractor = { type: YearType.Calendar, yearFormat: 'YYYY' };
      const result = dateDimensionReferenceTableCreator(extractor, [{ dateCode: '2023' }]);
      expect(result.length).toBe(2);
      expect(result[0].dateCode).toBe('2023');
    });
  });

  describe('dateDimensionReferenceTableCreator - year type start dates', () => {
    test('Calendar year starts Jan 1 and ends Dec 31', () => {
      const extractor: DateExtractor = { type: YearType.Calendar, yearFormat: 'YYYY' };
      const result = dateDimensionReferenceTableCreator(extractor, [{ dateCode: '2023' }]);
      expect(result[0].start).toEqual(utc('2023-01-01T00:00:00Z'));
      expect(result[0].end).toEqual(utc('2023-12-31T00:00:00Z'));
    });

    test('Financial year starts Apr 1 and ends Mar 31', () => {
      const extractor: DateExtractor = { type: YearType.Financial, yearFormat: 'YYYYYY' };
      const result = dateDimensionReferenceTableCreator(extractor, [{ dateCode: '202324' }]);
      expect(result[0].start).toEqual(utc('2023-04-01T00:00:00Z'));
      expect(result[0].end).toEqual(utc('2024-03-31T00:00:00Z'));
    });

    test('Tax year starts Apr 6 and ends Apr 5', () => {
      const extractor: DateExtractor = { type: YearType.Tax, yearFormat: 'YYYYYY' };
      const result = dateDimensionReferenceTableCreator(extractor, [{ dateCode: '202324' }]);
      expect(result[0].start).toEqual(utc('2023-04-06T00:00:00Z'));
      expect(result[0].end).toEqual(utc('2024-04-05T00:00:00Z'));
    });

    test('Academic year starts Sep 1 and ends Aug 31', () => {
      const extractor: DateExtractor = { type: YearType.Academic, yearFormat: 'YYYY/YY' };
      const result = dateDimensionReferenceTableCreator(extractor, [{ dateCode: '2023/24' }]);
      expect(result[0].start).toEqual(utc('2023-09-01T00:00:00Z'));
      expect(result[0].end).toEqual(utc('2024-08-31T00:00:00Z'));
    });

    test('Higher Academic year starts Aug 1 and ends Jul 31', () => {
      const extractor: DateExtractor = { type: YearType.HigherAcademic, yearFormat: 'YYYY/YY' };
      const result = dateDimensionReferenceTableCreator(extractor, [{ dateCode: '2023/24' }]);
      expect(result[0].start).toEqual(utc('2023-08-01T00:00:00Z'));
      expect(result[0].end).toEqual(utc('2024-07-31T00:00:00Z'));
    });

    test('Meteorological year starts Mar 1 and ends Feb 28/29', () => {
      const extractor: DateExtractor = { type: YearType.Meteorological, yearFormat: 'YYYY-YY' };
      const result = dateDimensionReferenceTableCreator(extractor, [{ dateCode: '2023-24' }]);
      expect(result[0].start).toEqual(utc('2023-03-01T00:00:00Z'));
      expect(result[0].end).toEqual(utc('2024-02-29T00:00:00Z'));
    });

    test('Rolling year uses custom startDay and startMonth', () => {
      const extractor: DateExtractor = {
        type: YearType.Rolling,
        yearFormat: 'YYYYYY',
        startDay: 15,
        startMonth: 6
      };
      const result = dateDimensionReferenceTableCreator(extractor, [{ dateCode: '202324' }]);
      expect(result[0].start).toEqual(utc('2023-06-15T00:00:00Z'));
      expect(result[0].end).toEqual(utc('2024-06-14T00:00:00Z'));
    });
  });

  describe('dateDimensionReferenceTableCreator - quarter format variants', () => {
    // QX format is already covered in dimension-processor.test.ts

    test('-QX format produces correct codes', () => {
      const extractor: DateExtractor = {
        type: YearType.Financial,
        yearFormat: 'YYYYYY',
        quarterFormat: '-QX'
      };
      const result = dateDimensionReferenceTableCreator(extractor, [{ dateCode: '202324' }, { dateCode: '202324-Q1' }]);
      const q1Items = result.filter((item) => item.dateCode === '202324-Q1');
      expect(q1Items.length).toBe(2);
      expect(q1Items[0].dateCode).toBe('202324-Q1');
    });

    test('_QX format produces correct codes', () => {
      const extractor: DateExtractor = {
        type: YearType.Financial,
        yearFormat: 'YYYYYY',
        quarterFormat: '_QX'
      };
      const result = dateDimensionReferenceTableCreator(extractor, [{ dateCode: '202324' }, { dateCode: '202324_Q1' }]);
      const q1Items = result.filter((item) => item.dateCode === '202324_Q1');
      expect(q1Items.length).toBe(2);
      expect(q1Items[0].dateCode).toBe('202324_Q1');
    });

    test('X format produces correct codes', () => {
      const extractor: DateExtractor = {
        type: YearType.Financial,
        yearFormat: 'YYYYYY',
        quarterFormat: 'X'
      };
      const result = dateDimensionReferenceTableCreator(extractor, [{ dateCode: '202324' }, { dateCode: '2023241' }]);
      const q1Items = result.filter((item) => item.dateCode === '2023241');
      expect(q1Items.length).toBe(2);
      expect(q1Items[0].dateCode).toBe('2023241');
    });

    test('_X format produces correct codes', () => {
      const extractor: DateExtractor = {
        type: YearType.Financial,
        yearFormat: 'YYYYYY',
        quarterFormat: '_X'
      };
      const result = dateDimensionReferenceTableCreator(extractor, [{ dateCode: '202324' }, { dateCode: '202324_1' }]);
      const q1Items = result.filter((item) => item.dateCode === '202324_1');
      expect(q1Items.length).toBe(2);
      expect(q1Items[0].dateCode).toBe('202324_1');
    });

    test('-X format produces correct codes', () => {
      const extractor: DateExtractor = {
        type: YearType.Financial,
        yearFormat: 'YYYYYY',
        quarterFormat: '-X'
      };
      const result = dateDimensionReferenceTableCreator(extractor, [{ dateCode: '202324' }, { dateCode: '202324-1' }]);
      const q1Items = result.filter((item) => item.dateCode === '202324-1');
      expect(q1Items.length).toBe(2);
      expect(q1Items[0].dateCode).toBe('202324-1');
    });
  });

  describe('dateDimensionReferenceTableCreator - month format variants', () => {
    // MMM format is already covered in dimension-processor.test.ts

    test('mMM format produces correct codes (e.g. "202324m04" for April)', () => {
      const extractor: DateExtractor = {
        type: YearType.Financial,
        yearFormat: 'YYYYYY',
        monthFormat: 'mMM'
      };
      const result = dateDimensionReferenceTableCreator(extractor, [{ dateCode: '202324' }, { dateCode: '202324m04' }]);
      const aprItems = result.filter((item) => item.dateCode === '202324m04');
      expect(aprItems.length).toBe(2);
      expect(aprItems[0].dateCode).toBe('202324m04');
    });

    test('mm format produces correct codes (e.g. "20232404" for April)', () => {
      const extractor: DateExtractor = {
        type: YearType.Financial,
        yearFormat: 'YYYYYY',
        monthFormat: 'mm'
      };
      const result = dateDimensionReferenceTableCreator(extractor, [{ dateCode: '202324' }, { dateCode: '20232404' }]);
      const aprItems = result.filter((item) => item.dateCode === '20232404');
      expect(aprItems.length).toBe(2);
      expect(aprItems[0].dateCode).toBe('20232404');
    });
  });

  describe('dateDimensionReferenceTableCreator - point-in-time format variants', () => {
    // dd/MM/yyyy and yyyyMMdd are already covered in dimension-processor.test.ts

    test('dd-MM-yyyy format parses correctly', () => {
      const extractor: DateExtractor = { type: YearType.PointInTime, dateFormat: 'dd-MM-yyyy' };
      const result = dateDimensionReferenceTableCreator(extractor, [{ dateCode: '01-12-2023' }]);
      expect(result.length).toBe(2);
      expect(result[0].dateCode).toBe('01-12-2023');
      expect(result[0].type).toBe('specific_day');
      expect(result[0].start).toEqual(utc('2023-12-01T00:00:00Z'));
    });

    test('yyyy-MM-dd format parses correctly', () => {
      const extractor: DateExtractor = { type: YearType.PointInTime, dateFormat: 'yyyy-MM-dd' };
      const result = dateDimensionReferenceTableCreator(extractor, [{ dateCode: '2023-12-01' }]);
      expect(result.length).toBe(2);
      expect(result[0].dateCode).toBe('2023-12-01');
      expect(result[0].type).toBe('specific_day');
    });

    test('invalid date value throws an error', () => {
      const extractor: DateExtractor = { type: YearType.PointInTime, dateFormat: 'dd/MM/yyyy' };
      expect(() => {
        dateDimensionReferenceTableCreator(extractor, [{ dateCode: 'not-a-date' }]);
      }).toThrow('Unable to parse date based on supplied format of dd/MM/yyyy.');
    });
  });

  describe('dateDimensionReferenceTableCreator - rolling / period-ending dates', () => {
    const extractor: DateExtractor = { type: YearType.Rolling, dateFormat: 'dd/MM/yyyy' };
    const endDateStr = '31/03/2024';
    const parsedEndDate = utc('2024-03-31T00:00:00Z');

    function expectedDates(increment: Duration) {
      return {
        start: sub(add(parsedEndDate, { days: 1 }), increment),
        end: parsedEndDate
      };
    }

    test('YE (year ending) produces correct start/end span of ~1 year', () => {
      const result = dateDimensionReferenceTableCreator(extractor, [{ dateCode: `YE${endDateStr}` }]);
      const { start, end } = expectedDates({ years: 1 });
      expect(result[0].dateCode).toBe('YE31/03/2024');
      expect(result[0].start).toEqual(start);
      expect(result[0].end).toEqual(end);
    });

    test('ME (month ending) produces correct start/end span of ~1 month', () => {
      const result = dateDimensionReferenceTableCreator(extractor, [{ dateCode: `ME${endDateStr}` }]);
      const { start, end } = expectedDates({ months: 1 });
      expect(result[0].start).toEqual(start);
      expect(result[0].end).toEqual(end);
    });

    test('QE (quarter ending) produces correct start/end span of ~3 months', () => {
      const result = dateDimensionReferenceTableCreator(extractor, [{ dateCode: `QE${endDateStr}` }]);
      const { start, end } = expectedDates({ months: 3 });
      expect(result[0].start).toEqual(start);
      expect(result[0].end).toEqual(end);
    });

    test('HE (half-year ending) produces correct start/end span of ~6 months', () => {
      const result = dateDimensionReferenceTableCreator(extractor, [{ dateCode: `HE${endDateStr}` }]);
      const { start, end } = expectedDates({ months: 6 });
      expect(result[0].start).toEqual(start);
      expect(result[0].end).toEqual(end);
    });

    test('FE (fortnight ending) produces correct start/end span of 2 weeks', () => {
      const result = dateDimensionReferenceTableCreator(extractor, [{ dateCode: `FE${endDateStr}` }]);
      const { start, end } = expectedDates({ weeks: 2 });
      expect(result[0].start).toEqual(start);
      expect(result[0].end).toEqual(end);
    });

    test('WE (week ending) produces correct start/end span of 1 week', () => {
      const result = dateDimensionReferenceTableCreator(extractor, [{ dateCode: `WE${endDateStr}` }]);
      const { start, end } = expectedDates({ weeks: 1 });
      expect(result[0].start).toEqual(start);
      expect(result[0].end).toEqual(end);
    });

    test('1Y (1-year rolling ending) produces correct span', () => {
      const result = dateDimensionReferenceTableCreator(extractor, [{ dateCode: `1Y${endDateStr}` }]);
      const { start, end } = expectedDates({ years: 1 });
      expect(result[0].start).toEqual(start);
      expect(result[0].end).toEqual(end);
    });

    test('2Y (2-year rolling ending) produces correct span', () => {
      const result = dateDimensionReferenceTableCreator(extractor, [{ dateCode: `2Y${endDateStr}` }]);
      const { start, end } = expectedDates({ years: 2 });
      expect(result[0].start).toEqual(start);
      expect(result[0].end).toEqual(end);
    });

    test('5Y (5-year rolling ending) produces correct span', () => {
      const result = dateDimensionReferenceTableCreator(extractor, [{ dateCode: `5Y${endDateStr}` }]);
      const { start, end } = expectedDates({ years: 5 });
      expect(result[0].start).toEqual(start);
      expect(result[0].end).toEqual(end);
    });

    test('XY (10-year rolling ending) produces correct span', () => {
      const result = dateDimensionReferenceTableCreator(extractor, [{ dateCode: `XY${endDateStr}` }]);
      const { start, end } = expectedDates({ years: 10 });
      expect(result[0].start).toEqual(start);
      expect(result[0].end).toEqual(end);
    });

    test('unknown rolling type throws an error', () => {
      expect(() => {
        dateDimensionReferenceTableCreator(extractor, [{ dateCode: `ZE${endDateStr}` }]);
      }).toThrow('Unable to parse date based on supplied format.');
    });
  });

  describe('dateDimensionReferenceTableCreator - hierarchy linking', () => {
    test('quarter entries link to their parent year code', () => {
      const extractor: DateExtractor = {
        type: YearType.Financial,
        yearFormat: 'YYYYYY',
        quarterFormat: 'QX'
      };
      const result = dateDimensionReferenceTableCreator(extractor, [{ dateCode: '202324' }, { dateCode: '202324Q1' }]);
      const q1 = result.find((item) => item.dateCode === '202324Q1');
      expect(q1?.hierarchy).toBe('202324');
    });

    test('month entries with quarters link to parent quarter code', () => {
      const extractor: DateExtractor = {
        type: YearType.Financial,
        yearFormat: 'YYYYYY',
        quarterFormat: 'QX',
        monthFormat: 'MMM'
      };
      const result = dateDimensionReferenceTableCreator(extractor, [
        { dateCode: '202324' },
        { dateCode: '202324Q1' },
        { dateCode: '202324Apr' }
      ]);
      const apr = result.find((item) => item.dateCode === '202324Apr');
      expect(apr?.hierarchy).toBe('202324Q1');
    });

    test('month entries without quarters link to parent year code', () => {
      const extractor: DateExtractor = {
        type: YearType.Financial,
        yearFormat: 'YYYYYY',
        monthFormat: 'MMM'
      };
      const result = dateDimensionReferenceTableCreator(extractor, [{ dateCode: '202324' }, { dateCode: '202324Apr' }]);
      const apr = result.find((item) => item.dateCode === '202324Apr');
      expect(apr?.hierarchy).toBe('202324');
    });

    test('hierarchy is set to null if parent code is not in data column', () => {
      const extractor: DateExtractor = {
        type: YearType.Financial,
        yearFormat: 'YYYYYY',
        quarterFormat: 'QX'
      };
      // Intentionally omit the parent year code '202324'
      const result = dateDimensionReferenceTableCreator(extractor, [{ dateCode: '202324Q1' }]);
      const q1 = result.find((item) => item.dateCode === '202324Q1');
      expect(q1?.hierarchy).toBeNull();
    });
  });

  describe('dateDimensionReferenceTableCreator - timezone independence', () => {
    const originalTZ = process.env.TZ;

    afterEach(() => {
      if (originalTZ === undefined) {
        delete process.env.TZ;
      } else {
        process.env.TZ = originalTZ;
      }
    });

    test.each(['America/New_York', 'Asia/Kolkata', 'Pacific/Auckland'])(
      'Financial year dates are identical under TZ=%s',
      (tz) => {
        process.env.TZ = tz;
        const extractor: DateExtractor = { type: YearType.Financial, yearFormat: 'YYYYYY' };
        const result = dateDimensionReferenceTableCreator(extractor, [{ dateCode: '202324' }]);
        expect(result[0].start.getTime()).toBe(Date.UTC(2023, 3, 1));
        expect(result[0].end.getTime()).toBe(Date.UTC(2024, 2, 31));
      }
    );

    test.each(['America/New_York', 'Asia/Kolkata', 'Pacific/Auckland'])(
      'Point-in-time dates are identical under TZ=%s',
      (tz) => {
        process.env.TZ = tz;
        const extractor: DateExtractor = { type: YearType.PointInTime, dateFormat: 'dd/MM/yyyy' };
        const result = dateDimensionReferenceTableCreator(extractor, [{ dateCode: '01/04/2023' }]);
        expect(result[0].start.getTime()).toBe(Date.UTC(2023, 3, 1));
        expect(result[0].end.getTime()).toBe(Date.UTC(2023, 3, 1));
      }
    );

    test.each(['America/New_York', 'Asia/Kolkata', 'Pacific/Auckland'])(
      'Rolling period-ending dates are identical under TZ=%s',
      (tz) => {
        process.env.TZ = tz;
        const extractor: DateExtractor = { type: YearType.Rolling, dateFormat: 'dd/MM/yyyy' };
        const result = dateDimensionReferenceTableCreator(extractor, [{ dateCode: 'YE31/03/2024' }]);
        expect(result[0].start.getTime()).toBe(Date.UTC(2023, 3, 1));
        expect(result[0].end.getTime()).toBe(Date.UTC(2024, 2, 31));
      }
    );
  });
});
