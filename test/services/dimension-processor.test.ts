import fs from 'fs';

import { add, parseISO, sub } from 'date-fns';

import { logger } from '../../src/utils/logger';
import { YearType } from '../../src/enums/year-type';
import { dateDimensionReferenceTableCreator } from '../../src/services/time-matching';
import { DateExtractor } from '../../src/extractors/date-extractor';

interface AllFormats {
  years: DateExtractor[];
  quarters: DateExtractor[];
  months: DateExtractor[];
  specific: DateExtractor[];
}

const formatsDict: AllFormats = {
  years: [
    { type: YearType.Financial, yearFormat: 'YYYYYYYY' },
    { type: YearType.Financial, yearFormat: 'YYYY/YYYY' },
    { type: YearType.Financial, yearFormat: 'YYYY-YYYY' },
    { type: YearType.Financial, yearFormat: 'YYYY/YY' },
    { type: YearType.Financial, yearFormat: 'YYYY-YY' },
    { type: YearType.Calendar, yearFormat: 'YYYY' },
    { type: YearType.Financial, yearFormat: 'YYYYYY' }
  ],
  quarters: [
    { quarterFormat: 'QX' },
    { quarterFormat: '_QX' },
    { quarterFormat: 'X' },
    { quarterFormat: '_X' },
    { quarterFormat: '-X' }
  ],
  months: [{ monthFormat: 'MMM' }, { monthFormat: 'mMM' }, { monthFormat: 'mm' }],
  specific: [
    { type: YearType.PointInTime, dateFormat: 'dd/MM/yyyy' },
    { type: YearType.PointInTime, dateFormat: 'dd/MM/yyyy hh:mm:ss' },
    { type: YearType.PointInTime, dateFormat: 'dd-MM-yyyy' },
    { type: YearType.PointInTime, dateFormat: 'yyyy-MM-dd' },
    { type: YearType.PointInTime, dateFormat: 'yyyyMMdd' }
  ]
};

function generateAllSampleCSVs(filePath: string) {
  const dateColumn: any[] = [2019, 2020, 2021, 2022, 2023, 2024];
  // Generate year files
  formatsDict.years.forEach((year, index) => {
    let csv = 'DateCode,Data\n';
    const refTable = dateDimensionReferenceTableCreator(year, dateColumn);
    refTable.map((row) => {
      csv = `${csv}${row.dateCode},${Math.random()}\n`;
    });
    const fileName = `${filePath}/year-format-${index}`;
    fs.writeFileSync(`${fileName}.json`, JSON.stringify(year));
    fs.writeFileSync(`${fileName}.csv`, csv);
  });

  // Generate month files (Months always have yearly totals)
  formatsDict.years.forEach((year, yIndex) => {
    formatsDict.months.forEach((month, mIndex) => {
      const ext = {
        type: year.type,
        yearFormat: year.yearFormat,
        monthFormat: month.monthFormat
      };
      logger.debug(`Creating specific month table based on: ${JSON.stringify(ext, null, 2)}`);
      const refTable = dateDimensionReferenceTableCreator(ext, dateColumn);
      let csv = 'DateCode,Data\n';
      refTable.map((row) => {
        csv = `${csv}${row.dateCode},${Math.random()}\n`;
      });
      const fileName = `${filePath}/month-format-m${mIndex}-y${yIndex}`;
      fs.writeFileSync(`${fileName}.json`, JSON.stringify(ext));
      fs.writeFileSync(`${fileName}.csv`, csv);
    });
  });

  // Generate quarter files (Quarter files always have yearly totals)
  formatsDict.years.forEach((year, yIndex) => {
    formatsDict.quarters.forEach((quarter, qIndex) => {
      const ext = {
        type: year.type,
        yearFormat: year.yearFormat,
        quarterFormat: quarter.quarterFormat
      };
      const refTable = dateDimensionReferenceTableCreator(ext, dateColumn);
      let csv = 'DateCode,Data\n';
      refTable.map((row) => {
        csv = `${csv}${row.dateCode},${Math.random()}\n`;
      });
      const fileName = `${filePath}/quarter-format-q${qIndex}-y${yIndex}`;
      fs.writeFileSync(`${fileName}.json`, JSON.stringify(ext));
      fs.writeFileSync(`${fileName}.csv`, csv);
    });
  });

  // Generate quarter files with magic 5th quarter
  formatsDict.years.forEach((year, yIndex) => {
    formatsDict.quarters.forEach((quarter, qIndex) => {
      const ext = {
        type: year.type,
        yearFormat: year.yearFormat,
        quarterFormat: quarter.quarterFormat,
        quarterTotalIsFifthQuart: true
      };
      const refTable = dateDimensionReferenceTableCreator(ext, dateColumn);
      let csv = 'DateCode,Data\n';
      refTable.map((row) => {
        csv = `${csv}${row.dateCode},${Math.random()}\n`;
      });
      const fileName = `${filePath}/magic-quarter-format-q${qIndex}-y${yIndex}`;
      fs.writeFileSync(`${fileName}.json`, JSON.stringify(ext));
      fs.writeFileSync(`${fileName}.csv`, csv);
    });
  });

  // Generate month files with quarterly totals
  formatsDict.months.forEach((month, mIndex) => {
    formatsDict.quarters.forEach((quarter, qIndex) => {
      formatsDict.years.forEach((year, yIndex) => {
        const ext = {
          type: year.type,
          yearFormat: year.yearFormat,
          quarterFormat: quarter.quarterFormat,
          monthFormat: month.monthFormat
        };
        const refTable = dateDimensionReferenceTableCreator(ext, dateColumn);
        let csv = 'DateCode,Data\n';
        refTable.map((row) => {
          csv = `${csv}${row.dateCode},${Math.random()}\n`;
        });
        const fileName = `${filePath}/month-formats-with-totals-y${yIndex}-q${qIndex}-m${mIndex}`;
        fs.writeFileSync(`${fileName}.json`, JSON.stringify(ext));
        fs.writeFileSync(`${fileName}.csv`, csv);
      });
    });
  });

  // Generate month files with magic 5th quarterly totals
  formatsDict.months.forEach((month, mIndex) => {
    formatsDict.quarters.forEach((quarter, qIndex) => {
      formatsDict.years.forEach((year, yIndex) => {
        const ext = {
          type: year.type,
          yearFormat: year.yearFormat,
          quarterFormat: quarter.quarterFormat,
          quarterTotalIsFifthQuart: true,
          monthFormat: month.monthFormat
        };
        const refTable = dateDimensionReferenceTableCreator(ext, dateColumn);
        let csv = 'DateCode,Data\n';
        refTable.map((row) => {
          csv = `${csv}${row.dateCode},${Math.random()}\n`;
        });
        const fileName = `${filePath}/magic-5th-month-formats-with-totals-y${yIndex}-q${qIndex}-m${mIndex}`;
        fs.writeFileSync(`${fileName}.json`, JSON.stringify(ext));
        fs.writeFileSync(`${fileName}.csv`, csv);
      });
    });
  });
}

describe('Date matching table generation', () => {
  describe('Date Period Matching', () => {
    test('Given a correct year format it returns a valid table', async () => {
      const extractor: DateExtractor = {
        type: YearType.Financial,
        yearFormat: 'YYYYYY'
      };
      const dateColumn: any[] = [{ yearCode: 2023 }];
      const refTable = dateDimensionReferenceTableCreator(extractor, dateColumn);
      expect(refTable.length).toBe(1);
      expect(refTable[0].dateCode).toBe('202324');
    });

    test('Given a correct year and quarter format it returns a valid table', async () => {
      const extractor: DateExtractor = {
        type: YearType.Financial,
        yearFormat: 'YYYYYY',
        quarterFormat: 'QX'
      };
      const dateColumn: any[] = [{ yearCode: 2023 }];
      const refTable = dateDimensionReferenceTableCreator(extractor, dateColumn);
      expect(refTable.length).toBe(5);
      expect(refTable[0].dateCode).toBe('202324');
      expect(refTable[1].dateCode).toBe('202324Q1');
      expect(refTable[2].dateCode).toBe('202324Q2');
      expect(refTable[3].dateCode).toBe('202324Q3');
      expect(refTable[4].dateCode).toBe('202324Q4');
    });

    test('Given a the magic 5th setting and correct year and quarter format it returns a valid table', async () => {
      const extractor: DateExtractor = {
        type: YearType.Financial,
        yearFormat: 'YYYYYY',
        quarterFormat: 'QX',
        quarterTotalIsFifthQuart: true
      };
      const dateColumn: any[] = [{ yearCode: 2023 }];
      const refTable = dateDimensionReferenceTableCreator(extractor, dateColumn);
      expect(refTable.length).toBe(5);
      expect(refTable[0].dateCode).toBe('202324Q1');
      expect(refTable[1].dateCode).toBe('202324Q2');
      expect(refTable[2].dateCode).toBe('202324Q3');
      expect(refTable[3].dateCode).toBe('202324Q4');
      expect(refTable[4].dateCode).toBe('202324Q5');
    });

    test('Given a correct year and month format it returns a valid table', async () => {
      const extractor: DateExtractor = {
        type: YearType.Financial,
        yearFormat: 'YYYYYY',
        monthFormat: 'MMM'
      };
      const dateColumn: any[] = [{ yearCode: 2023 }];
      const refTable = dateDimensionReferenceTableCreator(extractor, dateColumn);
      expect(refTable.length).toBe(13);
      expect(refTable[0].dateCode).toBe('202324');
      expect(refTable[1].dateCode).toBe('202324Apr');
      expect(refTable[2].dateCode).toBe('202324May');
      expect(refTable[3].dateCode).toBe('202324Jun');
      expect(refTable[4].dateCode).toBe('202324Jul');
      expect(refTable[5].dateCode).toBe('202324Aug');
      expect(refTable[6].dateCode).toBe('202324Sep');
      expect(refTable[7].dateCode).toBe('202324Oct');
      expect(refTable[8].dateCode).toBe('202324Nov');
      expect(refTable[9].dateCode).toBe('202324Dec');
      expect(refTable[10].dateCode).toBe('202324Jan');
      expect(refTable[11].dateCode).toBe('202324Feb');
      expect(refTable[12].dateCode).toBe('202324Mar');
    });

    test('If given an unknown year format it errors', async () => {
      const input: any[] = [{ dateCode: '01/12/2023' }];
      const extractor: DateExtractor = {
        type: YearType.Financial,
        yearFormat: 'XXXX'
      };
      try {
        const refTable = dateDimensionReferenceTableCreator(extractor, input);
        expect(refTable).toBe(false);
      } catch (error) {
        expect((error as Error).message).toBe('Unknown year format');
      }
    });

    test('If given an unknown month format it errors', async () => {
      const input: any[] = [{ dateCode: '01/12/2023' }];
      const extractor: DateExtractor = {
        type: YearType.Financial,
        yearFormat: 'YYYY',
        monthFormat: 'XXXX'
      };
      try {
        const refTable = dateDimensionReferenceTableCreator(extractor, input);
        expect(refTable).toBe(false);
      } catch (error) {
        expect((error as Error).message).toBe('Unknown month format');
      }
    });

    test('If given an unknown quarter format it errors', async () => {
      const input: any[] = [{ dateCode: '01/12/2023' }];
      const extractor: DateExtractor = {
        type: YearType.Financial,
        yearFormat: 'YYYY',
        quarterFormat: 'XXXX'
      };
      try {
        const refTable = dateDimensionReferenceTableCreator(extractor, input);
        expect(refTable).toBe(false);
      } catch (error) {
        expect((error as Error).message).toBe('Unknown quarter format');
      }
    });
  });

  describe('Point in time matching table generation', () => {
    test('Given the format "yyyyMMdd" it generates matches for a table', async () => {
      const input: any[] = [
        { dateCode: '20231201' },
        { dateCode: '20240101' },
        { dateCode: '20240201' },
        { dateCode: '20240301' },
        { dateCode: '20240401' },
        { dateCode: '20240501' },
        { dateCode: '20240601' },
        { dateCode: '20240701' },
        { dateCode: '20240801' },
        { dateCode: '20240901' },
        { dateCode: '20241001' },
        { dateCode: '20241101' },
        { dateCode: '20241201' }
      ];
      const extractor: DateExtractor = {
        type: YearType.Financial,
        dateFormat: 'yyyyMMdd'
      };
      const refTable = dateDimensionReferenceTableCreator(extractor, input);
      expect(refTable.length).toBe(13);
      expect(refTable[0].dateCode).toBe('20231201');
      expect(refTable[0].start).toStrictEqual(parseISO('2023-12-01T00:00:00.000Z'));
      expect(refTable[0].end).toStrictEqual(
        sub(add(parseISO('2023-12-01T00:00:00.000Z'), { days: 1 }), { seconds: 1 })
      );
      expect(refTable[0].type).toBe('specific_day');
      expect(refTable[1].dateCode).toBe('20240101');
      expect(refTable[1].start).toStrictEqual(parseISO('2024-01-01T00:00:00.000Z'));
      expect(refTable[1].end).toStrictEqual(
        sub(add(parseISO('2024-01-01T00:00:00.000Z'), { days: 1 }), { seconds: 1 })
      );
      expect(refTable[1].type).toBe('specific_day');
    });

    test('Given the format "dd/MM/yyyy" it generates matches for a table', async () => {
      const input: any[] = [
        { dateCode: '01/12/2023' },
        { dateCode: '01/01/2024' },
        { dateCode: '01/02/2024' },
        { dateCode: '01/03/2024' },
        { dateCode: '01/04/2024' },
        { dateCode: '01/05/2024' },
        { dateCode: '01/06/2024' },
        { dateCode: '01/07/2024' },
        { dateCode: '01/08/2024' },
        { dateCode: '01/09/2024' },
        { dateCode: '01/10/2024' },
        { dateCode: '01/11/2024' },
        { dateCode: '01/12/2024' }
      ];
      const extractor: DateExtractor = {
        type: YearType.Financial,
        dateFormat: 'dd/MM/yyyy'
      };
      const refTable = dateDimensionReferenceTableCreator(extractor, input);
      expect(refTable.length).toBe(13);
      expect(refTable[0].dateCode).toBe('01/12/2023');
      expect(refTable[0].start).toStrictEqual(parseISO('2023-12-01T00:00:00.000Z'));
      expect(refTable[0].end).toStrictEqual(
        sub(add(parseISO('2023-12-01T00:00:00.000Z'), { days: 1 }), { seconds: 1 })
      );
      expect(refTable[0].type).toBe('specific_day');
      expect(refTable[1].dateCode).toBe('01/01/2024');
      expect(refTable[1].start).toStrictEqual(parseISO('2024-01-01T00:00:00.000Z'));
      expect(refTable[1].end).toStrictEqual(
        sub(add(parseISO('2024-01-01T00:00:00.000Z'), { days: 1 }), { seconds: 1 })
      );
      expect(refTable[1].type).toBe('specific_day');
    });

    test('If given an unknown format it errors', async () => {
      const input: any[] = [{ dateCode: '01/12/2023' }];
      const extractor: DateExtractor = {
        type: YearType.Financial,
        dateFormat: 'ddXXyyyy'
      };
      try {
        const refTable = dateDimensionReferenceTableCreator(extractor, input);
        expect(refTable).toBe(false);
      } catch (error) {
        expect((error as Error).message).toBe('Unknown Date Format.  Format given: ddXXyyyy');
      }
    });
  });

  test('Generate sample files for date/time matching', async () => {
    const filePath = `${__dirname}/sample-files/date-period-csv/`;

    if (process.env.generateDatePeriodSamples) {
      generateAllSampleCSVs(filePath);
    }
  });
});
