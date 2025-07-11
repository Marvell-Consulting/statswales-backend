import { add, format, isBefore, isDate, isValid, parse, parseISO, sub } from 'date-fns';
import { TableData } from 'duckdb-async';

import { logger } from '../utils/logger';
import { YearType } from '../enums/year-type';
import { DateExtractor } from '../extractors/date-extractor';
import { SUPPORTED_LOCALES, t } from '../middleware/translation';

export interface SnifferResult {
  extractor: DateExtractor;
  previewTable: TableData;
}

// The following interfaces are all internal to the time matcher
interface YearTypeDetails {
  start: string;
  type: YearType;
}

export interface DateReferenceDataItem {
  dateCode: string;
  lang: string;
  description: string;
  start: Date;
  end: Date;
  type: string;
  hierarchy: string | null;
}

enum GeneratorType {
  Year = 'year',
  Quarter = 'quarter',
  Month = 'month'
}

// Date parsing methods start here
function yearType(type: YearType, startDay = 1, startMonth = 1): YearTypeDetails {
  switch (type) {
    case YearType.Financial:
      return { start: '04-01T01:00:00Z', type: YearType.Financial };
    case YearType.Tax:
      return { start: '04-06T01:00:00Z', type: YearType.Tax };
    case YearType.Academic:
      return { start: '09-01T01:00:00Z', type: YearType.Academic };
    case YearType.Meteorological:
      return { start: '03-01T01:00:00Z', type: YearType.Meteorological };
    case YearType.Calendar:
      return { start: '01-01T01:00:00Z', type: YearType.Calendar };
    default:
      return {
        start: `${String(startMonth).padStart(2, '0')}-${String(startDay).padStart(2, '0')}T01:00:00Z`,
        type: YearType.Rolling
      };
  }
}

interface DateFormat {
  formatStr: string;
  increment: number;
}

function yearFormats(yearFormat: string): DateFormat {
  const increment = 12;
  switch (yearFormat.toUpperCase()) {
    case 'YYYYYYYY':
      return { formatStr: '[full-start][full-end]', increment };
    case 'YYYY/YYYY':
      return { formatStr: '[full-start]/[full-end]', increment };
    case 'YYYY-YYYY':
      return { formatStr: '[full-start]-[full-end]', increment };
    case 'YYYYYY':
      return { formatStr: '[full-start][end-year]', increment };
    case 'YYYY/YY':
      return { formatStr: '[full-start]/[end-year]', increment };
    case 'YYYY-YY':
      return { formatStr: '[full-start]-[end-year]', increment };
    case 'YYYY':
      return { formatStr: '[full-start]', increment };
  }
  throw new Error('Unknown year format');
}

function quarterFormats(quarterFormat: string, yearFormat: string): DateFormat {
  const increment = 3;
  switch (quarterFormat) {
    case 'QX':
      return { increment, formatStr: `${yearFormat}Q[quarterNo]` };
    case '_QX':
      return { increment, formatStr: `${yearFormat}_Q[quarterNo]` };
    case 'X':
      return { increment, formatStr: `${yearFormat}[quarterNo]` };
    case '_X':
      return { increment, formatStr: `${yearFormat}_[quarterNo]` };
    case '-X':
      return { increment, formatStr: `${yearFormat}-[quarterNo]` };
  }
  throw new Error('Unknown quarter format');
}

function monthFormats(monthFormat: string, yearFormat: string): DateFormat {
  const increment = 1;
  switch (monthFormat) {
    case 'MMM':
      return { increment, formatStr: `${yearFormat}[monthStr]` };
    case 'mMM':
      return { increment, formatStr: `${yearFormat}m[monthNo]` };
    case 'mm':
      return { increment, formatStr: `${yearFormat}[monthNo]` };
  }
  throw new Error('Unknown month format');
}

enum ParentType {
  None = 'None',
  Year = 'year',
  Quarter = 'quarter'
}

function createAllTypesOfPeriod(dateFormat: DateExtractor, dataColumn: TableData): DateReferenceDataItem[] {
  let referenceTable: DateReferenceDataItem[] = [];
  logger.debug(`date extractor = ${JSON.stringify(dateFormat)}`);
  if (dateFormat.quarterFormat && dateFormat.quarterTotalIsFifthQuart) {
    logger.debug('5th quarter used to represent whole year totals');
    return periodTableCreator(dateFormat, dataColumn, GeneratorType.Quarter, ParentType.Year);
  } else {
    // We always need years generate those first
    logger.debug('Creating table for year');
    referenceTable = referenceTable.concat(
      periodTableCreator(dateFormat, dataColumn, GeneratorType.Year, ParentType.None)
    );
    let parentType = ParentType.Year;
    if (dateFormat.quarterFormat) parentType = ParentType.Quarter;
    // If monthFormat is present create month entries
    if (dateFormat.monthFormat) {
      logger.debug('Month format present... creating month entries');
      referenceTable = referenceTable.concat(
        periodTableCreator(dateFormat, dataColumn, GeneratorType.Month, parentType)
      );
    }
    // If quarterFormat is present create quarters
    if (dateFormat.quarterFormat) {
      logger.debug('Quarter format present... creating quarter entries');
      referenceTable = referenceTable.concat(
        periodTableCreator(dateFormat, dataColumn, GeneratorType.Quarter, ParentType.Year)
      );
    }
  }
  return referenceTable;
}

function periodTableCreator(
  dateFormat: DateExtractor,
  dataColumn: TableData,
  generationType: GeneratorType,
  parentType: ParentType
): DateReferenceDataItem[] {
  let subType = 'year';
  let formatObj;
  let yearFormat: string = '[full-start]';
  let quarterFormat: string = `${yearFormat}Q[quarterNo]`;
  try {
    yearFormat = yearFormats(dateFormat.yearFormat ? dateFormat.yearFormat : 'YYYY').formatStr;
    formatObj = yearFormats(dateFormat.yearFormat ? dateFormat.yearFormat : 'YYYY');
    if (generationType === GeneratorType.Quarter) {
      formatObj = quarterFormats(dateFormat.quarterFormat!, formatObj.formatStr);
      subType = 'quarter';
    } else if (generationType === GeneratorType.Month) {
      if (dateFormat.quarterFormat) {
        quarterFormat = quarterFormats(dateFormat.quarterFormat!, formatObj.formatStr).formatStr;
      }
      formatObj = monthFormats(dateFormat.monthFormat!, formatObj.formatStr);
      subType = 'month';
    }
  } catch (error) {
    logger.error(`Failed to process data format`);
    throw error;
  }

  const dataYears: number[] = [];
  dataColumn.forEach((row) => {
    dataYears.push(parseInt(String(row).substring(0, 4), 10));
  });
  const startYear = Math.min(...dataYears);
  const endYear = Math.max(...dataYears);
  const type = yearType(dateFormat.type, dateFormat.startDay, dateFormat.startMonth);

  let year = parseISO(`${startYear}-${type.start}`);
  const end = add(parseISO(`${endYear}-${type.start}`), { years: 1 });

  // Quarters and month numbers are different depending on the type of year
  let quarterIndex = 1;
  let monthIndex = 1;

  // Create an array to handle our reference table
  const referenceTable: DateReferenceDataItem[] = [];
  // While loop builds our table we can match against and load into our Cube
  let displayYear = year;
  while (isBefore(year, end)) {
    const monthNo = parseInt(format(year, 'MM'), 10);
    const dateStr = formatObj.formatStr
      .replace('[full-start]', format(displayYear, 'yyyy'))
      .replace('[full-end]', format(add(displayYear, { years: 1 }), 'yyyy'))
      .replace('[end-year]', format(add(displayYear, { years: 1 }), 'yy'))
      .replace('[quarterNo]', quarterIndex.toString())
      .replace('[monthStr]', format(year, 'MMM'))
      .replace('[monthNo]', String(monthNo).padStart(2, '0'));

    let parent: string | null = null;
    if (parentType === ParentType.Year) {
      parent = yearFormat
        .replace('[full-start]', format(displayYear, 'yyyy'))
        .replace('[full-end]', format(add(displayYear, { years: 1 }), 'yyyy'))
        .replace('[end-year]', format(add(displayYear, { years: 1 }), 'yy'));
    }
    if (parentType === ParentType.Quarter) {
      parent = quarterFormat
        .replace('[full-start]', format(displayYear, 'yyyy'))
        .replace('[full-end]', format(add(displayYear, { years: 1 }), 'yyyy'))
        .replace('[end-year]', format(add(displayYear, { years: 1 }), 'yy'))
        .replace('[quarterNo]', quarterIndex.toString());
    }

    for (const locale of SUPPORTED_LOCALES) {
      let description = '';
      switch (generationType) {
        case GeneratorType.Year:
          description =
            dateFormat.type === YearType.Calendar
              ? format(displayYear, 'yyyy')
              : `${format(displayYear, 'yyyy')}-${format(add(displayYear, { years: 1 }), 'yy')}`;
          break;
        case GeneratorType.Quarter:
          description =
            dateFormat.type === YearType.Calendar
              ? `${t('date_format.quarter_abr', { lng: locale })}${quarterIndex} ${format(displayYear, 'yyyy')}`
              : `${t('date_format.quarter_abr', { lng: locale })}${quarterIndex} ${format(displayYear, 'yyyy')}-${format(add(displayYear, { years: 1 }), 'yy')}`;
          break;
        case GeneratorType.Month:
          description = `${t(`months.${monthNo}`, { lng: locale })} ${format(displayYear, 'yyyy')}`;
          break;
      }

      referenceTable.push({
        dateCode: dateStr,
        lang: locale.toLowerCase(),
        description,
        start: year,
        end: sub(add(year, { months: formatObj.increment }), { seconds: 1 }),
        type: t(`date_format.${subType}.${dateFormat.type}`, { lng: locale }),
        hierarchy: parent
      });
    }

    if (dateFormat.quarterTotalIsFifthQuart && quarterIndex === 4) {
      const yearStr = formatObj.formatStr
        .replace('[full-start]', format(displayYear, 'yyyy'))
        .replace('[full-end]', format(add(displayYear, { years: 1 }), 'yyyy'))
        .replace('[end-year]', format(add(displayYear, { years: 1 }), 'yy'))
        .replace('[quarterNo]', (quarterIndex + 1).toString())
        .replace('[monthStr]', format(year, 'MMM'))
        .replace('[monthNo]', String(monthNo).padStart(2, '0'));
      for (const locale of SUPPORTED_LOCALES) {
        referenceTable.push({
          dateCode: yearStr,
          lang: locale.toLowerCase(),
          description:
            dateFormat.type === YearType.Calendar
              ? format(displayYear, 'yyyy')
              : `${format(displayYear, 'yyyy')}-${format(add(displayYear, { years: 1 }), 'yy')}`,
          start: year,
          end: sub(add(year, { months: 12 }), { seconds: 1 }),
          type: t(`date_format.year.${dateFormat.type}`, { lng: locale }),
          hierarchy: null
        });
      }
    }

    year = add(year, { months: formatObj.increment });
    switch (generationType) {
      case GeneratorType.Year:
        if (displayYear !== year) displayYear = year;
        break;
      case GeneratorType.Quarter:
        if (quarterIndex < 4) {
          quarterIndex++;
        } else {
          quarterIndex = 1;
        }
        if (year !== displayYear && quarterIndex === 1) {
          displayYear = year;
        }
        break;
      case GeneratorType.Month:
        if (monthIndex <= 12) {
          if (monthIndex % 3 === 0) quarterIndex++;
          monthIndex++;
        } else {
          monthIndex = 1;
          quarterIndex = 1;
        }
        if (displayYear !== displayYear && monthIndex === 1) {
          displayYear = year;
        }
    }
  }
  return referenceTable;
}

function specificDateTableCreator(dateFormat: DateExtractor, dataColumn: TableData): DateReferenceDataItem[] {
  const referenceTable: DateReferenceDataItem[] = [];
  dataColumn.map((row) => {
    const value = row.toString();
    let parsedDate: Date;
    let day: string;
    let month: string;
    let year: string;

    switch (dateFormat.dateFormat) {
      case 'dd/MM/yyyy':
      case 'DD/MM/YYYY':
        parsedDate = parse(value, 'dd/MM/yyyy', new Date());
        break;

      case 'dd-MM-yyyy':
      case 'DD-MM-YYYY':
        parsedDate = parse(value, 'dd-MM-yyyy', new Date());
        break;

      case 'yyyy-MM-dd':
      case 'YYYY-MM-DD':
        parsedDate = parseISO(`${value}T00:00:00Z`);
        break;

      case 'yyyyMMdd':
      case 'YYYYMMDD':
        year = value.substring(0, 4);
        month = value.substring(4, 6);
        day = value.substring(6, 8);
        parsedDate = parseISO(`${year}-${month}-${day}T00:00:00Z`);
        break;

      default:
        throw new Error(`Unknown Date Format.  Format given: ${dateFormat.dateFormat}`);
    }

    if (!isDate(parsedDate) || !isValid(parsedDate)) {
      logger.error(`Date is invalid... ${parsedDate}`);
      throw Error(`Unable to parse date based on supplied format of ${dateFormat.dateFormat}.`);
    }
    for (const locale of SUPPORTED_LOCALES) {
      referenceTable.push({
        dateCode: value,
        lang: locale.toLowerCase(),
        description: format(parsedDate, 'dd/MM/yyyy'),
        start: parsedDate,
        end: sub(add(parsedDate, { days: 1 }), { seconds: 1 }),
        type: 'specific_day',
        hierarchy: null
      });
    }
  });
  return referenceTable;
}

export function dateDimensionReferenceTableCreator(
  extractor: DateExtractor,
  dataColumn: TableData
): DateReferenceDataItem[] {
  const columnData = [];

  for (const row of dataColumn) {
    columnData.push(Object.values(row)[0]);
  }

  if (extractor.dateFormat) {
    logger.debug('Creating specific date table...');
    return specificDateTableCreator(extractor, columnData);
  } else {
    logger.debug('Creating period table...');
    return createAllTypesOfPeriod(extractor, columnData);
  }
}
