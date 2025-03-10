import { add, format, isBefore, isDate, isValid, parse, parseISO, sub } from 'date-fns';
import { TableData } from 'duckdb-async';

import { logger } from '../utils/logger';
import { YearType } from '../enums/year-type';
import { DateExtractor } from '../extractors/date-extractor';

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
    description: string;
    start: Date;
    end: Date;
    type: string;
}

// Date parsing methods start here
function yearType(type: YearType | undefined): YearTypeDetails {
    switch (type) {
        case YearType.Financial:
            return { start: '04-01T00:00:00Z', type: YearType.Financial };
        case YearType.Tax:
            return { start: '04-06T00:00:00Z', type: YearType.Tax };
        case YearType.Academic:
            return { start: '09-01T00:00:00Z', type: YearType.Academic };
        case YearType.Meteorological:
            return { start: '03-01T00:00:00Z', type: YearType.Meteorological };
        default:
            return { start: '01-01T00:00:00Z', type: YearType.Calendar };
    }
}

function yearFormats(yearFormat: string) {
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

function quarterFormats(quarterFormat: string, yearFormat: string) {
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

function monthFormats(monthFormat: string, yearFormat: string) {
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

function createAllTypesOfPeriod(dateFormat: DateExtractor, dataColumn: TableData) {
    let referenceTable: DateReferenceDataItem[] = [];
    logger.debug(`date extractor = ${JSON.stringify(dateFormat)}`);
    if (dateFormat.quarterFormat && dateFormat.quarterTotalIsFifthQuart) {
        logger.debug('5th quarter used to represent whole year totals');
        return periodTableCreator(dateFormat, dataColumn);
    } else {
        // We always need years generate those first
        logger.debug('Creating table for year');
        const yearExtractor: DateExtractor = {
            type: dateFormat.type,
            yearFormat: dateFormat.yearFormat
        };
        referenceTable = referenceTable.concat(periodTableCreator(yearExtractor, dataColumn));
        // If monthFormat is present create month entries
        if (dateFormat.monthFormat) {
            logger.debug('Month format present... creating month entries');
            const monthExtractor: DateExtractor = {
                type: dateFormat.type,
                yearFormat: dateFormat.yearFormat,
                monthFormat: dateFormat.monthFormat
            };
            referenceTable = referenceTable.concat(periodTableCreator(monthExtractor, dataColumn));
        }
        // If quarterFormat is present create quarters
        if (dateFormat.quarterFormat) {
            logger.debug('Quarter format present... creating quarter entries');
            const quarterExtractor: DateExtractor = {
                type: dateFormat.type,
                yearFormat: dateFormat.yearFormat,
                quarterFormat: dateFormat.quarterFormat
            };
            referenceTable = referenceTable.concat(periodTableCreator(quarterExtractor, dataColumn));
        }
    }
    return referenceTable;
}

function periodTableCreator(dateFormat: DateExtractor, dataColumn: TableData) {
    let subType = 'year';
    let formatObj;
    try {
        formatObj = yearFormats(dateFormat.yearFormat ? dateFormat.yearFormat : 'YYYY');
        if (dateFormat.quarterFormat) {
            formatObj = quarterFormats(dateFormat.quarterFormat, formatObj.formatStr);
            subType = 'quarter';
        } else if (dateFormat.monthFormat) {
            formatObj = monthFormats(dateFormat.monthFormat, formatObj.formatStr);
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
    const type = yearType(dateFormat.type);

    let year = parseISO(`${startYear}-${type.start}`);
    const end = add(parseISO(`${endYear}-${type.start}`), { years: 1 });

    // Quarters and month numbers are different depending on the type of year
    let quarterIndex = 1;
    let monthIndex = 1;

    // Create an array to handle our reference table
    const referenceTable: DateReferenceDataItem[] = [];
    // While loop builds our table we can match against and load into our Cube
    while (isBefore(year, end)) {
        let displayYear = year;
        let monthNo = parseInt(format(year, 'MM'), 10);
        // Special rules around financial and tax where reporting can be monthly or quarterly
        if (dateFormat.type === YearType.Financial || dateFormat.type === YearType.Tax) {
            if (subType === 'quarter') {
                if (quarterIndex > 3) {
                    displayYear = sub(displayYear, { years: 1 });
                }
            }
            if (subType === 'month') {
                monthNo = monthIndex;
                if (monthIndex > 9) {
                    displayYear = sub(displayYear, { years: 1 });
                }
            }
        }
        const dateStr = formatObj.formatStr
            .replace('[full-start]', format(displayYear, 'yyyy'))
            .replace('[full-end]', format(add(displayYear, { years: 1 }), 'yyyy'))
            .replace('[end-year]', format(add(displayYear, { years: 1 }), 'yy'))
            .replace('[quarterNo]', quarterIndex.toString())
            .replace('[monthStr]', format(year, 'MMM'))
            .replace('[monthNo]', String(monthNo).padStart(2, '0'));

        const description =
            dateFormat.type === YearType.Calendar
                ? format(displayYear, 'yyyy')
                : `${format(displayYear, 'yyyy')}-${format(add(displayYear, { years: 1 }), 'yy')}`;

        referenceTable.push({
            dateCode: dateStr,
            description,
            start: year,
            end: sub(add(year, { months: formatObj.increment }), { seconds: 1 }),
            type: `${dateFormat.type}_${subType}`
        });

        if (dateFormat.quarterTotalIsFifthQuart && quarterIndex === 4) {
            const yearStr = formatObj.formatStr
                .replace('[full-start]', format(displayYear, 'yyyy'))
                .replace('[full-end]', format(add(displayYear, { years: 1 }), 'yyyy'))
                .replace('[end-year]', format(add(displayYear, { years: 1 }), 'yy'))
                .replace('[quarterNo]', (quarterIndex + 1).toString())
                .replace('[monthStr]', format(year, 'MMM'))
                .replace('[monthNo]', String(monthNo).padStart(2, '0'));
            referenceTable.push({
                dateCode: yearStr,
                description,
                start: year,
                end: sub(add(year, { months: 12 }), { seconds: 1 }),
                type: `${dateFormat.type}_year`
            });
        }

        year = add(year, { months: formatObj.increment });

        // This is needed because quarters aren't really things
        if (quarterIndex < 4) {
            quarterIndex++;
        } else {
            quarterIndex = 1;
        }
        // This is needed for Financial and Tax months which run April to March
        if (monthIndex < 12) {
            monthIndex++;
        } else {
            monthIndex = 1;
        }
    }
    return referenceTable;
}

function specificDateTableCreator(dateFormat: DateExtractor, dataColumn: TableData) {
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

        referenceTable.push({
            dateCode: value,
            description: format(parsedDate, 'dd/MM/yyyy'),
            start: parsedDate,
            end: sub(add(parsedDate, { days: 1 }), { seconds: 1 }),
            type: 'specific_day'
        });
    });
    return referenceTable;
}

export function dateDimensionReferenceTableCreator(dateFormat: DateExtractor, dataColumn: TableData) {
    const columnData = [];

    for (const row of dataColumn) {
        columnData.push(Object.values(row)[0]);
    }

    if (dateFormat.dateFormat) {
        logger.debug('Creating specific date table...');
        return specificDateTableCreator(dateFormat, columnData);
    } else {
        logger.debug('Creating period table...');
        return createAllTypesOfPeriod(dateFormat, columnData);
    }
}
