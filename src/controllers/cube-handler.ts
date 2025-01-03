import fs from 'node:fs';

import tmp, { FileResult } from 'tmp';
import { Database } from 'duckdb-async';
import { t } from 'i18next';
import { NextFunction, Request, Response } from 'express';

import { Dataset } from '../entities/dataset/dataset';
import { FactTableAction } from '../enums/fact-table-action';
import { FactTableColumnType } from '../enums/fact-table-column-type';
import { FactTableInfo } from '../entities/dataset/fact-table-info';
import { FileImport } from '../entities/dataset/file-import';
import { SUPPORTED_LOCALES } from '../middleware/translation';
import { DimensionType } from '../enums/dimension-type';
import { Dimension } from '../entities/dataset/dimension';
import { FileType } from '../enums/file-type';
import { logger } from '../utils/logger';
import { Locale } from '../enums/locale';
import { Revision } from '../entities/dataset/revision';
import { FactTable } from '../entities/dataset/fact-table';
import { DuckdbOutputType } from '../enums/duckdb-outputs';
import { DatasetRepository } from '../repositories/dataset';
import { CSVHeader, ViewDTO, ViewErrDTO } from '../dtos/view-dto';
import { DatasetDTO } from '../dtos/dataset-dto';
import { validateParams } from '../utils/paging-validation';
import { getFileImportAndSaveToDisk } from '../utils/file-utils';
import { LookupTableExtractor } from '../extractors/lookup-table-extractor';
import { MeasureLookupTableExtractor } from '../extractors/measure-lookup-extractor';
import { getLatestRevision } from '../utils/latest';
import { UnknownException } from '../exceptions/unknown.exception';
import { DataLakeService } from '../services/datalake';

import { dateDimensionReferenceTableCreator } from './time-matching';

export const FACT_TABLE_NAME = 'fact_table';

export const makeCubeSafeString = (str: string): string => {
    return str
        .toLowerCase()
        .replace(/[ ]/g, '_')
        .replace(/[^a-zA-Z_]/g, '');
};

export const createFactTableQuery = async (
    tableName: string,
    tempFileName: string,
    fileType: FileType,
    quack: Database
): Promise<string> => {
    switch (fileType) {
        case FileType.Csv:
        case FileType.GzipCsv:
            return `CREATE TABLE ${tableName} AS SELECT * FROM read_csv('${tempFileName}', auto_type_candidates = ['BOOLEAN', 'BIGINT', 'DOUBLE', 'VARCHAR']);`;
        case FileType.Parquet:
            return `CREATE TABLE ${tableName} AS SELECT * FROM '${tempFileName}';`;
        case FileType.Json:
        case FileType.GzipJson:
            return `CREATE TABLE ${tableName} AS SELECT * FROM read_json_auto('${tempFileName}');`;
        case FileType.Excel:
            await quack.exec('INSTALL spatial;');
            await quack.exec('LOAD spatial;');
            return `CREATE TABLE ${tableName} AS SELECT * FROM st_read('${tempFileName}');`;
        default:
            throw new Error('Unknown file type');
    }
};

export const loadFileIntoCube = async (
    quack: Database,
    fileImport: FileImport,
    tempFile: FileResult,
    tableName: string
) => {
    const insertQuery = await createFactTableQuery(tableName, tempFile.name, fileImport.fileType, quack);
    try {
        await quack.exec(insertQuery);
    } catch (error) {
        logger.error(
            `Failed to load file in to the cube using query ${insertQuery} with the following error: ${error}`
        );
        throw error;
    }
};

// This function differs from loadFileIntoDatabase in that it only loads a file into an existing table
export const loadFileDataIntoTable = async (
    quack: Database,
    fileImport: FileImport,
    tempFile: FileResult,
    tableName: string
) => {
    let insertQuery: string;
    switch (fileImport.fileType) {
        case FileType.Csv:
        case FileType.GzipCsv:
            insertQuery = `INSERT INTO ${tableName} SELECT * FROM read_csv('${tempFile.name}', auto_type_candidates = ['BOOLEAN', 'BIGINT', 'DOUBLE', 'VARCHAR']);`;
            break;
        case FileType.Parquet:
            insertQuery = `INSERT INTO ${tableName} SELECT * FROM ${tempFile.name};`;
            break;
        case FileType.Json:
        case FileType.GzipJson:
            insertQuery = `INSERT INTO ${tableName} SELECT * FROM read_json_auto('${tempFile.name}');`;
            break;
        case FileType.Excel:
            insertQuery = `INSERT INTO ${tableName} SELECT * FROM st_read('${tempFile.name}');`;
            break;
        default:
            throw new Error('Unknown file type');
    }
    try {
        await quack.exec(insertQuery);
    } catch (error) {
        logger.error(`Failed to load file into table using query ${insertQuery} with the following error: ${error}`);
        throw error;
    }
};

// This is a short version of validate date dimension code found in the dimension processor.
// This concise version doesn't return any information on why the creation failed.  Just that it failed
async function createAndValidateDateDimension(quack: Database, extractor: object | null, factTableColumn: string) {
    if (!extractor) {
        throw new Error('Extractor not supplied');
    }
    const columnData = await quack.all(`SELECT "${factTableColumn}" FROM ${FACT_TABLE_NAME};`);
    const dateDimensionTable = dateDimensionReferenceTableCreator(extractor, columnData);
    await quack.exec(
        `CREATE TABLE ${makeCubeSafeString(factTableColumn)}_lookup (date_code VARCHAR, description VARCHAR, start_date datetime, end_date datetime, date_type varchar);`
    );
    // Create the date_dimension table
    const stmt = await quack.prepare(`INSERT INTO ${makeCubeSafeString(factTableColumn)}_lookup VALUES (?,?,?,?,?);`);
    dateDimensionTable.map(async (row) => {
        await stmt.run(row.dateCode, row.description, row.start, row.end, row.type);
    });
    await stmt.finalize();
    const nonMatchedRows = await quack.all(
        `SELECT line_number, fact_table_date, ${makeCubeSafeString(factTableColumn)}_lookup.date_code FROM (SELECT row_number() OVER () as line_number, "${factTableColumn}" as fact_table_date FROM ${FACT_TABLE_NAME}) as fact_table LEFT JOIN ${makeCubeSafeString(factTableColumn)}_lookup ON CAST(fact_table.fact_table_date AS VARCHAR)=CAST(${makeCubeSafeString(factTableColumn)}_lookup.date_code AS VARCHAR) where date_code IS NULL;`
    );
    if (nonMatchedRows.length > 0) {
        throw new Error(`Failed to validate date dimension`);
    }
    return `${makeCubeSafeString(factTableColumn)}_lookup`;
}

// This is a short version of the validate lookup table code found in the dimension process.
// This concise version doesn't return any information on why the creation failed.  Just that it failed
async function createAndValidateLookupTableDimension(quack: Database, dataset: Dataset, dimension: Dimension) {
    if (!dimension.lookupTable) return;
    if (!dimension.extractor) return;
    const extractor = dimension.extractor as LookupTableExtractor;
    const lookupTableFile = await getFileImportAndSaveToDisk(dataset, dimension.lookupTable);
    if (dimension.lookupTable.isStatsWales2Format) {
        await loadFileIntoCube(
            quack,
            dimension.lookupTable,
            lookupTableFile,
            `${makeCubeSafeString(dimension.factTableColumn)}_lookup_sw2`
        );
        let sortOrderCol = '';
        if (extractor.sortColumn) {
            sortOrderCol = `"${extractor.sortColumn}", `;
        }
        const viewParts: string[] = SUPPORTED_LOCALES.map((locale) => {
            const descriptionCol = extractor.descriptionColumns.find(
                (col) => col.lang.toLowerCase() === locale.split('-')[0]
            );
            const descriptionColStr = descriptionCol ? `${descriptionCol.name} as description, ` : '';
            const notesCol = extractor.notesColumns?.find((col) => col.lang.toLowerCase() === locale.split('-')[0]);
            const notesColStr = notesCol ? `${notesCol.name} as notes, ` : '';
            return (
                `SELECT "${dimension.joinColumn}", ${sortOrderCol} '${locale.toLowerCase()}' as language,\n` +
                `${descriptionColStr} ${notesColStr} from ${makeCubeSafeString(dimension.factTableColumn)}_lookup_sw2`
            );
        });
        await quack.exec(`CREATE TABLE "${dimension.factTableColumn}_lookup" AS ${viewParts.join('\nUNION\n')};`);
    } else {
        await loadFileIntoCube(
            quack,
            dimension.lookupTable,
            lookupTableFile,
            `${makeCubeSafeString(dimension.factTableColumn)}_lookup`
        );
    }
    const nonMatchedRows = await quack.all(
        `SELECT line_number, fact_table_column, ${makeCubeSafeString(dimension.factTableColumn)}_lookup.${dimension.joinColumn} as lookup_table_column FROM (SELECT row_number() OVER () as line_number, "${dimension.factTableColumn}" as fact_table_column FROM ${FACT_TABLE_NAME}) as fact_table LEFT JOIN ${makeCubeSafeString(dimension.factTableColumn)}_lookup ON CAST(fact_table.fact_table_column AS VARCHAR)=CAST(${makeCubeSafeString(dimension.factTableColumn)}_lookup.${dimension.joinColumn} AS VARCHAR) where lookup_table_column IS NULL;`
    );
    if (nonMatchedRows.length > 0) {
        throw new Error('Failed to validate lookup table dimension');
    }
}

function setupFactTableUpdateJoins(factTableInfos: FactTableInfo[]): string {
    return factTableInfos.map((info) => `update_table."${info.columnName}"="${info.columnName}"`).join(' AND ');
}

async function loadFactTablesWithUpdates(
    quack: Database,
    dataset: Dataset,
    allFactTables: FactTable[],
    dataValuesColumn: FactTableInfo,
    notesCodeColumn: FactTableInfo,
    factIdentifiers: FactTableInfo[]
) {
    for (const factTable of allFactTables.sort((ftA, ftB) => ftA.uploadedAt.getTime() - ftB.uploadedAt.getTime())) {
        logger.info(`Loading fact table data for fact table ${factTable.id}`);
        const factTableFile = await getFileImportAndSaveToDisk(dataset, factTable);
        const updateQuery =
            `UPDATE ${FACT_TABLE_NAME} SET "${dataValuesColumn.columnName}"=update_table."${dataValuesColumn.columnName}", ` +
            `"${notesCodeColumn.columnName}"=(CASE ${FACT_TABLE_NAME}."${notesCodeColumn.columnName}" = NULL THEN 'r' ELSE concat(${FACT_TABLE_NAME}."${notesCodeColumn.columnName}"', ',r') END) ` +
            `FROM update_table WHERE ${setupFactTableUpdateJoins(factIdentifiers)} ` +
            `AND update_table."${notesCodeColumn.columnName}" LIKE '%r';`;
        switch (factTable.action) {
            case FactTableAction.ReplaceAll:
                await quack.exec(`DELETE FROM ${FACT_TABLE_NAME};`);
                await loadFileDataIntoTable(quack, factTable, factTableFile, FACT_TABLE_NAME);
                break;
            case FactTableAction.Add:
                await loadFileDataIntoTable(quack, factTable, factTableFile, FACT_TABLE_NAME);
                break;
            case FactTableAction.Revise:
                await loadFileDataIntoTable(quack, factTable, factTableFile, 'update_table');
                await quack.exec(updateQuery);
                await quack.exec(`DROP TABLE update_table;`);
                break;
            case FactTableAction.AddRevise:
                await loadFileDataIntoTable(quack, factTable, factTableFile, 'update_table');
                await quack.exec(updateQuery);
                await quack.exec(`DELETE FROM update_table WHERE ${notesCodeColumn.columnName} LIKE '%r';`);
                await quack.exec(`INSERT INTO ${FACT_TABLE_NAME} (SELECT * FROM update_table);`);
                await quack.exec(`DROP TABLE update_table;`);
                break;
        }
        factTableFile.removeCallback();
    }
}

async function loadFactTablesWithoutUpdates(quack: Database, dataset: Dataset, allFactTables: FactTable[]) {
    logger.warn(
        'There is no notes column present in this dataset.  Action allowed are limited to adding data and replacing all data'
    );
    for (const factTable of allFactTables.sort((ftA, ftB) => ftA.uploadedAt.getTime() - ftB.uploadedAt.getTime())) {
        logger.info(`Loading fact table data for fact table ${factTable.id}`);
        const factTableFile = await getFileImportAndSaveToDisk(dataset, factTable);
        switch (factTable.action) {
            case FactTableAction.ReplaceAll:
                await quack.exec(`DELETE FROM ${FACT_TABLE_NAME};`);
                await loadFileDataIntoTable(quack, factTable, factTableFile, FACT_TABLE_NAME);
                break;
            case FactTableAction.Add:
                await loadFileDataIntoTable(quack, factTable, factTableFile, FACT_TABLE_NAME);
                break;
        }
        factTableFile.removeCallback();
    }
}

async function loadFactTables(
    quack: Database,
    dataset: Dataset,
    endRevision: Revision,
    dataValuesColumn: FactTableInfo | undefined,
    notesCodeColumn: FactTableInfo | undefined,
    factIdentifiers: FactTableInfo[]
): Promise<void> {
    // Find all the fact tables for the given revision
    logger.debug('Finding all fact tables for this revision and those that came before');
    let allFactTables: FactTable[] = [];
    if (endRevision.revisionIndex > 0) {
        // If we have a revision index we start here
        const validRevisions = dataset.revisions.filter(
            (rev) => rev.revisionIndex <= endRevision.revisionIndex && rev.revisionIndex > 0
        );
        allFactTables = validRevisions.flatMap((revision) => revision.factTables);
    } else {
        // If we don't have a revision index we need to find the previous revision to this one that does
        allFactTables = allFactTables.concat(endRevision.factTables);
        const validRevisions = dataset.revisions.filter(
            (rev) => rev.createdAt < endRevision.createdAt && rev.revisionIndex > 0
        );
        allFactTables = validRevisions.flatMap((revision) => revision.factTables);
    }

    if (allFactTables.length === 0) {
        logger.error(`No fact tables found in this dataset to revision ${endRevision.id}`);
        throw new Error(`No fact tables found in this dataset to revision ${endRevision.id}`);
    }

    // Process all the fact tables
    logger.debug('Loading all fact tables in to database');
    try {
        if (dataValuesColumn && notesCodeColumn) {
            await loadFactTablesWithUpdates(
                quack,
                dataset,
                allFactTables,
                dataValuesColumn,
                notesCodeColumn,
                factIdentifiers
            );
        } else {
            await loadFactTablesWithoutUpdates(quack, dataset, allFactTables);
        }
    } catch (error) {
        logger.error(`Something went wrong trying to create the core fact table with error: ${error}`);
        await quack.close();
        throw error;
    }
}

interface NoteCodeItem {
    code: string;
    tag: string;
}

const NoteCodes: NoteCodeItem[] = [
    { code: 'a', tag: 'average' },
    { code: 'c', tag: 'confidential' },
    { code: 'e', tag: 'estimated' },
    { code: 'f', tag: 'forecast' },
    { code: 'k', tag: 'low_figure' },
    { code: 'p', tag: 'provisional' },
    { code: 'r', tag: 'revised' },
    { code: 't', tag: 'total' },
    { code: 'u', tag: 'low_reliability' },
    { code: 'x', tag: 'missing_data' },
    { code: 'z', tag: 'not_applicable' }
];

async function createNotesTable(
    quack: Database,
    notesColumn: FactTableInfo,
    selectStatementsMap: Map<Locale, string[]>,
    joinStatements: string[]
): Promise<void> {
    logger.info('Creating notes table...');
    try {
        await quack.exec(
            `CREATE TABLE note_codes (code VARCHAR, language VARCHAR, tag VARCHAR, description VARCHAR, notes VARCHAR);`
        );
        const insertStmt = await quack.prepare(
            `INSERT INTO note_codes (code, language, tag, description, notes) VALUES (?,?,?,?,?);`
        );
        for (const locale of SUPPORTED_LOCALES) {
            for (const noteCode of NoteCodes) {
                await insertStmt.run(
                    noteCode.code,
                    locale.toLowerCase(),
                    noteCode.tag,
                    t(`note_codes.${noteCode.tag}`, { lng: locale }),
                    null
                );
            }
        }
        await insertStmt.finalize();
        logger.info('Creating notes table view...');
        // We perform join operations to this view as we want to turn a csv such as `a,r` in to `Average, Revised`.
        await quack.exec(
            `CREATE TABLE all_notes AS SELECT fact_table."${notesColumn.columnName}" as code, note_codes.language as language, string_agg(DISTINCT note_codes.description, ', ') as description
            from fact_table JOIN note_codes ON LIST_CONTAINS(string_split(fact_table."${notesColumn.columnName}", ','), note_codes.code)
            GROUP BY fact_table."${notesColumn.columnName}", note_codes.language;`
        );
    } catch (error) {
        logger.error(`Something went wrong trying to create the notes table with error: ${error}`);
        throw new Error(
            `Something went wrong trying to create the notes code table with the following error: ${error}`
        );
    }
    for (const locale of SUPPORTED_LOCALES) {
        selectStatementsMap
            .get(locale)
            ?.push(`all_notes.description as "${t('column_headers.notes', { lng: locale })}"`);
    }
    joinStatements.push(
        `LEFT JOIN all_notes on all_notes.code=fact_table."${notesColumn.columnName}" AND all_notes.language='#LANG#'`
    );
}

interface MeasureFormat {
    name: string;
    method: string;
}

function measureFormats(): Map<string, MeasureFormat> {
    const measureFormats: Map<string, MeasureFormat> = new Map();
    measureFormats.set('decimal', {
        name: 'Decimal',
        method: "WHEN measure.display_type = 'Decimal' THEN printf('%,.2f', |COL|)"
    });
    measureFormats.set('float', {
        name: 'Float',
        method: "WHEN measure.display_type = 'Float' THEN printf('%,.2f', |COL|)"
    });
    measureFormats.set('integer', {
        name: 'Integer',
        method: "WHEN measure.display_type = 'Integer' THEN printf('%,d', CAST(|COL| AS INTEGER))"
    });
    measureFormats.set('long', { name: 'Long', method: "WHEN measure.display_type = 'Long' THEN printf('%f', |COL|)" });
    measureFormats.set('percentage', {
        name: 'Percentage',
        method: "WHEN measure.display_type = 'Long' THEN printf('%f', |COL|)"
    });
    measureFormats.set('string', {
        name: 'String',
        method: "WHEN measure.display_type = 'String' THEN printf('%s', CAST(|COL| AS VARCHAR))"
    });
    measureFormats.set('text', {
        name: 'Text',
        method: "WHEN measure.display_type = 'Text' THEN printf('%s', CAST(|COL| AS VARCHAR))"
    });
    measureFormats.set('date', {
        name: 'Date',
        method: "WHEN measure.display_type = 'Date' THEN printf('%s', CAST(|COL| AS VARCHAR))"
    });
    measureFormats.set('datetime', {
        name: 'DateTime',
        method: "WHEN measure.display_type = 'DateTime' THEN printf('%s', CAST(|COL| AS VARCHAR))"
    });
    measureFormats.set('time', {
        name: 'Time',
        method: "WHEN measure.display_type = 'Time' THEN printf('%s', CAST(|COL| AS VARCHAR))"
    });
    return measureFormats;
}

async function setupMeasures(
    quack: Database,
    dataset: Dataset,
    dataValuesColumn: FactTableInfo | undefined,
    selectStatementsMap: Map<Locale, string[]>,
    joinStatements: string[],
    orderByStatements: string[],
    measureColumn?: FactTableInfo
) {
    logger.info('Setting up measure table if present...');
    // Process the column that represents the measure
    if (measureColumn && dataset.measure && dataset.measure.joinColumn) {
        // If we parsed the lookup table or the user
        // has used a user journey to define measures
        // use this first
        if (dataset.measure.measureInfo && dataset.measure.measureInfo.length > 0) {
            logger.debug('Using measure info to build measure table');
            try {
                await quack.exec(
                    `CREATE TABLE measure (measure_id ${measureColumn.columnType}, sort_order INT, language VARCHAR(5), description VARCHAR, notes VARCHAR, data_type VARCHAR, display_type VARCHAR);`
                );
                const insertStmt = await quack.prepare(`INSERT INTO measure (?,?,?,?,?,?);`);
                dataset.measure.measureInfo.map(async (measure) => {
                    await insertStmt.run(
                        measure.id,
                        measure.sortOrder,
                        measure.language,
                        measure.description,
                        measure.notes,
                        measure.displayType
                    );
                });
                await insertStmt.finalize();
            } catch (error) {
                logger.error(`Unable to create or load measure table in to the cube with error: ${error}`);
                await quack.close();
                throw error;
            }
        } else if (dataset.measure && dataset.measure.lookupTable) {
            logger.debug('Measure lookup table present using this to build measure table');
            const measure = dataset.measure;
            const extractor = measure.extractor as MeasureLookupTableExtractor;
            const measureFile = await getFileImportAndSaveToDisk(dataset, dataset.measure.lookupTable);
            if (dataset.measure.lookupTable.isStatsWales2Format) {
                logger.debug('Lookup table is marked as in StatsWales 2 format building view...');
                try {
                    await loadFileIntoCube(quack, dataset.measure.lookupTable, measureFile, 'measure_sw2');
                    const viewComponents: string[] = [];
                    for (const locale of SUPPORTED_LOCALES) {
                        let formatColumn = `"${extractor.formatColumn}"`;
                        if (!formatColumn) {
                            formatColumn = `'Text'`;
                        } else if (formatColumn.toLowerCase().indexOf('decimal') > -1) {
                            formatColumn = `CASE WHEN "${extractor.formatColumn}" = 1 THEN 'Decimal' ELSE 'Integer' END`;
                        }
                        let measureTypeColumn = `"${extractor.formatColumn}"`;
                        if (!extractor.measureTypeColumn) {
                            measureTypeColumn = `'Unknown'`;
                        }
                        viewComponents.push(
                            `SELECT
                            "${measure.joinColumn}" as measure_id,
                            "${extractor.sortColumn}" as sort_order,
                            '${locale.toLowerCase()}' AS language,
                            "${extractor.descriptionColumns.find((col) => col.lang === locale.split('-')[0])?.name}" AS description,
                            ${formatColumn} AS display_type,
                            ${measureTypeColumn} AS data_type FROM measure_sw2\n`
                        );
                    }
                    const buildMeasureViewQuery = `CREATE TABLE measure AS ${viewComponents.join('\nUNION\n')};`;
                    await quack.exec(buildMeasureViewQuery);
                } catch (error) {
                    logger.error(`Unable to create or load measure table in to the cube with error: ${error}`);
                    await quack.close();
                    throw error;
                }
            } else {
                try {
                    logger.debug('Lookup table in preferred format... loading straight in to cube');
                    await loadFileIntoCube(quack, dataset.measure.lookupTable, measureFile, 'measure');
                } catch (error) {
                    logger.error(`Unable to load measure table in to the cube with error: ${error}`);
                    await quack.close();
                    throw error;
                }
            }
            measureFile.removeCallback();
        } else {
            logger.error(`Measure is defined in the dataset but it has no lookup nor definitions`);
            await quack.close();
            throw new Error('No measure definitions found');
        }
        logger.debug('Creating query part to format the data value correctly');
        const caseStatement: string[] = ['CASE'];
        const presentFormats = await quack.all('SELECT DISTINCT display_type FROM measure');
        for (const dataFormat of presentFormats.map((type) => type.display_type)) {
            caseStatement.push(
                measureFormats()
                    .get(dataFormat.toLowerCase())
                    ?.method.replace('|COL|', `${FACT_TABLE_NAME}."${dataValuesColumn?.columnName}"`) || ''
            );
        }
        caseStatement.push('END');
        logger.debug(`Data view case statement ended up as: ${caseStatement.join('\n')}`);
        SUPPORTED_LOCALES.map((locale) => {
            if (dataValuesColumn)
                selectStatementsMap
                    .get(locale)
                    ?.push(`${caseStatement.join('\n')} as "${t('column_headers.data_values', { lng: locale })}"`);
            selectStatementsMap
                .get(locale)
                ?.push(`measure.description as "${t('column_headers.measure', { lng: locale })}"`);
        });
        joinStatements.push(
            `LEFT JOIN measure on measure.measure_id=${FACT_TABLE_NAME}.${dataset.measure.factTableColumn} AND measure.language='#LANG#'`
        );
        orderByStatements.push(`measure.measure_id`);
    } else {
        SUPPORTED_LOCALES.map((locale) => {
            if (dataValuesColumn)
                selectStatementsMap
                    .get(locale)
                    ?.push(
                        `${FACT_TABLE_NAME}."${dataValuesColumn?.columnName}" as "${t('column_headers.data_values', { lng: locale })}"`
                    );
            if (measureColumn)
                selectStatementsMap
                    .get(locale)
                    ?.push(
                        `${FACT_TABLE_NAME}."${measureColumn.columnName}" as "${t('column_headers.measure', { lng: locale })}"`
                    );
        });
    }
}

async function setupDimensions(
    quack: Database,
    dataset: Dataset,
    selectStatementsMap: Map<Locale, string[]>,
    joinStatements: string[],
    orderByStatements: string[]
) {
    logger.info('Setting up dimension tables...');
    for (const dimension of dataset.dimensions) {
        logger.info(`Setting up dimension ${dimension.id} for fact table column ${dimension.factTableColumn}`);
        const dimTable = `${makeCubeSafeString(dimension.factTableColumn)}_lookup`;
        try {
            switch (dimension.type) {
                case DimensionType.TimePeriod:
                case DimensionType.TimePoint:
                    if (dimension.extractor) {
                        await createAndValidateDateDimension(quack, dimension.extractor, dimension.factTableColumn);
                        SUPPORTED_LOCALES.map((locale) => {
                            const columnName =
                                dimension.dimensionInfo.find((info) => info.language === locale)?.name ||
                                dimension.factTableColumn;
                            selectStatementsMap.get(locale)?.push(`${dimTable}.description as "${columnName}"`);
                            selectStatementsMap
                                .get(locale)
                                ?.push(
                                    `strftime(${dimTable}.start_date, '%d/%m/%Y') as "${t('column_headers.start_date', { lng: locale })}"`
                                );
                            selectStatementsMap
                                .get(locale)
                                ?.push(
                                    `strftime(${dimTable}.end_date, '%d/%m/%Y') as "${t('column_headers.end_date', { lng: locale })}"`
                                );
                        });
                        joinStatements.push(
                            `LEFT JOIN ${dimTable} on ${dimTable}."${dimension.joinColumn}"=${FACT_TABLE_NAME}."${dimension.factTableColumn}"`
                        );
                        orderByStatements.push(`${dimTable}.end_date`);
                    } else {
                        SUPPORTED_LOCALES.map((locale) => {
                            const columnName =
                                dimension.dimensionInfo.find((info) => info.language === locale)?.name ||
                                dimension.factTableColumn;
                            selectStatementsMap.get(locale)?.push(`${dimension.factTableColumn} as "${columnName}"`);
                        });
                    }
                    break;
                case DimensionType.LookupTable:
                    await createAndValidateLookupTableDimension(quack, dataset, dimension);
                    SUPPORTED_LOCALES.map((locale) => {
                        const columnName =
                            dimension.dimensionInfo.find((info) => info.language === locale)?.name ||
                            dimension.factTableColumn;
                        selectStatementsMap.get(locale)?.push(`${dimTable}.description as "${columnName}"`);
                    });
                    joinStatements.push(
                        `LEFT JOIN ${dimTable} on ${dimTable}."${dimension.joinColumn}"=${FACT_TABLE_NAME}."${dimension.factTableColumn}" AND ${dimTable}.language='#LANG#'`
                    );
                    if ((dimension.extractor as LookupTableExtractor).sortColumn) {
                        orderByStatements.push(
                            `${dimTable}."${(dimension.extractor as LookupTableExtractor).sortColumn}"`
                        );
                    }
                    break;
                case DimensionType.ReferenceData:
                    logger.error(`Reference data dimensions not implemented`);
                    SUPPORTED_LOCALES.map((locale) => {
                        const columnName =
                            dimension.dimensionInfo.find((info) => info.language === locale)?.name ||
                            dimension.factTableColumn;
                        selectStatementsMap.get(locale)?.push(`${dimension.factTableColumn} as "${columnName}"`);
                    });
                    break;
                case DimensionType.Raw:
                case DimensionType.Numeric:
                case DimensionType.Text:
                case DimensionType.Symbol:
                    SUPPORTED_LOCALES.map((locale) => {
                        const columnName =
                            dimension.dimensionInfo.find((info) => info.language === locale)?.name ||
                            dimension.factTableColumn;
                        selectStatementsMap.get(locale)?.push(`${dimension.factTableColumn} as "${columnName}"`);
                    });
                    break;
            }
        } catch (err) {
            logger.error(`Something went wrong trying to load dimension ${dimension.id} in to the cube`);
            await quack.close();
            throw new Error(
                `Could not load dimensions ${dimension.id} in to the cube with the following error: ${err}`
            );
        }
    }
}

// Builds a fresh cube based on all revisions and returns the file pointer
// to the duckdb file on disk.  This is based on the recipe in our cube miro
// board and our candidate cube format repo.  It is limited to building a
// simple default view based on the available locales.
//
// DO NOT put validation against columns which should be present here.
// Function should be able to generate a cube just from a fact table or collection
// of fact tables.
export const createBaseCube = async (dataset: Dataset, endRevision: Revision): Promise<string> => {
    const selectStatementsMap = new Map<Locale, string[]>();
    SUPPORTED_LOCALES.map((locale) => selectStatementsMap.set(locale, []));
    const joinStatements: string[] = [];
    const orderByStatements: string[] = [];
    let notesCodeColumn: FactTableInfo | undefined;
    let dataValuesColumn: FactTableInfo | undefined;
    let measureColumn: FactTableInfo | undefined;

    const firstRevision = dataset.revisions.find((rev) => rev.revisionIndex === 1);
    if (!firstRevision) {
        throw new Error(`Unable to find first revision for dataset ${dataset.id}`);
    }
    const firstFactTable = firstRevision.factTables[0];
    const compositeKey: string[] = [];
    const factIdentifiers: FactTableInfo[] = [];

    const factTableDef = firstFactTable.factTableInfo.map((field) => {
        switch (field.columnType) {
            case FactTableColumnType.Dimension:
            case FactTableColumnType.Time:
                compositeKey.push(`"${field.columnName}"`);
                factIdentifiers.push(field);
                break;
            case FactTableColumnType.Measure:
                compositeKey.push(`"${field.columnName}"`);
                factIdentifiers.push(field);
                measureColumn = field;
                break;
            case FactTableColumnType.NoteCodes:
                notesCodeColumn = field;
                break;
            case FactTableColumnType.DataValues:
                dataValuesColumn = field;
                break;
        }
        return `"${field.columnName}" ${field.columnDatatype}`;
    });

    logger.debug('Creating an in-memory database to hold the cube using DuckDB 🐤');
    const quack = await Database.create(':memory:');

    logger.info('Creating initial fact table in cube');
    try {
        let key = '';
        if (compositeKey.length > 0) {
            key = `, PRIMARY KEY (${compositeKey.join(', ')})`;
        }
        await quack.exec(`CREATE TABLE ${FACT_TABLE_NAME} (${factTableDef.join(', ')}${key});`);
    } catch (err) {
        logger.error(`Failed to create fact table in cube: ${err}`);
        await quack.close();
        throw new Error(`Failed to create fact table in cube: ${err}`);
    }

    await loadFactTables(quack, dataset, endRevision, dataValuesColumn, notesCodeColumn, factIdentifiers);

    await setupMeasures(
        quack,
        dataset,
        dataValuesColumn,
        selectStatementsMap,
        joinStatements,
        orderByStatements,
        measureColumn
    );

    await setupDimensions(quack, dataset, selectStatementsMap, joinStatements, orderByStatements);

    logger.debug('Adding notes code column to the select statement.');
    if (notesCodeColumn) {
        await createNotesTable(quack, notesCodeColumn, selectStatementsMap, joinStatements);
    }

    logger.info(`Creating default views...`);
    // Build the default views
    for (const locale of SUPPORTED_LOCALES) {
        const defaultViewSQL = `CREATE VIEW default_view_${locale.toLowerCase().split('-')[0]} AS SELECT\n${selectStatementsMap
            .get(locale)
            ?.join(
                ',\n'
            )} FROM ${FACT_TABLE_NAME}\n${joinStatements.join('\n').replace(/#LANG#/g, locale.toLowerCase())}\n ${orderByStatements.length > 0 ? `ORDER BY ${orderByStatements.join(', ')}` : ''};`;
        await quack.exec(defaultViewSQL);
    }
    const tmpFile = tmp.tmpNameSync({ postfix: '.db' });
    try {
        logger.debug(`Writing memory database to disk at ${tmpFile}`);
        await quack.exec(`ATTACH '${tmpFile}' as outDB;`);
        await quack.exec(`COPY FROM DATABASE memory TO outDB;`);
        await quack.exec('DETACH outDB;');
    } catch (err) {
        logger.error(`Failed to write memory database to disk with error: ${err}`);
        throw err;
    }
    await quack.close();
    // Pass the file handle to the calling method
    // If used for preview you just want the file
    // If it's the end of the publishing step you'll
    // want to upload the file to the data lake.
    return tmpFile;
};

export const getCubeDataTable = async (cubeFile: string, lang: string) => {
    const quack = await Database.create(cubeFile);
    const defaultView = await quack.all(`SELECT * FROM default_view_${lang};`);
    await quack.close();
    return defaultView;
};

export const getCubePreview = async (
    cubeFile: string,
    lang: string,
    dataset: Dataset,
    page: number,
    size: number
): Promise<ViewDTO | ViewErrDTO> => {
    const quack = await Database.create(cubeFile);
    const totalsQuery = `SELECT count(*) as totalLines, ceil(count(*)/${size}) as totalPages from default_view_${lang};`;
    const totals = await quack.all(totalsQuery);
    const totalPages = Number(totals[0].totalPages);
    const totalLines = Number(totals[0].totalLines);
    const errors = validateParams(page, totalPages, size);
    if (errors.length > 0) {
        return {
            status: 400,
            errors,
            dataset_id: dataset.id
        };
    }
    const previewQuery = `SELECT int_line_number, * FROM (SELECT row_number() OVER () as int_line_number, * FROM default_view_${lang}) LIMIT ${size} OFFSET ${(page - 1) * size}`;
    const preview = await quack.all(previewQuery);
    const startLine = Number(preview[0].int_line_number);
    const lastLine = Number(preview[preview.length - 1].int_line_number);
    const tableHeaders = Object.keys(preview[0]);
    const dataArray = preview.map((row) => Object.values(row));
    const currentDataset = await DatasetRepository.getById(dataset.id);
    const headers: CSVHeader[] = [];
    for (let i = 0; i < tableHeaders.length; i++) {
        headers.push({
            index: i - 1,
            name: tableHeaders[i],
            source_type:
                tableHeaders[i] === 'int_line_number' ? FactTableColumnType.LineNumber : FactTableColumnType.Unknown
        });
    }
    return {
        dataset: DatasetDTO.fromDataset(currentDataset),
        current_page: page,
        page_info: {
            total_records: totalLines,
            start_record: startLine,
            end_record: lastLine
        },
        page_size: size,
        total_pages: totalPages,
        headers,
        data: dataArray
    };
};

export const outputCube = async (cubeFile: string, lang: string, mode: DuckdbOutputType) => {
    const quack = await Database.create(cubeFile);
    const outputFile: FileResult = tmp.fileSync({ postfix: `.${mode}` });
    switch (mode) {
        case DuckdbOutputType.Csv:
            await quack.exec(`COPY default_view_${lang} TO '${outputFile.name}' (HEADER, DELIMITER ',');`);
            break;
        case DuckdbOutputType.Parquet:
            await quack.exec(`COPY default_view_${lang} TO '${outputFile.name}' (FORMAT PARQUET);`);
            break;
        case DuckdbOutputType.Excel:
            await quack.exec(`INSTALL spatial;`);
            await quack.exec('LOAD spatial;');
            await quack.exec(`COPY default_view_${lang} TO '${outputFile.name}' WITH (FORMAT GDAL, DRIVER 'xlsx');`);
            break;
        case DuckdbOutputType.Json:
            await quack.exec(`COPY default_view_${lang} TO '${outputFile.name}' (FORMAT JSON);`);
            break;
        case DuckdbOutputType.DuckDb:
            return cubeFile;
        default:
            throw new Error(`Format ${mode} not supported`);
    }
    return outputFile.name;
};

export const cleanUpCube = async (tmpFile: string) => {
    fs.unlinkSync(tmpFile);
};

export const downloadCubeFile = async (req: Request, res: Response, next: NextFunction) => {
    const dataset = res.locals.dataset;
    const latestRevision = getLatestRevision(dataset);
    if (!latestRevision) {
        next(new UnknownException('errors.no_revision'));
        return;
    }
    let cubeBuffer: Buffer;
    if (latestRevision.onlineCubeFilename) {
        const dataLakeService = new DataLakeService();
        cubeBuffer = await dataLakeService.getFileBuffer(latestRevision.onlineCubeFilename, dataset.id);
    } else {
        try {
            const cubeFile = await createBaseCube(dataset, latestRevision);
            cubeBuffer = Buffer.from(fs.readFileSync(cubeFile));
        } catch (err) {
            logger.error(`Something went wrong trying to create the cube with the error: ${err}`);
            next(new UnknownException('errors.cube_create_error'));
            return;
        }
    }
    logger.info(`Sending original cube file (size: ${cubeBuffer.length})`);
    res.writeHead(200, {
        // eslint-disable-next-line @typescript-eslint/naming-convention
        'Content-Type': 'application/octet-stream',
        // eslint-disable-next-line @typescript-eslint/naming-convention
        'Content-disposition': `attachment;filename=${dataset.id}.duckdb`,
        // eslint-disable-next-line @typescript-eslint/naming-convention
        'Content-Length': cubeBuffer.length
    });
    res.end(cubeBuffer);
};

export const downloadCubeAsJSON = async (req: Request, res: Response, next: NextFunction) => {
    const dataset = res.locals.dataset;
    const lang = req.language.split('-')[0];
    const latestRevision = getLatestRevision(dataset);
    if (!latestRevision) {
        next(new UnknownException('errors.no_revision'));
        return;
    }
    let cubeFile: string;
    if (latestRevision.onlineCubeFilename) {
        const dataLakeService = new DataLakeService();
        const fileBuffer = await dataLakeService.getFileBuffer(latestRevision.onlineCubeFilename, dataset.id);
        cubeFile = tmp.tmpNameSync({ postfix: '.duckdb' });
        fs.writeFileSync(cubeFile, fileBuffer);
    } else {
        try {
            logger.info('Creating fresh cube file.');
            cubeFile = await createBaseCube(dataset, latestRevision);
        } catch (err) {
            logger.error(`Something went wrong trying to create the cube with the error: ${err}`);
            next(new UnknownException('errors.cube_create_error'));
            return;
        }
    }
    const downloadFile = await outputCube(cubeFile, lang, DuckdbOutputType.Json);
    fs.unlinkSync(cubeFile);
    const downloadStream = fs.createReadStream(downloadFile);
    // eslint-disable-next-line @typescript-eslint/naming-convention
    res.writeHead(200, { 'Content-Type': '\tapplication/json' });
    downloadStream.pipe(res);

    // Handle errors in the file stream
    downloadStream.on('error', (err) => {
        logger.error('File stream error:', err);
        // eslint-disable-next-line @typescript-eslint/naming-convention
        res.writeHead(500, { 'Content-Type': 'text/plain' });
        fs.unlinkSync(downloadFile);
        res.end('Server Error');
    });

    // Optionally listen for the end of the stream
    downloadStream.on('end', () => {
        fs.unlinkSync(downloadFile);
        logger.debug('File stream ended');
    });
};

export const downloadCubeAsCSV = async (req: Request, res: Response, next: NextFunction) => {
    const dataset = res.locals.dataset;
    const lang = req.language.split('-')[0];
    const latestRevision = getLatestRevision(dataset);
    if (!latestRevision) {
        next(new UnknownException('errors.no_revision'));
        return;
    }
    let cubeFile: string;
    if (latestRevision.onlineCubeFilename) {
        const dataLakeService = new DataLakeService();
        const fileBuffer = await dataLakeService.getFileBuffer(latestRevision.onlineCubeFilename, dataset.id);
        cubeFile = tmp.tmpNameSync({ postfix: '.duckdb' });
        fs.writeFileSync(cubeFile, fileBuffer);
    } else {
        try {
            cubeFile = await createBaseCube(dataset, latestRevision);
        } catch (err) {
            logger.error(`Something went wrong trying to create the cube with the error: ${err}`);
            next(new UnknownException('errors.cube_create_error'));
            return;
        }
    }
    const downloadFile = await outputCube(cubeFile, lang, DuckdbOutputType.Csv);
    fs.unlinkSync(cubeFile);
    const downloadStream = fs.createReadStream(downloadFile);
    // eslint-disable-next-line @typescript-eslint/naming-convention
    res.writeHead(200, { 'Content-Type': '\ttext/csv' });
    downloadStream.pipe(res);

    // Handle errors in the file stream
    downloadStream.on('error', (err) => {
        logger.error('File stream error:', err);
        // eslint-disable-next-line @typescript-eslint/naming-convention
        res.writeHead(500, { 'Content-Type': 'text/plain' });
        fs.unlinkSync(downloadFile);
        res.end('Server Error');
    });

    // Optionally listen for the end of the stream
    downloadStream.on('end', () => {
        fs.unlinkSync(downloadFile);
        logger.debug('File stream ended');
    });
};

export const downloadCubeAsParquet = async (req: Request, res: Response, next: NextFunction) => {
    const dataset = res.locals.dataset;
    const lang = req.language.split('-')[0];
    const latestRevision = getLatestRevision(dataset);
    if (!latestRevision) {
        next(new UnknownException('errors.no_revision'));
        return;
    }
    let cubeFile: string;
    if (latestRevision.onlineCubeFilename) {
        const dataLakeService = new DataLakeService();
        const fileBuffer = await dataLakeService.getFileBuffer(latestRevision.onlineCubeFilename, dataset.id);
        cubeFile = tmp.tmpNameSync({ postfix: '.duckdb' });
        fs.writeFileSync(cubeFile, fileBuffer);
    } else {
        try {
            cubeFile = await createBaseCube(dataset, latestRevision);
        } catch (err) {
            logger.error(`Something went wrong trying to create the cube with the error: ${err}`);
            next(new UnknownException('errors.cube_create_error'));
            return;
        }
    }
    const downloadFile = await outputCube(cubeFile, lang, DuckdbOutputType.Parquet);
    fs.unlinkSync(cubeFile);
    const downloadStream = fs.createReadStream(downloadFile);
    // eslint-disable-next-line @typescript-eslint/naming-convention
    res.writeHead(200, { 'Content-Type': '\tapplication/vnd.apache.parquet' });
    downloadStream.pipe(res);

    // Handle errors in the file stream
    downloadStream.on('error', (err) => {
        logger.error('File stream error:', err);
        // eslint-disable-next-line @typescript-eslint/naming-convention
        res.writeHead(500, { 'Content-Type': 'text/plain' });
        fs.unlinkSync(downloadFile);
        res.end('Server Error');
    });

    // Optionally listen for the end of the stream
    downloadStream.on('end', () => {
        fs.unlinkSync(downloadFile);
        logger.debug('File stream ended');
    });
};

export const downloadCubeAsExcel = async (req: Request, res: Response, next: NextFunction) => {
    const dataset = res.locals.dataset;
    const lang = req.language.split('-')[0];
    const latestRevision = getLatestRevision(dataset);
    if (!latestRevision) {
        next(new UnknownException('errors.no_revision'));
        return;
    }
    let cubeFile: string;
    if (latestRevision.onlineCubeFilename) {
        const dataLakeService = new DataLakeService();
        const fileBuffer = await dataLakeService.getFileBuffer(latestRevision.onlineCubeFilename, dataset.id);
        cubeFile = tmp.tmpNameSync({ postfix: '.duckdb' });
        fs.writeFileSync(cubeFile, fileBuffer);
    } else {
        try {
            cubeFile = await createBaseCube(dataset, latestRevision);
        } catch (err) {
            logger.error(`Something went wrong trying to create the cube with the error: ${err}`);
            next(new UnknownException('errors.cube_create_error'));
            return;
        }
    }
    const downloadFile = await outputCube(cubeFile, lang, DuckdbOutputType.Excel);
    logger.info(`Cube file located at: ${cubeFile}`);
    // fs.unlinkSync(cubeFile);
    const downloadStream = fs.createReadStream(downloadFile);
    // eslint-disable-next-line @typescript-eslint/naming-convention
    res.writeHead(200, { 'Content-Type': '\tapplication/vnd.ms-excel' });
    downloadStream.pipe(res);

    // Handle errors in the file stream
    downloadStream.on('error', (err) => {
        logger.error('File stream error:', err);
        // eslint-disable-next-line @typescript-eslint/naming-convention
        res.writeHead(500, { 'Content-Type': 'text/plain' });
        fs.unlinkSync(downloadFile);
        res.end('Server Error');
    });

    // Optionally listen for the end of the stream
    downloadStream.on('end', () => {
        fs.unlinkSync(downloadFile);
        logger.debug('File stream ended');
    });
};
