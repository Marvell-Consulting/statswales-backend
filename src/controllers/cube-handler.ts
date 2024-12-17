import fs from 'node:fs';

import tmp, { FileResult } from 'tmp';
import { Database } from 'duckdb-async';
import { t } from 'i18next';

import { Dataset } from '../entities/dataset/dataset';
import { FactTableAction } from '../enums/fact-table-action';
import { DataLakeService } from '../services/datalake';
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

import { dateDimensionReferenceTableCreator } from './time-matching';
// eslint-disable-next-line import/no-cycle
import { LookupTableExtractor } from './lookup-table-handler';

export const FACT_TABLE_NAME = 'fact_table';

export const makeCubeSafeString = (str: string): string => {
    return str
        .toLowerCase()
        .replace(/[ ]/g, '_')
        .replace(/[^a-zA-Z_]/g, '');
};

export const getFileImportAndSaveToDisk = async (dataset: Dataset, importFile: FileImport): Promise<FileResult> => {
    const dataLakeService = new DataLakeService();
    const importTmpFile = tmp.fileSync({ postfix: `.${importFile.fileType}` });
    const buffer = await dataLakeService.getFileBuffer(importFile.filename, dataset.id);
    fs.writeFileSync(importTmpFile.name, buffer);
    return importTmpFile;
};

// This function creates a table in a duckdb database based on a file and loads the files contents directly into the table
export const loadFileIntoDatabase = async (
    quack: Database,
    fileImport: FileImport,
    tempFile: FileResult,
    tableName: string
) => {
    let createTableQuery: string;
    switch (fileImport.fileType) {
        case FileType.Csv:
        case FileType.GzipCsv:
            createTableQuery = `CREATE TABLE ${tableName} AS SELECT * FROM read_csv('${tempFile.name}', auto_type_candidates = ['BOOLEAN', 'BIGINT', 'DOUBLE', 'VARCHAR']);`;
            break;
        case FileType.Parquet:
            createTableQuery = `CREATE TABLE ${tableName} AS SELECT * FROM '${tempFile.name}';`;
            break;
        case FileType.Json:
        case FileType.GzipJson:
            createTableQuery = `CREATE TABLE ${tableName} AS SELECT * FROM read_json_auto('${tempFile.name}');`;
            break;
        case FileType.Excel:
            await quack.exec('INSTALL spatial;');
            await quack.exec('LOAD spatial;');
            createTableQuery = `CREATE TABLE ${tableName} AS SELECT * FROM st_read('${tempFile.name}');`;
            break;
        default:
            throw new Error('Unknown file type');
    }
    await quack.exec(createTableQuery);
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
    await quack.exec(insertQuery);
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
        await loadFileIntoDatabase(
            quack,
            dimension.lookupTable,
            lookupTableFile,
            `${makeCubeSafeString(dimension.factTableColumn)}_lookup_sw2`
        );
        let sortOrderCol = '';
        if (extractor.sortColumn) {
            sortOrderCol = `${extractor.sortColumn}, `;
        }
        const viewParts: string[] = SUPPORTED_LOCALES.map((locale) => {
            const descriptionCol = extractor.descriptionColumns.find((col) => col.lang === locale.split('-')[0]);
            const descriptionColStr = descriptionCol ? `${descriptionCol.name} as description, ` : '';
            const notesCol = extractor.notesColumns.find((col) => col.lang === locale.split('-')[0]);
            const notesColStr = notesCol ? `${notesCol.name} as notes, ` : '';
            return (
                `SELECT "${dimension.joinColumn}", ${sortOrderCol} '${locale.toLowerCase()}' as language,\n` +
                `${descriptionColStr} ${notesColStr} from ${makeCubeSafeString(dimension.factTableColumn)}_lookup_sw2`
            );
        });
        await quack.exec(`CREATE VIEW ${dimension.factTableColumn}_lookup AS ${viewParts.join('\nUNION\n')};`);
    } else {
        await loadFileIntoDatabase(
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

// Builds a fresh cube based on all revisions and returns the file pointer
// to the duckdb file on disk.  This is based on the recipe in our cube miro
// board and our candidate cube format repo.  It is limited to building a
// simple default view based on the available locales.
export const createBaseCube = async (dataset: Dataset, endRevision: Revision): Promise<string> => {
    const firstRevision = dataset.revisions.find((rev) => rev.revisionIndex === 1);
    if (!firstRevision) {
        throw new Error(`Unable to find first revision for dataset ${dataset.id}`);
    }
    const firstFactTable = firstRevision.factTables[0];
    const quack = await Database.create(':memory:');
    const compositeKey: string[] = [];
    const factIdentifiers: FactTableInfo[] = [];
    let notesCodeColumn: FactTableInfo | undefined;
    let dataValuesColumn: FactTableInfo | undefined;
    let measureColumn: FactTableInfo | undefined;
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

    if (!notesCodeColumn) {
        throw Error(`No column representing notes codes was found`);
    }
    if (!dataValuesColumn) {
        throw Error(`No column representing data was found`);
    }
    logger.info('Creating fact table in cube');
    try {
        await quack.exec(
            `CREATE TABLE ${FACT_TABLE_NAME} (${factTableDef.join(', ')}, PRIMARY KEY (${compositeKey.join(', ')}));`
        );
    } catch (err) {
        logger.error(`Failed to create fact table in cube: ${err}`);
        await quack.close();
        throw new Error(`Failed to create fact table in cube: ${err}`);
    }

    // Find all the fact tables for the given revision
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

    // Process all the fact tables
    logger.debug('Loading all fact tables in to database');
    try {
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
    } catch (error) {
        logger.error(`Something went wrong trying to create the core fact table with error: ${error}`);
        await quack.close();
        throw error;
    }

    const selectStatementsMap = new Map<Locale, string[]>();
    SUPPORTED_LOCALES.map((locale) => selectStatementsMap.set(locale, []));
    const joinStatements: string[] = [];
    const orderByStatements: string[] = [];

    logger.info('Setting up measure table if present...');
    // Process the column that represents the measure
    if (measureColumn && dataset.measure && dataset.measure.joinColumn) {
        // If we parsed the lookup table or the user
        // has used a user journey to define measures
        // use this first
        if (dataset.measure.measureInfo.length > 0) {
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
            const measureFile = await getFileImportAndSaveToDisk(dataset, dataset.measure.lookupTable);
            if (dataset.measure.lookupTable.isStatsWales2Format) {
                try {
                    await quack.exec(`INSERT INTO measure_sw2 SELECT * FROM '${measureFile.name}`);
                    const viewComponents: string[] = [];
                    for (const locale of SUPPORTED_LOCALES) {
                        viewComponents.push(
                            `SELECT MeasureCode as measure_id, sort_order, '${locale.toLowerCase()}' AS language, "Description_${locale.split('-')[0]}" AS description, "Format" AS format, data_type AS data_type FROM measure_sw2\n`
                        );
                    }
                    const buildMeasureViewQuery = `CREATE VIEW measure AS SELECT * FROM ${measureFile.name}\n${viewComponents.join('\nUNION\n')};`;
                    await quack.exec(buildMeasureViewQuery);
                } catch (error) {
                    logger.error(`Unable to create or load measure table in to the cube with error: ${error}`);
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
        SUPPORTED_LOCALES.map((locale) => {
            selectStatementsMap
                .get(locale)
                ?.push(
                    `measure_id as ${dataset.measure.measureInfo.find((info) => info.language === locale.toLowerCase())?.description || dataset.measure.factTableColumn}`
                );
            // UPDATE THIS TO SUPPORT SPECIFIC DISPLAY TYPES
            if (dataValuesColumn)
                selectStatementsMap
                    .get(locale)
                    ?.push(
                        `${FACT_TABLE_NAME}."${dataValuesColumn?.columnName}" as "${t('column_headers.data_values', { lng: locale })}"`
                    );
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
        });
    }

    for (const dimension of dataset.dimensions) {
        logger.info(`Setting up dimension ${dimension.id} for fact table column ${dimension.factTableColumn}`);
        const dimTable = `${makeCubeSafeString(dimension.factTableColumn)}_lookup`;
        try {
            switch (dimension.type) {
                case DimensionType.TimePeriod:
                case DimensionType.TimePoint:
                    await createAndValidateDateDimension(quack, dimension.extractor, dimension.factTableColumn);
                    SUPPORTED_LOCALES.map((locale) => {
                        const columnName =
                            dimension.dimensionInfo.find((info) => info.language === locale)?.name ||
                            dimension.factTableColumn;
                        selectStatementsMap.get(locale)?.push(`${dimTable}.description as "${columnName}"`);
                        selectStatementsMap
                            .get(locale)
                            ?.push(`${dimTable}.start_date as "${t('column_headers.start_date', { lng: locale })}"`);
                        selectStatementsMap
                            .get(locale)
                            ?.push(`${dimTable}.end_date as "${t('column_headers.end_date', { lng: locale })}"`);
                    });
                    joinStatements.push(
                        `LEFT JOIN ${dimTable} on ${dimTable}."${dimension.joinColumn}"=${FACT_TABLE_NAME}."${dimension.factTableColumn}"`
                    );
                    orderByStatements.push(`${dimTable}.end_date`);
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
                    orderByStatements.push(`${dimTable}.sort_order`);
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

    SUPPORTED_LOCALES.map((locale) => {
        if (notesCodeColumn)
            selectStatementsMap
                .get(locale)
                ?.push(
                    `${FACT_TABLE_NAME}."${notesCodeColumn?.columnName}" as "${t('column_headers.note_codes', { lng: locale })}"`
                );
    });

    logger.info(`Creating default views...`);
    // Build the default views
    for (const locale of SUPPORTED_LOCALES) {
        const defaultViewSQL = `CREATE VIEW default_view_${locale.toLowerCase().split('-')[0]} AS SELECT\n${selectStatementsMap
            .get(locale)
            ?.join(
                ',\n'
            )} FROM ${FACT_TABLE_NAME}\n${joinStatements.join('\n').replace('#LANG#', locale.toLowerCase())}\n ${orderByStatements.length > 0 ? `ORDER BY ${orderByStatements.join(', ')}` : ''};`;
        await quack.exec(defaultViewSQL);
    }
    logger.debug(`Writing memory database to disk`);
    const tmpFile = tmp.tmpNameSync({ postfix: '.db' });
    logger.debug(`Writing memory database to disk at ${tmpFile}`);
    await quack.exec(`ATTACH '${tmpFile}' as outDB;`);
    await quack.exec(`COPY FROM DATABASE memory TO outDB;`);
    await quack.exec('DETACH outDB;');
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
