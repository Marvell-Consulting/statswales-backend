import fs from 'node:fs';
import path from 'path';

import { Database, DuckDbError } from 'duckdb-async';
import tmp from 'tmp';
import { t } from 'i18next';

import { FileType } from '../enums/file-type';
import { FileImportInterface } from '../entities/dataset/file-import.interface';
import { logger } from '../utils/logger';
import { Dataset } from '../entities/dataset/dataset';
import { Dimension } from '../entities/dataset/dimension';
import { LookupTableExtractor } from '../extractors/lookup-table-extractor';
import { getFileImportAndSaveToDisk } from '../utils/file-utils';
import { SUPPORTED_LOCALES } from '../middleware/translation';
import { DataTable } from '../entities/dataset/data-table';
import { DataTableAction } from '../enums/data-table-action';
import { Revision } from '../entities/dataset/revision';
import { Locale } from '../enums/locale';
import { MeasureLookupTableExtractor } from '../extractors/measure-lookup-extractor';
import { DimensionType } from '../enums/dimension-type';
import { FactTableColumnType } from '../enums/fact-table-column-type';
import { ReferenceDataExtractor } from '../extractors/reference-data-extractor';
import { FactTable } from '../entities/dataset/fact-table';
import { CubeValidationException, CubeValidationType } from '../exceptions/cube-error-exception';
import { DataTableDescription } from '../entities/dataset/data-table-description';

import { dateDimensionReferenceTableCreator } from './time-matching';
import { duckdb } from './duckdb';

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
            return `CREATE TABLE ${tableName} AS SELECT * FROM read_csv('${tempFileName}', auto_type_candidates = ['BIGINT', 'DOUBLE', 'VARCHAR']);`;
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
    fileImport: FileImportInterface,
    tempFile: string,
    tableName: string
) => {
    const insertQuery = await createFactTableQuery(tableName, tempFile, fileImport.fileType, quack);
    try {
        await quack.exec(insertQuery);
    } catch (error) {
        logger.error(
            `Failed to load file in to the cube using query ${insertQuery} with the following error: ${error}`
        );
        throw error;
    }
};

function parseKeyValueString<T extends Record<string, any>>(str: string): T {
    return str.split(',').reduce((acc, pair) => {
        // e.g. "YearCode: 201314"
        const [key, value] = pair.split(':').map((part) => part.trim());
        // Attempt to convert to a number if it looks numeric
        const numValue = Number(value);
        (acc as any)[key] = isNaN(numValue) ? value : numValue;
        return acc;
    }, {} as T);
}

// This function differs from loadFileIntoDatabase in that it only loads a file into an existing table
export const loadFileDataTableIntoTable = async (
    quack: Database,
    dataTable: DataTable,
    factTableDef: string[],
    tempFile: string,
    tableName: string
) => {
    let insertQuery: string;
    const dataTableColumnSelect: string[] = [];
    for (const factTableCol of factTableDef) {
        const dataTableCol = dataTable.dataTableDescriptions.find(
            (col) => col.factTableColumn === factTableCol
        )?.columnName;
        if (dataTableCol) dataTableColumnSelect.push(dataTableCol);
        else dataTableColumnSelect.push(factTableCol);
    }
    switch (dataTable.fileType) {
        case FileType.Csv:
        case FileType.GzipCsv:
            insertQuery = `INSERT INTO ${tableName} (${factTableDef.join(',')}) SELECT ${dataTableColumnSelect.join(',')} FROM read_csv('${tempFile}', auto_type_candidates = ['BOOLEAN', 'BIGINT', 'DOUBLE', 'VARCHAR']);`;
            break;
        case FileType.Parquet:
            insertQuery = `INSERT INTO ${tableName} (${factTableDef.join(',')}) SELECT ${dataTableColumnSelect.join(',')} FROM ${tempFile};`;
            break;
        case FileType.Json:
        case FileType.GzipJson:
            insertQuery = `INSERT INTO ${tableName} (${factTableDef.join(',')}) SELECT ${dataTableColumnSelect.join(',')} FROM read_json_auto('${tempFile}');`;
            break;
        case FileType.Excel:
            insertQuery = `INSERT INTO ${tableName} (${factTableDef.join(',')}) SELECT ${dataTableColumnSelect.join(',')} FROM st_read('${tempFile}');`;
            break;
        default:
            throw new Error('Unknown file type');
    }
    try {
        logger.debug(`Loading file data table into table ${tableName} with query: ${insertQuery}`);
        await quack.exec(insertQuery);
    } catch (error) {
        logger.error(`Failed to load file into table using query ${insertQuery} with the following error: ${error}`);
        const duckDBError = error as DuckDbError;
        if (duckDBError.errorType === 'Constraint') {
            const err = new CubeValidationException('Failed to load data table in to the cube due to a duplicate fact');
            err.type = CubeValidationType.DuplicateFact;
            err.stack = duckDBError.stack;
            const keyGrep = /"[^"]*"/gu;
            const key = keyGrep.exec(duckDBError.message);
            if (key) {
                err.fact = parseKeyValueString(key[0]);
            }
            throw err;
        }
        throw error;
    }
};

async function createReferenceDataTablesInCube(quack: Database) {
    logger.debug('Creating empty reference data tables');
    try {
        logger.debug('Creating categories tables');
        await quack.exec(`CREATE TABLE "categories" ("category" TEXT PRIMARY KEY);`);
        logger.debug('Creating category_keys table');
        await quack.exec(`CREATE TABLE "category_keys" (
                            "category_key" TEXT PRIMARY KEY,
                            "category" TEXT NOT NULL,
                            );`);
        logger.debug('Creating reference_data table');
        await quack.exec(`CREATE TABLE "reference_data" (
                            "item_id" TEXT NOT NULL,
                            "version_no" INTEGER NOT NULL,
                            "sort_order" INTEGER,
                            "category_key" TEXT NOT NULL,
                            "validity_start" TEXT NOT NULL,
                            "validity_end" TEXT,
                            PRIMARY KEY("item_id","version_no","category_key"),
                            );`);
        logger.debug('Creating reference_data_all table');
        await quack.exec(`CREATE TABLE "reference_data_all" (
                            "item_id" TEXT NOT NULL,
                            "version_no" INTEGER NOT NULL,
                            "sort_order" INTEGER,
                            "category_key" TEXT NOT NULL,
                            "validity_start" TEXT NOT NULL,
                            "validity_end" TEXT,
                            PRIMARY KEY("item_id","version_no","category_key"),
                            );`);
        logger.debug('Creating reference_data_info table');
        await quack.exec(`CREATE TABLE "reference_data_info" (
                            "item_id" TEXT NOT NULL,
                            "version_no" INTEGER NOT NULL,
                            "category_key" TEXT NOT NULL,
                            "lang" TEXT NOT NULL,
                            "description" TEXT NOT NULL,
                            "notes" TEXT,
                            PRIMARY KEY("item_id","version_no","category_key","lang"),
                            );`);
        logger.debug('Creating category_key_info table');
        await quack.exec(`CREATE TABLE "category_key_info" (
                            "category_key" TEXT NOT NULL,
                            "lang" TEXT NOT NULL,
                            "description" TEXT NOT NULL,
                            "notes" TEXT,
                            PRIMARY KEY("category_key","lang"),
                            );`);
        logger.debug('Creating category_info table');
        await quack.exec(`CREATE TABLE "category_info" (
                            "category" TEXT NOT NULL,
                            "lang" TEXT NOT NULL,
                            "description" TEXT NOT NULL,
                            "notes" TEXT,
                            PRIMARY KEY("category","lang"),
                            );`);
        logger.debug('Creating hierarchy table');
        await quack.exec(`CREATE TABLE "hierarchy" (
                            "item_id" TEXT NOT NULL,
                            "version_no" INTEGER NOT NULL,
                            "category_key" TEXT NOT NULL,
                            "parent_id" TEXT NOT NULL,
                            "parent_version" INTEGER NOT NULL,
                            "parent_category" TEXT NOT NULL,
                            PRIMARY KEY("item_id","version_no","category_key","parent_id","parent_version","parent_category")
                            );`);
    } catch (error) {
        logger.error(`Something went wrong trying to create the initial reference data tables with error: ${error}`);
        throw new Error(`Something went wrong trying to create the initial reference data tables with error: ${error}`);
    }
}

export async function loadReferenceDataFromCSV(quack: Database) {
    logger.debug(`Loading reference data from CSV`);
    logger.debug(`Loading categories from CSV`);
    await quack.exec(
        `COPY categories FROM '${path.resolve(__dirname, `../resources/reference-data/v1/categories.csv`)}';`
    );
    logger.debug(`Loading category_keys from CSV`);
    await quack.exec(
        `COPY category_keys FROM '${path.resolve(__dirname, `../resources/reference-data/v1/category_key.csv`)}';`
    );
    logger.debug(`Loading reference_data_all from CSV`);
    await quack.exec(
        `COPY reference_data_all FROM '${path.resolve(__dirname, `../resources/reference-data/v1/reference_data.csv`)}';`
    );
    logger.debug(`Loading reference_data_info from CSV`);
    await quack.exec(
        `COPY reference_data_info FROM '${path.resolve(__dirname, `../resources/reference-data/v1/reference_data_info.csv`)}';`
    );
    logger.debug(`Loading category_key_info from CSV`);
    await quack.exec(
        `COPY category_key_info FROM '${path.resolve(__dirname, `../resources/reference-data/v1/category_key_info.csv`)}';`
    );
    logger.debug(`Loading category_info from CSV`);
    await quack.exec(
        `COPY category_info FROM '${path.resolve(__dirname, `../resources/reference-data/v1/category_info.csv`)}';`
    );
    logger.debug(`Loading hierarchy from CSV`);
    await quack.exec(
        `COPY hierarchy FROM '${path.resolve(__dirname, `../resources/reference-data/v1/hierarchy.csv`)}';`
    );
}

export const loadReferenceDataIntoCube = async (quack: Database) => {
    await createReferenceDataTablesInCube(quack);
    await loadReferenceDataFromCSV(quack);
    logger.debug(`Reference data tables created and populated successfully.`);
};

export const cleanUpReferenceDataTables = async (quack: Database) => {
    await quack.exec('DROP TABLE reference_data_all;');
    await quack.exec('DELETE FROM reference_data_info WHERE item_id NOT IN (SELECT item_id FROM reference_data);');
    await quack.exec('DELETE FROM category_keys WHERE category_key NOT IN (SELECT category_key FROM reference_data);');
    await quack.exec(
        'DELETE FROM category_Key_info WHERE category_key NOT IN (select category_key FROM category_keys);'
    );
    await quack.exec('DELETE FROM categories where category NOT IN (SELECT category FROM category_keys);');
    await quack.exec('DELETE FROM category_info WHERE category NOT IN (SELECT category FROM categories);');
    await quack.exec('DELETE FROM hierarchy WHERE item_id NOT IN (SELECT item_id FROM reference_data);');
};

export const loadCorrectReferenceDataIntoReferenceDataTable = async (quack: Database, dimension: Dimension) => {
    const extractor = dimension.extractor as ReferenceDataExtractor;
    for (const category of extractor.categories) {
        const categoryPresent = await quack.all(
            `SELECT DISTINCT category_key FROM reference_data WHERE category_key='${category}';`
        );
        if (categoryPresent.length > 0) {
            continue;
        }
        logger.debug(`Copying ${category} reference data in to reference_data table`);
        await quack.exec(
            `INSERT INTO reference_data (SELECT * FROM reference_data_all WHERE category_key='${category}');`
        );
    }
};

// This is a short version of validate date dimension code found in the dimension processor.
// This concise version doesn't return any information on why the creation failed.  Just that it failed
export async function createAndValidateDateDimension(
    quack: Database,
    extractor: object | null,
    factTableColumn: string
) {
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
        const err = new CubeValidationException('Failed to validate date dimension');
        err.type = CubeValidationType.Dimension;
        throw err;
    }
    return `${makeCubeSafeString(factTableColumn)}_lookup`;
}

// This is a short version of the validate lookup table code found in the dimension process.
// This concise version doesn't return any information on why the creation failed.  Just that it failed
export async function createAndValidateLookupTableDimension(quack: Database, dataset: Dataset, dimension: Dimension) {
    logger.debug(`Creating and validating lookup table dimension ${dimension.factTableColumn}`);
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
        const err = new CubeValidationException('Failed to validate lookup table dimension');
        err.type = CubeValidationType.DimensionNonMatchedRows;
        throw err;
    }
}

function setupFactTableUpdateJoins(
    factTableName: string,
    factIdentifiers: FactTable[],
    dataTableIdentifiers: DataTableDescription[]
): string {
    const joinParts: string[] = [];
    for (const factTableCol of factIdentifiers) {
        const dataTableCol = dataTableIdentifiers.find((col) => col.factTableColumn === factTableCol.columnName);
        joinParts.push(`${factTableName}."${factTableCol.columnName}"=update_table."${dataTableCol?.columnName}"`);
    }
    return joinParts.join(' AND ');
}

async function loadFactTablesWithUpdates(
    quack: Database,
    dataset: Dataset,
    allDataTables: DataTable[],
    factTableDef: string[],
    dataValuesColumn: FactTable,
    notesCodeColumn: FactTable,
    factIdentifiers: FactTable[]
) {
    for (const dataTable of allDataTables.sort((ftA, ftB) => ftA.uploadedAt.getTime() - ftB.uploadedAt.getTime())) {
        logger.info(`Loading fact table data for fact table ${dataTable.id}`);
        const factTableFile: string = await getFileImportAndSaveToDisk(dataset, dataTable);
        const updateTableDataCol = dataTable.dataTableDescriptions.find(
            (col) => col.factTableColumn === dataValuesColumn.columnName
        )?.columnName;
        const updateQuery = `UPDATE ${FACT_TABLE_NAME} SET "${dataValuesColumn.columnName}"=update_table."${updateTableDataCol}",
             "${notesCodeColumn.columnName}"=(CASE
                WHEN ${FACT_TABLE_NAME}."${notesCodeColumn.columnName}" IS NULL THEN 'r'
                WHEN ${FACT_TABLE_NAME}."${notesCodeColumn.columnName}" LIKE '%r%' THEN ${FACT_TABLE_NAME}."${notesCodeColumn.columnName}"
                ELSE concat(${FACT_TABLE_NAME}."${notesCodeColumn.columnName}", ',r') END)
             FROM update_table WHERE ${setupFactTableUpdateJoins(FACT_TABLE_NAME, factIdentifiers, dataTable.dataTableDescriptions)}
             AND ${FACT_TABLE_NAME}."${dataValuesColumn.columnName}"!=update_table."${updateTableDataCol}";`;
        const dataTableColumnSelect: string[] = [];
        for (const factTableCol of factTableDef) {
            const dataTableCol = dataTable.dataTableDescriptions.find(
                (col) => col.factTableColumn === factTableCol
            )?.columnName;
            if (dataTableCol) dataTableColumnSelect.push(dataTableCol);
        }
        try {
            switch (dataTable.action) {
                case DataTableAction.ReplaceAll:
                    await quack.exec(`DELETE FROM ${FACT_TABLE_NAME};`);
                    await loadFileDataTableIntoTable(quack, dataTable, factTableDef, factTableFile, FACT_TABLE_NAME);
                    break;
                case DataTableAction.Add:
                    await loadFileDataTableIntoTable(quack, dataTable, factTableDef, factTableFile, FACT_TABLE_NAME);
                    break;
                case DataTableAction.Revise:
                    await loadFileIntoCube(quack, dataTable, factTableFile, 'update_table');
                    await quack.exec(updateQuery);
                    await quack.exec(`DROP TABLE update_table;`);
                    break;
                case DataTableAction.AddRevise:
                    await loadFileIntoCube(quack, dataTable, factTableFile, 'update_table');
                    logger.debug(`Executing update query: ${updateQuery}`);
                    await quack.exec(updateQuery);
                    await quack.exec(
                        `DELETE FROM update_table USING ${FACT_TABLE_NAME} WHERE ${setupFactTableUpdateJoins(FACT_TABLE_NAME, factIdentifiers, dataTable.dataTableDescriptions)};`
                    );
                    await quack.exec(
                        `INSERT INTO ${FACT_TABLE_NAME} (${factTableDef.join(', ')}) (SELECT ${dataTableColumnSelect.join(', ')} FROM update_table);`
                    );
                    await quack.exec(`DROP TABLE update_table;`);
                    break;
            }
        } finally {
            fs.unlinkSync(factTableFile);
        }
    }
}

async function loadFactTablesWithoutUpdates(
    quack: Database,
    dataset: Dataset,
    factTableDef: string[],
    allFactTables: DataTable[]
) {
    logger.warn(
        'There is no notes column present in this dataset.  Action allowed are limited to adding data and replacing all data'
    );
    for (const factTable of allFactTables.sort((ftA, ftB) => ftA.uploadedAt.getTime() - ftB.uploadedAt.getTime())) {
        logger.info(`Loading fact table data for fact table ${factTable.id}`);
        const factTableFile = await getFileImportAndSaveToDisk(dataset, factTable);
        switch (factTable.action) {
            case DataTableAction.ReplaceAll:
                await quack.exec(`DELETE FROM ${FACT_TABLE_NAME};`);
                await loadFileDataTableIntoTable(quack, factTable, factTableDef, factTableFile, FACT_TABLE_NAME);
                break;
            case DataTableAction.Add:
                await loadFileDataTableIntoTable(quack, factTable, factTableDef, factTableFile, FACT_TABLE_NAME);
                break;
        }
        fs.unlinkSync(factTableFile);
    }
}

export async function loadFactTables(
    quack: Database,
    dataset: Dataset,
    endRevision: Revision,
    factTableDef: string[],
    dataValuesColumn: FactTable | undefined,
    notesCodeColumn: FactTable | undefined,
    factIdentifiers: FactTable[]
): Promise<void> {
    // Find all the fact tables for the given revision
    logger.debug('Finding all fact tables for this revision and those that came before');
    const allFactTables: DataTable[] = [];
    if (endRevision.revisionIndex && endRevision.revisionIndex > 0) {
        // If we have a revision index we start here
        const validRevisions = dataset.revisions.filter(
            (rev) => rev.revisionIndex <= endRevision.revisionIndex && rev.revisionIndex > 0
        );
        validRevisions.forEach((revision) => {
            if (revision.dataTable) allFactTables.push(revision.dataTable);
        });
    } else {
        logger.debug('Must be a draft revision, so we need to find all revisions before this one');
        // If we don't have a revision index we need to find the previous revision to this one that does
        if (endRevision.dataTable) {
            logger.debug('Adding end revision to list of fact tables');
            allFactTables.push(endRevision.dataTable);
        }
        const validRevisions = dataset.revisions.filter((rev) => rev.revisionIndex > 0);
        validRevisions.forEach((revision) => {
            if (revision.dataTable) allFactTables.push(revision.dataTable);
        });
    }

    if (allFactTables.length === 0) {
        logger.error(`No fact tables found in this dataset to revision ${endRevision.id}`);
        throw new Error(`No fact tables found in this dataset to revision ${endRevision.id}`);
    }

    // Process all the fact tables
    logger.debug(`Loading ${allFactTables.length} fact tables in to database`);
    try {
        if (dataValuesColumn && notesCodeColumn) {
            await loadFactTablesWithUpdates(
                quack,
                dataset,
                allFactTables.reverse(),
                factTableDef,
                dataValuesColumn,
                notesCodeColumn,
                factIdentifiers
            );
        } else {
            await loadFactTablesWithoutUpdates(quack, dataset, factTableDef, allFactTables);
        }
    } catch (error) {
        if (error instanceof CubeValidationException) {
            throw error;
        }
        const err = new CubeValidationException('Something went wrong trying to create the core fact table');
        err.type = CubeValidationType.FactTable;
        err.stack = (error as Error).stack;
        err.originalError = (error as Error).message;
        logger.error(`Something went wrong trying to create the core fact table with error: ${error}`);
        await quack.close();
        throw err;
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
    notesColumn: FactTable,
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
    dataValuesColumn: FactTable | undefined,
    selectStatementsMap: Map<Locale, string[]>,
    joinStatements: string[],
    orderByStatements: string[],
    measureColumn?: FactTable
) {
    logger.info('Setting up measure table if present...');
    logger.debug(`Dataset Measure = ${JSON.stringify(dataset.measure)}`);
    logger.debug(`Measure column = ${JSON.stringify(measureColumn)}`);
    // Process the column that represents the measure
    if (measureColumn && dataset.measure && dataset.measure.joinColumn) {
        logger.debug('Measure present in dataset.  Creating measure table...');
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
            fs.unlinkSync(measureFile);
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
        caseStatement.push(`ELSE CAST(${FACT_TABLE_NAME}."${dataValuesColumn?.columnName}" AS VARCHAR) END`);
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
        const languageColumn = (dataset.measure.extractor as MeasureLookupTableExtractor).languageColumn || 'language';
        joinStatements.push(
            `LEFT JOIN measure on CAST (measure.measure_id AS VARCHAR)=CAST(${FACT_TABLE_NAME}.${dataset.measure.factTableColumn} AS VARCHAR) AND measure."${languageColumn}"='#LANG#'`
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
    endRevision: Revision,
    selectStatementsMap: Map<Locale, string[]>,
    joinStatements: string[],
    orderByStatements: string[]
) {
    logger.info('Setting up dimension tables...');
    for (const dimension of dataset.dimensions) {
        logger.info(`Setting up dimension ${dimension.id} for fact table column ${dimension.factTableColumn}`);
        const dimTable = `${makeCubeSafeString(dimension.factTableColumn)}_lookup`;
        let languageColumn = 'lang';
        try {
            switch (dimension.type) {
                case DimensionType.TimePeriod:
                case DimensionType.TimePoint:
                    if (dimension.extractor) {
                        await createAndValidateDateDimension(quack, dimension.extractor, dimension.factTableColumn);
                        SUPPORTED_LOCALES.map((locale) => {
                            const columnName =
                                dimension.metadata.find((info) => info.language === locale)?.name ||
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
                            `LEFT JOIN ${dimTable} on CAST(${dimTable}."${dimension.joinColumn}" AS VARCHAR)=CAST(${FACT_TABLE_NAME}."${dimension.factTableColumn}" AS VARCHAR)`
                        );
                        orderByStatements.push(`${dimTable}.end_date`);
                    } else {
                        SUPPORTED_LOCALES.map((locale) => {
                            const columnName =
                                dimension.metadata.find((info) => info.language === locale)?.name ||
                                dimension.factTableColumn;
                            selectStatementsMap.get(locale)?.push(`${dimension.factTableColumn} as "${columnName}"`);
                        });
                    }
                    break;
                case DimensionType.LookupTable:
                    // To allow preview to continue working for dimensions which are in progress
                    // we check to see if there's a task for the dimension and if its been update
                    // if its been update we skip it.
                    if (endRevision.tasks) {
                        const updateInProgressDimension = endRevision.tasks.dimensions.find(
                            (dim) => dim.id === dimension.id
                        );
                        if (updateInProgressDimension && !updateInProgressDimension.lookupTableUpdated) {
                            logger.warn(`Skipping dimension ${dimension.id} as it has not been updated`);
                            SUPPORTED_LOCALES.map((locale) => {
                                const columnName =
                                    dimension.metadata.find((info) => info.language === locale)?.name ||
                                    dimension.factTableColumn;
                                selectStatementsMap
                                    .get(locale)
                                    ?.push(`${dimension.factTableColumn} as "${columnName}"`);
                            });
                            continue;
                        }
                    }

                    await createAndValidateLookupTableDimension(quack, dataset, dimension);
                    SUPPORTED_LOCALES.map((locale) => {
                        const columnName =
                            dimension.metadata.find((info) => info.language === locale)?.name ||
                            dimension.factTableColumn;
                        selectStatementsMap.get(locale)?.push(`${dimTable}.description as "${columnName}"`);
                    });
                    languageColumn = (dimension.extractor as LookupTableExtractor).languageColumn || 'language';
                    joinStatements.push(
                        `LEFT JOIN ${dimTable} on CAST(${dimTable}."${dimension.joinColumn}" AS VARCHAR)=CAST(${FACT_TABLE_NAME}."${dimension.factTableColumn}" AS VARCHAR) AND ${dimTable}."${languageColumn}"='#LANG#'`
                    );
                    if ((dimension.extractor as LookupTableExtractor).sortColumn) {
                        orderByStatements.push(
                            `${dimTable}."${(dimension.extractor as LookupTableExtractor).sortColumn}"`
                        );
                    }
                    break;
                case DimensionType.ReferenceData:
                    await loadCorrectReferenceDataIntoReferenceDataTable(quack, dimension);
                    SUPPORTED_LOCALES.map((locale) => {
                        const columnName =
                            dimension.metadata.find((info) => info.language === locale)?.name ||
                            dimension.factTableColumn;
                        selectStatementsMap.get(locale)?.push(`reference_data_info.description as "${columnName}"`);
                    });
                    joinStatements.push(
                        `LEFT JOIN reference_data on CAST(${FACT_TABLE_NAME}."${dimension.factTableColumn}" AS VARCHAR)=reference_data.item_id`
                    );
                    break;
                case DimensionType.Raw:
                case DimensionType.Numeric:
                case DimensionType.Text:
                case DimensionType.Symbol:
                    SUPPORTED_LOCALES.map((locale) => {
                        const columnName =
                            dimension.metadata.find((info) => info.language === locale)?.name ||
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

function referenceDataPresent(dataset: Dataset) {
    if (dataset.dimensions.find((dim) => dim.type === DimensionType.ReferenceData)) {
        return true;
    }
    return false;
}

async function createBaseFactTable(quack: Database, dataset: Dataset) {
    let notesCodeColumn: FactTable | undefined;
    let dataValuesColumn: FactTable | undefined;
    let measureColumn: FactTable | undefined;

    const firstRevision = dataset.revisions.find((rev) => rev.revisionIndex === 1);
    if (!firstRevision) {
        throw new Error(`Unable to find first revision for dataset ${dataset.id}`);
    }
    const factTable = dataset.factTable;
    const compositeKey: string[] = [];
    const factIdentifiers: FactTable[] = [];
    const factTableDef: string[] = [];
    if (!factTable) {
        throw new Error(`Unable to find fact table for dataset ${dataset.id}`);
    }

    const factTableCreationDef = factTable
        .sort((col1, col2) => col1.columnIndex - col2.columnIndex)
        .map((field) => {
            switch (field.columnType) {
                case FactTableColumnType.Measure:
                    measureColumn = field;
                // eslint-disable-next-line no-fallthrough
                case FactTableColumnType.Dimension:
                case FactTableColumnType.Time:
                    compositeKey.push(`"${field.columnName}"`);
                    factIdentifiers.push(field);
                    break;
                case FactTableColumnType.NoteCodes:
                    notesCodeColumn = field;
                    break;
                case FactTableColumnType.DataValues:
                    dataValuesColumn = field;
                    break;
            }
            factTableDef.push(field.columnName);
            return `"${field.columnName}" ${field.columnDatatype}`;
        });

    logger.info('Creating initial fact table in cube');
    try {
        let key = '';
        if (compositeKey.length > 0) {
            key = `, PRIMARY KEY (${compositeKey.join(', ')})`;
        }
        const createTableQuery = `CREATE TABLE ${FACT_TABLE_NAME} (${factTableCreationDef.join(', ')}${key});`;
        logger.debug(`Creating fact table with query: '${createTableQuery}'`);
        await quack.exec(createTableQuery);
    } catch (err) {
        logger.error(`Failed to create fact table in cube: ${err}`);
        await quack.close();
        throw new Error(`Failed to create fact table in cube: ${err}`);
    }
    return { measureColumn, notesCodeColumn, dataValuesColumn, factTableDef, factIdentifiers };
}

export const updateFactTableValidator = async (
    quack: Database,
    dataset: Dataset,
    revision: Revision
): Promise<Database> => {
    const { notesCodeColumn, dataValuesColumn, factTableDef, factIdentifiers } = await createBaseFactTable(
        quack,
        dataset
    );
    await loadFactTables(quack, dataset, revision, factTableDef, dataValuesColumn, notesCodeColumn, factIdentifiers);
    return quack;
};

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

    const firstRevision = dataset.revisions.find((rev) => rev.revisionIndex === 1);
    if (!firstRevision) {
        throw new Error(`Unable to find first revision for dataset ${dataset.id}`);
    }

    logger.debug('Creating an in-memory database to hold the cube using DuckDB 🐤');
    const quack = await duckdb();

    const { measureColumn, notesCodeColumn, dataValuesColumn, factTableDef, factIdentifiers } =
        await createBaseFactTable(quack, dataset);

    await loadFactTables(quack, dataset, endRevision, factTableDef, dataValuesColumn, notesCodeColumn, factIdentifiers);

    await setupMeasures(
        quack,
        dataset,
        dataValuesColumn,
        selectStatementsMap,
        joinStatements,
        orderByStatements,
        measureColumn
    );

    if (referenceDataPresent(dataset)) {
        await loadReferenceDataIntoCube(quack);
    }

    await setupDimensions(quack, dataset, endRevision, selectStatementsMap, joinStatements, orderByStatements);

    if (referenceDataPresent(dataset)) {
        await cleanUpReferenceDataTables(quack);
        joinStatements.push(`JOIN reference_data_info ON reference_data.item_id=reference_data_info.item_id`);
        joinStatements.push(`    AND reference_data.category_key=reference_data_info.category_key`);
        joinStatements.push(`    AND reference_data.version_no=reference_data_info.version_no`);
        joinStatements.push(`    AND reference_data_info.lang='#LANG#'`);
    }

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
        logger.debug(defaultViewSQL);
        await quack.exec(defaultViewSQL);
    }
    const tmpFile = tmp.tmpNameSync({ postfix: '.db' });
    try {
        logger.debug(`Writing memory database to disk at ${tmpFile}`);
        await quack.exec(`ATTACH '${tmpFile}' as outDB (BLOCK_SIZE 16384);`);
        await quack.exec(`COPY FROM DATABASE memory TO outDB;`);
        await quack.exec('DETACH outDB;');
    } catch (err) {
        logger.error(`Failed to write memory database to disk with error: ${err}`);
        throw err;
    } finally {
        await quack.close();
    }
    // Pass the file handle to the calling method
    // If used for preview you just want the file
    // If it's the end of the publishing step you'll
    // want to upload the file to the data lake.
    return tmpFile;
};

export const cleanUpCube = async (tmpFile: string) => {
    logger.debug('Cleaning up cube file');
    if (fs.existsSync(tmpFile)) {
        fs.unlink(tmpFile, async (err) => {
            if (err) logger.error(`Unable to remove file ${tmpFile} with error: ${err}`);
        });
    }
};

export const getCubeDataTable = async (cubeFile: string, lang: string) => {
    const quack = await duckdb(cubeFile);
    try {
        const defaultView = await quack.all(`SELECT * FROM default_view_${lang};`);
        return defaultView;
    } finally {
        await quack.close();
    }
};
