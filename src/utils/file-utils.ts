import { writeFile } from 'node:fs/promises';

import { format as pgformat } from '@scaleleap/pg-format';
import { randomUUID } from 'node:crypto';

import { Dataset } from '../entities/dataset/dataset';
import { FileImportInterface } from '../entities/dataset/file-import.interface';
import { FileType } from '../enums/file-type';

import { logger } from './logger';
import { getFileService } from './get-file-service';
import { asyncTmpName } from './async-tmp';
import { duckdb } from '../services/duckdb';
import { FACT_TABLE_NAME } from '../services/cube-builder';
import { DataTable } from '../entities/dataset/data-table';
import { performance } from 'node:perf_hooks';
import { performanceReporting } from './performance-reporting';
import { LookupTable } from '../entities/dataset/lookup-table';
import { LookupTableExtractor } from '../extractors/lookup-table-extractor';
import { FactTableColumn } from '../entities/dataset/fact-table-column';
import { SUPPORTED_LOCALES, t } from '../middleware/translation';
import { languageMatcherCaseStatement } from './lookup-table-utils';
import { DuckDBConnection } from '@duckdb/node-api';
import { runQueryBlock } from './run-query-block';

export const getFileImportAndSaveToDisk = async (
  dataset: Dataset,
  importFile: FileImportInterface
): Promise<string> => {
  const fileService = getFileService();
  const importTmpFile = await asyncTmpName({ postfix: `.${importFile.fileType}` });
  const buffer = await fileService.loadBuffer(importFile.filename, dataset.id);
  await writeFile(importTmpFile, buffer);
  return importTmpFile;
};

export async function loadFileIntoDataTablesSchema(
  dataset: Dataset,
  dataTable: DataTable,
  filePath?: string
): Promise<void> {
  const start = performance.now();
  const quack = await duckdb();
  let dataTableFile;
  if (filePath) {
    dataTableFile = filePath;
  } else {
    dataTableFile = await getFileImportAndSaveToDisk(dataset, dataTable);
  }
  await loadFileIntoCube(quack, dataTable.fileType, dataTableFile, FACT_TABLE_NAME, 'memory');
  await quack.run(
    pgformat('CREATE TABLE data_tables_db.%I AS SELECT * FROM memory.%I;', dataTable.id, FACT_TABLE_NAME)
  );
  quack.disconnectSync();
  performanceReporting(Math.round(start - performance.now()), 500, 'Loading a data table in to postgres');
}

export const createLookupTableQuery = (
  schemaName: string,
  lookupTableName: string,
  referenceColumnName: string,
  referenceColumnType: string,
  otherColumns?: string[]
): string => {
  const otherColumnsStatement: string[] = [];
  if (otherColumns && otherColumns.length > 0) {
    otherColumns.forEach((column) => {
      otherColumnsStatement.push(pgformat('%I text', column));
    });
  }
  return pgformat(
    'CREATE TABLE %I.%I (%I %s NOT NULL, language VARCHAR(5) NOT NULL, description TEXT NOT NULL, notes TEXT, sort_order INTEGER, hierarchy %s %s);',
    schemaName,
    lookupTableName,
    referenceColumnName,
    referenceColumnType,
    referenceColumnType,
    otherColumnsStatement.length > 0 ? `, ${otherColumnsStatement.join(', ')}` : ''
  );
};

export async function loadFileIntoLookupTablesSchema(
  dataset: Dataset,
  lookupTable: LookupTable,
  extractor: LookupTableExtractor,
  factTableColumn: FactTableColumn,
  joinColumn: string,
  filePath?: string
): Promise<void> {
  const start = performance.now();
  const quack = await duckdb();
  const dimTable = randomUUID();
  await quack.run(
    createLookupTableQuery('memory', dimTable, factTableColumn.columnName, factTableColumn.columnDatatype)
  );
  let lookupTableFile;
  if (filePath) {
    lookupTableFile = filePath;
  } else {
    lookupTableFile = await getFileImportAndSaveToDisk(dataset, lookupTable!);
  }
  const lookupTableName = randomUUID();
  await loadFileIntoCube(quack, lookupTable.fileType, lookupTableFile, lookupTableName, 'memory');
  if (extractor.isSW2Format) {
    logger.debug('Lookup table is SW2 format');
    const dataExtractorParts = [];
    for (const locale of SUPPORTED_LOCALES) {
      const descriptionCol = extractor.descriptionColumns.find(
        (col) => col.lang.toLowerCase() === locale.toLowerCase()
      );
      const notesCol = extractor.notesColumns?.find((col) => col.lang.toLowerCase() === locale.toLowerCase());
      const notesColStr = notesCol ? pgformat('%I', notesCol.name) : 'NULL';
      const sortStr = extractor.sortColumn ? pgformat('%I', extractor.sortColumn) : 'NULL';
      const hierarchyCol = extractor.hierarchyColumn ? pgformat('%I', extractor.hierarchyColumn) : 'NULL';
      dataExtractorParts.push(
        pgformat(
          'SELECT %I AS %I, %L as language, %I as description, %s as notes, %s as sort_order, %s as hierarchy FROM %I.%I',
          joinColumn,
          factTableColumn.columnName,
          locale.toLowerCase(),
          descriptionCol?.name,
          notesColStr,
          sortStr,
          hierarchyCol,
          'memory',
          lookupTableName
        )
      );
    }
    const builtInsertQuery = pgformat(`INSERT INTO %I.%I %s;`, 'memory', dimTable, dataExtractorParts.join(' UNION '));
    await quack.run(builtInsertQuery);
  } else {
    const languageMatcher = languageMatcherCaseStatement(extractor.languageColumn);
    const notesStr = extractor.notesColumns ? pgformat('%I', extractor.notesColumns[0].name) : 'NULL';
    const sortStr = extractor.sortColumn ? pgformat('%I', extractor.sortColumn) : 'NULL';
    const hierarchyStr = extractor.hierarchyColumn ? pgformat('%I', extractor.hierarchyColumn) : 'NULL';
    const dataExtractorParts = pgformat(
      `SELECT %I AS %I, %s as language, %I as description, %s as notes, %s as sort_order, %s as hierarchy FROM %I.%I;`,
      joinColumn,
      factTableColumn.columnName,
      languageMatcher,
      extractor.descriptionColumns[0].name,
      notesStr,
      sortStr,
      hierarchyStr,
      'memory',
      lookupTableName
    );
    const builtInsertQuery = pgformat(`INSERT INTO %I.%I %s`, 'memory', dimTable, dataExtractorParts);
    await quack.run(builtInsertQuery);
  }
  logger.debug(`Dropping original lookup table ${lookupTableName}`);
  const statements = [
    pgformat('DROP TABLE %I.%I;', 'memory', lookupTableName),
    pgformat('DROP TABLE IF EXISTS %I.%I;', 'lookup_tables_db', lookupTable.id),
    pgformat('CREATE TABLE %I.%I AS SELECT * FROM %I.%I;', 'lookup_tables_db', lookupTable.id, 'memory', dimTable)
  ];
  await quack.run(statements.join('\n'));
  quack.disconnectSync();
  performanceReporting(Math.round(start - performance.now()), 500, 'Loading a lookup table in to postgres');
}

export const loadFileIntoCube = async (
  quack: DuckDBConnection,
  fileType: FileType,
  tempFile: string,
  tableName: string,
  schema: string
): Promise<void> => {
  logger.debug(`Loading file in to DuckDB`);
  logger.debug(`Creating data table ${tableName} with file ${tempFile} and file type ${fileType}`);
  let fileLoaderMethod = pgformat('%L', tempFile);
  switch (fileType) {
    case FileType.Csv:
    case FileType.GzipCsv:
      fileLoaderMethod = pgformat(
        "read_csv(%L, auto_type_candidates = ['BIGINT', 'DOUBLE', 'VARCHAR'], sample_size = -1);",
        tempFile
      );
      break;
    case FileType.Json:
    case FileType.GzipJson:
      fileLoaderMethod = pgformat('read_json_auto(%L);', tempFile);
      break;
    case FileType.Excel:
      fileLoaderMethod = pgformat('read_xlsx(%L);', tempFile);
      break;
  }
  const insertQuery = pgformat('CREATE TABLE %I.%I AS SELECT * FROM %s;', schema, tableName, fileLoaderMethod);
  try {
    logger.trace(`Running create data table query:\n\n${insertQuery}\n\n`);
    await quack.run(insertQuery);
  } catch (error) {
    logger.error(error, `Failed to load file in to DuckDB.`);
    throw error;
  }
};

export async function convertLookupTableToSW3Format(
  mockCubeId: string,
  lookupTable: LookupTable,
  extractor: LookupTableExtractor,
  factTableColumn: FactTableColumn,
  lookupReferenceColumn: string
): Promise<void> {
  const statements = ['BEGIN TRANSACTION;'];
  statements.push(
    createLookupTableQuery(
      mockCubeId,
      lookupTable.id,
      factTableColumn.columnName,
      factTableColumn.columnDatatype,
      extractor.otherColumns
    )
  );
  let additionalCols: string[] | undefined;
  if (extractor.otherColumns && extractor.otherColumns.length > 0) {
    additionalCols = extractor.otherColumns.map((col) => pgformat('CAST (%I AS TEXT)', col));
  }

  if (extractor.isSW2Format) {
    const dataExtractorParts = [];
    for (const locale of SUPPORTED_LOCALES) {
      const descriptionCol = extractor.descriptionColumns.find(
        (col) => col.lang.toLowerCase() === locale.toLowerCase()
      );
      const notesCol = extractor.notesColumns?.find((col) => col.lang.toLowerCase() === locale.toLowerCase());
      const notesColStr = notesCol ? pgformat('%I', notesCol.name) : 'NULL';
      const sortStr = extractor.sortColumn ? pgformat('%I', extractor.sortColumn) : 'NULL';
      const hierarchyCol = extractor.hierarchyColumn ? pgformat('%I', extractor.hierarchyColumn) : 'NULL';
      dataExtractorParts.push(
        pgformat(
          'SELECT %I AS %I, %L as language, %I as description, %s as notes, %s as sort_order, %s as hierarchy %s FROM %I.%I',
          lookupReferenceColumn,
          factTableColumn.columnName,
          locale.toLowerCase(),
          descriptionCol?.name,
          notesColStr,
          sortStr,
          hierarchyCol,
          additionalCols ? `, ${additionalCols.join(', ')}` : '',
          mockCubeId,
          `lookup_table`
        )
      );
    }
    statements.push(pgformat(`INSERT INTO %I.%I %s;`, mockCubeId, lookupTable.id, dataExtractorParts.join(' UNION ')));
  } else {
    const languageMatcher = languageMatcherCaseStatement(extractor.languageColumn);
    const notesStr = extractor.notesColumns ? pgformat('%I', extractor.notesColumns[0].name) : 'NULL';
    const sortStr = extractor.sortColumn ? pgformat('%I', extractor.sortColumn) : 'NULL';
    const hierarchyStr = extractor.hierarchyColumn ? pgformat('%I', extractor.hierarchyColumn) : 'NULL';
    const dataExtractorParts = pgformat(
      `SELECT %I AS %I, %s as language, %I as description, %s as notes, %s as sort_order, %s as hierarchy %s FROM %I.%I;`,
      lookupReferenceColumn,
      factTableColumn.columnName,
      languageMatcher,
      extractor.descriptionColumns[0].name,
      notesStr,
      sortStr,
      hierarchyStr,
      additionalCols ? `, ${additionalCols.join(', ')}` : '',
      mockCubeId,
      `lookup_table`
    );
    statements.push(pgformat(`INSERT INTO %I.%I %s`, mockCubeId, lookupTable.id, dataExtractorParts));
  }

  for (const locale of SUPPORTED_LOCALES) {
    statements.push(
      pgformat(
        'UPDATE %I.%I SET language = %L WHERE language = lower(%L);',
        mockCubeId,
        lookupTable.id,
        locale.toLowerCase(),
        locale.split('-')[0]
      )
    );
    statements.push(
      pgformat(
        'UPDATE %I.%I SET language = %L WHERE language = lower(%L);',
        mockCubeId,
        lookupTable.id,
        locale.toLowerCase(),
        locale.toLowerCase()
      )
    );
    for (const sublocale of SUPPORTED_LOCALES) {
      statements.push(
        pgformat(
          'UPDATE %I.%I SET language = %L WHERE language = lower(%L);',
          mockCubeId,
          lookupTable.id,
          sublocale.toLowerCase(),
          t(`language.${sublocale.split('-')[0]}`, { lng: locale })
        ).toLowerCase()
      );
    }
  }

  statements.push('COMMIT;');
  logger.debug('Converting lookup table to correct SW3 format');
  return runQueryBlock(statements);
}
