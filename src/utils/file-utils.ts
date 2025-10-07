import { writeFile } from 'node:fs/promises';

import { format as pgformat } from '@scaleleap/pg-format';

import { Dataset } from '../entities/dataset/dataset';
import { FileImportInterface } from '../entities/dataset/file-import.interface';
import { FileType } from '../enums/file-type';

import { logger } from './logger';
import { getFileService } from './get-file-service';
import { asyncTmpName } from './async-tmp';
import { duckdb } from '../services/duckdb';
import { FACT_TABLE_NAME, makeCubeSafeString } from '../services/cube-handler';
import { DataTable } from '../entities/dataset/data-table';
import { performance } from 'node:perf_hooks';
import { performanceReporting } from './performance-reporting';
import { LookupTable } from '../entities/dataset/lookup-table';
import { LookupTableExtractor } from '../extractors/lookup-table-extractor';
import { FactTableColumn } from '../entities/dataset/fact-table-column';
import { SUPPORTED_LOCALES } from '../middleware/translation';
import { languageMatcherCaseStatement } from './lookup-table-utils';
import { DuckDBConnection } from '@duckdb/node-api';

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
  await loadFileIntoCube(quack, dataTable.fileType, dataTableFile, FACT_TABLE_NAME);
  await quack.run(
    pgformat('CREATE TABLE data_tables_db.%I AS SELECT * FROM memory.%I;', dataTable.id, FACT_TABLE_NAME)
  );
  quack.disconnectSync();
  performanceReporting(Math.round(start - performance.now()), 500, 'Loading a data table in to postgres');
}

export const createLookupTableQuery = (
  lookupTableName: string,
  referenceColumnName: string,
  referenceColumnType: string
): string => {
  return pgformat(
    'CREATE TABLE %I (%I %s NOT NULL, language VARCHAR(5) NOT NULL, description TEXT NOT NULL, notes TEXT, sort_order INTEGER, hierarchy %s);',
    lookupTableName,
    referenceColumnName,
    referenceColumnType,
    referenceColumnType
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
  const dimTable = `${makeCubeSafeString(factTableColumn.columnName)}_lookup`;
  await quack.run(createLookupTableQuery(dimTable, factTableColumn.columnName, factTableColumn.columnDatatype));
  let lookupTableFile;
  if (filePath) {
    lookupTableFile = filePath;
  } else {
    lookupTableFile = await getFileImportAndSaveToDisk(dataset, lookupTable!);
  }
  const lookupTableName = `${makeCubeSafeString(factTableColumn.columnName)}_lookup_draft`;
  await loadFileIntoCube(quack, lookupTable.fileType, lookupTableFile, lookupTableName);
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
          'SELECT %I AS %I, %L as language, %I as description, %s as notes, %s as sort_order, %s as hierarchy FROM %I',
          joinColumn,
          factTableColumn.columnName,
          locale.toLowerCase(),
          descriptionCol?.name,
          notesColStr,
          sortStr,
          hierarchyCol,
          lookupTableName
        )
      );
    }
    const builtInsertQuery = pgformat(`INSERT INTO %I %s;`, dimTable, dataExtractorParts.join(' UNION '));
    await quack.run(builtInsertQuery);
  } else {
    const languageMatcher = languageMatcherCaseStatement(extractor.languageColumn);
    const notesStr = extractor.notesColumns ? pgformat('%I', extractor.notesColumns[0].name) : 'NULL';
    const sortStr = extractor.sortColumn ? pgformat('%I', extractor.sortColumn) : 'NULL';
    const hierarchyStr = extractor.hierarchyColumn ? pgformat('%I', extractor.hierarchyColumn) : 'NULL';
    const dataExtractorParts = pgformat(
      `SELECT %I AS %I, %s as language, %I as description, %s as notes, %s as sort_order, %s as hierarchy FROM %I;`,
      joinColumn,
      factTableColumn.columnName,
      languageMatcher,
      extractor.descriptionColumns[0].name,
      notesStr,
      sortStr,
      hierarchyStr,
      lookupTableName
    );
    const builtInsertQuery = pgformat(`INSERT INTO %I %s`, dimTable, dataExtractorParts);
    await quack.run(builtInsertQuery);
  }
  logger.debug(`Dropping original lookup table ${lookupTableName}`);
  await quack.run(pgformat('DROP TABLE %I', lookupTableName));
  await quack.run(pgformat('CREATE TABLE lookup_tables_db.%I AS SELECT * FROM memory.%I;', lookupTable.id, dimTable));
  quack.disconnectSync();
  performanceReporting(Math.round(start - performance.now()), 500, 'Loading a lookup table in to postgres');
}

export const loadFileIntoCube = async (
  quack: DuckDBConnection,
  fileType: FileType,
  tempFile: string,
  tableName: string
): Promise<void> => {
  logger.debug(`Loading file in to DuckDB`);
  let insertQuery = '';
  logger.debug(`Creating data table ${tableName} with file ${tempFile} and file type ${fileType}`);
  switch (fileType) {
    case FileType.Csv:
    case FileType.GzipCsv:
      insertQuery = pgformat(
        "CREATE TEMPORARY TABLE %I AS SELECT * FROM read_csv(%L, auto_type_candidates = ['BIGINT', 'DOUBLE', 'VARCHAR'], sample_size = -1);",
        makeCubeSafeString(tableName),
        tempFile
      );
      break;
    case FileType.Parquet:
      insertQuery = pgformat('CREATE TEMPORARY TABLE %I AS SELECT * FROM %L;', makeCubeSafeString(tableName), tempFile);
      break;
    case FileType.Json:
    case FileType.GzipJson:
      insertQuery = pgformat(
        'CREATE TEMPORARY TABLE %I AS SELECT * FROM read_json_auto(%L);',
        makeCubeSafeString(tableName),
        tempFile
      );
      break;
    case FileType.Excel:
      insertQuery = pgformat(
        'CREATE TEMPORARY TABLE %I AS SELECT * FROM read_xlsx(%L);',
        makeCubeSafeString(tableName),
        tempFile
      );
      break;
    default:
      throw new Error('Unknown file type');
  }
  try {
    await quack.run(insertQuery);
  } catch (error) {
    logger.error(`Failed to load file in to DuckDB using query ${insertQuery} with the following error: ${error}`);
    throw error;
  }
};
