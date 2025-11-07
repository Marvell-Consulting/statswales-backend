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
import { performance } from 'node:perf_hooks';
import { performanceReporting } from './performance-reporting';
import { LookupTable } from '../entities/dataset/lookup-table';
import { LookupTableExtractor } from '../extractors/lookup-table-extractor';
import { FactTableColumn } from '../entities/dataset/fact-table-column';
import { SUPPORTED_LOCALES } from '../middleware/translation';
import { languageMatcherCaseStatement } from './lookup-table-utils';
import { DuckDBConnection } from '@duckdb/node-api';
import { runQueryBlockInPostgres } from './run-postgres-statement-block';
import { MeasureLookupTableExtractor } from '../extractors/measure-lookup-extractor';
import { t } from 'i18next';
import { measureTableCreateStatement } from '../services/measure-handler';

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

export const createLookupTableQuery = (
  schemaName: string,
  lookupTableName: string,
  referenceColumnName: string,
  // referenceColumnType: string,
  otherColumns?: string[]
): string => {
  const otherColumnsStatement: string[] = [];
  if (otherColumns && otherColumns.length > 0) {
    otherColumns.forEach((column) => {
      otherColumnsStatement.push(pgformat('%I text', column));
    });
  }
  return pgformat(
    'CREATE TABLE %I.%I (%I TEXT NOT NULL, language VARCHAR(5) NOT NULL, description TEXT NOT NULL, notes TEXT, sort_order INTEGER, hierarchy TEXT %s);',
    schemaName,
    lookupTableName,
    referenceColumnName,
    // referenceColumnType,
    // referenceColumnType,
    otherColumnsStatement.length > 0 ? `, ${otherColumnsStatement.join(', ')}` : ''
  );
};

export async function convertLookupTableToSW3Format(
  mockCubeId: string,
  lookupTable: LookupTable,
  extractor: LookupTableExtractor | MeasureLookupTableExtractor,
  factTableColumn: FactTableColumn,
  lookupReferenceColumn: string,
  type: 'lookup_table' | 'measure'
): Promise<void> {
  const statements = ['BEGIN TRANSACTION;'];
  if (type === 'lookup_table') {
    statements.push(
      createLookupTableQuery(
        mockCubeId,
        lookupTable.id,
        factTableColumn.columnName,
        // factTableColumn.columnDatatype,
        extractor.otherColumns
      )
    );
  } else {
    statements.push(
      measureTableCreateStatement(
        mockCubeId,
        lookupTable.id,
        factTableColumn.columnName,
        // factTableColumn.columnDatatype,
        extractor.otherColumns
      )
    );
  }

  const refColumnName = type === 'measure' ? 'reference' : factTableColumn.columnName;

  const insertColumnList = [refColumnName, 'language', 'description', 'notes', 'sort_order', 'hierarchy'];
  if (type === 'measure') {
    insertColumnList.push(...['format', 'decimals', 'measure_type']);
  }

  let additionalCols: string[] | undefined;
  if (extractor.otherColumns && extractor.otherColumns.length > 0) {
    additionalCols = extractor.otherColumns.map((col) => {
      insertColumnList.push(pgformat('%I', col));
      return pgformat('CAST (%I AS TEXT)', col);
    });
  }

  let measureSpecificCols = '';
  if (type === 'measure') {
    const measureExtractor = extractor as MeasureLookupTableExtractor;
    let formatColumn = 'NULL AS format,';
    if (measureExtractor.formatColumn) {
      formatColumn = `"${measureExtractor.formatColumn}"`;
    } else if (!measureExtractor.formatColumn && !measureExtractor.decimalColumn) {
      formatColumn = `'text'`;
    } else if (!measureExtractor.formatColumn && measureExtractor.decimalColumn) {
      formatColumn = `CASE WHEN CAST("${measureExtractor.decimalColumn}" AS INT) > 0 THEN 'float' ELSE 'integer' END`;
    }
    const decimalColumnDef = measureExtractor.decimalColumn ? `"${measureExtractor.decimalColumn}"` : 'NULL';
    const measureTypeDef = measureExtractor.measureTypeColumn ? `"${measureExtractor.measureTypeColumn}"` : 'NULL';
    measureSpecificCols = pgformat(
      ', %s AS format, CAST(%s as integer) AS decimals, %s AS measure_type',
      formatColumn,
      decimalColumnDef,
      measureTypeDef
    );
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
          'SELECT %I AS %I, %L as language, %I as description, %s as notes, %s as sort_order, %s as hierarchy %s %s FROM %I.%I',
          lookupReferenceColumn,
          refColumnName,
          locale.toLowerCase(),
          descriptionCol?.name,
          notesColStr,
          sortStr,
          hierarchyCol,
          measureSpecificCols,
          additionalCols ? `, ${additionalCols.join(', ')}` : '',
          mockCubeId,
          `lookup_table`
        )
      );
    }
    statements.push(
      pgformat(
        `INSERT INTO %I.%I (%I) %s;`,
        mockCubeId,
        lookupTable.id,
        insertColumnList,
        dataExtractorParts.join(' UNION ')
      )
    );
  } else {
    const languageMatcher = languageMatcherCaseStatement(extractor.languageColumn);
    const notesStr = extractor.notesColumns ? pgformat('%I', extractor.notesColumns[0].name) : 'NULL';
    const sortStr = extractor.sortColumn ? pgformat('%I', extractor.sortColumn) : 'NULL';
    const hierarchyStr = extractor.hierarchyColumn ? pgformat('%I', extractor.hierarchyColumn) : 'NULL';
    const dataExtractorParts = pgformat(
      `SELECT %I AS %I, %s as language, %I as description, %s as notes, %s as sort_order, %s as hierarchy %s %s FROM %I.%I;`,
      lookupReferenceColumn,
      refColumnName,
      languageMatcher,
      extractor.descriptionColumns[0].name,
      notesStr,
      sortStr,
      hierarchyStr,
      measureSpecificCols,
      additionalCols ? `, ${additionalCols.join(', ')}` : '',
      mockCubeId,
      `lookup_table`
    );
    statements.push(
      pgformat(`INSERT INTO %I.%I (%I) %s`, mockCubeId, lookupTable.id, insertColumnList, dataExtractorParts)
    );
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

  statements.push('END TRANSACTION;');
  logger.debug('Converting lookup table to correct SW3 format');
  return runQueryBlockInPostgres(statements);
}

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
