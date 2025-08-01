import { format as pgformat } from '@scaleleap/pg-format';
import { Database } from 'duckdb-async';
import { t } from 'i18next';

import { DimensionType } from '../enums/dimension-type';
import { LookupTable } from '../entities/dataset/lookup-table';
import { DataTable } from '../entities/dataset/data-table';
import { LookupTablePatchDTO } from '../dtos/lookup-patch-dto';
import { LookupTableExtractor } from '../extractors/lookup-table-extractor';
import {
  columnIdentification,
  convertDataTableToLookupTable,
  languageMatcherCaseStatement,
  lookForJoinColumn,
  validateLookupTableLanguages,
  validateLookupTableReferenceValues
} from '../utils/lookup-table-utils';
import { ColumnDescriptor } from '../extractors/column-descriptor';
import { Dataset } from '../entities/dataset/dataset';
import { CSVHeader, ViewDTO, ViewErrDTO } from '../dtos/view-dto';
import { logger } from '../utils/logger';
import { Dimension } from '../entities/dataset/dimension';
import { viewErrorGenerators, viewGenerator } from '../utils/view-error-generators';
import { DatasetRepository } from '../repositories/dataset';
import { FactTableColumnType } from '../enums/fact-table-column-type';
import { cleanUpDimension } from './dimension-processor';
import { SUPPORTED_LOCALES } from '../middleware/translation';
import { createLookupTableQuery, loadFileIntoLookupTablesSchema, makeCubeSafeString } from './cube-handler';
import { FactTableColumn } from '../entities/dataset/fact-table-column';
import { CubeValidationException } from '../exceptions/cube-error-exception';
import { Locale } from '../enums/locale';
import { FileValidationErrorType, FileValidationException } from '../exceptions/validation-exception';
import { CubeValidationType } from '../enums/cube-validation-type';
import { QueryRunner } from 'typeorm';
import { dbManager } from '../db/database-manager';

const sampleSize = 5;

async function setupDimension(
  dimension: Dimension,
  lookupTable: LookupTable,
  protoLookupTable: DataTable,
  confirmedJoinColumn: string,
  tableLanguage: Locale,
  tableMatcher?: LookupTablePatchDTO
): Promise<Dimension> {
  // Clean up previously uploaded dimensions
  if (dimension.lookupTable) await cleanUpDimension(dimension);
  lookupTable.isStatsWales2Format = !protoLookupTable.dataTableDescriptions.find((info) =>
    info.columnName.toLowerCase().startsWith('lang')
  );
  const updateDimension = await Dimension.findOneByOrFail({ id: dimension.id });
  updateDimension.type = DimensionType.LookupTable;
  updateDimension.joinColumn = confirmedJoinColumn;
  updateDimension.lookupTable = lookupTable;
  logger.debug(`Creating extractor...`);
  updateDimension.extractor = createExtractor(protoLookupTable, tableLanguage, tableMatcher);
  logger.debug('Saving the lookup table');
  await lookupTable.save();
  logger.debug('Saving the dimension');
  updateDimension.lookupTable = lookupTable;
  updateDimension.type = DimensionType.LookupTable;
  return updateDimension;
}

function createExtractor(
  protoLookupTable: DataTable,
  tableLanguage: Locale,
  tableMatcher?: LookupTablePatchDTO
): LookupTableExtractor {
  if (tableMatcher?.description_columns) {
    logger.debug(`Table matcher is supplied using user supplied information to create extractor...`);
    return {
      tableLanguage,
      sortColumn: tableMatcher.sort_column,
      hierarchyColumn: tableMatcher.hierarchy,
      descriptionColumns: tableMatcher.description_columns.map(
        (desc) =>
          protoLookupTable.dataTableDescriptions
            .filter((info) => info.columnName === desc)
            .map((info) => columnIdentification(info))[0]
      ),
      notesColumns: tableMatcher.notes_column?.map(
        (desc) =>
          protoLookupTable.dataTableDescriptions
            .filter((info) => info.columnName === desc)
            .map((info) => columnIdentification(info))[0]
      ),
      languageColumn: tableMatcher.language,
      isSW2Format: !tableMatcher.language
    };
  } else {
    logger.debug('Detecting column types from column names');
    const noteStr = t('lookup_column_headers.notes', { lng: tableLanguage });
    const sortStr = t('lookup_column_headers.sort', { lng: tableLanguage });
    const hierarchyStr = t('lookup_column_headers.hierarchy', { lng: tableLanguage });
    const descriptionStr = t('lookup_column_headers.description', { lng: tableLanguage });
    const langStr = t('lookup_column_headers.lang', { lng: tableLanguage });
    let notesColumns: ColumnDescriptor[] | undefined;
    if (protoLookupTable.dataTableDescriptions.filter((info) => info.columnName.toLowerCase().startsWith(noteStr)))
      notesColumns = protoLookupTable.dataTableDescriptions
        .filter((info) => info.columnName.toLowerCase().startsWith(noteStr))
        .map((info) => columnIdentification(info));
    if (notesColumns && notesColumns.length === 0) notesColumns = undefined;
    const extractor: LookupTableExtractor = {
      tableLanguage,
      sortColumn: protoLookupTable.dataTableDescriptions.find((info) =>
        info.columnName.toLowerCase().startsWith(sortStr)
      )?.columnName,
      languageColumn: protoLookupTable.dataTableDescriptions.find((info) =>
        info.columnName.toLowerCase().startsWith(langStr)
      )?.columnName,
      hierarchyColumn: protoLookupTable.dataTableDescriptions.find((info) =>
        info.columnName.toLowerCase().includes(hierarchyStr)
      )?.columnName,
      descriptionColumns: protoLookupTable.dataTableDescriptions
        .filter((info) => info.columnName.toLowerCase().includes(descriptionStr))
        .map((info) => columnIdentification(info)),
      notesColumns,
      isSW2Format: !protoLookupTable.dataTableDescriptions.find((info) =>
        info.columnName.toLowerCase().startsWith(langStr)
      )
    };
    // logger.debug(`Extracted extractor from lookup table:\n${JSON.stringify(extractor, null, 2)}`);
    if (extractor.descriptionColumns.length === 0) {
      throw new FileValidationException(
        'errors.measure_validation.no_description_columns',
        FileValidationErrorType.InvalidCsv
      );
    }
    return extractor;
  }
}

export const createLookupTableInCube = async (
  quack: Database,
  factTableColumn: FactTableColumn,
  dimension: Dimension,
  lookupTableName: string
): Promise<void> => {
  const extractor = dimension.extractor as LookupTableExtractor;
  const dimensionTableName = `${makeCubeSafeString(factTableColumn.columnName).toLowerCase()}_lookup`;
  await quack.exec(
    createLookupTableQuery(dimensionTableName, factTableColumn.columnName, factTableColumn.columnDatatype)
  );

  if (extractor.isSW2Format) {
    logger.debug('Lookup table is SW2 format');
    const dataExtractorParts = [];
    for (const locale of SUPPORTED_LOCALES) {
      const descriptionCol = extractor.descriptionColumns.find(
        (col) => col.lang.toLowerCase() === locale.toLowerCase()
      );
      const notesCol = extractor.notesColumns?.find((col) => col.lang.toLowerCase() === locale.toLowerCase());
      const descriptionColStr = descriptionCol ? `"${descriptionCol.name}"` : 'NULL';
      const notesColStr = notesCol ? `"${notesCol.name}"` : 'NULL';
      const sortStr = extractor.sortColumn ? `"${extractor.sortColumn}"` : 'NULL';
      const hierarchyCol = extractor.hierarchyColumn ? `"${extractor.hierarchyColumn}"` : 'NULL';
      dataExtractorParts.push(
        `SELECT "${dimension.joinColumn}" as "${factTableColumn.columnName}",
        '${locale.toLowerCase()}' as language,
        ${descriptionColStr} as description,
        ${notesColStr} as notes,
        ${sortStr} as sort_order,
        ${hierarchyCol} as hierarchy
        FROM ${lookupTableName}`
      );
    }
    const builtInsertQuery = `
      INSERT INTO ${makeCubeSafeString(dimension.factTableColumn)}_lookup (${dataExtractorParts.join(' UNION ')});
    `;

    // logger.debug(`Built insert query: ${builtInsertQuery}`);
    await quack.exec(builtInsertQuery);
  } else {
    const languageMatcher = languageMatcherCaseStatement(extractor.languageColumn);
    const notesStr = extractor.notesColumns ? `"${extractor.notesColumns[0].name}"` : 'NULL';
    const dataExtractorParts = `
      SELECT
        "${dimension.joinColumn}" as "${factTableColumn.columnName}",
        ${languageMatcher} as language,
        "${extractor.descriptionColumns[0].name}" as description,
        ${notesStr} as notes,
        ${extractor.sortColumn ? `"${extractor.sortColumn}"` : 'NULL'} as sort_order,
        ${extractor.hierarchyColumn ? `"${extractor.hierarchyColumn}"` : 'NULL'} as hierarchy
      FROM ${lookupTableName}
    `;
    const builtInsertQuery = `
      INSERT INTO ${makeCubeSafeString(dimension.factTableColumn)}_lookup ${dataExtractorParts};
    `;
    await quack.exec(builtInsertQuery);
  }
};

export const checkForReferenceErrors = async (
  cubeDB: QueryRunner,
  dataset: Dataset,
  dimension: Dimension,
  factTableColumn: FactTableColumn
): Promise<void> => {
  const referenceErrors = await validateLookupTableReferenceValues(
    cubeDB,
    dataset,
    dimension.factTableColumn,
    factTableColumn.columnName,
    `${makeCubeSafeString(dimension.factTableColumn)}_lookup`,
    'fact_table',
    'dimension'
  );
  if (referenceErrors) {
    const err = new CubeValidationException('Validation failed');
    err.type = CubeValidationType.DimensionNonMatchedRows;
    throw err;
  }
};

export const validateLookupTable = async (
  protoLookupTable: DataTable,
  dataset: Dataset,
  dimension: Dimension,
  path: string,
  language: string,
  tableMatcher?: LookupTablePatchDTO
): Promise<ViewDTO | ViewErrDTO> => {
  const revision = dataset.draftRevision;
  if (!revision?.id) {
    logger.error(`Could not find the draft revision for dataset ${dataset.id}`);
    throw new Error('Could not find the draft revision for dataset');
  }
  const lookupTable = convertDataTableToLookupTable(protoLookupTable);
  const factTableName = 'fact_table';
  const factTableColumn = dataset.factTable?.find(
    (col) => dimension.factTableColumn === col.columnName && col.columnType === FactTableColumnType.Dimension
  );
  if (!factTableColumn) {
    logger.error(`Could not find the fact table column ${dimension.factTableColumn} in the dataset`);
    return viewErrorGenerators(500, dataset.id, 'patch', 'errors.dimension_validation.fact_table_column_not_found', {
      mismatch: false
    });
  }
  const tableLanguageArr: Locale[] = [];

  SUPPORTED_LOCALES.map((locale) => {
    if (
      protoLookupTable.dataTableDescriptions.find((col) =>
        col.columnName.toLowerCase().includes(t('lookup_column_headers.description', { lng: locale.toLowerCase() }))
      )
    ) {
      tableLanguageArr.push(locale);
    }
  });
  if (tableLanguageArr.length < 1) {
    return viewErrorGenerators(400, dataset.id, 'csv', 'errors.measure_validation.no_description_columns', {
      mismatch: false
    });
  }
  const tableLanguage = tableLanguageArr[0];

  let confirmedJoinColumn: string | undefined;
  try {
    confirmedJoinColumn = lookForJoinColumn(protoLookupTable, dimension.factTableColumn, tableLanguage, tableMatcher);
  } catch (_err) {
    return viewErrorGenerators(400, dataset.id, 'patch', 'errors.lookup_validation.no_join_column', {
      mismatch: false
    });
  }

  if (!confirmedJoinColumn) {
    return viewErrorGenerators(400, dataset.id, 'patch', 'errors.lookup_validation.no_join_column', {
      mismatch: false
    });
  }

  const updatedDimension = await setupDimension(
    dimension,
    lookupTable,
    protoLookupTable,
    confirmedJoinColumn,
    tableLanguage,
    tableMatcher
  );

  try {
    logger.debug(`Loading lookup table into Postgres`);
    await loadFileIntoLookupTablesSchema(
      dataset,
      lookupTable,
      updatedDimension.extractor as LookupTableExtractor,
      factTableColumn,
      confirmedJoinColumn,
      path
    );
  } catch (err) {
    logger.error(err, `Something went wrong trying to load the lookup table into the cube`);
    return viewErrorGenerators(500, dataset.id, 'patch', 'errors.dimension_validation.lookup_table_loading_failed', {
      mismatch: false
    });
  }

  const cubeDB = dbManager.getCubeDataSource().createQueryRunner();

  try {
    await cubeDB.query(pgformat(`SET search_path TO %I;`, revision.id));
  } catch (error) {
    await lookupTable.remove();
    cubeDB.release();
    logger.error(error, 'Unable to connect to postgres schema for revision.');
    return viewErrorGenerators(500, dataset.id, 'patch', 'errors.dimension_validation.lookup_table_loading_failed', {
      mismatch: false
    });
  }

  logger.debug('Copying lookup table from lookup_tables schema into cube');
  const actionId = crypto.randomUUID();
  try {
    await cubeDB.query(pgformat('CREATE TABLE %I AS SELECT * FROM lookup_tables.%I;', actionId, lookupTable.id));
  } catch (error) {
    await lookupTable.remove();
    cubeDB.release();
    logger.error(error, 'Unable to copy lookup table from lookup tables schema.');
    return viewErrorGenerators(500, dataset.id, 'patch', 'errors.dimension_validation.lookup_table_loading_failed', {
      mismatch: false
    });
  }

  const referenceErrors = await validateLookupTableReferenceValues(
    cubeDB,
    dataset,
    updatedDimension.factTableColumn,
    factTableColumn.columnName,
    actionId,
    factTableName,
    'dimension'
  );
  if (referenceErrors) {
    await cubeDB.query(pgformat('DROP TABLE IF EXISTS %I', actionId));
    await cubeDB.query(pgformat('DROP TABLE IF EXISTS lookup_tables.%I', lookupTable.id));
    await lookupTable.remove();
    cubeDB.release();
    return referenceErrors;
  }

  const languageErrors = await validateLookupTableLanguages(
    cubeDB,
    dataset,
    revision.id,
    factTableColumn.columnName,
    actionId,
    'dimension'
  );
  if (languageErrors) {
    await cubeDB.query(pgformat('DROP TABLE IF EXISTS %I', actionId));
    await cubeDB.query(pgformat('DROP TABLE IF EXISTS lookup_tables.%I', lookupTable.id));
    await lookupTable.remove();
    cubeDB.release();
    return languageErrors;
  }

  logger.debug(`Lookup table passed validation. Saving the dimension, lookup table and extractor.`);
  await updatedDimension.save();

  try {
    const previewQuery = pgformat('SELECT * FROM lookup_tables.%I WHERE language = %L;', lookupTable.id, language);
    const dimensionTable = await cubeDB.query(previewQuery);
    const tableHeaders = Object.keys(dimensionTable[0]);
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const dataArray = dimensionTable.map((row: any) => Object.values(row));
    const currentDataset = await DatasetRepository.getById(dataset.id);
    const headers: CSVHeader[] = [];
    for (let i = 0; i < tableHeaders.length; i++) {
      let sourceType: FactTableColumnType;
      if (tableHeaders[i] === 'int_line_number') sourceType = FactTableColumnType.LineNumber;
      else sourceType = FactTableColumnType.Unknown;
      headers.push({
        index: i - 1,
        name: tableHeaders[i],
        source_type: sourceType
      });
    }
    const pageInfo = {
      total_records: dimensionTable.length,
      start_record: 1,
      end_record: dataArray.length
    };
    const pageSize = dimensionTable.length < sampleSize ? dimensionTable.length : sampleSize;
    return viewGenerator(currentDataset, 1, pageInfo, pageSize, 1, headers, dataArray);
  } catch (error) {
    logger.error(error, `Something went wrong trying to generate the preview of the lookup.`);
    return viewErrorGenerators(500, dataset.id, 'preview', 'errors.dimension.lookup_preview_generation_failed', {});
  } finally {
    await cubeDB.query(pgformat('DROP TABLE IF EXISTS %I', actionId));
    cubeDB.release();
  }
};
