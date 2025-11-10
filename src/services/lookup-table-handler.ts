import { format as pgformat } from '@scaleleap/pg-format';
import { t } from 'i18next';

import { DimensionType } from '../enums/dimension-type';
import { LookupTable } from '../entities/dataset/lookup-table';
import { DataTable } from '../entities/dataset/data-table';
import { LookupTableExtractor } from '../extractors/lookup-table-extractor';
import {
  columnIdentification,
  convertDataTableToLookupTable,
  lookForPossibleJoinColumn,
  validateLookupTableLanguages
} from '../utils/lookup-table-utils';
import { Dataset } from '../entities/dataset/dataset';
import { ViewDTO, ViewErrDTO } from '../dtos/view-dto';
import { logger } from '../utils/logger';
import { Dimension } from '../entities/dataset/dimension';
import { viewErrorGenerators } from '../utils/view-error-generators';
import { FactTableColumnType } from '../enums/fact-table-column-type';
import { cleanUpDimension } from './dimension-processor';
import { SUPPORTED_LOCALES } from '../middleware/translation';
import { Locale } from '../enums/locale';
import { FileValidationErrorType, FileValidationException } from '../exceptions/validation-exception';
import { dbManager } from '../db/database-manager';
import { convertLookupTableToSW3Format } from '../utils/file-utils';
import { previewGenerator } from '../utils/preview-generator';
import { Revision } from '../entities/dataset/revision';
import { randomUUID } from 'node:crypto';
import {
  cleanUpPostgresValidationSchema,
  createPostgresValidationSchema,
  saveValidatedLookupTableToDatabase
} from '../utils/mock-cube-handler';
import { FACT_TABLE_NAME } from './cube-builder';
import { DataTableDescription } from '../entities/dataset/data-table-description';

export const validateLookupTable = async (
  protoLookupTable: DataTable,
  dataset: Dataset,
  draftRevision: Revision,
  dimension: Dimension,
  language: string
): Promise<ViewDTO | ViewErrDTO> => {
  const mockCubeId = randomUUID();
  const mockCubePromise = createPostgresValidationSchema(
    mockCubeId,
    draftRevision.id,
    dimension.factTableColumn,
    `${protoLookupTable.id}_tmp`
  );

  logger.info('Validating lookup table...');
  const lookupTableColumns = protoLookupTable.dataTableDescriptions;

  const lookupTable = convertDataTableToLookupTable(protoLookupTable);
  const factTableColumn = dataset.factTable?.find(
    (col) => dimension.factTableColumn === col.columnName && col.columnType === FactTableColumnType.Dimension
  );

  if (!factTableColumn) {
    logger.error(`Could not find the fact table column ${dimension.factTableColumn} in the dataset`);
    await mockCubePromise
      .finally(() => {
        return cleanUpPostgresValidationSchema(mockCubeId, lookupTable.id);
      })
      .catch((err) => {
        logger.error(err, 'Something went wrong trying to clean up the mock cube');
      });
    return viewErrorGenerators(500, dataset.id, 'patch', 'errors.dimension_validation.fact_table_column_not_found', {
      mismatch: false
    });
  }

  const tableLanguageArr: Locale[] = [];

  SUPPORTED_LOCALES.map((locale) => {
    const descriptionCol = t('lookup_column_headers.description', { lng: locale.toLowerCase() });
    if (protoLookupTable.dataTableDescriptions.find((col) => col.columnName.toLowerCase().includes(descriptionCol))) {
      tableLanguageArr.push(locale);
    }
  });

  if (tableLanguageArr.length < 1) {
    logger.error('No description columns found in lookup table');
    await mockCubePromise
      .finally(() => {
        return cleanUpPostgresValidationSchema(mockCubeId, lookupTable.id);
      })
      .catch((err) => {
        logger.error(err, 'Something went wrong trying to clean up the mock cube');
      });
    return viewErrorGenerators(400, dataset.id, 'csv', 'errors.measure_validation.no_description_columns', {
      mismatch: false
    });
  }

  const tableLanguage = tableLanguageArr[0];
  let possibleJoinColumns: string[];

  try {
    possibleJoinColumns = lookForPossibleJoinColumn(lookupTableColumns, dimension.factTableColumn, tableLanguage);
  } catch (_err) {
    logger.error('There was a problem trying to find the join column');
    await mockCubePromise
      .catch((err) => {
        logger.error(err, 'Something went wrong trying to clean up the mock cube');
      })
      .finally(() => {
        return cleanUpPostgresValidationSchema(mockCubeId, lookupTable.id);
      });
    return viewErrorGenerators(400, dataset.id, 'patch', 'errors.lookup_validation.no_join_column', {
      mismatch: false
    });
  }

  await mockCubePromise;
  let lookupReferenceColumn: string;
  try {
    lookupReferenceColumn = await confirmJoinColumnAndValidateReferenceValues(
      possibleJoinColumns,
      factTableColumn.columnName,
      mockCubeId,
      draftRevision.id,
      'lookup_table'
    );
  } catch (err) {
    const error = err as FileValidationException;
    void cleanUpPostgresValidationSchema(mockCubeId, lookupTable.id).catch((err) => {
      logger.error(err, 'Something went wrong trying to clean up the mock cube');
    });
    return viewErrorGenerators(400, dataset.id, 'patch', error.errorTag, error.extension);
  }

  const updatedDimension = await setupDimension(
    dimension,
    lookupTable,
    lookupTableColumns,
    lookupReferenceColumn,
    tableLanguage
  );

  try {
    logger.debug(`Converting lookup table to the correct format...`);
    await convertLookupTableToSW3Format(
      mockCubeId,
      lookupTable,
      updatedDimension.extractor as LookupTableExtractor,
      factTableColumn,
      lookupReferenceColumn
    );
  } catch (err) {
    logger.error(err, `Something went wrong trying to covert the lookup table to SW3 format`);
    return viewErrorGenerators(500, dataset.id, 'patch', 'errors.dimension_validation.lookup_table_loading_failed', {
      mismatch: false
    });
  }

  const languageErrors = await validateLookupTableLanguages(
    dataset,
    mockCubeId,
    factTableColumn.columnName,
    lookupTable.id,
    'dimension'
  );
  if (languageErrors) {
    void cleanUpPostgresValidationSchema(mockCubeId, lookupTable.id).catch((err) => {
      logger.error(err, 'Something went wrong trying to clean up the mock cube');
    });
    return languageErrors;
  }

  logger.debug(`Lookup table passed validation. Saving the dimension, lookup table and extractor.`);
  try {
    await saveValidatedLookupTableToDatabase(mockCubeId, lookupTable.id);
  } catch (err) {
    logger.error(err, 'Something went wrong trying to save the lookup table or clean up the mock cube.');
    return viewErrorGenerators(500, dataset.id, 'patch', `errors.lookup_table_validation.unknown_error`, {});
  }

  lookupTable.isStatsWales2Format = false;
  await lookupTable.save();
  updatedDimension.lookupTable = lookupTable;
  await updatedDimension.save();

  const lookupTablePreviewRunner = dbManager.getCubeDataSource().createQueryRunner();
  let dimensionTable: Record<string, never>[];
  const previewQuery = pgformat('SELECT * FROM lookup_tables.%I WHERE language = %L;', lookupTable.id, language);
  try {
    dimensionTable = await lookupTablePreviewRunner.query(previewQuery);
  } catch (error) {
    logger.error(error, `Something went wrong trying to generate the preview of the lookup.`);
    return viewErrorGenerators(500, dataset.id, 'preview', 'errors.dimension.lookup_preview_generation_failed', {});
  } finally {
    void lookupTablePreviewRunner.release();
  }

  if (dimensionTable.length === 0) {
    logger.error('Dimension table is empty, cannot generate preview.');
    return viewErrorGenerators(500, dataset.id, 'preview', 'errors.dimension.lookup_preview_generation_failed', {});
  }
  return previewGenerator(dimensionTable, { totalLines: dimensionTable.length }, dataset, false);
};

async function setupDimension(
  dimension: Dimension,
  lookupTable: LookupTable,
  lookupTableColumns: DataTableDescription[],
  lookupReferenceColumn: string,
  tableLanguage: Locale
): Promise<Dimension> {
  // Clean up previously uploaded dimensions
  if (dimension.lookupTable) await cleanUpDimension(dimension);
  lookupTable.isStatsWales2Format = !lookupTableColumns.find((info) =>
    info.columnName.toLowerCase().startsWith('lang')
  );
  const updateDimension = await Dimension.findOneByOrFail({ id: dimension.id });
  updateDimension.type = DimensionType.LookupTable;
  updateDimension.joinColumn = lookupReferenceColumn;
  updateDimension.lookupTable = lookupTable;
  logger.debug(`Creating extractor...`);
  updateDimension.extractor = createLookupExtractor(lookupReferenceColumn, lookupTableColumns, tableLanguage);
  logger.debug('Saving the lookup table');
  await lookupTable.save();
  logger.debug('Saving the dimension');
  updateDimension.lookupTable = lookupTable;
  updateDimension.type = DimensionType.LookupTable;
  return updateDimension;
}

function createLookupExtractor(
  confirmedJoinColumn: string,
  tableColumns: DataTableDescription[],
  tableLanguage: Locale
): LookupTableExtractor {
  logger.debug('Detecting column types from column names');

  // Possible headings based on language used for the description column
  const noteStr = t('lookup_column_headers.notes', { lng: tableLanguage });
  const sortStr = t('lookup_column_headers.sort', { lng: tableLanguage });
  const hierarchyStr = t('lookup_column_headers.hierarchy', { lng: tableLanguage });
  const descriptionStr = t('lookup_column_headers.description', { lng: tableLanguage });
  const langStr = t('lookup_column_headers.lang', { lng: tableLanguage });

  const extractor: LookupTableExtractor = {
    tableLanguage,
    isSW2Format: true,
    notesColumns: [],
    descriptionColumns: [],
    otherColumns: []
  };

  tableColumns.forEach((column) => {
    const columnName = column.columnName.toLowerCase();
    if (columnName === confirmedJoinColumn.toLowerCase()) {
      extractor.joinColumn = columnName;
    } else if (columnName.includes(descriptionStr)) {
      extractor.descriptionColumns.push(columnIdentification(column));
    } else if (columnName.includes(langStr)) {
      extractor.languageColumn = column.columnName;
      extractor.isSW2Format = false;
    } else if (columnName.includes(noteStr)) {
      extractor.notesColumns?.push(columnIdentification(column));
    } else if (columnName.includes(sortStr)) {
      extractor.sortColumn = column.columnName;
    } else if (columnName.includes(hierarchyStr)) {
      extractor.hierarchyColumn = column.columnName;
    } else {
      extractor.otherColumns?.push(column.columnName);
    }
  });

  if (extractor.notesColumns!.length > 0) {
    extractor.notesColumns = undefined;
  }

  return extractor;
}

// Finds the correct join column and confirms all the reference values are present
// If the query returns 0 matches, this is the join column
// If multiple columns partially match we mismatch on the column with the most matches
export async function confirmJoinColumnAndValidateReferenceValues(
  possibleJoinColumns: string[],
  factTableColumn: string,
  mockCubeId: string,
  revisionId: string,
  type: string
): Promise<string> {
  let joinColumn: string | undefined;
  let closestMatch: { col: string; missingValues: { fact: string; lookup: string | null }[] } | undefined;
  const queryRunner = dbManager.getCubeDataSource().createQueryRunner();
  const referenceTotalCountRun: Promise<{ total: number }[]> = queryRunner.query(
    pgformat('SELECT COUNT(DISTINCT %I) as total FROM %I.%I;', factTableColumn, revisionId, FACT_TABLE_NAME)
  );

  for (const col of possibleJoinColumns) {
    const query = pgformat(
      'SELECT DISTINCT %I.%I as fact, %I.%I as lookup FROM %I.%I LEFT JOIN %I.%I ON CAST(%I.%I AS TEXT) = CAST(%I.%I AS TEXT) WHERE %I.%I IS NULL;',
      FACT_TABLE_NAME,
      factTableColumn,
      'lookup_table',
      col,
      mockCubeId,
      FACT_TABLE_NAME,
      mockCubeId,
      'lookup_table',
      FACT_TABLE_NAME,
      factTableColumn,
      'lookup_table',
      col,
      'lookup_table',
      col
    );
    let missingValues: { fact: string; lookup: string | null }[];
    try {
      logger.trace(`Running query to check lookup tables values:\n\n${query}\n\n`);
      missingValues = await queryRunner.query(query);
    } catch (err) {
      logger.error(err, 'Something went wrong trying to query the mock cube');
      continue;
    }
    if (missingValues.length === 0) {
      joinColumn = col;
      break;
    }
    if (!closestMatch) {
      closestMatch = { col, missingValues };
    } else if (missingValues.length < closestMatch.missingValues.length) {
      closestMatch = { col, missingValues };
    }
  }

  if (joinColumn) {
    await referenceTotalCountRun;
    void queryRunner.release();
    return joinColumn;
  }

  if (!closestMatch) {
    await referenceTotalCountRun;
    void queryRunner.release();
    const err = new FileValidationException(
      'We failed to find a join column for the lookup table',
      FileValidationErrorType.LookupNoJoinColumn
    );
    err.errorTag = `errors.${type}_validation.lookup_no_join_column`;
    throw err;
  }

  const referenceTotalCount = (await referenceTotalCountRun)[0].total;
  if (referenceTotalCount === closestMatch!.missingValues.length) {
    logger.error(`The user supplied an incorrect lookup table and none of the rows matched`);
    const err = new FileValidationException(
      'None of the references in the join column matched the fact table',
      FileValidationErrorType.LookupMissingValues
    );
    err.extension = {
      mismatch: true,
      totalNonMatching: referenceTotalCount,
      nonMatchingDataTableValues: closestMatch?.missingValues.map((val) => val.fact)
    } as never;
    err.errorTag = `errors.${type}_validation.lookup_no_join_column`;
    throw err;
  } else {
    const exception = new FileValidationException(
      'Some of the references in the join column failed to match those in the fact table',
      FileValidationErrorType.LookupMissingValues
    );
    exception.errorTag = `errors.${type}_validation.some_references_failed_to_match`;
    const getUnmachedLinesFromFactTableQuery = pgformat(
      'SELECT * FROM (SELECT row_number() OVER () as line_number, * FROM %I.%I) WHERE %I IN (%L) LIMIT 500;',
      revisionId,
      FACT_TABLE_NAME,
      factTableColumn,
      closestMatch!.missingValues.map((missingValue) => missingValue.fact)
    );
    const getUnmatchedLinesFromLookupQuery = pgformat(
      'SELECT %I as ref FROM %I.%I WHERE %I NOT IN (%L) LIMIT 500;',
      closestMatch.col,
      mockCubeId,
      'lookup_table',
      closestMatch!.missingValues.map((missingValue) => missingValue.fact)
    );
    let unmatchedLinesFromFactTable: unknown;
    let unmatchedLinesFromLookup: { ref: string }[];
    try {
      unmatchedLinesFromFactTable = await queryRunner.query(getUnmachedLinesFromFactTableQuery);
      unmatchedLinesFromLookup = await queryRunner.query(getUnmatchedLinesFromLookupQuery);
    } catch (err) {
      logger.error(err, 'Unable to get non-matching rows from the revisions fact table');
      throw exception;
    } finally {
      void queryRunner.release();
    }
    exception.extension = {
      mismatch: true,
      totalNonMatching: referenceTotalCount,
      nonMatchingDataTableValues: unmatchedLinesFromFactTable,
      nonMatchedLookupValues: unmatchedLinesFromLookup.map((val) => val.ref)
    } as never;
    throw exception;
  }
}
