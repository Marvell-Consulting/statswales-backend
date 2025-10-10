import { format as pgformat } from '@scaleleap/pg-format';
import { t } from 'i18next';

import { DimensionType } from '../enums/dimension-type';
import { LookupTable } from '../entities/dataset/lookup-table';
import { DataTable } from '../entities/dataset/data-table';
import { LookupTablePatchDTO } from '../dtos/lookup-patch-dto';
import { LookupTableExtractor } from '../extractors/lookup-table-extractor';
import {
  columnIdentification,
  convertDataTableToLookupTable,
  lookForJoinColumn,
  validateLookupTableLanguages,
  validateLookupTableReferenceValues
} from '../utils/lookup-table-utils';
import { ColumnDescriptor } from '../extractors/column-descriptor';
import { Dataset } from '../entities/dataset/dataset';
import { ViewDTO, ViewErrDTO } from '../dtos/view-dto';
import { logger } from '../utils/logger';
import { Dimension } from '../entities/dataset/dimension';
import { viewErrorGenerators } from '../utils/view-error-generators';
import { FactTableColumnType } from '../enums/fact-table-column-type';
import { cleanUpDimension, previewGenerator } from './dimension-processor';
import { SUPPORTED_LOCALES } from '../middleware/translation';
import { Locale } from '../enums/locale';
import { FileValidationErrorType, FileValidationException } from '../exceptions/validation-exception';
import { dbManager } from '../db/database-manager';
import { loadFileIntoLookupTablesSchema } from '../utils/file-utils';

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

export const validateLookupTable = async (
  protoLookupTable: DataTable,
  dataset: Dataset,
  dimension: Dimension,
  path: string,
  language: string,
  tableMatcher?: LookupTablePatchDTO
): Promise<ViewDTO | ViewErrDTO> => {
  logger.info('Validating lookup table...');

  const revision = dataset.draftRevision;
  if (!revision?.id) {
    logger.error(`Could not find the draft revision for dataset ${dataset.id}`);
    throw new Error('Could not find the draft revision for dataset');
  }

  const lookupTable = convertDataTableToLookupTable(protoLookupTable);
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
    const descriptionCol = t('lookup_column_headers.description', { lng: locale.toLowerCase() });
    if (protoLookupTable.dataTableDescriptions.find((col) => col.columnName.toLowerCase().includes(descriptionCol))) {
      tableLanguageArr.push(locale);
    }
  });

  if (tableLanguageArr.length < 1) {
    logger.error('No description columns found in lookup table');
    return viewErrorGenerators(400, dataset.id, 'csv', 'errors.measure_validation.no_description_columns', {
      mismatch: false
    });
  }

  const tableLanguage = tableLanguageArr[0];
  let confirmedJoinColumn: string | undefined;

  try {
    confirmedJoinColumn = lookForJoinColumn(protoLookupTable, dimension.factTableColumn, tableLanguage, tableMatcher);
  } catch (_err) {
    logger.error('There was a problem trying to find the join column');
    return viewErrorGenerators(400, dataset.id, 'patch', 'errors.lookup_validation.no_join_column', {
      mismatch: false
    });
  }

  if (!confirmedJoinColumn) {
    logger.error('No confirmed join column found');
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

  logger.debug('Copying lookup table from lookup_tables schema into cube');
  const actionId = crypto.randomUUID();
  const createLookupInCubeRunner = dbManager.getCubeDataSource().createQueryRunner();
  try {
    await createLookupInCubeRunner.query(
      pgformat('CREATE TABLE %I.%I AS SELECT * FROM lookup_tables.%I;', revision.id, actionId, lookupTable.id)
    );
  } catch (error) {
    await lookupTable.remove();
    logger.error(error, 'Unable to copy lookup table from lookup tables schema.');
    return viewErrorGenerators(500, dataset.id, 'patch', 'errors.dimension_validation.lookup_table_loading_failed', {
      mismatch: false
    });
  } finally {
    void createLookupInCubeRunner.release();
  }

  const referenceErrors = await validateLookupTableReferenceValues(
    revision.id,
    dataset,
    updatedDimension.factTableColumn,
    factTableColumn.columnName,
    actionId,
    'dimension'
  );

  if (referenceErrors) {
    void cleanupBadLookup(lookupTable, revision.id, actionId);
    return referenceErrors;
  }

  const languageErrors = await validateLookupTableLanguages(
    dataset,
    revision.id,
    factTableColumn.columnName,
    actionId,
    'dimension'
  );
  if (languageErrors) {
    void cleanupBadLookup(lookupTable, revision.id, actionId);
    return languageErrors;
  }

  logger.debug(`Lookup table passed validation. Saving the dimension, lookup table and extractor.`);
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
    await lookupTablePreviewRunner.query(pgformat('DROP TABLE IF EXISTS %I.%I', revision.id, actionId));
    void lookupTablePreviewRunner.release();
  }

  return previewGenerator(dimensionTable, { totalLines: dimensionTable.length }, dataset);
};

async function cleanupBadLookup(lookupTable: LookupTable, revisionId: string, actionId: string): Promise<void> {
  const cleanUpStatements = [
    pgformat('DROP TABLE IF EXISTS %I.%I;', revisionId, actionId),
    pgformat('DROP TABLE IF EXISTS lookup_tables.%I;', lookupTable.id)
  ];
  const cleanupRunner = dbManager.getCubeDataSource().createQueryRunner();
  try {
    await cleanupRunner.query(cleanUpStatements.join('/n'));
  } catch (err) {
    logger.error(err, 'Something went wrong trying to clean up the cube');
  } finally {
    void cleanupRunner.release();
  }
  await lookupTable.remove();
}
