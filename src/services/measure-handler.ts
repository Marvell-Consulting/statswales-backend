import { format as pgformat } from '@scaleleap/pg-format';
import { t } from 'i18next';

import { LookupTable } from '../entities/dataset/lookup-table';
import { DataTable } from '../entities/dataset/data-table';
import { MeasureLookupTableExtractor } from '../extractors/measure-lookup-extractor';
import {
  columnIdentification,
  convertDataTableToLookupTable,
  lookForJoinColumn,
  validateLookupTableLanguages,
  validateMeasureTableContent
} from '../utils/lookup-table-utils';
import { Dataset } from '../entities/dataset/dataset';
import { ColumnHeader, ViewDTO, ViewErrDTO } from '../dtos/view-dto';
import { viewErrorGenerators, viewGenerator } from '../utils/view-error-generators';
import { logger } from '../utils/logger';
import { Measure } from '../entities/dataset/measure';
import { DatasetRepository } from '../repositories/dataset';
import { FactTableColumnType } from '../enums/fact-table-column-type';
import { MeasureRow } from '../entities/dataset/measure-row';
import { SUPPORTED_LOCALES } from '../middleware/translation';
import { DisplayType } from '../enums/display-type';
import { FACT_TABLE_NAME } from './cube-builder';
import { FileValidationErrorType, FileValidationException } from '../exceptions/validation-exception';
import { Locale } from '../enums/locale';
import { Revision } from '../entities/dataset/revision';
import { dbManager } from '../db/database-manager';
import { MeasureMetadata } from '../entities/dataset/measure-metadata';
import { RevisionTask } from '../interfaces/revision-task';
import { convertLookupTableToSW3Format } from '../utils/file-utils';
import { randomUUID } from 'node:crypto';
import {
  cleanUpPostgresValidationSchema,
  createPostgresValidationSchema,
  saveValidatedLookupTableToDatabase
} from '../utils/validation-schema-handler';
import { DataTableDescription } from '../entities/dataset/data-table-description';
import { confirmJoinColumnAndValidateReferenceValues } from './lookup-table-handler';
import { previewGenerator } from '../utils/preview-generator';

const sampleSize = 5;

interface MeasureTable {
  reference: string;
  language: string;
  description: string;
  notes: string;
  sort_order: number;
  format: string;
  decimals: number;
  measure_type: string;
  hierarchy: string;
}

function createMeasureExtractor(
  confirmedJoinColumn: string,
  tableColumns: DataTableDescription[],
  tableLanguage: Locale
): MeasureLookupTableExtractor {
  // Possible headings based on language used for the description column
  const noteStr = t('lookup_column_headers.notes', { lng: tableLanguage });
  const sortStr = t('lookup_column_headers.sort', { lng: tableLanguage });
  const formatStr = t('lookup_column_headers.format', { lng: tableLanguage });
  const decimalStr = t('lookup_column_headers.decimal', { lng: tableLanguage });
  const measureTypeStr = t('lookup_column_headers.type', { lng: tableLanguage });
  const hierarchyStr = t('lookup_column_headers.hierarchy', { lng: tableLanguage });
  const descriptionStr = t('lookup_column_headers.description', { lng: tableLanguage });
  const langStr = t('lookup_column_headers.lang', { lng: tableLanguage });

  const extractor: MeasureLookupTableExtractor = {
    tableLanguage,
    isSW2Format: true,
    notesColumns: [],
    descriptionColumns: [],
    otherColumns: []
  };

  logger.debug('Detecting measure table column types from column names');
  tableColumns.forEach((column) => {
    const columnName = column.columnName.toLowerCase();
    if (columnName === confirmedJoinColumn.toLowerCase()) {
      extractor.joinColumn = confirmedJoinColumn;
    } else if (columnName.includes(descriptionStr)) {
      extractor.descriptionColumns.push(columnIdentification(column));
    } else if (columnName.startsWith(langStr)) {
      extractor.languageColumn = column.columnName;
      extractor.isSW2Format = false;
    } else if (columnName.startsWith(noteStr)) {
      extractor.notesColumns?.push(columnIdentification(column));
    } else if (columnName.startsWith(sortStr)) {
      extractor.sortColumn = column.columnName;
    } else if (columnName.includes(hierarchyStr)) {
      extractor.hierarchyColumn = column.columnName;
    } else if (columnName.includes(decimalStr)) {
      extractor.decimalColumn = column.columnName;
    } else if (columnName.includes(formatStr)) {
      extractor.formatColumn = column.columnName;
    } else if (columnName.includes(measureTypeStr)) {
      extractor.measureTypeColumn = column.columnName;
    } else {
      extractor.otherColumns?.push(column.columnName);
    }
  });

  if (extractor.notesColumns!.length > 0) {
    extractor.notesColumns = undefined;
  }
  logger.trace(`Extractor created as ${JSON.stringify(extractor)}`);
  return extractor;
}

async function updateMeasure(
  dataset: Dataset,
  lookupTable: LookupTable,
  confirmedJoinColumn: string,
  measureTable: MeasureRow[],
  extractor: MeasureLookupTableExtractor
): Promise<Measure> {
  lookupTable.isStatsWales2Format = extractor.isSW2Format;
  const updateMeasure = await Measure.findOneByOrFail({ id: dataset.measure.id });
  updateMeasure.joinColumn = confirmedJoinColumn;
  updateMeasure.lookupTable = lookupTable;
  updateMeasure.extractor = extractor;

  // logger.debug(`Saving measure table to database using rows ${JSON.stringify(measureTable, null, 2)}`);
  for (const row of measureTable) {
    row.id = updateMeasure.id;
    row.measure = updateMeasure;
  }
  updateMeasure.measureTable = measureTable;
  updateMeasure.lookupTable = lookupTable;
  return updateMeasure;
}

async function createMeasureTable(mockCubeId: string, lookupTableId: string): Promise<MeasureRow[]> {
  const getMeasureTable = pgformat(
    'SELECT reference, language, description, format, notes, sort_order, decimals, measure_type, hierarchy FROM %I.%I;',
    mockCubeId,
    lookupTableId
  );
  let tableContents: MeasureTable[];
  const queryRunner = dbManager.getCubeDataSource().createQueryRunner();
  try {
    tableContents = await queryRunner.query(getMeasureTable);
  } catch (err) {
    logger.error(err, 'Something went wrong trying to get the measure lookup from the mock cube');
    throw err;
  } finally {
    void queryRunner.release();
  }

  const measureTable: MeasureRow[] = [];
  for (const row of tableContents) {
    const item = new MeasureRow();
    item.reference = row.reference;
    item.language = row.language;
    item.description = row.description;
    item.format = row.format.toLowerCase() as DisplayType;
    item.notes = row.notes;
    item.sortOrder = row.sort_order;
    item.decimal = row.decimals;
    item.measureType = row.measure_type;
    item.hierarchy = row.hierarchy;
    measureTable.push(item);
  }
  logger.trace(`Measure table: ${JSON.stringify(measureTable)}`);
  return measureTable;
}

export const validateMeasureLookupTable = async (
  protoLookupTable: DataTable,
  dataset: Dataset,
  draftRevision: Revision,
  path: string,
  lang: string
): Promise<ViewDTO | ViewErrDTO> => {
  const mockCubeId = randomUUID();
  const mockCubePromise = createPostgresValidationSchema(
    mockCubeId,
    draftRevision.id,
    dataset.measure.factTableColumn,
    `${protoLookupTable.id}_tmp`
  );

  const measureColumn = dataset.factTable?.find((col) => col.columnType === FactTableColumnType.Measure);
  if (!measureColumn) {
    await mockCubePromise
      .finally(() => {
        return cleanUpPostgresValidationSchema(mockCubeId, lookupTable.id);
      })
      .catch((err) => {
        logger.error(err, 'Something went wrong trying to clean up the mock cube');
      });
    logger.error(`Something went wrong trying to find the measure column for dataset ${dataset.id}`);
    return viewErrorGenerators(500, dataset.id, 'patch', 'errors.dataset.measure_not_found', {});
  }

  const tableLanguageArr: Locale[] = [];
  SUPPORTED_LOCALES.map((locale) => {
    if (
      protoLookupTable.dataTableDescriptions.find((col) =>
        col.columnName.toLowerCase().includes(t('lookup_column_headers.description', { lng: locale }))
      )
    ) {
      tableLanguageArr.push(locale);
    }
  });
  if (tableLanguageArr.length < 1) {
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

  const lookupTableColumns = protoLookupTable.dataTableDescriptions;
  const lookupTable = convertDataTableToLookupTable(protoLookupTable);

  const measure = dataset.measure;
  let possibleJoinColumns: string[];
  try {
    possibleJoinColumns = lookForJoinColumn(lookupTableColumns, measure.factTableColumn, tableLanguage);
  } catch (_err) {
    await mockCubePromise
      .finally(() => {
        return cleanUpPostgresValidationSchema(mockCubeId, lookupTable.id);
      })
      .catch((err) => {
        logger.error(err, 'Something went wrong trying to clean up the mock cube');
      });
    return viewErrorGenerators(400, dataset.id, 'csv', 'errors.measure_validation.no_join_column', {
      mismatch: false
    });
  }

  await mockCubePromise;
  let lookupReferenceColumn: string;
  try {
    lookupReferenceColumn = await confirmJoinColumnAndValidateReferenceValues(
      possibleJoinColumns,
      measureColumn.columnName,
      mockCubeId,
      draftRevision.id,
      'measure'
    );
  } catch (err) {
    const error = err as FileValidationException;
    void cleanUpPostgresValidationSchema(mockCubeId, lookupTable.id).catch((err) => {
      logger.error(err, 'Something went wrong trying to clean up the mock cube');
    });
    return viewErrorGenerators(400, dataset.id, 'patch', error.errorTag, error.extension);
  }

  let extractor: MeasureLookupTableExtractor;
  try {
    extractor = createMeasureExtractor(lookupReferenceColumn, lookupTableColumns, tableLanguage);
  } catch (error) {
    logger.error(error, `Something went wrong trying to create the measure lookup table extractor`);
    return viewErrorGenerators(400, dataset.id, 'csv', 'errors.measure_validation.no_description_columns', {
      mismatch: false
    });
  }

  try {
    logger.debug(`Converting lookup table to the correct format...`);
    await convertLookupTableToSW3Format(
      mockCubeId,
      lookupTable,
      extractor,
      measureColumn,
      lookupReferenceColumn,
      'measure'
    );
  } catch (err) {
    logger.error(err, `Something went wrong trying to extract the lookup tables contents to measure.`);
    const error = err as { errorType?: string; message: string };
    if (error.errorType === 'Conversion') {
      if (error.message.toLowerCase().includes('decimal')) {
        throw new FileValidationException(
          'errors.measure_validation.invalid_decimals_present',
          FileValidationErrorType.BadDecimalColumn
        );
      }
      throw new FileValidationException(
        'errors.measure_validation.wrong_column_type',
        FileValidationErrorType.WrongDataTypeInReference
      );
    } else if (error.errorType) {
      throw new FileValidationException(
        'errors.measure_validation.extracting_data_failed',
        FileValidationErrorType.InvalidCsv
      );
    }
    throw new FileValidationException(
      'An unknown error occurred while trying to extract the lookup table contents to measure.',
      FileValidationErrorType.unknown,
      500
    );
  }

  let measureTable: MeasureRow[];
  try {
    measureTable = await createMeasureTable(mockCubeId, lookupTable.id);
  } catch (err) {
    const error = err as FileValidationException;
    logger.error(err, `Something went wrong trying to create the measure table with the following error: ${err}`);
    return viewErrorGenerators(400, dataset.id, 'csv', error.errorTag, {
      mismatch: false
    });
  }

  const updatedMeasure = await updateMeasure(dataset, lookupTable, lookupReferenceColumn, measureTable, extractor);

  const languageErrors = await validateLookupTableLanguages(
    dataset,
    mockCubeId,
    'reference',
    lookupTable.id,
    'measure'
  );
  if (languageErrors) {
    await lookupTable.remove();
    return languageErrors;
  }

  const tableValidationErrors = await validateMeasureTableContent(dataset.id, mockCubeId, lookupTable.id, extractor);
  if (tableValidationErrors) {
    await lookupTable.remove();
    return tableValidationErrors;
  }

  logger.debug(`Measure table validation successful. Now saving measure.`);

  // Clean up previously uploaded measure
  logger.debug(`Lookup table passed validation. Saving the dimension, lookup table and extractor.`);
  try {
    await saveValidatedLookupTableToDatabase(mockCubeId, lookupTable.id);
  } catch (err) {
    logger.error(err, 'Something went wrong trying to save the lookup table or clean up the mock cube.');
    return viewErrorGenerators(500, dataset.id, 'patch', `errors.lookup_table_validation.unknown_error`, {});
  }
  updatedMeasure.lookupTable = lookupTable;
  await lookupTable.save();
  const savedMeasure = await updatedMeasure.save();

  for (const locale of SUPPORTED_LOCALES) {
    const measureMetadata = new MeasureMetadata();
    measureMetadata.measure = savedMeasure;
    measureMetadata.name = t('column_headers.measure', { lng: locale });
    measureMetadata.language = locale;
    await measureMetadata.save();
  }

  logger.debug(`Generating preview of measure table`);
  const previewQuery = pgformat(
    'SELECT reference, description, notes, sort_order, format, decimals, measure_type, hierarchy FROM %I.%I WHERE language = %L;',
    'lookup_tables',
    lookupTable.id,
    lang
  );
  const createPreviewRunner = dbManager.getCubeDataSource().createQueryRunner();
  let measureTablePreview: Record<string, string>[];
  try {
    measureTablePreview = await createPreviewRunner.query(previewQuery);
  } catch (error) {
    logger.error(error, `Something went wrong trying to generate the preview of the lookup table`);
    return viewErrorGenerators(500, dataset.id, 'csv', 'errors.measure.unknown_error', {
      mismatch: false
    });
  } finally {
    void createPreviewRunner.release();
  }

  if (!measureTablePreview || measureTablePreview.length === 0) {
    logger.warn(`Measure table preview is empty for language: ${lang}`);
    return viewErrorGenerators(404, dataset.id, 'csv', 'errors.measure.empty_table', {
      mismatch: false
    });
  }
  return previewGenerator(measureTablePreview, { totalLines: measureTablePreview.length }, dataset, false);
};

async function getMeasurePreviewWithoutExtractor(
  dataset: Dataset,
  measure: Measure,
  revision: Revision
): Promise<ViewDTO> {
  const cubeDB = dbManager.getCubeDataSource().createQueryRunner();
  let preview: Record<string, string>[];
  try {
    preview = await cubeDB.query(
      pgformat(
        'SELECT DISTINCT %I FROM %I.%I ORDER BY %I ASC LIMIT %L;',
        measure.factTableColumn,
        revision.id,
        FACT_TABLE_NAME,
        measure.factTableColumn,
        sampleSize
      )
    );
  } catch (error) {
    logger.error(error, `Something went wrong trying to generate the preview of the measure column`);
    throw error;
  } finally {
    void cubeDB.release();
  }

  const tableHeaders = Object.keys(preview[0]);
  const dataArray = preview.map((row: Record<string, string>) => Object.values(row));
  const currentDataset = await DatasetRepository.getById(dataset.id);
  const headers: ColumnHeader[] = [];
  for (let i = 0; i < tableHeaders.length; i++) {
    headers.push({
      index: i,
      name: tableHeaders[i],
      source_type: FactTableColumnType.Unknown
    });
  }
  const pageInfo = {
    total_records: preview.length,
    start_record: 1,
    end_record: dataArray.length
  };
  const pageSize = preview.length < sampleSize ? preview.length : sampleSize;
  return viewGenerator(currentDataset, 1, pageInfo, pageSize, 1, headers, dataArray);
}

async function getMeasurePreviewWithExtractor(
  dataset: Dataset,
  measure: Measure,
  revision: Revision,
  lang: string
): Promise<ViewDTO> {
  logger.debug(`Generating lookup table preview for measure ${measure.id}`);
  const cubeDB = dbManager.getCubeDataSource().createQueryRunner();
  const previewQuery = pgformat(
    'SELECT reference, description, notes, sort_order, format, decimals, measure_type, hierarchy FROM %I.%I WHERE language = %L;',
    revision.id,
    'measure',
    lang
  );
  let measureTablePreview: MeasureTable[];
  try {
    measureTablePreview = await cubeDB.query(previewQuery);
  } catch (error) {
    logger.error(error, `Something went wrong trying to generate the preview of the measure table`);
    throw error;
  } finally {
    void cubeDB.release();
  }

  // Guard against empty result
  if (!measureTablePreview || measureTablePreview.length === 0) {
    const currentDataset = await DatasetRepository.getById(dataset.id);
    const headers: ColumnHeader[] = [];
    const dataArray: never[] = [];
    const pageInfo = {
      total_records: 0,
      start_record: 1,
      end_record: 0
    };
    const pageSize = 0;
    return viewGenerator(currentDataset, 1, pageInfo, pageSize, 1, headers, dataArray);
  }
  const tableHeaders = Object.keys(measureTablePreview[0]);
  const dataArray = measureTablePreview.map((row: MeasureTable) => Object.values(row));
  const currentDataset = await DatasetRepository.getById(dataset.id);
  const headers: ColumnHeader[] = tableHeaders.map((name, idx) => ({
    name,
    index: idx,
    source_type: FactTableColumnType.Unknown
  }));
  const pageInfo = {
    total_records: dataArray.length,
    start_record: 1,
    end_record: dataArray.length
  };
  const pageSize = dataArray.length < sampleSize ? dataArray.length : sampleSize;
  return viewGenerator(currentDataset, 1, pageInfo, pageSize, 1, headers, dataArray);
}

export const getMeasurePreview = async (
  dataset: Dataset,
  lang: string,
  revisionTasks?: RevisionTask
): Promise<ViewDTO | ViewErrDTO> => {
  logger.debug(`Getting preview for measure: ${dataset.measure.id}`);
  const measure = dataset.measure;

  if (!measure) {
    return viewErrorGenerators(500, dataset.id, 'measure', 'errors.dataset.measure_not_found', {});
  }

  try {
    // If there's a revision task for the measure empty the measure table to preview the raw column
    if (revisionTasks && revisionTasks.measure) measure.measureTable = [];

    if (measure.measureTable && measure.measureTable.length > 0) {
      return await getMeasurePreviewWithExtractor(dataset, measure, dataset.draftRevision!, lang);
    }

    return await getMeasurePreviewWithoutExtractor(dataset, measure, dataset.draftRevision!);
  } catch (error) {
    logger.error(error, `Something went wrong trying to generate the preview of the measure`);
    return viewErrorGenerators(500, dataset.id, 'csv', 'errors.measure.unknown_error', { mismatch: false });
  }
};

export const measureTableCreateStatement = (
  schemaName: string,
  lookupTableName: string,
  referenceColumnName: string,
  // referenceColumnType: string,
  otherColumns?: string[]
): string => {
  let otherColumnsChunk: string[] | undefined;
  if (otherColumns && otherColumns.length > 0) {
    otherColumnsChunk = otherColumns.map((col) => pgformat('%I text', col));
  }
  return pgformat(
    'CREATE TABLE %I.%I (reference TEXT NOT NULL, language VARCHAR(5) NOT NULL, description TEXT NOT NULL, notes TEXT, sort_order INTEGER, hierarchy TEXT, format TEXT, decimals INTEGER, measure_type TEXT%s);',
    schemaName,
    lookupTableName,
    // referenceColumnType,
    // referenceColumnType,
    otherColumnsChunk ? `, ${otherColumnsChunk.join(',')}` : ''
  );
};
