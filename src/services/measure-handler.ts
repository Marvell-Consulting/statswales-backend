import { performance } from 'node:perf_hooks';
import { DuckDbError } from 'duckdb-async';
import { format as pgformat } from '@scaleleap/pg-format';
import { t } from 'i18next';

import { LookupTable } from '../entities/dataset/lookup-table';
import { DataTable } from '../entities/dataset/data-table';
import { MeasureLookupPatchDTO } from '../dtos/measure-lookup-patch-dto';
import { MeasureLookupTableExtractor } from '../extractors/measure-lookup-extractor';
import {
  columnIdentification,
  convertDataTableToLookupTable,
  languageMatcherCaseStatement,
  lookForJoinColumn,
  validateLookupTableLanguages,
  validateLookupTableReferenceValues,
  validateMeasureTableContent
} from '../utils/lookup-table-utils';
import { ColumnDescriptor } from '../extractors/column-descriptor';
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
import { getFileService } from '../utils/get-file-service';
import { loadFileIntoCube, measureTableCreateStatement } from './cube-handler';
import { FileValidationErrorType, FileValidationException } from '../exceptions/validation-exception';
import { FactTableColumn } from '../entities/dataset/fact-table-column';
import { Locale } from '../enums/locale';
import { DataValueFormat } from '../enums/data-value-format';
import { duckdb, linkToPostgresSchema } from './duckdb';
import { Revision } from '../entities/dataset/revision';
import { performanceReporting } from '../utils/performance-reporting';
import { FileType } from '../enums/file-type';
import { dbManager } from '../db/database-manager';
import { MeasureMetadata } from '../entities/dataset/measure-metadata';

const sampleSize = 5;

async function cleanUpMeasure(measureId: string): Promise<void> {
  const measure = await Measure.findOneByOrFail({ id: measureId });
  logger.info(`Cleaning up previous measure lookup table`);
  if (measure.lookupTable) {
    logger.debug(`Removing previously uploaded lookup table from measure`);
    try {
      const fileService = getFileService();
      await fileService.delete(measure.lookupTable.filename, measure.dataset.id);
    } catch (err) {
      logger.warn(err, `Something went wrong trying to remove previously uploaded lookup table`);
    }
  }

  try {
    const lookupTableId = measure.lookupTable?.id;
    measure.measureTable = null;
    measure.joinColumn = null;
    measure.extractor = null;
    measure.lookupTable = null;
    await measure.save();
    await MeasureRow.delete({ id: measure.id });
    logger.debug(`Removing orphaned measure lookup table`);
    if (!lookupTableId) {
      await LookupTable.delete({ id: lookupTableId });
    }
  } catch (err) {
    logger.error(err, `Something has gone wrong trying to unlink the previous lookup table from the measure`);
    throw err;
  }
}

function createExtractor(
  protoLookupTable: DataTable,
  tableLanguage: Locale,
  tableMatcher?: MeasureLookupPatchDTO
): MeasureLookupTableExtractor {
  if (tableMatcher?.description_columns) {
    logger.debug('Using user supplied table matcher to match columns');
    return {
      tableLanguage,
      sortColumn: tableMatcher?.sort_column,
      formatColumn: tableMatcher?.format_column,
      decimalColumn: tableMatcher?.decimal_column,
      measureTypeColumn: tableMatcher?.measure_type_column,
      descriptionColumns: tableMatcher.description_columns.map(
        (desc) =>
          protoLookupTable.dataTableDescriptions
            .filter((info) => info.columnName === desc)
            .map((info) => columnIdentification(info))[0]
      ),
      notesColumns: tableMatcher.notes_columns?.map(
        (desc) =>
          protoLookupTable.dataTableDescriptions
            .filter((info) => info.columnName === desc)
            .map((info) => columnIdentification(info))[0]
      ),
      languageColumn: tableMatcher?.language_column,
      isSW2Format: !tableMatcher?.language_column
    };
  } else {
    logger.debug('Detecting column types from column names');
    const noteStr = t('lookup_column_headers.notes', { lng: tableLanguage });
    const sortStr = t('lookup_column_headers.sort', { lng: tableLanguage });
    const formatStr = t('lookup_column_headers.format', { lng: tableLanguage });
    const decimalStr = t('lookup_column_headers.decimal', { lng: tableLanguage });
    const measureTypeStr = t('lookup_column_headers.type', { lng: tableLanguage });
    const hierarchyStr = t('lookup_column_headers.hierarchy', { lng: tableLanguage });
    const descriptionStr = t('lookup_column_headers.description', { lng: tableLanguage });
    const langStr = t('lookup_column_headers.lang', { lng: tableLanguage });
    let notesColumns: ColumnDescriptor[] | undefined;
    if (protoLookupTable.dataTableDescriptions.filter((info) => info.columnName.toLowerCase().startsWith(noteStr)))
      notesColumns = protoLookupTable.dataTableDescriptions
        .filter((info) => info.columnName.toLowerCase().startsWith(noteStr))
        .map((info) => columnIdentification(info));
    const extractor = {
      tableLanguage,
      sortColumn: protoLookupTable.dataTableDescriptions.find((info) =>
        info.columnName.toLowerCase().startsWith(sortStr)
      )?.columnName,
      languageColumn: protoLookupTable.dataTableDescriptions.find((info) =>
        info.columnName.toLowerCase().startsWith(langStr)
      )?.columnName,
      formatColumn: protoLookupTable.dataTableDescriptions.find((info) =>
        info.columnName.toLowerCase().includes(formatStr)
      )?.columnName,
      decimalColumn: protoLookupTable.dataTableDescriptions.find((info) =>
        info.columnName.toLowerCase().includes(decimalStr)
      )?.columnName,
      measureTypeColumn: protoLookupTable.dataTableDescriptions.find((info) =>
        info.columnName.toLowerCase().includes(measureTypeStr)
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
    if (extractor.descriptionColumns.length === 0) {
      throw new FileValidationException(
        'errors.measure_validation.no_description_columns',
        FileValidationErrorType.InvalidCsv
      );
    }
    return extractor;
  }
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

async function createMeasureTable(
  revisionId: string,
  measureId: string,
  measureColumn: FactTableColumn,
  joinColumn: string,
  extractor: MeasureLookupTableExtractor,
  fileType: FileType,
  path: string
): Promise<MeasureRow[]> {
  const start = performance.now();
  const lookupTableName = 'measure_draft';
  const quack = await duckdb();
  logger.debug(`Creating empty measure table`);
  await quack.exec(measureTableCreateStatement(measureColumn.columnDatatype));
  await loadFileIntoCube(quack, fileType, path, lookupTableName);
  const measureTable: MeasureRow[] = [];
  const viewComponents: string[] = [];
  logger.debug(`Setting up measure insert query`);
  let formatColumn = 'NULL AS format,';
  if (extractor.formatColumn) {
    formatColumn = `"${extractor.formatColumn}"`;
  } else if (!extractor.formatColumn && !extractor.decimalColumn) {
    formatColumn = `'text'`;
  } else if (!extractor.formatColumn && extractor.decimalColumn) {
    formatColumn = `CASE WHEN "${extractor.decimalColumn}" > 0 THEN 'float' ELSE 'integer' END`;
  }
  const decimalColumnDef = extractor.decimalColumn ? `"${extractor.decimalColumn}"` : 'NULL';
  const sortOrderDef = extractor.sortColumn ? `"${extractor.sortColumn}"` : 'NULL';
  const measureTypeDef = extractor.measureTypeColumn ? `"${extractor.measureTypeColumn}"` : 'NULL';
  const hierarchyDef = extractor.hierarchyColumn ? `"${extractor.hierarchyColumn}"` : 'NULL';
  let notesColumnDef = 'NULL';
  let buildMeasureViewQuery: string;
  if (extractor.isSW2Format) {
    if (extractor.descriptionColumns.length < SUPPORTED_LOCALES.length) {
      throw new FileValidationException(
        'errors.measure_validation.missing_languages',
        FileValidationErrorType.MissingLanguages
      );
    }
    for (const locale of SUPPORTED_LOCALES) {
      if (extractor.notesColumns) {
        const notesCol = extractor.notesColumns.find((col) => col.lang === locale.toLowerCase())?.name;
        if (notesCol) {
          notesColumnDef = `"${notesCol}"`;
        }
      }
      viewComponents.push(
        `SELECT
            "${joinColumn}" AS reference,
            '${locale.toLowerCase()}' AS language,
            "${extractor.descriptionColumns.find((col) => col.lang === locale.toLowerCase())?.name}" AS description,
            ${notesColumnDef} AS notes,
            ${sortOrderDef} AS sort_order,
            ${formatColumn} AS format,
            ${decimalColumnDef} AS decimals,
            ${measureTypeDef} AS measure_type,
            ${hierarchyDef} AS hierarchy
         FROM ${lookupTableName}\n`
      );
    }
    buildMeasureViewQuery = `${viewComponents.join('\nUNION\n')}`;
  } else {
    if (extractor.notesColumns && extractor.notesColumns.length > 0) {
      notesColumnDef = `"${extractor.notesColumns[0].name}"`;
    } else {
      notesColumnDef = 'NULL';
    }

    const measureMatcher = languageMatcherCaseStatement(extractor.languageColumn);

    buildMeasureViewQuery = `
      SELECT
        "${joinColumn}" AS reference,
        ${measureMatcher} AS language,
        "${extractor.descriptionColumns[0].name}" AS description,
        ${notesColumnDef} AS notes,
        ${sortOrderDef} AS sort_order,
        ${formatColumn} AS format,
        ${decimalColumnDef} AS decimals,
        ${measureTypeDef} AS measure_type,
        ${hierarchyDef} AS hierarchy
      FROM ${lookupTableName}
    `;
  }
  try {
    const insertQuery = `INSERT INTO measure (${buildMeasureViewQuery});`;
    await quack.exec(insertQuery);
    for (const locale of SUPPORTED_LOCALES) {
      await quack.exec(
        pgformat(
          'UPDATE measure SET language = %L WHERE language = lower(%L)',
          locale.toLowerCase(),
          locale.split('-')[0]
        )
      );
      await quack.exec(
        pgformat(
          'UPDATE measure SET language = %L WHERE language = lower(%L)',
          locale.toLowerCase(),
          locale.toLowerCase()
        )
      );
      for (const sublocale of SUPPORTED_LOCALES) {
        await quack.exec(
          pgformat(
            'UPDATE measure SET language = %L WHERE language = lower(%L)',
            sublocale.toLowerCase(),
            t(`language.${sublocale.split('-')[0]}`, { lng: locale })
          ).toLowerCase()
        );
      }
    }
  } catch (err) {
    logger.error(err, `Something went wrong trying to extract the lookup tables contents to measure.`);
    const error = err as DuckDbError;
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

  // Convert formats if they're in something other than English
  if (!extractor.tableLanguage.includes('en')) {
    for (const format of Object.values(DataValueFormat)) {
      await quack.exec(`
        UPDATE measure
        SET format = '${format}'
        WHERE format = LOWER('${t(`formats.${format}`, { lng: extractor.tableLanguage.toLowerCase() })}');
      `);
    }
  }

  try {
    await linkToPostgresSchema(quack, 'lookup_tables');
    await quack.exec(pgformat('DROP TABLE IF EXISTS %I', measureId));
    await quack.exec(pgformat('CREATE TABLE lookup_tables_db.%I AS SELECT * FROM memory.measure;', measureId));
  } catch (err) {
    logger.error(err, 'Something went wrong trying to copy the measure table to postgres');
    await quack.close();
    throw new FileValidationException('errors.measure_validation.copy_failure', FileValidationErrorType.unknown);
  }

  const tableContents = await quack.all(`SELECT * FROM memory.measure;`);
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
  await quack.close();
  performanceReporting(start - performance.now(), 500, 'Loading measure lookup table into postgres');
  return measureTable;
}

export const validateMeasureLookupTable = async (
  protoLookupTable: DataTable,
  dataset: Dataset,
  path: string,
  lang: string,
  tableMatcher?: MeasureLookupPatchDTO
): Promise<ViewDTO | ViewErrDTO> => {
  const draftRevision = dataset.draftRevision;
  const measureColumn = dataset.factTable?.find((col) => col.columnType === FactTableColumnType.Measure);
  if (!measureColumn) {
    logger.error(`Something went wrong trying to find the measure column for dataset ${dataset.id}`);
    return viewErrorGenerators(500, dataset.id, 'patch', 'errors.dataset.measure_not_found', {});
  }
  if (!draftRevision) {
    logger.error(`Something went wrong trying to find the draft revision for dataset ${dataset.id}`);
    return viewErrorGenerators(500, dataset.id, 'patch', 'errors.dataset.draft_not_found', {});
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
    return viewErrorGenerators(400, dataset.id, 'csv', 'errors.measure_validation.no_description_columns', {
      mismatch: false
    });
  }
  const tableLanguage = tableLanguageArr[0];

  const lookupTable = convertDataTableToLookupTable(protoLookupTable);
  const factTableName = 'fact_table';

  const measure = dataset.measure;
  let confirmedJoinColumn: string | undefined;
  try {
    confirmedJoinColumn = lookForJoinColumn(protoLookupTable, measure.factTableColumn, tableLanguage, tableMatcher);
  } catch (_err) {
    return viewErrorGenerators(400, dataset.id, 'csv', 'errors.measure_validation.no_join_column', {
      mismatch: false
    });
  }

  if (!confirmedJoinColumn) {
    return viewErrorGenerators(400, dataset.id, 'csv', 'errors.measure_validation.no_join_column', {
      mismatch: false
    });
  }

  let extractor: MeasureLookupTableExtractor;
  try {
    extractor = createExtractor(protoLookupTable, tableLanguage, tableMatcher);
  } catch (error) {
    logger.error(error, `Something went wrong trying to create the measure lookup table extractor`);
    return viewErrorGenerators(400, dataset.id, 'csv', 'errors.measure_validation.no_description_columns', {
      mismatch: false
    });
  }

  let measureTable: MeasureRow[];
  try {
    measureTable = await createMeasureTable(
      draftRevision.id,
      measure.id,
      measureColumn,
      confirmedJoinColumn,
      extractor,
      protoLookupTable.fileType,
      path
    );
  } catch (err) {
    const error = err as FileValidationException;
    logger.error(err, `Something went wrong trying to create the measure table with the following error: ${err}`);
    return viewErrorGenerators(400, dataset.id, 'csv', error.errorTag, {
      mismatch: false
    });
  }

  const cubeDB = dbManager.getCubeDataSource().createQueryRunner();
  try {
    await cubeDB.query(pgformat(`SET search_path TO %I;`, draftRevision.id));
  } catch (error) {
    await lookupTable.remove();
    cubeDB.release();
    logger.error(error, 'Unable to connect to postgres schema for revision.');
    return viewErrorGenerators(500, dataset.id, 'patch', 'errors.measure_validation.lookup_table_loading_failed', {
      mismatch: false
    });
  }
  logger.debug('Copying lookup table from lookup_tables schema into cube');
  const actionId = crypto.randomUUID();
  try {
    await cubeDB.query(measureTableCreateStatement(measureColumn.columnDatatype, actionId));
    for (const row of measureTable) {
      logger.debug(`Inserting into measure table ${JSON.stringify(row)}`);
      const values = [
        row.reference,
        row.language,
        row.description,
        row.notes,
        row.sortOrder,
        row.format,
        row.decimal,
        row.measureType,
        row.hierarchy
      ];
      await cubeDB.query(pgformat('INSERT INTO %I VALUES (%L);', actionId, values));
    }
  } catch (error) {
    await lookupTable.remove();
    cubeDB.release();
    logger.error(error, 'Unable to copy lookup table from lookup tables schema.');
    return viewErrorGenerators(500, dataset.id, 'patch', 'errors.dimension_validation.lookup_table_loading_failed', {
      mismatch: false
    });
  }

  const updatedMeasure = await updateMeasure(dataset, lookupTable, confirmedJoinColumn, measureTable, extractor);

  const referenceErrors = await validateLookupTableReferenceValues(
    cubeDB,
    dataset,
    updatedMeasure.factTableColumn,
    'reference',
    actionId,
    factTableName,
    'measure'
  );
  if (referenceErrors) {
    await lookupTable.remove();
    await cubeDB.query(pgformat('DROP TABLE %I;', actionId));
    cubeDB.release();
    return referenceErrors;
  }

  const languageErrors = await validateLookupTableLanguages(
    cubeDB,
    dataset,
    draftRevision.id,
    'reference',
    actionId,
    'measure'
  );
  if (languageErrors) {
    await lookupTable.remove();
    await cubeDB.query(pgformat('DROP TABLE %I;', actionId));
    cubeDB.release();
    return languageErrors;
  }

  const tableValidationErrors = await validateMeasureTableContent(cubeDB, dataset.id, actionId, extractor);
  if (tableValidationErrors) {
    await lookupTable.remove();
    await cubeDB.query(pgformat('DROP TABLE %I;', actionId));
    cubeDB.release();
    return tableValidationErrors;
  }

  logger.debug(`Measure table validation successful. Now saving measure.`);
  // Clean up previously uploaded measure
  await cleanUpMeasure(dataset.measure.id);
  if (updatedMeasure.lookupTable) await updatedMeasure.lookupTable.save();
  const savedMeasure = await updatedMeasure.save();
  for (const locale of SUPPORTED_LOCALES) {
    const measureMetadata = new MeasureMetadata();
    measureMetadata.measure = savedMeasure;
    measureMetadata.name = t('column_headers.measure', { lng: locale });
    measureMetadata.language = locale;
    await measureMetadata.save();
  }

  try {
    logger.debug(`Generating preview of measure table`);
    const previewQuery = pgformat(
      'SELECT reference, description, notes, sort_order, format, decimals, measure_type, hierarchy FROM %I WHERE language = %L;',
      actionId,
      lang
    );
    const measureTable = await cubeDB.query(previewQuery);
    const tableHeaders = Object.keys(measureTable[0]);
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const dataArray = measureTable.map((row: any) => Object.values(row));
    const currentDataset = await DatasetRepository.getById(dataset.id);
    const headers: ColumnHeader[] = [];
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
      total_records: dataArray.length,
      start_record: 1,
      end_record: dataArray.length
    };
    const pageSize = dataArray.length;
    return viewGenerator(currentDataset, 1, pageInfo, pageSize, 1, headers, dataArray);
  } catch (error) {
    logger.error(error, `Something went wrong trying to generate the preview of the lookup table`);
    return viewErrorGenerators(500, dataset.id, 'csv', 'errors.dimension.unknown_error', {
      mismatch: false
    });
  } finally {
    await cubeDB.query(pgformat('DROP TABLE %I;', actionId));
    cubeDB.release();
  }
};

async function getMeasurePreviewWithoutExtractor(
  dataset: Dataset,
  measure: Measure,
  revision: Revision
): Promise<ViewDTO> {
  const cubeDB = dbManager.getCubeDataSource().createQueryRunner();
  try {
    await cubeDB.query(pgformat(`SET search_path TO %I;`, revision.id));
    const preview = await cubeDB.query(
      pgformat(
        'SELECT DISTINCT %I FROM %I ORDER BY %I ASC LIMIT %L;',
        measure.factTableColumn,
        'fact_table',
        measure.factTableColumn,
        sampleSize
      )
    );

    const tableHeaders = Object.keys(preview[0]);
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const dataArray = preview.map((row: any) => Object.values(row));
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
  } catch (error) {
    logger.error(error, `Something went wrong trying to generate the preview of the measure column`);
    throw error;
  } finally {
    cubeDB.release();
  }
}

async function getMeasurePreviewWithExtractor(
  dataset: Dataset,
  measure: Measure,
  revision: Revision,
  lang: string
): Promise<ViewDTO> {
  logger.debug(`Generating lookup table preview for measure ${measure.id}`);
  const cubeDB = dbManager.getCubeDataSource().createQueryRunner();
  try {
    await cubeDB.query(pgformat(`SET search_path TO %I;`, revision.id));
    const previewQuery = pgformat(
      'SELECT reference, description, notes, sort_order, format, decimals, measure_type, hierarchy FROM %I WHERE language = %L;',
      'measure',
      lang
    );
    const measureTable = await cubeDB.query(previewQuery);
    const tableHeaders = Object.keys(measureTable[0]);
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const dataArray = measureTable.map((row: any) => Object.values(row));
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
  } catch (error) {
    logger.error(error, `Something went wrong trying to generate the preview of the measure table`);
    throw error;
  } finally {
    cubeDB.release();
  }
}

export const getMeasurePreview = async (dataset: Dataset, lang: string): Promise<ViewDTO | ViewErrDTO> => {
  logger.debug(`Getting preview for measure: ${dataset.measure.id}`);
  const measure = dataset.measure;

  if (!measure) {
    return viewErrorGenerators(500, dataset.id, 'measure', 'errors.dataset.measure_not_found', {});
  }

  try {
    if (measure.measureTable && measure.measureTable.length > 0) {
      return await getMeasurePreviewWithExtractor(dataset, measure, dataset.draftRevision!, lang);
    } else {
      logger.debug('Straight column preview');
      return await getMeasurePreviewWithoutExtractor(dataset, measure, dataset.draftRevision!);
    }
  } catch (error) {
    logger.error(error, `Something went wrong trying to generate the preview of the measure`);
    return viewErrorGenerators(500, dataset.id, 'csv', 'errors.measure.unknown_error', { mismatch: false });
  }
};
