import fs from 'fs';

import { Database } from 'duckdb-async';
import tmp from 'tmp';

import { LookupTable } from '../entities/dataset/lookup-table';
import { DataTable } from '../entities/dataset/data-table';
import { MeasureLookupPatchDTO } from '../dtos/measure-lookup-patch-dto';
import { MeasureLookupTableExtractor } from '../extractors/measure-lookup-extractor';
import {
  columnIdentification,
  convertDataTableToLookupTable,
  lookForJoinColumn,
  validateLookupTableLanguages,
  validateLookupTableReferenceValues,
  validateMeasureTableContent
} from '../utils/lookup-table-utils';
import { ColumnDescriptor } from '../extractors/column-descriptor';
import { Dataset } from '../entities/dataset/dataset';
import { CSVHeader, ViewDTO, ViewErrDTO } from '../dtos/view-dto';
import { viewErrorGenerators, viewGenerator } from '../utils/view-error-generators';
import { logger } from '../utils/logger';
import { Measure } from '../entities/dataset/measure';
import { loadFileIntoDatabase } from '../utils/file-utils';
import { DatasetRepository } from '../repositories/dataset';
import { FactTableColumnType } from '../enums/fact-table-column-type';
import { MeasureRow } from '../entities/dataset/measure-row';
import { SUPPORTED_LOCALES } from '../middleware/translation';
import { DisplayType } from '../enums/display-type';
import { getFileService } from '../utils/get-file-service';

import { createMeasureLookupTable, measureTableCreateStatement } from './cube-handler';
import { createEmptyCubeWithFactTable } from '../utils/create-facttable';
import { FileValidationErrorType, FileValidationException } from '../exceptions/validation-exception';
import { FactTableColumn } from '../entities/dataset/fact-table-column';

const sampleSize = 5;

async function cleanUpMeasure(measureId: string) {
  const measure = await Measure.findOneByOrFail({ id: measureId });
  logger.info(`Cleaning up previous measure lookup table`);
  if (measure.lookupTable) {
    logger.debug(`Removing previously uploaded lookup table from measure`);
    try {
      const fileService = getFileService();
      await fileService.delete(measure.lookupTable.filename, measure.dataset.id);
    } catch (err) {
      logger.warn(`Something went wrong trying to remove previously uploaded lookup table with error: ${err}`);
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
    logger.error(
      `Something has gone wrong trying to unlink the previous lookup table from the measure with the following error: ${err}`
    );
    throw err;
  }
}

function createExtractor(
  protoLookupTable: DataTable,
  tableMatcher?: MeasureLookupPatchDTO
): MeasureLookupTableExtractor {
  if (tableMatcher?.description_columns) {
    logger.debug('Using user supplied table matcher to match columns');
    return {
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
    let notesColumns: ColumnDescriptor[] | undefined;
    if (protoLookupTable.dataTableDescriptions.filter((info) => info.columnName.toLowerCase().startsWith('note')))
      notesColumns = protoLookupTable.dataTableDescriptions
        .filter((info) => info.columnName.toLowerCase().startsWith('note'))
        .map((info) => columnIdentification(info));
    const extractor = {
      sortColumn: protoLookupTable.dataTableDescriptions.find((info) =>
        info.columnName.toLowerCase().startsWith('sort')
      )?.columnName,
      languageColumn: protoLookupTable.dataTableDescriptions.find((info) =>
        info.columnName.toLowerCase().startsWith('lang')
      )?.columnName,
      formatColumn: protoLookupTable.dataTableDescriptions.find(
        (info) => info.columnName.toLowerCase().indexOf('format') > -1
      )?.columnName,
      decimalColumn: protoLookupTable.dataTableDescriptions.find(
        (info) => info.columnName.toLowerCase().indexOf('decimal') > -1
      )?.columnName,
      measureTypeColumn: protoLookupTable.dataTableDescriptions.find(
        (info) => info.columnName.toLowerCase().indexOf('type') > -1
      )?.columnName,
      descriptionColumns: protoLookupTable.dataTableDescriptions
        .filter((info) => info.columnName.toLowerCase().startsWith('description'))
        .map((info) => columnIdentification(info)),
      notesColumns,
      isSW2Format: !protoLookupTable.dataTableDescriptions.find((info) =>
        info.columnName.toLowerCase().startsWith('lang')
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
) {
  lookupTable.isStatsWales2Format = extractor.isSW2Format;
  const updateMeasure = await Measure.findOneByOrFail({ id: dataset.measure.id });
  updateMeasure.joinColumn = confirmedJoinColumn;
  updateMeasure.lookupTable = lookupTable;
  updateMeasure.extractor = extractor;

  logger.debug(`Saving measure table to database using rows ${JSON.stringify(measureTable, null, 2)}`);
  for (const row of measureTable) {
    row.id = updateMeasure.id;
    row.measure = updateMeasure;
  }
  updateMeasure.measureTable = measureTable;
  updateMeasure.lookupTable = lookupTable;
  return updateMeasure;
}

async function createMeasureTable(
  quack: Database,
  measureColumn: FactTableColumn,
  joinColumn: string,
  lookupTable: string,
  extractor: MeasureLookupTableExtractor
) {
  logger.debug(`Creating empty measure table`);
  await quack.exec(measureTableCreateStatement(measureColumn.columnDatatype));

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
  let notesColumnDef = 'NULL as notes,';
  let buildMeasureViewQuery: string;
  if (extractor.isSW2Format) {
    for (const locale of SUPPORTED_LOCALES) {
      if (extractor.notesColumns) {
        const notesCol = extractor.notesColumns.find((col) => col.lang === locale.split('-')[0])?.name;
        if (notesCol) {
          notesColumnDef = `"${notesCol}"`;
        }
      }
      viewComponents.push(
        `SELECT
            "${joinColumn}" AS reference,
            '${locale.toLowerCase()}' AS language,
            "${extractor.descriptionColumns.find((col) => col.lang === locale.split('-')[0])?.name}" AS description,
            ${notesColumnDef} AS notes,
            ${sortOrderDef} AS sort_order,
            ${formatColumn} AS format,
            ${decimalColumnDef} AS decimals,
            ${measureTypeDef} AS measure_type,
            ${hierarchyDef} AS hierarchy
         FROM ${lookupTable}\n`
      );
    }
    buildMeasureViewQuery = `${viewComponents.join('\nUNION\n')}`;
    logger.debug(`Extracting SW2 measure lookup table to measure table using query ${buildMeasureViewQuery}`);
  } else {
    if (extractor.notesColumns) {
      notesColumnDef = `"${extractor.notesColumns[0]}"`;
    }
    buildMeasureViewQuery = `SELECT
                    "${joinColumn}" AS reference,
                    "${extractor.languageColumn}" AS language,
                    "${extractor.descriptionColumns[0]}" AS description,
                    ${notesColumnDef} AS notes,
                    ${sortOrderDef} AS sort_order,
                    ${formatColumn} AS format,
                    ${decimalColumnDef} AS decimals,
                    ${measureTypeDef} AS measure_type,
                    ${hierarchyDef} AS hierarchy
                FROM ${lookupTable}\n`;
    logger.debug(`Extracting SW3 measure lookup table to measure table using query ${buildMeasureViewQuery}`);
  }
  try {
    const inertQuery = `INSERT INTO measure (${buildMeasureViewQuery});`;
    logger.debug(`Extracting lookup table contents to measure using query:\n ${inertQuery}`);
    await quack.exec(inertQuery);
    await quack.exec(`DROP TABLE ${lookupTable};`);
  } catch (error) {
    logger.error(error, `Something went wrong trying to extract the lookup tables contents to measure.`);
    throw new FileValidationException(
      'errors.measure_validation.extracting_data_failed',
      FileValidationErrorType.InvalidCsv
    );
  }

  const tableContents = await quack.all(`SELECT * FROM measure;`);
  logger.debug(`Creating measureTable from lookup using result:\n${JSON.stringify(tableContents, null, 2)}`);
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
  return measureTable;
}

export const validateMeasureLookupTable = async (
  protoLookupTable: DataTable,
  dataset: Dataset,
  buffer: Buffer,
  lang: string,
  tableMatcher?: MeasureLookupPatchDTO
): Promise<ViewDTO | ViewErrDTO> => {
  const measureColumn = dataset.factTable?.find((col) => col.columnType === FactTableColumnType.Measure);
  if (!measureColumn) {
    logger.error(`Something went wrong trying to find the measure column for dataset ${dataset.id}`);
    return viewErrorGenerators(500, dataset.id, 'patch', 'errors.dataset.measure_not_found', {});
  }
  const lookupTable = convertDataTableToLookupTable(protoLookupTable);
  const factTableName = 'fact_table';
  const lookupTableName = 'preview_lookup';
  const measure = dataset.measure;
  let quack: Database;
  try {
    quack = await createEmptyCubeWithFactTable(dataset);
  } catch (error) {
    logger.error(error, 'Something went wrong trying to create a new database');
    return viewErrorGenerators(500, dataset.id, 'patch', 'errors.cube_builder.fact_table_creation_failed', {});
  }

  const lookupTableTmpFile = tmp.tmpNameSync({ postfix: `.${lookupTable.fileType}` });
  try {
    fs.writeFileSync(lookupTableTmpFile, buffer);
    await loadFileIntoDatabase(quack, lookupTable, lookupTableTmpFile, lookupTableName);
    fs.unlinkSync(lookupTableTmpFile);
  } catch (err) {
    await quack.close();
    logger.error(err, `Something went wrong trying to load data in to DuckDB with the following error: ${err}`);
    return viewErrorGenerators(500, dataset.id, 'csv', 'errors.dimension.unknown_error', {
      mismatch: false
    });
  }

  let confirmedJoinColumn: string | undefined;
  try {
    confirmedJoinColumn = lookForJoinColumn(protoLookupTable, measure.factTableColumn, tableMatcher);
  } catch (_err) {
    await quack.close();
    return viewErrorGenerators(400, dataset.id, 'csv', 'errors.measure_validation.no_join_column', {
      mismatch: false
    });
  }

  if (!confirmedJoinColumn) {
    await quack.close();
    return viewErrorGenerators(400, dataset.id, 'csv', 'errors.measure_validation.no_join_column', {
      mismatch: false
    });
  }

  let extractor: MeasureLookupTableExtractor;
  try {
    extractor = createExtractor(protoLookupTable, tableMatcher);
  } catch (error) {
    logger.error(error, `Something went wrong trying to create the measure lookup table extractor`);
    await quack.close();
    return viewErrorGenerators(400, dataset.id, 'csv', 'errors.measure_validation.no_description_columns', {
      mismatch: false
    });
  }

  let measureTable: MeasureRow[];
  try {
    measureTable = await createMeasureTable(quack, measureColumn, confirmedJoinColumn, lookupTableName, extractor);
  } catch (err) {
    const error = err as FileValidationException;
    await quack.close();
    logger.error(err, `Something went wrong trying to create the measure table with the following error: ${err}`);
    return viewErrorGenerators(400, dataset.id, 'csv', error.errorTag, {
      mismatch: false
    });
  }
  const updatedMeasure = await updateMeasure(dataset, lookupTable, confirmedJoinColumn, measureTable, extractor);

  const referenceErrors = await validateLookupTableReferenceValues(
    quack,
    dataset,
    updatedMeasure.factTableColumn,
    'reference',
    'measure',
    factTableName,
    'measure'
  );
  if (referenceErrors) {
    await quack.close();
    return referenceErrors;
  }

  const languageErrors = await validateLookupTableLanguages(quack, dataset, 'reference', 'measure', 'measure');
  if (languageErrors) {
    await quack.close();
    return languageErrors;
  }

  const tableValidationErrors = await validateMeasureTableContent(quack, dataset.id, 'measure', extractor);
  if (tableValidationErrors) {
    await quack.close();
    return tableValidationErrors;
  }

  logger.debug(`Measure table validation successful.  Now saving measure.`);
  // Clean up previously uploaded measure
  await cleanUpMeasure(dataset.measure.id);
  if (updatedMeasure.lookupTable) await updatedMeasure.lookupTable.save();
  await updatedMeasure.save();

  try {
    const dimensionTable = await quack.all(`SELECT * FROM measure where language = '${lang}';`);
    await quack.close();
    const tableHeaders = Object.keys(dimensionTable[0]);
    const dataArray = dimensionTable.map((row) => Object.values(row));
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
      total_records: dataArray.length,
      start_record: 1,
      end_record: dataArray.length
    };
    const pageSize = dataArray.length;
    return viewGenerator(currentDataset, 1, pageInfo, pageSize, 1, headers, dataArray);
  } catch (error) {
    await quack.close();
    logger.error(`Something went wrong trying to generate the preview of the lookup table with error: ${error}`);
    return viewErrorGenerators(500, dataset.id, 'csv', 'errors.dimension.unknown_error', {
      mismatch: false
    });
  }
};

async function getMeasurePreviewWithoutExtractor(
  dataset: Dataset,
  measure: Measure,
  quack: Database,
  tableName: string
): Promise<ViewDTO> {
  const preview = await quack.all(
    `SELECT DISTINCT "${measure.factTableColumn}" FROM ${tableName} ORDER BY "${measure.factTableColumn}" ASC LIMIT ${sampleSize};`
  );
  const tableHeaders = Object.keys(preview[0]);
  const dataArray = preview.map((row) => Object.values(row));
  const currentDataset = await DatasetRepository.getById(dataset.id);
  const headers: CSVHeader[] = [];
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

async function getMeasurePreviewWithExtractor(dataset: Dataset, measure: Measure, quack: Database, lang: string) {
  logger.debug(`Generating lookup table preview for measure ${measure.id}`);
  const measureColumn = dataset.factTable?.find((col) => col.columnType === FactTableColumnType.Measure);
  if (!measureColumn) {
    logger.error(`Something went wrong trying to find the measure column for dataset ${dataset.id}`);
    return viewErrorGenerators(500, dataset.id, 'patch', 'errors.dataset.measure_not_found', {});
  }
  if (!measure.measureTable || measure.measureTable.length === 0) {
    logger.error(`Something went wrong trying to find the measure table for dataset ${dataset.id}`);
    return viewErrorGenerators(500, dataset.id, 'patch', 'errors.dataset.measure_not_found', {});
  }
  await createMeasureLookupTable(quack, measureColumn, measure.measureTable);
  const query = `SELECT * FROM measure WHERE language = '${lang.toLowerCase()}' ORDER BY sort_order, reference LIMIT ${sampleSize};`;
  logger.debug(`Querying the cube to get the preview using query: ${query}`);
  const measureTable = await quack.all(query);
  const tableHeaders = Object.keys(measureTable[0]);
  const dataArray = measureTable.map((row) => Object.values(row));
  const currentDataset = await DatasetRepository.getById(dataset.id);
  const headers: CSVHeader[] = tableHeaders.map((name, idx) => ({
    name,
    index: idx,
    source_type: FactTableColumnType.Unknown
  }));
  const pageInfo = {
    total_records: measureTable.length,
    start_record: 1,
    end_record: dataArray.length
  };
  const pageSize = measureTable.length < sampleSize ? measureTable.length : sampleSize;
  return viewGenerator(currentDataset, 1, pageInfo, pageSize, 1, headers, dataArray);
}

export const getMeasurePreview = async (dataset: Dataset, lang: string): Promise<ViewDTO | ViewErrDTO> => {
  logger.debug(`Getting preview for measure: ${dataset.measure.id}`);
  const tableName = 'fact_table';
  let quack: Database;
  try {
    quack = await createEmptyCubeWithFactTable(dataset);
  } catch (error) {
    logger.error(error, 'Something went wrong trying to create a new database');
    return viewErrorGenerators(500, dataset.id, 'patch', 'errors.cube_builder.fact_table_creation_failed', {});
  }

  const measure = dataset.measure;
  if (!measure) {
    return viewErrorGenerators(500, dataset.id, 'measure', 'errors.dataset.measure_not_found', {});
  }
  try {
    if (measure.measureTable && measure.measureTable.length > 0) {
      return await getMeasurePreviewWithExtractor(dataset, measure, quack, lang);
    } else {
      logger.debug('Straight column preview');
      return await getMeasurePreviewWithoutExtractor(dataset, measure, quack, tableName);
    }
  } catch (error) {
    logger.error(error, `Something went wrong trying to generate the preview of the measure`);
    return viewErrorGenerators(500, dataset.id, 'csv', 'errors.measure.unknown_error', {
      mismatch: false
    });
  } finally {
    await quack.close();
  }
};
