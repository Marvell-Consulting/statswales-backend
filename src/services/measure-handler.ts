import fs from 'fs';

import { Database } from 'duckdb-async';
import tmp from 'tmp';

import { LookupTable } from '../entities/dataset/lookup-table';
import { DataTable } from '../entities/dataset/data-table';
import { MeasureLookupPatchDTO } from '../dtos/measure-lookup-patch-dto';
import { MeasureLookupTableExtractor } from '../extractors/measure-lookup-extractor';
import { columnIdentification, convertFactTableToLookupTable, lookForJoinColumn } from '../utils/lookup-table-utils';
import { ColumnDescriptor } from '../extractors/column-descriptor';
import { Dataset } from '../entities/dataset/dataset';
import { CSVHeader, ViewDTO, ViewErrDTO } from '../dtos/view-dto';
import { viewErrorGenerator } from '../utils/view-error-generator';
import { DataValueFormat } from '../enums/data-value-format';
import { logger } from '../utils/logger';
import { Measure } from '../entities/dataset/measure';
import { getFileImportAndSaveToDisk, loadFileIntoDatabase } from '../utils/file-utils';
import { DatasetRepository } from '../repositories/dataset';
import { FactTableColumnType } from '../enums/fact-table-column-type';
import { DatasetDTO } from '../dtos/dataset-dto';
import { DataTableDto } from '../dtos/data-table-dto';
import { MeasureRow } from '../entities/dataset/measure-row';
import { SUPPORTED_LOCALES } from '../middleware/translation';
import { DisplayType } from '../enums/display-type';
import { getFileService } from '../utils/get-storage';

import { createFactTableQuery, createMeasureLookupTable } from './cube-handler';
import { duckdb } from './duckdb';

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
    await MeasureRow.delete({ measure });
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
    return {
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
  }
}

async function setupMeasure(
  dataset: Dataset,
  lookupTable: LookupTable,
  confirmedJoinColumn: string,
  measureTable: MeasureRow[],
  extractor: MeasureLookupTableExtractor
) {
  // Clean up previously uploaded dimensions
  await cleanUpMeasure(dataset.measure.id);
  lookupTable.isStatsWales2Format = extractor.isSW2Format;
  const updateMeasure = await Measure.findOneByOrFail({ id: dataset.measure.id });
  updateMeasure.joinColumn = confirmedJoinColumn;
  updateMeasure.lookupTable = lookupTable;
  updateMeasure.extractor = extractor;

  logger.debug('Saving the lookup table');
  await lookupTable.save();

  logger.debug(`Saving measure table to database using rows ${JSON.stringify(measureTable, null, 2)}`);
  for (const row of measureTable) {
    row.id = updateMeasure.id;
    row.measure = updateMeasure;
    await row.save();
  }

  logger.debug('Saving the measure');
  updateMeasure.lookupTable = lookupTable;
  await updateMeasure.save();
}

async function rowMatcher(
  quack: Database,
  measure: Measure,
  datasetId: string,
  lookupTableName: string,
  factTableName: string,
  confirmedJoinColumn: string
): Promise<ViewErrDTO | undefined> {
  try {
    const nonMatchedRowQuery = `SELECT line_number, fact_table_column, ${lookupTableName}."${confirmedJoinColumn}" as lookup_table_column
            FROM (SELECT row_number() OVER () as line_number, "${measure.factTableColumn}" as fact_table_column FROM
            ${factTableName}) as fact_table LEFT JOIN ${lookupTableName} ON
            CAST(fact_table.fact_table_column AS VARCHAR)=CAST(${lookupTableName}."${confirmedJoinColumn}" AS VARCHAR)
            WHERE ${lookupTableName}."${confirmedJoinColumn}" IS NULL;`;
    logger.debug(`Running row matching query: ${nonMatchedRowQuery}`);
    const nonMatchedRows = await quack.all(nonMatchedRowQuery);
    const rows = await quack.all(`SELECT COUNT(*) as total_rows FROM ${factTableName}`);
    if (nonMatchedRows.length === rows[0].total_rows) {
      logger.error(`The user supplied an incorrect lookup table and none of the rows matched`);
      const nonMatchedFactTableValues = await quack.all(
        `SELECT DISTINCT ${measure.factTableColumn} FROM ${factTableName};`
      );
      const nonMatchedLookupValues = await quack.all(
        `SELECT DISTINCT ${lookupTableName}."${confirmedJoinColumn}" FROM ${lookupTableName};`
      );
      return viewErrorGenerator(400, datasetId, 'patch', 'errors.dimensionValidation.matching_error', {
        totalNonMatching: rows[0].total_rows,
        nonMatchingDataTableValues: nonMatchedFactTableValues.map((row) => Object.values(row)[0]),
        nonMatchingLookupValues: nonMatchedLookupValues.map((row) => Object.values(row)[0])
      });
    }
    if (nonMatchedRows.length > 0) {
      logger.error(`Seems some of the rows didn't match.`);
      const nonMatchedFactTableValues = await quack.all(
        `SELECT DISTINCT fact_table_column FROM (SELECT "${measure.factTableColumn}" as fact_table_column
                FROM ${factTableName}) as fact_table
                LEFT JOIN ${lookupTableName} ON CAST(fact_table.fact_table_column AS VARCHAR)=CAST(${lookupTableName}."${confirmedJoinColumn}" AS VARCHAR)
                where ${lookupTableName}."${confirmedJoinColumn}" IS NULL;`
      );
      const nonMatchingLookupValues = await quack.all(
        `SELECT DISTINCT measure_table_column FROM (SELECT "${confirmedJoinColumn}" as measure_table_column
                 FROM ${lookupTableName}) AS measure_table
                 LEFT JOIN ${factTableName} ON CAST(measure_table.measure_table_column AS VARCHAR)=CAST(${factTableName}."${measure.factTableColumn}" AS VARCHAR)
                 WHERE ${factTableName}."${measure.factTableColumn}" IS NULL;`
      );
      logger.error(
        `The user supplied an incorrect or incomplete lookup table and ${nonMatchedRows.length} rows didn't match`
      );
      return viewErrorGenerator(400, datasetId, 'patch', 'errors.dimensionValidation.matching_error', {
        totalNonMatching: nonMatchedRows.length,
        nonMatchingDataTableValues: nonMatchedFactTableValues.map((row) => Object.values(row)[0]),
        nonMatchingLookupValues: nonMatchingLookupValues.map((row) => Object.values(row)[0])
      });
    }
  } catch (error) {
    logger.error(
      error,
      `Something went wrong, most likely an incorrect join column name, while trying to validate the lookup table with error: ${error}`
    );
    const nonMatchedRows = await quack.all(`SELECT COUNT(*) AS total_rows FROM ${factTableName};`);
    const nonMatchedFactTableValues = await quack.all(
      `SELECT DISTINCT ${measure.factTableColumn} FROM ${factTableName};`
    );
    return viewErrorGenerator(400, datasetId, 'patch', 'errors.dimensionValidation.invalid_lookup_table', {
      totalNonMatching: nonMatchedRows[0].total_rows,
      nonMatchingDataTableValues: nonMatchedFactTableValues.map((row) => Object.values(row)[0]),
      nonMatchingLookupValues: null
    });
  }
  logger.debug('The measure lookup table passed row matching.');
  return undefined;
}

async function checkDecimalColumn(quack: Database, extractor: MeasureLookupTableExtractor, lookupTableName: string) {
  const unmatchedFormats: string[] = [];
  logger.debug('Decimal column is present.  Validating contains only integers.');
  const formats = await quack.all(`SELECT DISTINCT "${extractor.decimalColumn}" as formats FROM ${lookupTableName};`);
  for (const format of Object.values(formats.map((format) => format.formats))) {
    if (!Number.isInteger(Number(format))) unmatchedFormats.push(format);
  }
  return unmatchedFormats;
}

async function checkFormatColumn(quack: Database, extractor: MeasureLookupTableExtractor, lookupTableName: string) {
  const unmatchedFormats: string[] = [];
  logger.debug('Decimal column is present.  Validating contains only integers.');
  const formats = await quack.all(`SELECT DISTINCT "${extractor.formatColumn}" as formats FROM ${lookupTableName};`);
  logger.debug(`Formats = ${JSON.stringify(Object.values(DataValueFormat), null, 2)}`);
  for (const format of Object.values(formats.map((format) => format.formats))) {
    if (Object.values(DataValueFormat).indexOf(format.toLowerCase()) === -1) unmatchedFormats.push(format);
  }
  return unmatchedFormats;
}

async function validateTableContent(
  quack: Database,
  datasetId: string,
  lookupTableName: string,
  extractor: MeasureLookupTableExtractor
): Promise<ViewErrDTO | undefined> {
  if (extractor.formatColumn && extractor.formatColumn.toLowerCase().indexOf('format') > -1) {
    logger.debug('Formats column is present.  Validating all formats present are valid.');
    const unMatchedFormats = await checkFormatColumn(quack, extractor, lookupTableName);
    if (unMatchedFormats.length > 0) {
      logger.debug(
        `Found invalid formats while validating format column.  Formats found: ${JSON.stringify(unMatchedFormats)}`
      );
      return viewErrorGenerator(400, datasetId, 'patch', 'errors.dimensionValidation.invalid_formats_present', {
        totalNonMatching: unMatchedFormats.length,
        nonMatchingValues: unMatchedFormats
      });
    }
  }

  if (extractor.decimalColumn && extractor.decimalColumn.toLowerCase().indexOf('decimal') !== -1) {
    const unmatchedDecimals = await checkDecimalColumn(quack, extractor, lookupTableName);
    if (unmatchedDecimals.length > 0) {
      logger.debug(
        `Found invalid formats while validating decimals column.  Formats found: ${JSON.stringify(unmatchedDecimals)}`
      );
      return viewErrorGenerator(400, datasetId, 'patch', 'errors.dimensionValidation.invalid_decimals_present', {
        totalNonMatching: unmatchedDecimals.length,
        nonMatchingValues: unmatchedDecimals
      });
    }
  }
  logger.debug('Validating column contents complete.');
  return undefined;
}

async function createMeasureTable(
  quack: Database,
  joinColumn: string,
  lookupTable: string,
  extractor: MeasureLookupTableExtractor
) {
  const measureTable: MeasureRow[] = [];
  const viewComponents: string[] = [];
  let formatColumn = `"${extractor.formatColumn}"`;
  if (!extractor.formatColumn && !extractor.decimalColumn) {
    formatColumn = `'text'`;
  } else if (!extractor.formatColumn && extractor.decimalColumn) {
    formatColumn = `'float'`;
  }
  const decimalColumnDef = extractor.decimalColumn ? `"${extractor.decimalColumn}" as decimal,` : '';
  const sortOrderDef = extractor.sortColumn ? `"${extractor.sortColumn}" as sort_order,` : '';
  const measureTypeDef = extractor.measureTypeColumn ? `"${extractor.measureTypeColumn}" as measure_type,` : '';
  let notesColumnDef = '';
  let buildMeasureViewQuery: string;
  if (extractor.isSW2Format) {
    for (const locale of SUPPORTED_LOCALES) {
      if (extractor.notesColumns) {
        const notesCol = extractor.notesColumns.find((col) => col.lang === locale.split('-')[0])?.name;
        if (notesCol) {
          notesColumnDef = `"${notesCol}" as notes,`;
        }
      }
      viewComponents.push(
        `SELECT
                    "${joinColumn}" as reference,
                    '${locale.toLowerCase()}' AS language,
                    "${extractor.descriptionColumns.find((col) => col.lang === locale.split('-')[0])?.name}" AS description,
                    ${notesColumnDef}
                    ${sortOrderDef}
                    ${formatColumn} AS format,
                    ${decimalColumnDef}
                    ${measureTypeDef}
                FROM ${lookupTable}\n`
      );
    }
    buildMeasureViewQuery = `${viewComponents.join('\nUNION\n')};`;
    logger.debug(`Extracting SW2 measure lookup table to measure table using query ${buildMeasureViewQuery}`);
  } else {
    if (extractor.notesColumns) {
      notesColumnDef = `"${extractor.notesColumns[0]}" as notes,`;
    }
    buildMeasureViewQuery = `SELECT
                    "${joinColumn}" as reference,
                    "${extractor.languageColumn}" AS language,
                    "${extractor.descriptionColumns[0]}" AS description,
                    ${notesColumnDef}
                    ${sortOrderDef}
                    ${formatColumn} AS format,
                    ${decimalColumnDef}
                    ${measureTypeDef}
                FROM ${lookupTable}\n`;
    logger.debug(`Extracting SW3 measure lookup table to measure table using query ${buildMeasureViewQuery}`);
  }
  const tableContents = await quack.all(buildMeasureViewQuery);
  logger.debug(`Creating measureTable from lookup using result: ${JSON.stringify(tableContents)}`);
  for (const row of tableContents) {
    const item = new MeasureRow();
    item.reference = row.reference;
    item.language = row.language;
    item.description = row.description;
    item.format = row.format.toLowerCase() as DisplayType;
    item.notes = row.notes || null;
    item.sortOrder = row.sort_order || null;
    item.decimal = row.decimal || null;
    item.measureType = row.measure_type || null;
    measureTable.push(item);
  }
  return measureTable;
}

export const validateMeasureLookupTable = async (
  protoLookupTable: DataTable,
  factTable: DataTable,
  dataset: Dataset,
  buffer: Buffer,
  tableMatcher?: MeasureLookupPatchDTO
): Promise<ViewDTO | ViewErrDTO> => {
  const lookupTable = convertFactTableToLookupTable(protoLookupTable);
  const factTableName = 'fact_table';
  const lookupTableName = 'preview_lookup';
  const measure = dataset.measure;
  const quack = await duckdb();
  const lookupTableTmpFile = tmp.tmpNameSync({ postfix: `.${lookupTable.fileType}` });
  try {
    fs.writeFileSync(lookupTableTmpFile, buffer);
    const factTableTmpFile = await getFileImportAndSaveToDisk(dataset, factTable);
    await loadFileIntoDatabase(quack, factTable, factTableTmpFile, factTableName);
    await loadFileIntoDatabase(quack, lookupTable, lookupTableTmpFile, lookupTableName);
    fs.unlinkSync(lookupTableTmpFile);
    fs.unlinkSync(factTableTmpFile);
  } catch (err) {
    await quack.close();
    logger.error(`Something went wrong trying to load data in to DuckDB with the following error: ${err}`);
    throw err;
  }

  let confirmedJoinColumn: string | undefined;
  try {
    confirmedJoinColumn = lookForJoinColumn(protoLookupTable, measure.factTableColumn, tableMatcher);
  } catch (_err) {
    await quack.close();
    return viewErrorGenerator(400, dataset.id, 'patch', 'errors.dimensionValidation.no_join_column', {});
  }

  if (!confirmedJoinColumn) {
    await quack.close();
    return viewErrorGenerator(400, dataset.id, 'patch', 'errors.dimensionValidation.no_join_column', {});
  }

  const rowMatchingErrors = await rowMatcher(
    quack,
    measure,
    dataset.id,
    lookupTableName,
    factTableName,
    confirmedJoinColumn
  );
  if (rowMatchingErrors) return rowMatchingErrors;
  const extractor = createExtractor(protoLookupTable, tableMatcher);
  const measureTable = await createMeasureTable(quack, confirmedJoinColumn, lookupTableName, extractor);
  const tableValidationErrors = await validateTableContent(quack, dataset.id, lookupTableName, extractor);
  if (tableValidationErrors) return tableValidationErrors;

  await setupMeasure(dataset, lookupTable, confirmedJoinColumn, measureTable, extractor);

  try {
    const dimensionTable = await quack.all(`SELECT * FROM ${lookupTableName};`);
    await quack.close();
    const tableHeaders = Object.keys(dimensionTable[0]);
    const dataArray = dimensionTable.map((row) => Object.values(row));
    const currentDataset = await DatasetRepository.getById(dataset.id);
    const currentImport = await DataTable.findOneByOrFail({ id: factTable.id });
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
    return {
      dataset: DatasetDTO.fromDataset(currentDataset),
      data_table: DataTableDto.fromDataTable(currentImport),
      current_page: 1,
      page_info: {
        total_records: 1,
        start_record: 1,
        end_record: 10
      },
      page_size: 10,
      total_pages: 1,
      headers,
      data: dataArray
    };
  } catch (error) {
    await quack.close();
    logger.error(`Something went wrong trying to generate the preview of the lookup table with error: ${error}`);
    throw error;
  }
};

const sampleSize = 5;

async function getMeasurePreviewWithoutExtractor(
  dataset: Dataset,
  measure: Measure,
  factTable: DataTable,
  quack: Database,
  tableName: string
): Promise<ViewDTO> {
  const preview = await quack.all(
    `SELECT DISTINCT "${measure.factTableColumn}" FROM ${tableName} ORDER BY "${measure.factTableColumn}" ASC LIMIT ${sampleSize};`
  );
  const tableHeaders = Object.keys(preview[0]);
  const dataArray = preview.map((row) => Object.values(row));
  const currentDataset = await DatasetRepository.getById(dataset.id);
  const currentImport = await DataTable.findOneByOrFail({ id: factTable.id });
  const headers: CSVHeader[] = [];
  for (let i = 0; i < tableHeaders.length; i++) {
    headers.push({
      index: i,
      name: tableHeaders[i],
      source_type: FactTableColumnType.Unknown
    });
  }
  return {
    dataset: DatasetDTO.fromDataset(currentDataset),
    data_table: DataTableDto.fromDataTable(currentImport),
    current_page: 1,
    page_info: {
      total_records: preview.length,
      start_record: 1,
      end_record: preview.length
    },
    page_size: preview.length < sampleSize ? preview.length : sampleSize,
    total_pages: 1,
    headers,
    data: dataArray
  };
}

async function getMeasurePreviewWithExtractor(
  dataset: Dataset,
  measure: Measure,
  factTable: DataTable,
  quack: Database
) {
  logger.debug(`Generating lookup table preview for measure ${measure.id}`);
  await createMeasureLookupTable(quack, measure.measureTable);
  const query = `SELECT * FROM measure ORDER BY sort_order, reference LIMIT ${sampleSize};`;
  logger.debug(`Querying the cube to get the preview using query: ${query}`);
  const measureTable = await quack.all(query);
  const tableHeaders = Object.keys(measureTable[0]);
  const dataArray = measureTable.map((row) => Object.values(row));
  const currentDataset = await DatasetRepository.getById(dataset.id);
  const currentImport = await DataTable.findOneByOrFail({ id: factTable.id });
  const headers: CSVHeader[] = tableHeaders.map((name, idx) => ({
    name,
    index: idx,
    source_type: FactTableColumnType.Unknown
  }));
  return {
    dataset: DatasetDTO.fromDataset(currentDataset),
    fact_table: DataTableDto.fromDataTable(currentImport),
    current_page: 1,
    page_info: {
      total_records: measureTable.length,
      start_record: 1,
      end_record: measureTable.length < sampleSize ? measureTable.length : sampleSize
    },
    page_size: measureTable.length < sampleSize ? measureTable.length : sampleSize,
    total_pages: 1,
    headers,
    data: dataArray
  };
}

export const getMeasurePreview = async (dataset: Dataset, dataTable: DataTable) => {
  logger.debug(`Getting preview for measure: ${dataset.measure.id}`);
  const tableName = 'fact_table';
  const quack = await duckdb();
  const tempFile = tmp.tmpNameSync({ postfix: `.${dataTable.fileType}` });
  const measure = dataset.measure;
  if (!measure) {
    throw new Error('No measure present on the dataset');
  }
  // extract the data from the fact table
  try {
    const fileService = getFileService();
    const fileBuffer = await fileService.loadBuffer(dataTable.filename, dataset.id);
    fs.writeFileSync(tempFile, fileBuffer);
    const createTableQuery = await createFactTableQuery(tableName, tempFile, dataTable.fileType, quack);
    logger.debug(`Creating fact table with query: ${createTableQuery}`);
    await quack.exec(createTableQuery);
  } catch (error) {
    logger.error(`Something went wrong trying to create ${tableName} in DuckDB. Unable to do matching and validation`);
    await quack.close();
    fs.unlinkSync(tempFile);
    throw error;
  }
  let viewDto: ViewDTO;
  try {
    if (measure.measureTable && measure.measureTable.length > 0) {
      viewDto = await getMeasurePreviewWithExtractor(dataset, measure, dataTable, quack);
    } else {
      logger.debug('Straight column preview');
      viewDto = await getMeasurePreviewWithoutExtractor(dataset, measure, dataTable, quack, tableName);
    }
    await quack.close();
    fs.unlinkSync(tempFile);
    return viewDto;
  } catch (error) {
    logger.error(error, `Something went wrong trying to create measure preview`);
    await quack.close();
    fs.unlinkSync(tempFile);
    throw error;
  }
};
