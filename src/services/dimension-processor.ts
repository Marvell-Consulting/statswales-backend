import { Database } from 'duckdb-async';

import { SourceAssignmentDTO } from '../dtos/source-assignment-dto';
import { DataTable } from '../entities/dataset/data-table';
import { DataTableDescription } from '../entities/dataset/data-table-description';
import { FactTableColumnType } from '../enums/fact-table-column-type';
import { SourceAssignmentException } from '../exceptions/source-assignment.exception';
import { Dataset } from '../entities/dataset/dataset';
import { DimensionType } from '../enums/dimension-type';
import { logger } from '../utils/logger';
import { Dimension } from '../entities/dataset/dimension';
import { SUPPORTED_LOCALES } from '../middleware/translation';
import { DimensionMetadata } from '../entities/dataset/dimension-metadata';
import { Measure } from '../entities/dataset/measure';
import { DimensionPatchDto } from '../dtos/dimension-partch-dto';
import { CSVHeader, ViewDTO, ViewErrDTO } from '../dtos/view-dto';
import { DateExtractor } from '../extractors/date-extractor';
import { DatasetRepository } from '../repositories/dataset';
import { getFileImportAndSaveToDisk, loadFileIntoDatabase } from '../utils/file-utils';
import { LookupTable } from '../entities/dataset/lookup-table';
import { FactTableColumn } from '../entities/dataset/fact-table-column';
import { MeasureRow } from '../entities/dataset/measure-row';
import { MeasureMetadata } from '../entities/dataset/measure-metadata';

import { dateDimensionReferenceTableCreator, DateReferenceDataItem } from './time-matching';
import { getReferenceDataDimensionPreview } from './reference-data-handler';
import { NumberExtractor, NumberType } from '../extractors/number-extractor';
import { viewErrorGenerators, viewGenerator } from '../utils/view-error-generators';
import { getFileService } from '../utils/get-file-service';
import { createEmptyCubeWithFactTable } from '../utils/create-fact-table';
import { createLookupTableInCube } from './lookup-table-handler';
import { createDatePeriodTableQuery, makeCubeSafeString } from './cube-handler';
import { t } from 'i18next';
import { CubeValidationException } from '../exceptions/cube-error-exception';
import { CubeValidationType } from '../enums/cube-validation-type';

const createDateDimensionTable = `CREATE TABLE date_dimension (date_code VARCHAR, description VARCHAR, start_date datetime, end_date datetime, date_type varchar);`;
const sampleSize = 5;

export interface ValidatedSourceAssignment {
  dataValues: SourceAssignmentDTO | null;
  noteCodes: SourceAssignmentDTO | null;
  measure: SourceAssignmentDTO | null;
  dimensions: SourceAssignmentDTO[];
  ignore: SourceAssignmentDTO[];
}

export const cleanUpDimension = async (dimension: Dimension) => {
  dimension.extractor = null;
  dimension.joinColumn = null;
  dimension.type = DimensionType.Raw;
  const lookupTableId = dimension.lookupTable?.id;
  const lookupTableFilename = dimension.lookupTable?.filename;
  dimension.lookupTable = null;
  try {
    await dimension.save();
    if (lookupTableId) {
      const oldLookupTable = await LookupTable.findOneBy({ id: lookupTableId });
      await oldLookupTable?.remove();
    }
  } catch (err) {
    logger.error(
      `Something has gone wrong trying to unlink the previous lookup table from the dimension with the following error: ${err}`
    );
    throw err;
  }

  if (lookupTableId && lookupTableFilename) {
    logger.info(`Cleaning up previous lookup table`);
    try {
      const fileService = getFileService();
      await fileService.delete(lookupTableFilename, dimension.dataset.id);
    } catch (err) {
      logger.warn(`Something went wrong trying to remove previously uploaded lookup table with error: ${err}`);
    }
  }
};

export const setupTextDimension = async (dimension: Dimension) => {
  if (dimension.extractor) await cleanUpDimension(dimension);
  const updateDimension = await Dimension.findOneByOrFail({ id: dimension.id });
  updateDimension.type = DimensionType.Text;
  updateDimension.extractor = {
    type: 'text'
  };
  await updateDimension.save();
};

export const validateSourceAssignment = (
  fileImport: DataTable,
  sourceAssignment: SourceAssignmentDTO[]
): ValidatedSourceAssignment => {
  let dataValues: SourceAssignmentDTO | null = null;
  let noteCodes: SourceAssignmentDTO | null = null;
  let measure: SourceAssignmentDTO | null = null;
  const dimensions: SourceAssignmentDTO[] = [];
  const ignore: SourceAssignmentDTO[] = [];
  logger.debug(`Validating source assignment from: ${JSON.stringify(sourceAssignment, null, 2)}`);
  sourceAssignment.map((sourceInfo) => {
    if (
      !fileImport.dataTableDescriptions?.find(
        (info: DataTableDescription) => info.columnName === sourceInfo.column_name
      )
    ) {
      throw new SourceAssignmentException(`errors.source_assignment.invalid_column_name`);
    }

    switch (sourceInfo.column_type) {
      case FactTableColumnType.DataValues:
        if (dataValues) {
          throw new SourceAssignmentException('errors.source_assignment.too_many_data_values');
        }
        dataValues = sourceInfo;
        break;
      case FactTableColumnType.Measure:
        if (measure) {
          throw new SourceAssignmentException('errors.source_assignment.too_many_measure');
        }
        measure = sourceInfo;
        break;
      case FactTableColumnType.NoteCodes:
        if (noteCodes) {
          throw new SourceAssignmentException('errors.source_assignment.too_many_footnotes');
        }
        noteCodes = sourceInfo;
        break;
      case FactTableColumnType.Time:
      case FactTableColumnType.Dimension:
        dimensions.push(sourceInfo);
        break;
      case FactTableColumnType.Ignore:
        ignore.push(sourceInfo);
        break;
      default:
        throw new SourceAssignmentException(`errors.source_assignment.invalid_source_type`);
    }
  });

  return { dataValues, measure, noteCodes, dimensions, ignore };
};

async function createUpdateDimension(dataset: Dataset, columnDescriptor: SourceAssignmentDTO): Promise<void> {
  const columnInfo = await FactTableColumn.findOneByOrFail({
    columnName: columnDescriptor.column_name,
    id: dataset.id
  });
  columnInfo.columnType = columnDescriptor.column_type;
  await columnInfo.save();

  await Dimension.create({
    dataset,
    type: DimensionType.Raw,
    factTableColumn: columnInfo.columnName,
    metadata: SUPPORTED_LOCALES.map((language: string) =>
      DimensionMetadata.create({ language, name: columnInfo.columnName })
    )
  }).save();
}

async function updateDataValueColumn(dataset: Dataset, dataValueColumnDto: SourceAssignmentDTO) {
  const column = await FactTableColumn.findOneByOrFail({
    columnName: dataValueColumnDto.column_name,
    id: dataset.id
  });
  if (!column) {
    throw Error('No such column present in fact table');
  }
  if (column.columnType !== FactTableColumnType.DataValues) {
    column.columnType = FactTableColumnType.DataValues;
  }
  await column.save();
}

async function removeIgnoreAndUnknownColumns(dataset: Dataset, ignoreColumns: SourceAssignmentDTO[]) {
  let factTableColumns: FactTableColumn[] = [];
  try {
    factTableColumns = await FactTableColumn.findBy({ id: dataset.id, columnDatatype: FactTableColumnType.Unknown });
    logger.debug(
      `Found ${factTableColumns.length} columns in fact table... ${JSON.stringify(factTableColumns, null, 2)}`
    );
  } catch (error) {
    logger.error(error, `Something went wrong trying to find columns in fact table with error: ${error}`);
  }

  if (!factTableColumns.length && ignoreColumns.length === 0) {
    logger.debug(`No columns unknown column left and no columns to be ignored.`);
    return;
  }

  for (const column of ignoreColumns) {
    logger.debug(`Removing column ${column.column_name} from fact table`);
    const factTableCol = factTableColumns.find((columnInfo) => columnInfo.columnName === column.column_name);
    if (!factTableCol) {
      continue;
    }
    await factTableCol.remove();
  }

  const unknownColumns = await FactTableColumn.findBy({ id: dataset.id, columnDatatype: FactTableColumnType.Unknown });
  if (unknownColumns.length > 0)
    throw new SourceAssignmentException('Unknown columns found in fact table after dimension processing.');
}

async function createUpdateMeasure(dataset: Dataset, columnAssignment: SourceAssignmentDTO): Promise<void> {
  const columnInfo = await FactTableColumn.findOneByOrFail({
    columnName: columnAssignment.column_name,
    id: dataset.id
  });

  columnInfo.columnType = FactTableColumnType.Measure;
  await columnInfo.save();

  await Measure.create({
    dataset,
    factTableColumn: columnInfo.columnName,
    metadata: SUPPORTED_LOCALES.map((language: string) =>
      MeasureMetadata.create({ language, name: columnInfo.columnName })
    )
  }).save();
}

async function createUpdateNoteCodes(dataset: Dataset, columnAssignment: SourceAssignmentDTO) {
  const columnInfo = await FactTableColumn.findOneByOrFail({
    columnName: columnAssignment.column_name,
    id: dataset.id
  });

  columnInfo.columnType = FactTableColumnType.NoteCodes;
  columnInfo.columnDatatype = 'VARCHAR';
  await columnInfo.save();
}

async function createBaseFactTable(dataset: Dataset, dataTable: DataTable): Promise<void> {
  const factTable = dataTable.dataTableDescriptions.map((col) => {
    const factTableCol = new FactTableColumn();
    factTableCol.columnType = FactTableColumnType.Unknown;
    factTableCol.columnName = col.columnName;
    factTableCol.columnDatatype = col.columnDatatype;
    factTableCol.columnIndex = col.columnIndex;
    factTableCol.id = dataset.id;
    factTableCol.dataset = dataset;
    return factTableCol;
  });
  await FactTableColumn.save(factTable);
}

export async function removeAllDimensions(dataset: Dataset) {
  logger.warn(`Removing all dimensions for dataset ${dataset.id}`);
  if (dataset.dimensions) {
    for (const dimension of dataset.dimensions) {
      if (dimension.lookupTable) {
        try {
          const fileService = getFileService();
          await fileService.delete(dimension.lookupTable.filename, dataset.id);
        } catch (error) {
          logger.warn(
            error,
            `Something went wrong trying to remove previously uploaded lookup table with error: ${error}`
          );
        }
      }
    }
  }
  await Dimension.getRepository().delete({ dataset });
}

export async function removeMeasure(dataset: Dataset) {
  logger.warn(`Removing measure for dataset ${dataset.id}`);
  if (dataset.measure) {
    if (dataset.measure.lookupTable) {
      try {
        const fileService = getFileService();
        fileService.delete(dataset.measure.lookupTable.filename, dataset.id);
      } catch (error) {
        logger.warn(
          error,
          `Something went wrong trying to remove previously uploaded lookup table with error: ${error}`
        );
      }
    }
    await MeasureRow.getRepository().delete({ measure: dataset.measure });
    await MeasureMetadata.getRepository().delete({ measure: dataset.measure });
  }
  await Measure.getRepository().delete({ dataset });
}

export const cleanupDimensionMeasureAndFactTable = async (dataset: Dataset): Promise<void> => {
  logger.warn(`Removing all fact table columns for dataset ${dataset.id}`);
  await FactTableColumn.getRepository().delete({ id: dataset.id });
  await removeAllDimensions(dataset);
  await removeMeasure(dataset);
};

export const createDimensionsFromSourceAssignment = async (
  dataset: Dataset,
  dataTable: DataTable,
  sourceAssignment: ValidatedSourceAssignment
): Promise<void> => {
  const { dataValues, measure, ignore, noteCodes, dimensions } = sourceAssignment;
  await cleanupDimensionMeasureAndFactTable(dataset);
  await createBaseFactTable(dataset, dataTable);

  if (dataValues) {
    logger.debug('Creating data values column');
    await updateDataValueColumn(dataset, dataValues);
  }

  if (noteCodes) {
    logger.debug('Creating note codes column');
    await createUpdateNoteCodes(dataset, noteCodes);
  }

  if (measure) {
    logger.debug('Creating measure column');
    await createUpdateMeasure(dataset, measure);
  }

  await Promise.all(
    dimensions.map(async (dimensionCreationDTO: SourceAssignmentDTO) => {
      logger.debug(`Creating dimension column: ${JSON.stringify(dimensionCreationDTO)}`);
      await createUpdateDimension(dataset, dimensionCreationDTO);
    })
  );

  try {
    if (ignore) {
      logger.debug(`Removing ${ignore.length} ignore columns from fact table`);
      await removeIgnoreAndUnknownColumns(dataset, ignore);
    }
  } catch (error) {
    logger.error(
      error,
      `There were unknown columns left after removing ignore columns.  Unwinding dimension and measure creation.`
    );
    await cleanupDimensionMeasureAndFactTable(dataset);
    await createBaseFactTable(dataset, dataTable);
    throw error;
  }

  logger.debug('Finished creating dimensions');
};

export const validateNumericDimension = async (
  dimensionPatchRequest: DimensionPatchDto,
  dataset: Dataset,
  dimension: Dimension
): Promise<ViewDTO | ViewErrDTO> => {
  const numberType = dimensionPatchRequest.number_format;
  if (!numberType) {
    throw new Error('No number type supplied');
  }
  const decimalPlaces = dimensionPatchRequest.decimal_places;
  if (!decimalPlaces && numberType === NumberType.Decimal) {
    throw new Error('No decimal places supplied for non decimal number type');
  }
  const extractor: NumberExtractor = {
    type: numberType,
    decimalPlaces: decimalPlaces || 0
  };

  const tableName = 'fact_table';
  let quack: Database;
  try {
    quack = await createEmptyCubeWithFactTable(dataset);
  } catch (error) {
    logger.error(error, 'Something went wrong trying to create a new database');
    return viewErrorGenerators(500, dataset.id, 'patch', 'errors.cube_builder.fact_table_creation_failed', {});
  }

  // Validate column type in data table matches proposed type first using DuckDBs column detection
  const columnInfo = await quack.all(
    `SELECT * FROM (DESCRIBE ${tableName}) WHERE column_name = '${dimension.factTableColumn}';`
  );
  if (extractor.type === NumberType.Integer) {
    let savedDimension: Dimension;
    switch (columnInfo[0].column_type) {
      case 'BIGINT':
      case 'HUGEINT':
      case 'SMALLINT':
      case 'TINYINT':
      case 'INTEGER':
      case 'UBIGINT':
      case 'UHUGEINT':
      case 'UINTEGER':
      case 'USMALLINT':
      case 'UTINYINT':
        dimension.extractor = extractor;
        savedDimension = await dimension.save();
        return getPreviewWithNumberExtractor(dataset, savedDimension, quack, tableName);
    }
  } else if (extractor.type === NumberType.Decimal) {
    let savedDimension: Dimension;
    switch (columnInfo[0].column_type) {
      case 'DOUBLE':
      case 'FLOAT':
        dimension.extractor = extractor;
        savedDimension = await dimension.save();
        return getPreviewWithNumberExtractor(dataset, savedDimension, quack, tableName);
    }
  }

  let castType: string;
  if (extractor.type === NumberType.Integer) {
    castType = 'INTEGER';
  } else {
    castType = 'DOUBLE';
  }
  try {
    const nonMatchingQuery = `SELECT ${dimension.factTableColumn} FROM (SELECT TRY_CAST(${dimension.factTableColumn} AS ${castType}) as IS_NUMBER, ${dimension.factTableColumn} FROM ${tableName} WHERE IS_NUMBER IS NULL);`;
    const nonMatchingRows = await quack.all(nonMatchingQuery);
    if (nonMatchingRows.length > 0) {
      const nonMatchingDataTableValues = await quack.all(
        `SELECT DISTINCT ${dimension.factTableColumn}
        FROM (
          SELECT
            TRY_CAST(${dimension.factTableColumn} AS ${castType}) as IS_NUMBER,
            ${dimension.factTableColumn}
          FROM ${tableName}
          WHERE IS_NUMBER IS NULL
        );`
      );
      logger.error(
        `The user supplied a ${extractor.type} number format but there were ${nonMatchingRows.length} rows which didn't match the format`
      );
      await quack.close();
      return viewErrorGenerators(400, dataset.id, 'patch', 'errors.dimension_validation.non_numerical_values_present', {
        totalNonMatching: nonMatchingRows.length,
        nonMatchingDataTableValues: nonMatchingDataTableValues.map((row) => Object.values(row)[0])
      });
    }
  } catch (error) {
    await quack.close();
    logger.error(error, `Something went wrong trying to validate the data with the following error: ${error}`);
    const nonMatchedRows = await quack.all(`SELECT COUNT(*) AS total_rows FROM ${tableName};`);
    const nonMatchedValues = await quack.all(`SELECT DISTINCT ${dimension.factTableColumn} FROM ${tableName};`);
    return viewErrorGenerators(400, dataset.id, 'patch', 'errors.dimension_validation.unknown_error', {
      totalNonMatching: nonMatchedRows[0].total_rows,
      nonMatchingDataTableValues: nonMatchedValues.map((row) => Object.values(row)[0])
    });
  }
  logger.debug(`Validation finished, updating dimension ${dimension.id} with new extractor`);
  dimension.lookupTable = null;
  dimension.joinColumn = null;
  dimension.type = DimensionType.Numeric;
  dimension.extractor = extractor;
  const savedDimension = await dimension.save();
  return getPreviewWithNumberExtractor(dataset, savedDimension, quack, tableName);
};

export const validateUpdatedDateDimension = async (
  quack: Database,
  dataset: Dataset,
  dimension: Dimension,
  factTableColumn: FactTableColumn
): Promise<undefined> => {
  const errors = await validateDateDimension(quack, dataset, dimension, factTableColumn);
  if (errors) {
    const err = new CubeValidationException('Validation failed');
    err.type = CubeValidationType.DimensionNonMatchedRows;
    throw err;
  }
  return undefined;
};

export const validateDateDimension = async (
  quack: Database,
  dataset: Dataset,
  dimension: Dimension,
  factTableColumn: FactTableColumn
): Promise<ViewErrDTO | undefined> => {
  const extractor = dimension.extractor as DateExtractor;
  const tableName = 'fact_table';
  try {
    const preview = await quack.all(`SELECT DISTINCT "${dimension.factTableColumn}" FROM ${tableName};`);
    // Now validate everything matches
    const matchingQuery = `SELECT
        line_number, fact_table_date, "${makeCubeSafeString(factTableColumn.columnName)}_lookup"."${factTableColumn.columnName}"
      FROM (
        SELECT
          row_number() OVER () as line_number, "${dimension.factTableColumn}" as fact_table_date
        FROM
          ${tableName}
      ) as fact_table
      LEFT JOIN "${makeCubeSafeString(factTableColumn.columnName)}_lookup"
      ON fact_table.fact_table_date="${makeCubeSafeString(factTableColumn.columnName)}_lookup"."${factTableColumn.columnName}"
      WHERE "${factTableColumn.columnName}" IS NULL;`;
    logger.debug(`Matching query is:\n${matchingQuery}`);
    const nonMatchedRows = await quack.all(matchingQuery);
    if (nonMatchedRows.length > 0) {
      if (nonMatchedRows.length === preview.length) {
        logger.error(`The user supplied an incorrect format and none of the rows matched.`);
        return viewErrorGenerators(400, dataset.id, 'patch', 'errors.dimension_validation.invalid_date_format', {
          extractor,
          totalNonMatching: preview.length,
          nonMatchingValues: []
        });
      } else {
        logger.error(
          `There were ${nonMatchedRows.length} row(s) which didn't match based on the information given to us by the user`
        );
        const nonMatchingRowsQuery = `
            SELECT
              DISTINCT fact_table_date
            FROM (
              SELECT
                row_number() OVER () as line_number, "${dimension.factTableColumn}" as fact_table_date
              FROM ${tableName}) AS fact_table
              LEFT JOIN "${makeCubeSafeString(factTableColumn.columnName)}_lookup"
              ON fact_table.fact_table_date="${makeCubeSafeString(factTableColumn.columnName)}_lookup"."${factTableColumn.columnName}"
             WHERE "${factTableColumn.columnName}" IS NULL;`;
        logger.debug(`Non matching rows query is:\n${nonMatchingRowsQuery}`);
        const nonMatchedRowSample = await quack.all(nonMatchingRowsQuery);
        const nonMatchingValues = nonMatchedRowSample
          .map((item) => item.fact_table_date)
          .filter((item, i, ar) => ar.indexOf(item) === i);
        const totalNonMatching = nonMatchedRows.length;
        return viewErrorGenerators(400, dataset.id, 'patch', 'errors.dimension_validation.invalid_date_format', {
          extractor,
          totalNonMatching,
          nonMatchingValues
        });
      }
    }
  } catch (error) {
    logger.error(error, `Something unexpected went wrong trying to validate the data`);
    return viewErrorGenerators(500, dataset.id, 'patch', 'errors.dimension_validation.unknown_error', {});
  }
  return undefined;
};

export const createAndValidateDateDimension = async (
  dimensionPatchRequest: DimensionPatchDto,
  dataset: Dataset,
  dimension: Dimension,
  language: string
): Promise<ViewDTO | ViewErrDTO> => {
  const tableName = 'fact_table';
  const factTableColumn = dataset.factTable?.find(
    (col) => dimension.factTableColumn === col.columnName && col.columnType === FactTableColumnType.Dimension
  );
  if (!factTableColumn) {
    logger.error(`Could not find the fact table column ${dimension.factTableColumn} in the dataset`);
    return viewErrorGenerators(500, dataset.id, 'patch', 'errors.dimension_validation.fact_table_column_not_found', {
      mismatch: false
    });
  }
  let quack: Database;
  try {
    quack = await createEmptyCubeWithFactTable(dataset);
  } catch (error) {
    logger.error(error, 'Something went wrong trying to create a new database');
    return viewErrorGenerators(500, dataset.id, 'patch', 'errors.cube_builder.fact_table_creation_failed', {});
  }
  // Use the extracted data to try to create a reference table based on the user supplied information
  logger.debug(`Dimension patch request is: ${JSON.stringify(dimensionPatchRequest)}`);
  let dateDimensionTable: DateReferenceDataItem[] = [];
  const extractor: DateExtractor = {
    type: dimensionPatchRequest.date_type,
    yearFormat: dimensionPatchRequest.year_format,
    quarterFormat: dimensionPatchRequest.quarter_format,
    quarterTotalIsFifthQuart: dimensionPatchRequest.fifth_quarter,
    monthFormat: dimensionPatchRequest.month_format,
    dateFormat: dimensionPatchRequest.date_format
  };
  logger.debug(`Extractor created with: ${JSON.stringify(extractor)}`);
  const previewQuery = `SELECT DISTINCT "${dimension.factTableColumn}" FROM ${tableName}`;
  const preview = await quack.all(previewQuery);
  try {
    dateDimensionTable = dateDimensionReferenceTableCreator(extractor, preview);
    logger.debug(
      `Date dimension table created with the following JSON: ${JSON.stringify(dateDimensionTable, null, 2)}`
    );
  } catch (error) {
    logger.error(error, `Something went wrong trying to create the date reference table`);
    await quack.close();
    return viewErrorGenerators(400, dataset.id, 'patch', 'errors.dimension.invalid_date_format', {
      extractor,
      totalNonMatching: preview.length,
      nonMatchingValues: []
    });
  }

  try {
    await quack.exec(createDatePeriodTableQuery(factTableColumn));
    const stmt = await quack.prepare(
      `INSERT INTO ${makeCubeSafeString(factTableColumn.columnName)}_lookup
    ("${factTableColumn.columnName}", language, description, hierarchy, date_type, start_date, end_date) VALUES (?,?,?,?,?,?,?);`
    );
    for (const locale of SUPPORTED_LOCALES) {
      logger.debug(`populating ${makeCubeSafeString(factTableColumn.columnName)}_lookup table for locale ${locale}`);
      dateDimensionTable.map(async (row) => {
        await stmt.run(
          row.dateCode,
          locale.toLowerCase(),
          row.description,
          null,
          t(`date_type.${row.type}`, { lng: locale }),
          row.start,
          row.end
        );
      });
    }
    await stmt.finalize();
  } catch (error) {
    logger.error(error, `Something went wrong trying to create the date dimension table`);
    await quack.close();
    return viewErrorGenerators(500, dataset.id, 'patch', 'errors.dimension_validation.unknown_error', {
      extractor,
      totalNonMatching: preview.length,
      nonMatchingValues: [],
      mismatch: false
    });
  }

  const validationErrors = await validateDateDimension(quack, dataset, dimension, factTableColumn);
  if (validationErrors) {
    await quack.close();
    return validationErrors;
  }

  const coverage = await quack.all(
    `SELECT MIN(start_date) as start_date, MAX(end_date) AS end_date FROM ${makeCubeSafeString(factTableColumn.columnName)}_lookup;`
  );
  const updateDataset = await Dataset.findOneByOrFail({ id: dataset.id });
  updateDataset.startDate = coverage[0].start_date;
  updateDataset.endDate = coverage[0].end_date;
  await updateDataset.save();
  const updateDimension = await Dimension.findOneByOrFail({ id: dimension.id });
  updateDimension.extractor = extractor;
  updateDimension.joinColumn = 'date_code';
  updateDimension.type = dimensionPatchRequest.dimension_type;
  await updateDimension.save();
  const dimensionTable = await quack.all(`
    SELECT DISTINCT ${makeCubeSafeString(factTableColumn.columnName)}_lookup.* FROM ${makeCubeSafeString(factTableColumn.columnName)}_lookup
    LEFT JOIN fact_table
    ON ${makeCubeSafeString(factTableColumn.columnName)}_lookup."${factTableColumn.columnName}"=fact_table."${factTableColumn.columnName}"
    WHERE language = '${language}';
  `);
  await quack.close();
  const tableHeaders = Object.keys(dimensionTable[0]);
  const dataArray = dimensionTable.map((row) => Object.values(row));
  const currentDataset = await DatasetRepository.getById(dataset.id, { dimensions: { metadata: true } });
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
    total_records: 1,
    start_record: 1,
    end_record: 10
  };
  return viewGenerator(currentDataset, 1, pageInfo, 10, 1, headers, dataArray);
};

async function getDatePreviewWithExtractor(
  dataset: Dataset,
  extractor: object,
  factTableColumn: string,
  quack: Database,
  tableName: string
): Promise<ViewDTO> {
  const columnData = await quack.all(`SELECT DISTINCT "${factTableColumn}" FROM ${tableName}`);
  const dateDimensionTable = dateDimensionReferenceTableCreator(extractor, columnData);
  await quack.exec(createDateDimensionTable);
  // Create the date_dimension table
  const stmt = await quack.prepare('INSERT INTO date_dimension VALUES (?,?,?,?,?);');
  dateDimensionTable.map(async (row) => {
    await stmt.run(row.dateCode, row.description, row.start, row.end, row.type);
  });
  await stmt.finalize();
  const countQuery = `SELECT COUNT(DISTINCT date_dimension.date_code) AS total_rows FROM date_dimension`;
  const countResult = await quack.all(countQuery);
  const totalRows = countResult[0].total_rows;

  const previewQuery = `
        SELECT DISTINCT(date_dimension.date_code), date_dimension.description, date_dimension.start_date, date_dimension.end_date, date_dimension.date_type
        FROM date_dimension
        RIGHT JOIN "${tableName}" ON CAST("${tableName}"."${factTableColumn}" AS VARCHAR)=CAST(date_dimension.date_code AS VARCHAR)
        ORDER BY end_date ASC
        LIMIT ${sampleSize}
    `;
  const previewResult = await quack.all(previewQuery);

  const tableHeaders = Object.keys(previewResult[0]);
  const dataArray = previewResult.map((row) => Object.values(row));
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
    total_records: totalRows,
    start_record: 1,
    end_record: dataArray.length
  };
  const pageSize = dataArray.length < sampleSize ? dataArray.length : sampleSize;
  return viewGenerator(currentDataset, 1, pageInfo, pageSize, 1, headers, dataArray);
}

async function getPreviewWithNumberExtractor(
  dataset: Dataset,
  dimension: Dimension,
  quack: Database,
  tableName: string
): Promise<ViewDTO> {
  const extractor = dimension.extractor as NumberExtractor;
  let query: string;
  if (extractor.type === NumberType.Integer) {
    query = `SELECT DISTINCT CAST("${dimension.factTableColumn}" AS INTEGER) AS "${dimension.factTableColumn}" FROM ${tableName} ORDER BY "${dimension.factTableColumn}" ASC LIMIT ${sampleSize};`;
  } else {
    query = `SELECT DISTINCT CAST(CAST("${dimension.factTableColumn}" AS DECIMAL(18,${extractor.decimalPlaces})) AS VARCHAR) AS "${dimension.factTableColumn}" FROM ${tableName} ORDER BY "${dimension.factTableColumn}" ASC LIMIT ${sampleSize};`;
  }
  const totals = await quack.all(
    `SELECT COUNT(DISTINCT "${dimension.factTableColumn}") AS totalLines FROM ${tableName};`
  );
  const totalLines = Number(totals[0].totalLines);

  logger.debug(`query = ${query}`);
  const preview = await quack.all(query);
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
    total_records: totalLines,
    start_record: 1,
    end_record: preview.length
  };
  const pageSize = preview.length < sampleSize ? preview.length : sampleSize;
  return viewGenerator(currentDataset, 1, pageInfo, pageSize, 1, headers, dataArray);
}

async function getPreviewWithoutExtractor(
  dataset: Dataset,
  dimension: Dimension,
  quack: Database,
  tableName: string
): Promise<ViewDTO> {
  const totals = await quack.all(
    `SELECT COUNT(DISTINCT "${dimension.factTableColumn}") AS totalLines FROM ${tableName};`
  );
  const totalLines = Number(totals[0].totalLines);
  const preview = await quack.all(
    `SELECT DISTINCT "${dimension.factTableColumn}" FROM ${tableName} ORDER BY "${dimension.factTableColumn}" ASC LIMIT ${sampleSize};`
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
    total_records: totalLines,
    start_record: 1,
    end_record: preview.length
  };
  const pageSize = preview.length < sampleSize ? preview.length : sampleSize;
  return viewGenerator(currentDataset, 1, pageInfo, pageSize, 1, headers, dataArray);
}

async function getLookupPreviewWithExtractor(
  dataset: Dataset,
  dimension: Dimension,
  quack: Database,
  language: string
) {
  if (!dimension.lookupTable) {
    throw new Error(`Lookup table does does not exist on dimension ${dimension.id}`);
  }
  const factTableColumn = dataset.factTable?.find(
    (col) => dimension.factTableColumn === col.columnName && col.columnType === FactTableColumnType.Dimension
  );
  if (!factTableColumn) {
    logger.error(`Could not find the fact table column ${dimension.factTableColumn} in the dataset`);
    return viewErrorGenerators(500, dataset.id, 'patch', 'errors.dimension_validation.fact_table_column_not_found', {
      mismatch: false
    });
  }

  logger.debug(`Generating lookup table preview for dimension ${dimension.id}`);
  const lookupTmpFile = await getFileImportAndSaveToDisk(dataset, dimension.lookupTable);
  const lookupTableName = `lookup_table`;
  await loadFileIntoDatabase(quack, dimension.lookupTable, lookupTmpFile, lookupTableName);
  await createLookupTableInCube(quack, factTableColumn, dimension, lookupTableName);
  const lookupTableSize = await quack.all(
    `SELECT * FROM ${makeCubeSafeString(dimension.factTableColumn)}_lookup WHERE language = '${language.toLowerCase()}'`
  );
  const query = `SELECT * EXCLUDE(language) FROM ${makeCubeSafeString(dimension.factTableColumn)}_lookup WHERE language = '${language.toLowerCase()}' ORDER BY sort_order, "${dimension.factTableColumn}" LIMIT ${sampleSize};`;
  logger.debug(`Querying the cube to get the preview using query ${query}`);
  const dimensionTable = await quack.all(query);
  const tableHeaders = Object.keys(dimensionTable[0]);
  const dataArray = dimensionTable.map((row) => Object.values(row));
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
    total_records: lookupTableSize.length,
    start_record: 1,
    end_record: dataArray.length
  };
  const pageSize = dimensionTable.length < sampleSize ? dimensionTable.length : sampleSize;
  return viewGenerator(currentDataset, 1, pageInfo, pageSize, 1, headers, dataArray);
}

export const getDimensionPreview = async (dataset: Dataset, dimension: Dimension, lang: string) => {
  logger.info(`Getting dimension preview for ${dimension.id}`);
  const tableName = 'fact_table';
  let quack: Database;
  try {
    quack = await createEmptyCubeWithFactTable(dataset);
  } catch (error) {
    logger.error(error, 'Something went wrong trying to create the cube for the preview');
    return viewErrorGenerators(500, dataset.id, 'none', 'errors.dimension.preview_failed', {});
  }

  let viewDto: ViewDTO | ViewErrDTO;
  try {
    if (dimension.extractor) {
      switch (dimension.type) {
        case DimensionType.Date:
        case DimensionType.DatePeriod:
          logger.debug('Previewing a date type dimension');
          viewDto = await getDatePreviewWithExtractor(
            dataset,
            dimension.extractor,
            dimension.factTableColumn,
            quack,
            tableName
          );
          break;

        case DimensionType.LookupTable:
          logger.debug('Previewing a lookup table');
          viewDto = await getLookupPreviewWithExtractor(dataset, dimension, quack, lang);
          break;

        case DimensionType.ReferenceData:
          logger.debug('Previewing a lookup table');
          viewDto = await getReferenceDataDimensionPreview(dataset, dimension, quack, tableName, lang);
          break;

        case DimensionType.Text:
          logger.debug('Previewing text dimension');
          viewDto = await getPreviewWithoutExtractor(dataset, dimension, quack, tableName);
          break;

        case DimensionType.Numeric:
          logger.debug('Previewing a numeric dimension');
          viewDto = await getPreviewWithNumberExtractor(dataset, dimension, quack, tableName);
          break;

        default:
          logger.debug(`Previewing a dimension of an unknown type.  Type supplied is ${dimension.type}`);
          viewDto = await getPreviewWithoutExtractor(dataset, dimension, quack, tableName);
      }
    } else {
      logger.debug('Straight column preview');
      viewDto = await getPreviewWithoutExtractor(dataset, dimension, quack, tableName);
    }
    return viewDto;
  } catch (error) {
    logger.error(`Something went wrong trying to create dimension preview with the following error: ${error}`);
    return viewErrorGenerators(500, dataset.id, 'none', 'errors.dimension.preview_failed', {});
  } finally {
    await quack.close();
  }
};
