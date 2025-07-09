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
import { LookupTable } from '../entities/dataset/lookup-table';
import { FactTableColumn } from '../entities/dataset/fact-table-column';
import { MeasureRow } from '../entities/dataset/measure-row';
import { MeasureMetadata } from '../entities/dataset/measure-metadata';

import { dateDimensionReferenceTableCreator, DateReferenceDataItem } from './time-matching';
import { getReferenceDataDimensionPreview } from './reference-data-handler';
import { NumberExtractor, NumberType } from '../extractors/number-extractor';
import { viewErrorGenerators, viewGenerator } from '../utils/view-error-generators';
import { getFileService } from '../utils/get-file-service';
import { createDatePeriodTableQuery, makeCubeSafeString } from './cube-handler';
import { t } from 'i18next';
import { CubeValidationException } from '../exceptions/cube-error-exception';
import { CubeValidationType } from '../enums/cube-validation-type';
import { format as pgformat } from '@scaleleap/pg-format/lib/pg-format';
import { YearType } from '../enums/year-type';
import { PoolClient, QueryResult } from 'pg';
import { getCubeDB } from '../db/cube-db';

const sampleSize = 5;

export interface ValidatedSourceAssignment {
  dataValues: SourceAssignmentDTO | null;
  noteCodes: SourceAssignmentDTO | null;
  measure: SourceAssignmentDTO | null;
  dimensions: SourceAssignmentDTO[];
  ignore: SourceAssignmentDTO[];
}

export const cleanUpDimension = async (dimension: Dimension): Promise<void> => {
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

export const setupTextDimension = async (dimension: Dimension): Promise<void> => {
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
  // logger.debug(`Validating source assignment from: ${JSON.stringify(sourceAssignment, null, 2)}`);

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
  if (!noteCodes) {
    throw new SourceAssignmentException('errors.source_assignment.missing_footnotes');
  }

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
    metadata: SUPPORTED_LOCALES.map((language: string) => DimensionMetadata.create({ language, name: '' }))
  }).save();
}

async function updateDataValueColumn(dataset: Dataset, dataValueColumnDto: SourceAssignmentDTO): Promise<void> {
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

async function removeIgnoreAndUnknownColumns(dataset: Dataset, ignoreColumns: SourceAssignmentDTO[]): Promise<void> {
  let factTableColumns: FactTableColumn[] = [];
  factTableColumns = await FactTableColumn.findBy({ id: dataset.id });
  logger.debug('Unprocessed columns in fact table');

  for (const column of ignoreColumns) {
    const factTableCol = factTableColumns.find((columnInfo) => columnInfo.columnName === column.column_name);
    if (!factTableCol) {
      continue;
    }
    logger.debug(`Updating column ${column.column_name} from fact table`);
    factTableCol.columnType = FactTableColumnType.Ignore;
    await factTableCol.save();
  }

  try {
    factTableColumns = await FactTableColumn.findBy({ id: dataset.id, columnDatatype: FactTableColumnType.Unknown });
    logger.debug(`Found ${factTableColumns.length} columns in fact table...`);
  } catch (error) {
    logger.error(error, `Something went wrong trying to find columns in fact table`);
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
    metadata: SUPPORTED_LOCALES.map((language: string) => MeasureMetadata.create({ language, name: '' }))
  }).save();
}

async function createUpdateNoteCodes(dataset: Dataset, columnAssignment: SourceAssignmentDTO): Promise<void> {
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

export async function removeAllDimensions(dataset: Dataset): Promise<void> {
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

export async function removeMeasure(dataset: Dataset): Promise<void> {
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

  for (const dimensionCreationDTO of dimensions) {
    logger.debug(`Creating dimension column: ${JSON.stringify(dimensionCreationDTO)}`);
    await createUpdateDimension(dataset, dimensionCreationDTO);
  }

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
    throw error;
  }

  logger.debug('Finished creating dimensions');
};

export const validateNumericDimension = async (
  dimensionPatchRequest: DimensionPatchDto,
  dataset: Dataset,
  dimension: Dimension
): Promise<ViewDTO | ViewErrDTO> => {
  const revision = dataset.draftRevision!;
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
  // TODO REPLACE WITH POSTGRES POOL
  const connection = await getCubeDB().connect();
  try {
    await connection.query(pgformat(`SET search_path TO %I;`, revision.id));
  } catch (error) {
    logger.error(error, 'Something went wrong trying to link to postgres database');
    return viewErrorGenerators(500, dataset.id, 'patch', 'errors.cube_builder.fact_table_creation_failed', {});
  }

  // Validate column type in data table matches proposed type first using DuckDBs column detection
  const columnInfo: QueryResult<{ column_name: string; data_type: string }> = await connection.query(
    pgformat(
      'SELECT column_name, data_type FROM information_schema.columns WHERE table_name = %L AND column_name = %L;',
      tableName,
      dimension.factTableColumn
    )
  );
  if (extractor.type === NumberType.Integer) {
    let savedDimension: Dimension;
    switch (columnInfo.rows[0].data_type) {
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
        return getPreviewWithNumberExtractor(dataset, savedDimension, connection, tableName);
    }
  } else if (extractor.type === NumberType.Decimal) {
    let savedDimension: Dimension;
    switch (columnInfo.rows[0].data_type) {
      case 'DOUBLE':
      case 'FLOAT':
        dimension.extractor = extractor;
        savedDimension = await dimension.save();
        return getPreviewWithNumberExtractor(dataset, savedDimension, connection, tableName);
    }
  }

  let whereRegEx: string;
  if (extractor.type === NumberType.Integer) {
    whereRegEx = `'^-?[0-9]*$'`;
  } else {
    whereRegEx = `'^-?[0-9]*[.]?[0-9]*$'`;
  }
  try {
    const nonMatchingQuery = `${dimension.factTableColumn} FROM ${tableName} WHERE field !~ ${whereRegEx});`;
    const nonMatching = await connection.query(`SELECT ${nonMatchingQuery}`);
    if (nonMatching.rows.length > 0) {
      const nonMatchingDataTableValues = await connection.query(`SELECT DISTINCT ${nonMatchingQuery}`);
      logger.error(
        `The user supplied a ${extractor.type} number format but there were ${nonMatching.rows.length} rows which didn't match the format`
      );
      connection.release();
      return viewErrorGenerators(400, dataset.id, 'patch', 'errors.dimension_validation.non_numerical_values_present', {
        totalNonMatching: nonMatching.rows.length,
        nonMatchingDataTableValues: nonMatchingDataTableValues.rows.map((row) => Object.values(row)[0])
      });
    }
  } catch (error) {
    logger.error(error, `Something went wrong trying to validate the data with the following error: ${error}`);
    return viewErrorGenerators(400, dataset.id, 'patch', 'errors.dimension_validation.unknown_error', {});
  }
  logger.debug(`Validation finished, updating dimension ${dimension.id} with new extractor`);
  dimension.lookupTable = null;
  dimension.joinColumn = null;
  dimension.type = DimensionType.Numeric;
  dimension.extractor = extractor;
  const savedDimension = await dimension.save();
  return getPreviewWithNumberExtractor(dataset, savedDimension, connection, tableName);
};

export const validateUpdatedDateDimension = async (
  connection: PoolClient,
  dataset: Dataset,
  dimension: Dimension,
  factTableColumn: FactTableColumn
): Promise<undefined> => {
  const errors = await validateDateDimension(
    connection,
    dataset,
    dimension,
    factTableColumn,
    `${makeCubeSafeString(factTableColumn.columnName)}_lookup`
  );
  if (errors) {
    const err = new CubeValidationException('Validation failed');
    err.type = CubeValidationType.DimensionNonMatchedRows;
    throw err;
  }
  return undefined;
};

export const validateDateDimension = async (
  connection: PoolClient,
  dataset: Dataset,
  dimension: Dimension,
  factTableColumn: FactTableColumn,
  lookupTableName: string
): Promise<ViewErrDTO | undefined> => {
  const extractor = dimension.extractor as DateExtractor;
  const tableName = 'fact_table';
  try {
    const preview = await connection.query(`SELECT DISTINCT "${dimension.factTableColumn}" FROM ${tableName};`);
    // Now validate everything matches
    const matchingQuery = `SELECT
        line_number, fact_table_date, "${lookupTableName}"."${factTableColumn.columnName}"
      FROM (
        SELECT
          row_number() OVER () as line_number, "${dimension.factTableColumn}" as fact_table_date
        FROM
          ${tableName}
      ) as fact_table
      LEFT JOIN "${lookupTableName}"
      ON fact_table.fact_table_date="${lookupTableName}"."${factTableColumn.columnName}"
      WHERE "${factTableColumn.columnName}" IS NULL;`;
    // logger.debug(`Matching query is:\n${matchingQuery}`);

    const nonMatchedRows = await connection.query(matchingQuery);
    if (nonMatchedRows.rows.length > 0) {
      if (nonMatchedRows.rows.length === preview.rows.length) {
        logger.error(`The user supplied an incorrect format and none of the rows matched.`);
        return viewErrorGenerators(400, dataset.id, 'patch', 'errors.dimension_validation.invalid_date_format', {
          extractor,
          totalNonMatching: preview.rows.length,
          nonMatchingValues: []
        });
      } else {
        logger.error(
          `There were ${nonMatchedRows.rows.length} row(s) which didn't match based on the information given to us by the user`
        );
        const nonMatchingRowsQuery = `
            SELECT
              DISTINCT fact_table_date
            FROM (
              SELECT
                row_number() OVER () as line_number, "${dimension.factTableColumn}" as fact_table_date
              FROM ${tableName}) AS fact_table
              LEFT JOIN "${lookupTableName}"
              ON fact_table.fact_table_date="${lookupTableName}"."${factTableColumn.columnName}"
             WHERE "${factTableColumn.columnName}" IS NULL;`;
        const nonMatchedRowSample: QueryResult<{ fact_table_date: string }> =
          await connection.query(nonMatchingRowsQuery);
        const nonMatchingValues = nonMatchedRowSample.rows
          .map((item) => item.fact_table_date)
          .filter((item, i, ar) => ar.indexOf(item) === i);
        const totalNonMatching = nonMatchedRows.rows.length;
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
  const revision = dataset.draftRevision!;
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

  const connection = await getCubeDB().connect();
  try {
    await connection.query(pgformat(`SET search_path TO %I;`, revision.id));
  } catch (error) {
    logger.error(error, 'Unable to connect to postgres schema for revision.');
    return viewErrorGenerators(500, dataset.id, 'patch', 'errors.dimension_validation.lookup_table_loading_failed', {
      mismatch: false
    });
  }

  const actionId = crypto.randomUUID();

  // Use the extracted data to try to create a reference table based on the user supplied information
  let dateDimensionTable: DateReferenceDataItem[] = [];
  const extractor: DateExtractor = {
    type: dimensionPatchRequest.date_type || YearType.Calendar,
    yearFormat: dimensionPatchRequest.year_format,
    quarterFormat: dimensionPatchRequest.quarter_format,
    quarterTotalIsFifthQuart: dimensionPatchRequest.fifth_quarter,
    monthFormat: dimensionPatchRequest.month_format,
    dateFormat: dimensionPatchRequest.date_format
  };

  logger.debug(`Extractor created: ${JSON.stringify(extractor)}`);
  const previewQuery = pgformat(
    'SELECT DISTINCT %I as date_data FROM %I.%I;',
    dimension.factTableColumn,
    dataset.draftRevision!.id,
    tableName
  );

  // logger.debug(`Preview query is: ${previewQuery}`);
  const preview: QueryResult<{ data_data: string }> = await connection.query(previewQuery);
  try {
    // logger.debug(`Preview is: ${JSON.stringify(preview)}`);
    dateDimensionTable = dateDimensionReferenceTableCreator(extractor, preview.rows);
    // logger.debug(
    //   `Date dimension table created with the following JSON: ${JSON.stringify(dateDimensionTable, null, 2)}`
    // );
  } catch (error) {
    logger.error(error, `Something went wrong trying to create the date reference table`);
    connection.release();
    return viewErrorGenerators(400, dataset.id, 'patch', 'errors.dimension.invalid_date_format', {
      extractor,
      totalNonMatching: preview.rows.length,
      nonMatchingValues: []
    });
  }

  try {
    await connection.query(createDatePeriodTableQuery(factTableColumn, actionId));
    for (const locale of SUPPORTED_LOCALES) {
      logger.debug(`populating ${actionId} table for locale ${locale}`);
      const lang = locale.toLowerCase();
      for (const row of dateDimensionTable) {
        await connection.query(
          pgformat('INSERT INTO %I VALUES (%L)', actionId, [
            row.dateCode,
            lang,
            row.description,
            null,
            t(row.type, { lng: locale }),
            row.start,
            row.end
          ])
        );
      }
    }
  } catch (error) {
    logger.error(error, `Something went wrong trying to create the date dimension table`);
    await connection.query(pgformat('DROP TABLE %I;', actionId));
    connection.release();
    return viewErrorGenerators(500, dataset.id, 'patch', 'errors.dimension_validation.unknown_error', {
      extractor,
      totalNonMatching: preview.rows.length,
      nonMatchingValues: [],
      mismatch: false
    });
  }

  const validationErrors = await validateDateDimension(connection, dataset, dimension, factTableColumn, actionId);
  if (validationErrors) {
    connection.release();
    return validationErrors;
  }

  const coverage: QueryResult<{ start_date: Date; end_date: Date }> = await connection.query(
    pgformat(`SELECT MIN(start_date) as start_date, MAX(end_date) AS end_date FROM %I;`, actionId)
  );
  const updateDataset = await Dataset.findOneByOrFail({ id: dataset.id });
  updateDataset.startDate = coverage.rows[0].start_date;
  updateDataset.endDate = coverage.rows[0].end_date;
  await updateDataset.save();
  const updateDimension = await Dimension.findOneByOrFail({ id: dimension.id });
  updateDimension.extractor = extractor;
  updateDimension.joinColumn = 'date_code';
  updateDimension.type = dimensionPatchRequest.dimension_type;
  await updateDimension.save();
  try {
    const previewQuery = pgformat(
      'SELECT DISTINCT %I.* FROM %I LEFT JOIN fact_table ON %I.%I=fact_table.%I WHERE language = %L;',
      actionId,
      actionId,
      actionId,
      factTableColumn.columnName,
      factTableColumn.columnName,
      language
    );

    const dimensionTable = await connection.query(previewQuery);

    const tableHeaders = Object.keys(dimensionTable.rows[0]);
    const dataArray = dimensionTable.rows.map((row) => Object.values(row));
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
  } catch (error) {
    logger.error(error, 'Something went wrong trying to get preview of date dimension lookup table.');
    connection.release();
    return viewErrorGenerators(500, dataset.id, 'patch', 'errors.dimension_validation.unknown_error', {
      extractor,
      totalNonMatching: preview.rows.length,
      nonMatchingValues: [],
      mismatch: false
    });
  }
};

async function getDatePreviewWithExtractor(
  dataset: Dataset,
  factTableColumn: string,
  connection: PoolClient
): Promise<ViewDTO> {
  const tableName = `${makeCubeSafeString(factTableColumn)}_lookup`;
  const totalsQuery: QueryResult<{ totalLines: number }> = await connection.query(
    pgformat('SELECT COUNT(DISTINCT %I) AS totalLines FROM %I;', factTableColumn, 'fact_table')
  );
  const previewQuery = pgformat(
    `
        SELECT DISTINCT(%I.%I), %I.description, %I.start_date, %I.end_date, %I.date_type
        FROM %I.%I
        RIGHT JOIN fact_table ON CAST(fact_table.%I AS VARCHAR)=CAST(%I.%I AS VARCHAR)
        ORDER BY end_date ASC
        LIMIT %L
    `,
    tableName,
    factTableColumn,
    tableName,
    tableName,
    tableName,
    tableName,
    dataset.draftRevision!.id,
    tableName,
    factTableColumn,
    tableName,
    factTableColumn,
    sampleSize
  );
  const previewResult = await connection.query(previewQuery);
  const tableHeaders = Object.keys(previewResult.rows[0]);
  const dataArray = previewResult.rows.map((row) => Object.values(row));
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
    total_records: totalsQuery.rows[0].totalLines,
    start_record: 1,
    end_record: dataArray.length
  };
  const pageSize = dataArray.length < sampleSize ? dataArray.length : sampleSize;
  return viewGenerator(currentDataset, 1, pageInfo, pageSize, 1, headers, dataArray);
}

async function getPreviewWithNumberExtractor(
  dataset: Dataset,
  dimension: Dimension,
  connection: PoolClient,
  tableName: string
): Promise<ViewDTO> {
  const extractor = dimension.extractor as NumberExtractor;
  let query: string;
  if (extractor.type === NumberType.Integer) {
    query = `SELECT DISTINCT CAST("${dimension.factTableColumn}" AS INTEGER) AS "${dimension.factTableColumn}" FROM ${tableName} ORDER BY "${dimension.factTableColumn}" ASC LIMIT ${sampleSize};`;
  } else {
    query = `SELECT DISTINCT format('%s', TO_CHAR(ROUND(CAST(${dimension.factTableColumn} AS DECIMAL), '${extractor.decimalPlaces}'), '999,999,990.${extractor.decimalPlaces}')) AS "${dimension.factTableColumn}" FROM ${tableName} ORDER BY "${dimension.factTableColumn}" ASC LIMIT ${sampleSize};`;
  }
  const totals: QueryResult<{ totalLines: number }> = await connection.query(
    `SELECT COUNT(DISTINCT "${dimension.factTableColumn}") AS totalLines FROM ${tableName};`
  );
  const totalLines = Number(totals.rows[0].totalLines);

  const preview = await connection.query(query);
  const tableHeaders = Object.keys(preview.rows[0]);
  const dataArray = preview.rows.map((row) => Object.values(row));
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
    end_record: preview.rows.length
  };
  const pageSize = preview.rows.length < sampleSize ? preview.rows.length : sampleSize;
  return viewGenerator(currentDataset, 1, pageInfo, pageSize, 1, headers, dataArray);
}

async function getPreviewWithoutExtractor(
  dataset: Dataset,
  dimension: Dimension,
  connection: PoolClient,
  tableName: string
): Promise<ViewDTO> {
  const totals: QueryResult<{ total_lines: number }> = await connection.query(
    `SELECT COUNT(DISTINCT "${dimension.factTableColumn}") AS total_lines FROM ${tableName};`
  );
  const totalLines = Number(totals.rows[0].total_lines);
  const preview = await connection.query(
    pgformat(
      'SELECT DISTINCT %I FROM %I ORDER BY %I ASC LIMIT %L;',
      dimension.factTableColumn,
      tableName,
      dimension.factTableColumn,
      sampleSize
    )
  );
  const tableHeaders = Object.keys(preview.rows[0]);
  const dataArray = preview.rows.map((row) => Object.values(row));
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
    end_record: preview.rows.length
  };
  const pageSize = preview.rows.length < sampleSize ? preview.rows.length : sampleSize;
  return viewGenerator(currentDataset, 1, pageInfo, pageSize, 1, headers, dataArray);
}

async function getLookupPreviewWithExtractor(
  dataset: Dataset,
  dimension: Dimension,
  connection: PoolClient,
  language: string
): Promise<ViewDTO> {
  const safeColName = makeCubeSafeString(dimension.factTableColumn);
  const lookupTableName = `${safeColName}_lookup`;
  const lookupTableSize: QueryResult<{ total_rows: number }> = await connection.query(
    pgformat(`SELECT COUNT(*) as total_rows FROM %I WHERE language = %L;`, lookupTableName, language)
  );
  const tableDetails: QueryResult<{ column_name: string }> = await connection.query(
    pgformat(
      'SELECT column_name FROM information_schema.columns WHERE table_schema = %L AND table_name = %L;',
      dataset.draftRevision!.id,
      lookupTableName
    )
  );
  const columnNames = tableDetails.rows.filter((row) => row.column_name != 'language').map((row) => row.column_name);
  const query = pgformat(
    `SELECT %I FROM %I WHERE language = %L ORDER BY sort_order, %I LIMIT %L;`,
    columnNames,
    lookupTableName,
    language.toLowerCase(),
    dimension.factTableColumn,
    sampleSize
  );

  logger.debug(`Querying the cube to get the preview using query ${query}`);
  const dimensionTable = await connection.query(query);
  const tableHeaders = Object.keys(dimensionTable.rows[0]);
  const dataArray = dimensionTable.rows.map((row) => Object.values(row));
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
    total_records: lookupTableSize.rows[0].total_rows,
    start_record: 1,
    end_record: dataArray.length
  };
  const pageSize = dimensionTable.rows.length < sampleSize ? dimensionTable.rows.length : sampleSize;
  return viewGenerator(currentDataset, 1, pageInfo, pageSize, 1, headers, dataArray);
}

export const getDimensionPreview = async (
  dataset: Dataset,
  dimension: Dimension,
  lang: string
): Promise<ViewDTO | ViewErrDTO> => {
  logger.info(`Getting dimension preview for ${dimension.id}`);
  const tableName = 'fact_table';
  const connection = await getCubeDB().connect();
  try {
    await connection.query(pgformat(`SET search_path TO %I;`, dataset.draftRevision!.id));
  } catch (error) {
    logger.error(error, 'Unable to connect to postgres schema for revision.');
    return viewErrorGenerators(500, dataset.id, 'patch', 'errors.dimension_validation.lookup_table_loading_failed', {
      mismatch: false
    });
  }

  let viewDto: ViewDTO | ViewErrDTO;
  try {
    if (dimension.extractor) {
      switch (dimension.type) {
        case DimensionType.Date:
        case DimensionType.DatePeriod:
          logger.debug('Previewing a date type dimension');
          viewDto = await getDatePreviewWithExtractor(dataset, dimension.factTableColumn, connection);
          break;

        case DimensionType.LookupTable:
          logger.debug('Previewing a lookup table');
          viewDto = await getLookupPreviewWithExtractor(dataset, dimension, connection, lang);
          break;

        case DimensionType.ReferenceData:
          logger.debug('Previewing a lookup table');
          viewDto = await getReferenceDataDimensionPreview(dataset, dimension, connection, tableName, lang);
          break;

        case DimensionType.Text:
          logger.debug('Previewing text dimension');
          viewDto = await getPreviewWithoutExtractor(dataset, dimension, connection, tableName);
          break;

        case DimensionType.Numeric:
          logger.debug('Previewing a numeric dimension');
          viewDto = await getPreviewWithNumberExtractor(dataset, dimension, connection, tableName);
          break;

        default:
          logger.debug(`Previewing a dimension of an unknown type.  Type supplied is ${dimension.type}`);
          viewDto = await getPreviewWithoutExtractor(dataset, dimension, connection, tableName);
      }
    } else {
      logger.debug('Straight column preview');
      viewDto = await getPreviewWithoutExtractor(dataset, dimension, connection, tableName);
    }
    return viewDto;
  } catch (error) {
    logger.error(error, `Something went wrong trying to create dimension preview`);
    return viewErrorGenerators(500, dataset.id, 'none', 'errors.dimension.preview_failed', {});
  } finally {
    connection.release();
  }
};
