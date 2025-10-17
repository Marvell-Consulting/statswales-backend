import { format as pgformat } from '@scaleleap/pg-format/lib/pg-format';
import crypto from 'node:crypto';

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
import { ViewDTO, ViewErrDTO } from '../dtos/view-dto';
import { DateExtractor } from '../extractors/date-extractor';
import { LookupTable } from '../entities/dataset/lookup-table';
import { FactTableColumn } from '../entities/dataset/fact-table-column';
import { MeasureRow } from '../entities/dataset/measure-row';
import { MeasureMetadata } from '../entities/dataset/measure-metadata';
import { createDatePeriodTableQuery, dateDimensionReferenceTableCreator, DateReferenceDataItem } from './date-matching';
import { NumberExtractor, NumberType } from '../extractors/number-extractor';
import { viewErrorGenerators } from '../utils/view-error-generators';
import { getFileService } from '../utils/get-file-service';
import { FACT_TABLE_NAME, makeCubeSafeString } from './cube-builder';
import { CubeValidationException } from '../exceptions/cube-error-exception';
import { CubeValidationType } from '../enums/cube-validation-type';
import { YearType } from '../enums/year-type';
import { dbManager } from '../db/database-manager';
import { Revision } from '../entities/dataset/revision';
import { stringify } from 'csv-stringify/sync';
import { FileType } from '../enums/file-type';
import { previewGenerator, sampleSize } from '../utils/preview-generator';

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
  dataTable: DataTable,
  sourceAssignment: SourceAssignmentDTO[]
): ValidatedSourceAssignment => {
  const validated: ValidatedSourceAssignment = {
    dataValues: null,
    measure: null,
    noteCodes: null,
    dimensions: [],
    ignore: []
  };

  const validColumnNames = dataTable.dataTableDescriptions?.map((col: DataTableDescription) => col.columnName) || [];

  sourceAssignment.forEach((sourceInfo) => {
    if (!validColumnNames.includes(sourceInfo.column_name)) {
      throw new SourceAssignmentException(`errors.source_assignment.invalid_column_name`);
    }

    switch (sourceInfo.column_type) {
      case FactTableColumnType.DataValues:
        if (validated.dataValues) {
          throw new SourceAssignmentException('errors.source_assignment.too_many_data_values');
        }
        validated.dataValues = sourceInfo;
        break;

      case FactTableColumnType.Measure:
        if (validated.measure) {
          throw new SourceAssignmentException('errors.source_assignment.too_many_measure');
        }
        validated.measure = sourceInfo;
        break;

      case FactTableColumnType.NoteCodes:
        if (validated.noteCodes) {
          throw new SourceAssignmentException('errors.source_assignment.too_many_footnotes');
        }
        validated.noteCodes = sourceInfo;
        break;

      case FactTableColumnType.Time:
      case FactTableColumnType.Dimension:
        validated.dimensions.push(sourceInfo);
        break;

      case FactTableColumnType.Ignore:
        validated.ignore.push(sourceInfo);
        break;

      default:
        throw new SourceAssignmentException(`errors.source_assignment.invalid_source_type`);
    }
  });

  if (!validated.dataValues) {
    throw new SourceAssignmentException('errors.source_assignment.missing_data_values');
  }

  if (!validated.measure) {
    throw new SourceAssignmentException('errors.source_assignment.missing_measure');
  }

  if (!validated.noteCodes) {
    throw new SourceAssignmentException('errors.source_assignment.missing_footnotes');
  }

  if (validated.dimensions.length < 1) {
    throw new SourceAssignmentException('errors.source_assignment.missing_dimensions');
  }

  return validated;
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
  let factTableColumns: FactTableColumn[];
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
        void fileService.delete(dataset.measure.lookupTable.filename, dataset.id);
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

  const columnInfoRunner = dbManager.getCubeDataSource().createQueryRunner();
  let columnInfo: { column_name: string; data_type: string }[];
  // Validate column type in data table matches proposed type first using DuckDBs column detection
  try {
    columnInfo = await columnInfoRunner.query(
      pgformat(
        'SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = %L AND table_name = %L AND column_name = %L;',
        revision.id,
        FACT_TABLE_NAME,
        dimension.factTableColumn
      )
    );
  } catch (error) {
    logger.error(error, 'Something went wrong trying to query the information schema');
    return viewErrorGenerators(400, dataset.id, 'patch', 'errors.dimension_validation.unknown_error', {});
  } finally {
    void columnInfoRunner.release();
  }

  let savedDimension: Dimension;
  if (extractor.type === NumberType.Integer) {
    switch (columnInfo[0].data_type) {
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
        return await getPreviewWithNumberExtractor(
          revision.id,
          dataset,
          savedDimension,
          await getTotals(dataset, dimension)
        );
    }
  } else if (extractor.type === NumberType.Decimal) {
    switch (columnInfo[0].data_type) {
      case 'DOUBLE':
      case 'FLOAT':
        dimension.extractor = extractor;
        savedDimension = await dimension.save();
        return getPreviewWithNumberExtractor(revision.id, dataset, savedDimension, await getTotals(dataset, dimension));
    }
  }

  let whereRegEx: string;
  if (extractor.type === NumberType.Integer) {
    whereRegEx = '^-?[0-9]*$';
  } else {
    whereRegEx = '^-?[0-9]*[.]?[0-9]*$';
  }

  // SELECT a portion of the query added on lines 441 and 453
  const nonMatchingQuery = pgformat(
    '%I FROM %I.%I WHERE %I !~ %L)',
    dimension.factTableColumn,
    revision.id,
    FACT_TABLE_NAME,
    dimension.factTableColumn,
    whereRegEx
  );

  const nonMatchingQueryRunner = dbManager.getCubeDataSource().createQueryRunner();
  let nonMatchingResult: Record<string, unknown>[];
  try {
    nonMatchingResult = await nonMatchingQueryRunner.query(pgformat(`SELECT %s;`, nonMatchingQuery));
  } catch (error) {
    logger.error(error, 'Something went wrong trying to run the non matching result query.');
    return viewErrorGenerators(500, dataset.id, 'patch', 'errors.dimension_validation.unknown_error', {});
  } finally {
    void nonMatchingQueryRunner.release();
  }

  if (nonMatchingResult.length > 0) {
    const distinctNonMatchingQueryRunner = dbManager.getCubeDataSource().createQueryRunner();
    let nonMatchingDataTableValues: Record<string, unknown>[];
    try {
      nonMatchingDataTableValues = await distinctNonMatchingQueryRunner.query(
        pgformat(`SELECT DISTINCT %s;`, nonMatchingQuery)
      );
    } catch (error) {
      logger.error(error, `Something went wrong trying to validate the data with the following error: ${error}`);
      return viewErrorGenerators(400, dataset.id, 'patch', 'errors.dimension_validation.unknown_error', {});
    } finally {
      void distinctNonMatchingQueryRunner.release();
    }
    logger.error(
      `The user supplied a ${extractor.type} number format but there were ${nonMatchingResult.length} rows which didn't match the format`
    );
    return viewErrorGenerators(400, dataset.id, 'patch', 'errors.dimension_validation.non_numerical_values_present', {
      totalNonMatching: nonMatchingResult.length,
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      nonMatchingDataTableValues: nonMatchingDataTableValues.map((row: any) => Object.values(row)[0])
    });
  }

  logger.debug(`Validation finished, updating dimension ${dimension.id} with new extractor`);
  dimension.lookupTable = null;
  dimension.joinColumn = null;
  dimension.type = DimensionType.Numeric;
  dimension.extractor = extractor;
  savedDimension = await dimension.save();
  return getPreviewWithNumberExtractor(revision.id, dataset, savedDimension, await getTotals(dataset, dimension));
};

export const validateUpdatedDateDimension = async (
  dataset: Dataset,
  revision: Revision,
  dimension: Dimension,
  factTableColumn: FactTableColumn
): Promise<undefined> => {
  const lookupTableName = `${makeCubeSafeString(factTableColumn.columnName)}_lookup`;
  const errors = await validateDateDimension(dataset, revision, dimension, factTableColumn, lookupTableName);
  if (errors) {
    const err = new CubeValidationException('Validation failed');
    err.type = CubeValidationType.DimensionNonMatchedRows;
    throw err;
  }
  return undefined;
};

export const validateDateDimension = async (
  dataset: Dataset,
  revision: Revision,
  dimension: Dimension,
  factTableColumn: FactTableColumn,
  lookupTableName: string
): Promise<ViewErrDTO | undefined> => {
  const extractor = dimension.extractor as DateExtractor;
  const dateDateQuery = pgformat(
    `SELECT DISTINCT %I as date_column FROM %I.%I;`,
    dimension.factTableColumn,
    revision.id,
    FACT_TABLE_NAME
  );
  const cubeDB = dbManager.getCubeDataSource().createQueryRunner();
  try {
    const dateData: { date_column: string }[] = await cubeDB.query(dateDateQuery);
    // Now validate everything matches
    const matchingQuery = pgformat(
      `SELECT
         line_number, fact_table_date, %I.%I
       FROM (
         SELECT
         row_number() OVER () as line_number, %I as fact_table_date
         FROM
         %I.%I
         ) as fact_table
         LEFT JOIN %I.%I
       ON fact_table.fact_table_date=%I.%I
       WHERE %I IS NULL;`,
      lookupTableName,
      factTableColumn.columnName,
      dimension.factTableColumn,
      revision.id,
      FACT_TABLE_NAME,
      revision.id,
      lookupTableName,
      lookupTableName,
      factTableColumn.columnName,
      factTableColumn.columnName
    );

    const nonMatchedRows = await cubeDB.query(matchingQuery);

    if (nonMatchedRows.length > 0) {
      if (nonMatchedRows.length === dateData.length) {
        logger.error(`The user supplied an incorrect format and none of the rows matched.`);
        return viewErrorGenerators(400, dataset.id, 'patch', 'errors.dimension_validation.invalid_date_format', {
          extractor,
          totalNonMatching: nonMatchedRows.length,
          nonMatchingValues: []
        });
      } else {
        logger.error(
          `There were ${nonMatchedRows.length} row(s) which didn't match based on the information given to us by the user`
        );
        const nonMatchingRowsQuery = pgformat(
          `SELECT
             DISTINCT fact_table_date
           FROM (
                  SELECT
                    row_number() OVER () as line_number, %I as fact_table_date
                  FROM %I.%I) AS fact_table
                  LEFT JOIN %I
           ON fact_table.fact_table_date=%I.%I
           WHERE %I IS NULL;`,
          dimension.factTableColumn,
          revision.id,
          FACT_TABLE_NAME,
          lookupTableName,
          lookupTableName,
          factTableColumn.columnName,
          factTableColumn.columnName
        );
        const nonMatchedRowSample: { fact_table_date: string }[] = await cubeDB.query(nonMatchingRowsQuery);
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
  } finally {
    void cubeDB.release();
  }
  return undefined;
};

interface DateDimensionCreationError {
  status: number;
  message: string;
  dataLength: number;
  error: Error;
}

export const createDateDimensionLookup = async (
  schemaId: string,
  datasetId: string,
  dimensionTableName: string,
  factTableColumn: FactTableColumn,
  extractor: DateExtractor
): Promise<{ startDate: Date; endDate: Date; lookupTable: LookupTable }> => {
  const tableName = 'fact_table';
  const dataQuery = pgformat(
    'SELECT DISTINCT %I as date_data FROM %I.%I;',
    factTableColumn.columnName,
    schemaId,
    tableName
  );

  const getDateDataQueryRunner = dbManager.getCubeDataSource().createQueryRunner();
  let dateData: { date_data: string }[];
  try {
    dateData = await getDateDataQueryRunner.query(dataQuery);
  } catch (error) {
    logger.error(error, 'Unable to get date data from the fact table.');
    throw {
      status: 500,
      message: 'errors.dimension_validation.lookup_table_loading_failed',
      error,
      dataLength: -1
    };
  } finally {
    void getDateDataQueryRunner.release();
  }

  let dateDimensionTable: DateReferenceDataItem[] = [];
  try {
    dateDimensionTable = dateDimensionReferenceTableCreator(extractor, dateData);
  } catch (error) {
    logger.error(error, `Something went wrong trying to create the date reference table`);
    throw {
      status: 400,
      message: 'errors.dimension.invalid_date_format',
      error,
      dataLength: dateData.length
    };
  }

  const csv = Buffer.from(
    stringify(
      dateDimensionTable.map((row) => {
        return {
          reference: row.dateCode,
          language: row.lang,
          description: row.description,
          start_date: row.start,
          end_date: row.end,
          type: row.type,
          sort_order: row.end.getTime(),
          hierarchy: row.hierarchy
        };
      }),
      { bom: true, header: true, quoted_string: true }
    )
  );

  const lookupTable = new LookupTable();
  lookupTable.id = crypto.randomUUID();
  lookupTable.isStatsWales2Format = false;
  lookupTable.uploadedAt = new Date();
  lookupTable.originalFilename = `${lookupTable.id}_${factTableColumn.columnName}_date_tbl.csv`;
  lookupTable.filename = `${lookupTable.id}_${factTableColumn.columnName}_date_tbl.csv`;
  lookupTable.fileType = FileType.Csv;
  lookupTable.mimeType = 'text/csv';
  const hash = crypto.createHash('sha256');
  hash.update(csv);
  lookupTable.hash = hash.digest('hex');
  await lookupTable.save();

  void getFileService().saveBuffer(lookupTable.originalFilename, datasetId, csv);

  const statements = [
    'BEGIN TRANSACTION;',
    createDatePeriodTableQuery(factTableColumn, schemaId, dimensionTableName),
    createDatePeriodTableQuery(factTableColumn, 'lookup_tables', lookupTable.id),
    ...dateDimensionTable.map((row) => {
      return pgformat('INSERT INTO %I.%I VALUES (%L);', schemaId, dimensionTableName, [
        row.dateCode,
        row.lang,
        row.description,
        row.start,
        row.end,
        row.type,
        row.end.getTime(),
        row.hierarchy
      ]);
    }),
    ...dateDimensionTable.map((row) => {
      return pgformat('INSERT INTO %I.%I VALUES (%L);', 'lookup_tables', lookupTable.id, [
        row.dateCode,
        row.lang,
        row.description,
        row.start,
        row.end,
        row.type,
        row.end.getTime(),
        row.hierarchy
      ]);
    }),
    'END TRANSACTION;'
  ];

  const createDimensionQueryRunner = dbManager.getCubeDataSource().createQueryRunner();
  try {
    logger.trace(`Running create date dimension statements:\n\n${statements.join('\n')}\n\n`);
    await createDimensionQueryRunner.query(statements.join('\n'));
  } catch (error) {
    logger.error(error, `Something went wrong trying to create the date dimension table`);
    throw {
      status: 500,
      message: 'errors.dimension_validation.unknown_error',
      error,
      dataLength: dateData.length
    };
  } finally {
    void createDimensionQueryRunner.release();
  }

  return {
    startDate: new Date(Math.min(...dateDimensionTable.map((item) => item.start.getTime()))),
    endDate: new Date(Math.max(...dateDimensionTable.map((item) => item.end.getTime()))),
    lookupTable
  };
};

export const createAndValidateDateDimension = async (
  dimensionPatchRequest: DimensionPatchDto,
  dataset: Dataset,
  dimension: Dimension,
  language: string
): Promise<ViewDTO | ViewErrDTO> => {
  const revision = dataset.draftRevision!;
  const factTableColumn = dataset.factTable?.find(
    (col) => dimension.factTableColumn === col.columnName && col.columnType === FactTableColumnType.Dimension
  );
  if (!factTableColumn) {
    logger.error(`Could not find the fact table column ${dimension.factTableColumn} in the dataset`);
    return viewErrorGenerators(500, dataset.id, 'patch', 'errors.dimension_validation.fact_table_column_not_found', {
      mismatch: false
    });
  }

  const actionId = crypto.randomUUID();

  // Use the extracted data to try to create a reference table based on the user-supplied information
  const extractor: DateExtractor = {
    type: dimensionPatchRequest.date_type || YearType.Calendar,
    yearFormat: dimensionPatchRequest.year_format,
    quarterFormat: dimensionPatchRequest.quarter_format,
    quarterTotalIsFifthQuart: dimensionPatchRequest.fifth_quarter,
    monthFormat: dimensionPatchRequest.month_format,
    dateFormat: dimensionPatchRequest.date_format,
    startDay: dimensionPatchRequest.start_day,
    startMonth: dimensionPatchRequest.start_month
  };

  logger.debug(`Extractor created: ${JSON.stringify(extractor)}`);

  let lookupTable: LookupTable;
  try {
    const coverage = await createDateDimensionLookup(revision.id, dataset.id, actionId, factTableColumn, extractor);
    extractor.lookupTableStart = coverage.startDate;
    extractor.lookupTableEnd = coverage.endDate;
    lookupTable = coverage.lookupTable;
  } catch (err) {
    logger.error(err, 'Something went wrong trying to create date dimension lookup');
    const error = err as DateDimensionCreationError;
    switch (error.status) {
      case 400:
        return viewErrorGenerators(400, dataset.id, 'patch', error.message, {
          extractor,
          totalNonMatching: error.dataLength,
          nonMatchingValues: []
        });
      default:
        return viewErrorGenerators(500, dataset.id, 'patch', error.message, {
          mismatch: false
        });
    }
  }

  const validationErrors = await validateDateDimension(dataset, revision, dimension, factTableColumn, actionId);
  if (validationErrors) {
    return validationErrors;
  }

  const updateDimension = await Dimension.findOneByOrFail({ id: dimension.id });
  updateDimension.extractor = extractor;
  updateDimension.joinColumn = 'date_code';
  updateDimension.type = dimensionPatchRequest.dimension_type;
  updateDimension.lookupTable = lookupTable;
  await updateDimension.save();
  const previewQuery = pgformat(
    'SELECT DISTINCT %I.* FROM %I.%I LEFT JOIN %I.%I ON %I.%I=%I.%I WHERE language = %L LIMIT %L;',
    actionId,
    revision.id,
    actionId,
    revision.id,
    FACT_TABLE_NAME,
    actionId,
    factTableColumn.columnName,
    FACT_TABLE_NAME,
    factTableColumn.columnName,
    language,
    sampleSize
  );

  const getPreviewQueryRunner = dbManager.getCubeDataSource().createQueryRunner();
  let dimensionTable: Record<string, never>[];
  try {
    dimensionTable = await getPreviewQueryRunner.query(previewQuery);
  } catch (error) {
    logger.error(error, 'Something went wrong trying to get the preview of the dimension');
    return viewErrorGenerators(500, dataset.id, 'patch', 'errors.dimension_validation.lookup_table_loading_failed', {
      mismatch: false
    });
  } finally {
    void getPreviewQueryRunner.release();
  }

  return previewGenerator(dimensionTable, await getTotals(dataset, dimension), dataset, true);
};

async function getDatePreviewWithExtractor(
  schemaID: string,
  dataset: Dataset,
  factTableColumn: string,
  language: string,
  totals: { totalLines: number }
): Promise<ViewDTO | ViewErrDTO> {
  const tableName = `${makeCubeSafeString(factTableColumn)}_lookup`;

  const previewQuery = pgformat(
    `
      SELECT DISTINCT
          (%I.%I),
          %I.description,
          to_char(%I.start_date, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') as start_date,
          to_char(%I.end_date, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') as end_date,
          %I.date_type
      FROM %I.%I
      RIGHT JOIN %I.%I ON CAST(fact_table.%I AS VARCHAR)=CAST(%I.%I AS VARCHAR)
      WHERE %I.language = %L
      LIMIT %L
    `,
    tableName,
    factTableColumn,
    tableName,
    tableName,
    tableName,
    tableName,
    schemaID,
    tableName,
    schemaID,
    FACT_TABLE_NAME,
    factTableColumn,
    tableName,
    factTableColumn,
    tableName,
    language.toLowerCase(),
    sampleSize
  );
  let previewResult: Record<string, never>[];
  const getPreviewRunner = dbManager.getCubeDataSource().createQueryRunner();
  try {
    previewResult = await getPreviewRunner.query(previewQuery);
  } catch (error) {
    logger.error(error, 'Something went wrong trying to get date lookup table preview');
    return viewErrorGenerators(500, dataset.id, 'none', 'errors.dimension.preview_failed', {});
  } finally {
    void getPreviewRunner.release();
  }

  return previewGenerator(previewResult, totals, dataset, true);
}

async function getPreviewWithNumberExtractor(
  schemaID: string,
  dataset: Dataset,
  dimension: Dimension,
  totals: { totalLines: number }
): Promise<ViewDTO | ViewErrDTO> {
  const extractor = dimension.extractor as NumberExtractor;
  let query: string;
  if (extractor.type === NumberType.Integer) {
    query = pgformat(
      ` SELECT DISTINCT CAST(%I AS INTEGER) AS %I FROM %I.%I ORDER BY %I ASC LIMIT %L;`,
      dimension.factTableColumn,
      dimension.factTableColumn,
      schemaID,
      FACT_TABLE_NAME,
      dimension.factTableColumn,
      sampleSize
    );
  } else {
    query = pgformat(
      `SELECT DISTINCT format('%s', TO_CHAR(ROUND(CAST(%I AS DECIMAL), %L), %L)) AS %I FROM %I.%I ORDER BY %I ASC LIMIT %L;`,
      dimension.factTableColumn,
      extractor.decimalPlaces,
      `999,999,990.${extractor.decimalPlaces}`,
      dimension.factTableColumn,
      schemaID,
      FACT_TABLE_NAME,
      dimension.factTableColumn,
      sampleSize
    );
  }

  const previewQueryRunner = dbManager.getCubeDataSource().createQueryRunner();
  let preview: Record<string, never>[];
  try {
    preview = await previewQueryRunner.query(query);
  } catch (error) {
    logger.error(error, 'Something went wrong trying to get lookup table details');
    return viewErrorGenerators(500, dataset.id, 'none', 'errors.dimension.preview_failed', {});
  } finally {
    void previewQueryRunner.release();
  }

  return previewGenerator(preview, totals, dataset, true);
}

async function getPreviewWithoutExtractor(
  schemaID: string,
  dataset: Dataset,
  dimension: Dimension,
  totals: { totalLines: number }
): Promise<ViewDTO | ViewErrDTO> {
  const previewQueryRunner = dbManager.getCubeDataSource().createQueryRunner();
  let preview: Record<string, never>[];
  try {
    preview = await previewQueryRunner.query(
      pgformat(
        'SELECT DISTINCT %I FROM %I ORDER BY %I ASC LIMIT %L;',
        dimension.factTableColumn,
        schemaID,
        FACT_TABLE_NAME,
        dimension.factTableColumn,
        sampleSize
      )
    );
  } catch (error) {
    logger.error(error, 'Something went wrong trying to get lookup table details');
    return viewErrorGenerators(500, dataset.id, 'none', 'errors.dimension.preview_failed', {});
  } finally {
    void previewQueryRunner.release();
  }

  return previewGenerator(preview, totals, dataset, true);
}

async function getLookupPreviewWithExtractor(
  schemaID: string,
  dataset: Dataset,
  dimension: Dimension,
  language: string,
  totals: { totalLines: number }
): Promise<ViewDTO | ViewErrDTO> {
  const lookupTableName = `${makeCubeSafeString(dimension.factTableColumn)}_lookup`;

  let tableDetails: { column_name: string }[];
  const tableDetailsRunner = dbManager.getCubeDataSource().createQueryRunner();
  try {
    tableDetails = await tableDetailsRunner.query(
      pgformat(
        'SELECT column_name FROM information_schema.columns WHERE table_schema = %L AND table_name = %L;',
        schemaID,
        lookupTableName
      )
    );
  } catch (error) {
    logger.error(error, 'Something went wrong trying to get lookup table details');
    return viewErrorGenerators(500, dataset.id, 'none', 'errors.dimension.preview_failed', {});
  } finally {
    void tableDetailsRunner.release();
  }

  const columnNames = tableDetails.filter((row) => row.column_name != 'language').map((row) => row.column_name);
  const query = pgformat(
    `SELECT %I FROM %I.%I WHERE language = %L ORDER BY sort_order, %I LIMIT %L;`,
    columnNames,
    schemaID,
    lookupTableName,
    language.toLowerCase(),
    dimension.factTableColumn,
    sampleSize
  );

  logger.debug(`Querying the cube to get the lookup preview`);
  logger.trace(`lookup preview query: ${query}`);
  const previewQueryRunner = dbManager.getCubeDataSource().createQueryRunner();
  let dimensionTable: Record<string, never>[];
  try {
    dimensionTable = await previewQueryRunner.query(query);
  } catch (error) {
    logger.error(error, 'Something went wrong getting lookup table preview');
    return viewErrorGenerators(500, dataset.id, 'none', 'errors.dimension.preview_failed', {});
  } finally {
    void previewQueryRunner.release();
  }

  return previewGenerator(dimensionTable, totals, dataset, true);
}

async function getTotals(dataset: Dataset, dimension: Dimension): Promise<{ totalLines: number }> {
  const totalsQueryRunner = dbManager.getCubeDataSource().createQueryRunner();
  let totals: { totalLines: number }[];
  try {
    totals = await totalsQueryRunner.query(
      pgformat(
        'SELECT COUNT(DISTINCT %I) AS totalLines FROM %I.%I;',
        dimension.factTableColumn,
        dataset.draftRevision!.id,
        FACT_TABLE_NAME
      )
    );
  } catch (error) {
    logger.error(error, 'Something went wrong trying to extract the total distinct values from fact table column');
    return { totalLines: -1 };
  } finally {
    void totalsQueryRunner.release();
  }
  return totals[0];
}

export const getDimensionPreview = async (
  dataset: Dataset,
  dimension: Dimension,
  lang: string
): Promise<ViewDTO | ViewErrDTO> => {
  logger.info(`Getting dimension preview for ${dimension.id}`);

  const totals = await getTotals(dataset, dimension);

  let viewDto: ViewDTO | ViewErrDTO;

  if (!dimension.extractor) {
    logger.debug('Straight column preview');
    return await getPreviewWithoutExtractor(dataset.draftRevision!.id, dataset, dimension, totals);
  }

  switch (dimension.type) {
    case DimensionType.Date:
    case DimensionType.DatePeriod:
      logger.debug('Previewing a date type dimension');
      return await getDatePreviewWithExtractor(
        dataset.draftRevision!.id,
        dataset,
        dimension.factTableColumn,
        lang,
        totals
      );

    case DimensionType.LookupTable:
      logger.debug('Previewing a lookup table');
      return await getLookupPreviewWithExtractor(dataset.draftRevision!.id, dataset, dimension, lang, totals);

    case DimensionType.Text:
      logger.debug('Previewing text dimension');
      return await getPreviewWithoutExtractor(dataset.draftRevision!.id, dataset, dimension, totals);

    case DimensionType.Numeric:
      logger.debug('Previewing a numeric dimension');
      return = await getPreviewWithNumberExtractor(dataset.draftRevision!.id, dataset, dimension, totals);

    default:
      logger.debug(`Previewing a dimension of an unknown type.  Type supplied is ${dimension.type}`);
      return await getPreviewWithoutExtractor(dataset.draftRevision!.id, dataset, dimension, totals);
  }
};

export const getFactTableColumnPreview = async (
  dataset: Dataset,
  revision: Revision,
  columnName: string
): Promise<ViewDTO | ViewErrDTO> => {
  logger.debug(`Getting fact table column preview for ${columnName}`);
  const previewQuery = pgformat('SELECT DISTINCT %I FROM %I.%I', columnName, revision.id, FACT_TABLE_NAME);

  const totalsQuery = pgformat('SELECT COUNT(DISTINCT %I) AS totalLines FROM (%s)', columnName, previewQuery);
  const totalsQueryRunner = dbManager.getCubeDataSource().createQueryRunner();
  let totals: { totalLines: number }[];
  try {
    logger.trace(`Getting fact table column count using query:\n\n${totalsQuery}\n\n`);
    totals = await totalsQueryRunner.query(totalsQuery);
  } catch (error) {
    logger.error(error, 'Something went wrong trying to get total distinct values in column');
    return viewErrorGenerators(500, dataset.id, 'csv', 'dimension.preview.failed_to_preview_column', {});
  } finally {
    void totalsQueryRunner.release();
  }
  const previewQueryRunner = dbManager.getCubeDataSource().createQueryRunner();
  let preview: Record<string, never>[];
  try {
    logger.trace(`Getting distinct column values from fact table using query:\n\n${previewQuery}\n\n`);
    preview = await previewQueryRunner.query(pgformat('%s LIMIT %L;', previewQuery, sampleSize));
  } catch (error) {
    logger.error(error);
    return viewErrorGenerators(500, dataset.id, 'csv', 'dimension.preview.failed_to_preview_column', {});
  } finally {
    void previewQueryRunner.release();
  }

  return previewGenerator(preview, totals[0], dataset, false);
};
