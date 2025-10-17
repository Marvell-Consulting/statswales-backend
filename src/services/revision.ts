import { format as pgformat } from '@scaleleap/pg-format';

import { logger } from '../utils/logger';
import { dbManager } from '../db/database-manager';
import { DataTable } from '../entities/dataset/data-table';
import { DataTableDescription } from '../entities/dataset/data-table-description';
import { FactTableColumn } from '../entities/dataset/fact-table-column';
import { Revision } from '../entities/dataset/revision';
import { CubeValidationType } from '../enums/cube-validation-type';
import { DataTableAction } from '../enums/data-table-action';
import { DimensionType } from '../enums/dimension-type';
import { FactTableColumnType } from '../enums/fact-table-column-type';
import { BadRequestException } from '../exceptions/bad-request.exception';
import { CubeValidationException } from '../exceptions/cube-error-exception';
import { UnknownException } from '../exceptions/unknown.exception';
import { ColumnMatch } from '../interfaces/column-match';
import { RevisionTask } from '../interfaces/revision-task';
import { DatasetRepository } from '../repositories/dataset';
import { createDateDimensionLookup } from './dimension-processor';
import { FactTableValidationExceptionType } from '../enums/fact-table-validation-exception-type';
import { FactTableValidationException } from '../exceptions/fact-table-validation-exception';
import {
  createAllCubeFiles,
  createLookupTableDimension,
  createMeasureLookupTable,
  makeCubeSafeString
} from './cube-builder';
import { CubeBuildType } from '../enums/cube-build-type';
import { Dimension } from '../entities/dataset/dimension';
import { Dataset } from '../entities/dataset/dataset';
import { validateLookupTableReferenceValues } from '../utils/lookup-table-utils';
import { MeasureRow } from '../entities/dataset/measure-row';
import { DateExtractor } from '../extractors/date-extractor';
import { config } from '../config';

export async function attachUpdateDataTableToRevision(
  datasetId: string,
  revision: Revision,
  dataTable: DataTable,
  updateAction: DataTableAction,
  columnMatcher?: ColumnMatch[]
): Promise<void> {
  logger.debug('Attaching update data table to revision and validating cube');
  const start = performance.now();

  const dataset = await DatasetRepository.getById(datasetId, {
    factTable: true,
    measure: { measureTable: true, metadata: true },
    dimensions: { metadata: true, lookupTable: true },
    revisions: { dataTable: { dataTableDescriptions: true } }
  });

  // Validate all the columns against the fact table
  if (columnMatcher) {
    const matchedColumns: string[] = [];
    for (const col of columnMatcher) {
      const factTableCol: FactTableColumn | undefined = dataset.factTable?.find(
        (factTableCol: FactTableColumn) =>
          makeCubeSafeString(factTableCol.columnName) === makeCubeSafeString(col.fact_table_column_name)
      );
      const dataTableCol = dataTable.dataTableDescriptions.find(
        (dataTableCol: DataTableDescription) =>
          makeCubeSafeString(dataTableCol.columnName) === makeCubeSafeString(col.data_table_column_name)
      );
      if (factTableCol && dataTableCol) {
        matchedColumns.push(factTableCol.columnName);
        dataTableCol.factTableColumn = factTableCol.columnName;
      }
    }
    if (matchedColumns.length !== dataset.factTable?.length) {
      logger.error(`Could not match all columns to the fact table.`);
      throw new UnknownException('errors.failed_to_match_columns');
    }
  } else {
    // validate columns
    const matchedColumns: string[] = [];
    const unmatchedColumns: string[] = [];
    for (const col of dataTable.dataTableDescriptions) {
      const factTableCol: FactTableColumn | undefined = dataset.factTable?.find(
        (factTableCol: FactTableColumn) =>
          makeCubeSafeString(factTableCol.columnName) === makeCubeSafeString(col.columnName)
      );
      if (factTableCol) {
        matchedColumns.push(factTableCol.columnName);
        col.factTableColumn = factTableCol.columnName;
      } else {
        unmatchedColumns.push(col.columnName);
      }
    }

    if (matchedColumns.length !== dataset.factTable?.length) {
      logger.error(
        `Could not match all columns to the fact table. The following columns were not matched: ${unmatchedColumns.join(', ')}`
      );
      const end = performance.now();
      const time = Math.round(end - start);
      logger.info(`Cube update validation took ${time}ms`);
      throw new FactTableValidationException(
        'Could not match all columns to the fact table.',
        FactTableValidationExceptionType.UnmatchedColumns,
        400
      );
    }
  }

  logger.debug(`Setting the update action to: ${updateAction}`);
  dataTable.action = updateAction;
  revision.dataTable = dataTable;
  await revision.save();
  const buildId = crypto.randomUUID();

  try {
    await createAllCubeFiles(dataset.id, revision.id, CubeBuildType.ValidationCube, buildId);
  } catch (err) {
    const error = err as CubeValidationException;
    const end = performance.now();
    const time = Math.round(end - start);
    logger.info(`Cube update validation took ${time}ms`);
    await dataTable.remove();
    throw error;
  }

  const revisionTasks: RevisionTask = {
    measure: undefined,
    dimensions: []
  };

  const measureColumn = await FactTableColumn.findOneOrFail({
    where: {
      columnName: dataset.measure.factTableColumn,
      dataset: { id: dataset.id }
    }
  });

  try {
    await validateMeasure(buildId, dataset, measureColumn, dataset.measure.measureTable!);
  } catch (err) {
    logger.warn(err, 'Validating measure failed.  Adding it to the revision tasks');
    revisionTasks.measure = { id: dataset.measure.id, lookupTableUpdated: false };
  }

  const dimensionToUpdate: Dimension[] = [];
  for (const dimension of dataset.dimensions) {
    const factTableColumn = dataset.factTable.find(
      (factTableColumn) =>
        factTableColumn.columnName === dimension.factTableColumn &&
        factTableColumn.columnType === FactTableColumnType.Dimension
    );
    if (!factTableColumn) {
      logger.error(`Could not find fact table column for dimension ${dimension.id}`);
      throw new BadRequestException('errors.data_table_validation_error');
    }

    try {
      const lookupTableName = `${makeCubeSafeString(dimension.factTableColumn)}_lookup`;
      let updateDimension: Dimension | undefined;
      switch (dimension.type) {
        case DimensionType.LookupTable:
          await createLookupTableInValidationCube(buildId, dimension, factTableColumn);
          break;
        case DimensionType.DatePeriod:
        case DimensionType.Date:
          updateDimension = await createDateTableInValidationCube(
            buildId,
            datasetId,
            lookupTableName,
            factTableColumn,
            dimension
          );
          break;
      }
      await validateDimension(
        buildId,
        dataset,
        factTableColumn.columnName,
        factTableColumn.columnName,
        lookupTableName
      );
      if (updateDimension) dimensionToUpdate.push(updateDimension);
    } catch (error) {
      logger.warn(`An error occurred validating dimension ${dimension.id}: ${error}`);
      const err = error as CubeValidationException;
      if (err.type === CubeValidationType.DimensionNonMatchedRows) {
        revisionTasks.dimensions.push({
          id: dimension.id,
          lookupTableUpdated: false
        });
      } else {
        const end = performance.now();
        const time = Math.round(end - start);
        logger.info(`Cube update validation took ${time}ms`);
        logger.error(err, `An error occurred trying to validate the file`);
        throw new BadRequestException('errors.data_table_validation_error');
      }
    }
  }
  for (const dim of dimensionToUpdate) {
    await dim.save();
  }
  revision.tasks = revisionTasks;
  await revision.save();
  const end = performance.now();
  const time = Math.round(end - start);
  logger.info(`Cube update validation took ${time}ms`);

  dataTable.revision = revision;
  await dataTable.save();
  if (!config.cube_builder.preserve_failed) void cleanUpValidationCube(buildId);
}

async function cleanUpValidationCube(buildId: string): Promise<void> {
  const cubeDB = dbManager.getCubeDataSource().createQueryRunner();
  try {
    await cubeDB.query(pgformat('DROP SCHEMA %I CASCADE', buildId));
  } catch (err) {
    logger.error(err, 'Something went wrong trying to clean up validation cube');
  } finally {
    void cubeDB.release();
  }
}

async function validateMeasure(
  buildId: string,
  dataset: Dataset,
  measureColumn: FactTableColumn,
  measureTable: MeasureRow[]
): Promise<void> {
  const measureTableSQL = createMeasureLookupTable(buildId, measureColumn, measureTable);
  const createMeasureRunner = dbManager.getCubeDataSource().createQueryRunner();
  try {
    logger.trace(`Running create measure table lookup SQL:\n\n${measureTableSQL.join('\n')}\n\n`);
    await createMeasureRunner.query(measureTableSQL.join('\n'));
  } catch (err) {
    logger.error(err, 'Failed to create new measure table in validation cube');
  } finally {
    void createMeasureRunner.release();
  }

  const referenceErrors = await validateLookupTableReferenceValues(
    buildId,
    dataset,
    measureColumn.columnName,
    'reference',
    `measure`,
    'dimension'
  );
  if (referenceErrors) {
    const err = new CubeValidationException('Validation failed');
    err.type = CubeValidationType.MeasureNonMatchedRows;
    throw err;
  }
}

async function createDateTableInValidationCube(
  buildId: string,
  datasetId: string,
  lookupTableName: string,
  factTableColumn: FactTableColumn,
  dimension: Dimension
): Promise<Dimension> {
  const extractor = dimension.extractor as DateExtractor;
  const coverage = await createDateDimensionLookup(buildId, datasetId, lookupTableName, factTableColumn, extractor);
  extractor.lookupTableStart = coverage.startDate;
  extractor.lookupTableEnd = coverage.endDate;
  dimension.extractor = extractor;
  dimension.lookupTable = coverage.lookupTable;
  return dimension.save();
}

async function createLookupTableInValidationCube(
  buildId: string,
  dimension: Dimension,
  factTableColumn: FactTableColumn
): Promise<void> {
  logger.debug(`Validating lookup table dimension: ${dimension.id}`);
  const createLookupSQL = createLookupTableDimension(buildId, dimension, factTableColumn);
  const createLookupRunner = dbManager.getCubeDataSource().createQueryRunner();
  try {
    await createLookupRunner.query(createLookupSQL);
  } finally {
    void createLookupRunner.release();
  }
}

async function validateDimension(
  buildId: string,
  dataset: Dataset,
  factTableColumnName: string,
  joinColumn: string,
  lookupTableName: string
): Promise<void> {
  const referenceErrors = await validateLookupTableReferenceValues(
    buildId,
    dataset,
    factTableColumnName,
    joinColumn,
    lookupTableName,
    'dimension'
  );
  if (referenceErrors) {
    const err = new CubeValidationException('Validation failed');
    err.type = CubeValidationType.DimensionNonMatchedRows;
    throw err;
  }
}
