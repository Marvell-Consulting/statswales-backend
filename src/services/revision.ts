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
import { DimensionUpdateTask } from '../interfaces/revision-task';
import { DatasetRepository } from '../repositories/dataset';
import {
  makeCubeSafeString,
  updateFactTableValidator,
  createCubeMetadataTable,
  createLookupTableDimension,
  createDateDimension
} from './cube-handler';
import { validateUpdatedDateDimension } from './dimension-processor';
import { checkForReferenceErrors } from './lookup-table-handler';
import { FactTableValidationExceptionType } from '../enums/fact-table-validation-exception-type';
import { FactTableValidationException } from '../exceptions/fact-table-validation-exception';

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
  const buildId = `build-${crypto.randomUUID()}`;
  const cubeDB = dbManager.getCubeDataSource().createQueryRunner();

  try {
    await cubeDB.query(pgformat('CREATE SCHEMA IF NOT EXISTS %I;', buildId));
    await cubeDB.query(pgformat(`SET search_path TO %I;`, buildId));
    await updateFactTableValidator(cubeDB, buildId, dataset, revision);
  } catch (err) {
    const error = err as CubeValidationException;
    logger.debug('Closing DuckDB instance');
    const end = performance.now();
    const time = Math.round(end - start);
    logger.info(`Cube update validation took ${time}ms`);
    await cubeDB.query(pgformat('DROP SCHEMA %I CASCADE', buildId));
    cubeDB.release();
    throw error;
  }

  const dimensionUpdateTasks: DimensionUpdateTask[] = [];

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
      await createCubeMetadataTable(cubeDB, revision.id, buildId);
      switch (dimension.type) {
        case DimensionType.LookupTable:
          logger.debug(`Validating lookup table dimension: ${dimension.id}`);
          await createLookupTableDimension(cubeDB, dataset, dimension, factTableColumn);
          await checkForReferenceErrors(cubeDB, dataset, dimension, factTableColumn);
          break;
        case DimensionType.DatePeriod:
        case DimensionType.Date:
          logger.debug(`Validating time dimension: ${dimension.id}`);
          await createDateDimension(cubeDB, dimension.extractor, factTableColumn);
          await validateUpdatedDateDimension(cubeDB, dataset, dimension, factTableColumn);
      }
    } catch (error) {
      logger.warn(`An error occurred validating dimension ${dimension.id}: ${error}`);
      const err = error as CubeValidationException;
      if (err.type === CubeValidationType.DimensionNonMatchedRows) {
        dimensionUpdateTasks.push({
          id: dimension.id,
          lookupTableUpdated: false
        });
      } else {
        const end = performance.now();
        const time = Math.round(end - start);
        logger.info(`Cube update validation took ${time}ms`);
        await cubeDB.query(pgformat('DROP SCHEMA %I CASCADE', buildId));
        cubeDB.release();
        logger.error(err, `An error occurred trying to validate the file`);
        throw new BadRequestException('errors.data_table_validation_error');
      }
    }
  }

  // TODO Validate measure.  This requires a rewrite of how measures are created and stored

  revision.tasks = { dimensions: dimensionUpdateTasks };

  await cubeDB.query(pgformat('DROP SCHEMA %I CASCADE', buildId));
  cubeDB.release();
  await revision.save();
  const end = performance.now();
  const time = Math.round(end - start);
  logger.info(`Cube update validation took ${time}ms`);

  dataTable.revision = revision;
  await dataTable.save();
}
