import tmp from 'tmp';

import { ValidatedSourceAssignment } from './dimension-processor';
import { Dataset } from '../entities/dataset/dataset';
import {duckdb, DUCKDB_WRITE_TIMEOUT} from './duckdb';
import { FactTableColumn } from '../entities/dataset/fact-table-column';
import { FactTableColumnType } from '../enums/fact-table-column-type';
import { logger } from '../utils/logger';
import { FACT_TABLE_NAME, loadFileIntoCube, loadTableDataIntoFactTable } from './cube-handler';
import { FactTableValidationException } from '../exceptions/fact-table-validation-exception';
import { FactTableValidationExceptionType } from '../enums/fact-table-validation-exception-type';
import { getFileImportAndSaveToDisk } from '../utils/file-utils';
import { SourceAssignmentDTO } from '../dtos/source-assignment-dto';
import { tableDataToViewTable } from '../utils/table-data-to-view-table';
import { Database } from 'duckdb-async';

interface FactTableDefinition {
  factTableColumn: FactTableColumn;
  factTableColumnType: FactTableColumnType;
  sourceAssignment?: SourceAssignmentDTO;
}

export const factTableValidatorFromSource = async (
  dataset: Dataset,
  validatedSourceAssignment: ValidatedSourceAssignment
): Promise<string> => {
  const duckdbSaveFile = tmp.tmpNameSync({ postfix: '.duckdb' });
  const quack = await duckdb(duckdbSaveFile);

  if (!dataset.factTable) {
    throw new Error(`Unable to find fact table for dataset ${dataset.id}`);
  }
  const baseFactTable = dataset.factTable.sort((colA, colB) => colA.columnIndex - colB.columnIndex);
  const factTableDefinition: FactTableDefinition[] = baseFactTable.map((col) => {
    if (validatedSourceAssignment.dataValues?.column_name === col.columnName)
      return {
        factTableColumn: col,
        factTableColumnType: FactTableColumnType.DataValues,
        sourceAssignment: validatedSourceAssignment.dataValues
      };
    if (validatedSourceAssignment.measure?.column_name === col.columnName)
      return {
        factTableColumn: col,
        factTableColumnType: FactTableColumnType.Measure,
        sourceAssignment: validatedSourceAssignment.measure
      };
    if (validatedSourceAssignment.noteCodes?.column_name === col.columnName)
      return {
        factTableColumn: col,
        factTableColumnType: FactTableColumnType.NoteCodes,
        sourceAssignment: validatedSourceAssignment.noteCodes
      };
    for (const sourceAssignment of validatedSourceAssignment.dimensions) {
      if (sourceAssignment.column_name === col.columnName) {
        return {
          factTableColumn: col,
          factTableColumnType: FactTableColumnType.Dimension,
          sourceAssignment
        };
      }
    }
    for (const sourceAssignment of validatedSourceAssignment.ignore) {
      if (sourceAssignment.column_name === col.columnName) {
        return {
          factTableColumn: col,
          factTableColumnType: FactTableColumnType.Ignore,
          sourceAssignment
        };
      }
    }
    return {
      factTableColumn: col,
      factTableColumnType: FactTableColumnType.Unknown
    };
  });

  if (factTableDefinition.find((def) => def.factTableColumnType === FactTableColumnType.Unknown)) {
    throw new FactTableValidationException(
      'Found unknowns when doing column matching.',
      FactTableValidationExceptionType.UnknownSourcesStillPresent,
      400
    );
  }

  const primaryKeyColumns = factTableDefinition.filter(
    (def) =>
      def.factTableColumnType === FactTableColumnType.Dimension ||
      def.factTableColumnType === FactTableColumnType.Measure
  );
  const orderedFactTableDefinition = factTableDefinition.sort(
    (a, b) => a.factTableColumn.columnIndex - b.factTableColumn.columnIndex
  );

  const primaryKeyDef = primaryKeyColumns.map((def) => `"${def.factTableColumn.columnName}"`);
  const factTableCreateDef = orderedFactTableDefinition.map(
    (def) => `"${def.factTableColumn.columnName}" ${def.factTableColumn.columnDatatype}`
  );
  const factTableDef = orderedFactTableDefinition.map((def) => def.factTableColumn.columnName);
  const factTableCreationQuery = `CREATE TABLE ${FACT_TABLE_NAME} (${factTableCreateDef.join(', ')}, PRIMARY KEY (${primaryKeyDef.join(', ')}));`;

  logger.debug(`Creating initial fact table in cube using query:\n${factTableCreationQuery}`);
  try {
    await quack.exec(factTableCreationQuery);
  } catch (err) {
    logger.error(err, `Failed to create fact table in cube`);
    await quack.close();
    throw new FactTableValidationException(
      (err as Error).message,
      FactTableValidationExceptionType.FactTableCreationFailed,
      500
    );
  }

  const revision = dataset.draftRevision;
  if (!revision) {
    await quack.close();
    throw new FactTableValidationException(
      'Unable to find draft revision',
      FactTableValidationExceptionType.NoDraftRevision,
      500
    );
  }
  const dataTable = revision.dataTable;
  if (!dataTable) {
    await quack.close();
    throw new FactTableValidationException(
      'Unable to find data on revision',
      FactTableValidationExceptionType.NoDataTable,
      500
    );
  }

  logger.debug('Loading data table data into the new fact table to begin validation');
  const dataTableFile = await getFileImportAndSaveToDisk(dataset, dataTable);
  try {
    await loadFileIntoCube(quack, dataTable, dataTableFile, 'data_table');
    await loadTableDataIntoFactTable(quack, factTableDef, FACT_TABLE_NAME, 'data_table');
    await quack.exec('DROP TABLE data_table;');
    // const databases = await quack.all('SELECT database_name FROM duckdb_databases() WHERE database_name NOT IN (\'memory\', \'system\', \'temp\');');
    // const dbName = databases[0].database_name;
    // logger.debug(`Writing database ${dbName} to disk at ${duckdbSaveFile}`);
    // await quack.exec(`ATTACH '${duckdbSaveFile}' as outDB (BLOCK_SIZE 16384);`);
    // await quack.exec(`COPY FROM DATABASE "${dbName}" TO outDB;`);
    // await quack.exec('DETACH outDB;');
  } catch (err) {
    let error = err as FactTableValidationException;
    logger.error(error, 'Failed to load data table into fact table');
    // Attempt to augment the error with details of where the errors in the data table are
    if (error.type === FactTableValidationExceptionType.EmptyValue) {
      error = await identifyIncompleteFacts(quack, primaryKeyDef, error);
    } else if (error.type === FactTableValidationExceptionType.DuplicateFact) {
      error = await identifyDuplicateFacts(quack, primaryKeyDef, error);
    }
  } finally {
    logger.debug('Closing duckdb database');
    await quack.close();
    await new Promise((f) => setTimeout(f, DUCKDB_WRITE_TIMEOUT));
    logger.debug('Duckdb Closed');
  }
  return duckdbSaveFile;
};

async function identifyIncompleteFacts(
  quack: Database,
  primaryKeyDef: string[],
  error: FactTableValidationException
): Promise<FactTableValidationException> {
  try {
    const brokenFacts = await quack.all(
      `SELECT * FROM (SELECT row_number() OVER () as line_number, * FROM data_table) WHERE ${primaryKeyDef.join('IS NULL OR ')} IS NULL LIMIT 500;`
    );
    const { headers, data } = tableDataToViewTable(brokenFacts);
    error.data = data;
    error.headers = headers;
  } catch (extractionErr) {
    logger.error(extractionErr, 'Failed to extract data from data table.');
  }
  return error;
}

async function identifyDuplicateFacts(
  quack: Database,
  primaryKeyDef: string[],
  error: FactTableValidationException

): Promise<FactTableValidationException> {
  try {
    const brokenFacts = await quack.all(`
        SELECT  *
        FROM (SELECT row_number() OVER () as line_number, * FROM data_table)
        WHERE (${primaryKeyDef.join(', ')}) IN
        (
            SELECT ${primaryKeyDef.join(', ')}
            FROM (
                SELECT ${primaryKeyDef.join(', ')}, count(*) as fact_count
                FROM data_table GROUP BY ${primaryKeyDef.join(', ')} HAVING fact_count > 1
            )
        ) LIMIT 500;
      `);
    const { headers, data } = tableDataToViewTable(brokenFacts);
    error.data = data;
    error.headers = headers;
  } catch (extractionErr) {
    logger.error(extractionErr, 'Failed to extract data from data table.');
  }
  return error;
}
