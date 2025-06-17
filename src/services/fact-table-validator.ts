import { format as pgformat } from '@scaleleap/pg-format';
import { ValidatedSourceAssignment } from './dimension-processor';
import { Dataset } from '../entities/dataset/dataset';
import { duckdb, linkToPostgres, safelyCloseDuckDb } from './duckdb';
import { FactTableColumn } from '../entities/dataset/fact-table-column';
import { FactTableColumnType } from '../enums/fact-table-column-type';
import { logger } from '../utils/logger';
import { FACT_TABLE_NAME, loadTableDataIntoFactTableFromPostgres, NoteCodes } from './cube-handler';
import { FactTableValidationException } from '../exceptions/fact-table-validation-exception';
import { FactTableValidationExceptionType } from '../enums/fact-table-validation-exception-type';
import { SourceAssignmentDTO } from '../dtos/source-assignment-dto';
import { tableDataToViewTable } from '../utils/table-data-to-view-table';
import { Database, TableData } from 'duckdb-async';
import { asyncTmpName } from '../utils/async-tmp';

interface FactTableDefinition {
  factTableColumn: FactTableColumn;
  factTableColumnType: FactTableColumnType;
  sourceAssignment?: SourceAssignmentDTO;
}

export const factTableValidatorFromSource = async (
  dataset: Dataset,
  validatedSourceAssignment: ValidatedSourceAssignment
): Promise<string> => {
  const revision = dataset.draftRevision;

  if (!revision) {
    throw new FactTableValidationException(
      'Unable to find draft revision',
      FactTableValidationExceptionType.NoDraftRevision,
      500
    );
  }

  const duckdbSaveFile = await asyncTmpName({ postfix: '.duckdb' });
  const quack = await duckdb(duckdbSaveFile);

  try {
    await linkToPostgres(quack, revision.id, true);
  } catch (error) {
    logger.error(error, 'Failed to link to postgtres schemas');
    throw new FactTableValidationException(
      'Postgres linking fialed',
      FactTableValidationExceptionType.UnknownError,
      500
    );
  }

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

  const primaryKeyDef = primaryKeyColumns.map((def) => def.factTableColumn.columnName);
  const factTableCreateDef = orderedFactTableDefinition
    .filter((def) => def.factTableColumnType !== FactTableColumnType.Ignore)
    .map(
      (def) =>
        `"${def.factTableColumn.columnName}" ${def.factTableColumn.columnDatatype === 'DOUBLE' ? 'DOUBLE PRECISION' : def.factTableColumn.columnDatatype}`
    );
  const factTableDef = orderedFactTableDefinition
    .filter((def) => def.factTableColumnType !== FactTableColumnType.Ignore)
    .map((def) => def.factTableColumn.columnName);
  // Commented out for testing
  // const factTableCreationQuery = pgformat(
  //   `CREATE TABLE %I.%I (%s, PRIMARY KEY (%I));`,
  //   revision.id,
  //   FACT_TABLE_NAME,
  //   factTableCreateDef.join(', '),
  //   primaryKeyDef
  // );
  // No primary key table creation
  const factTableCreationQuery = pgformat(
    `CREATE TABLE %I.%I (%s);`,
    revision.id,
    FACT_TABLE_NAME,
    factTableCreateDef.join(', ')
  );
  const createQuery = pgformat(`CALL postgres_execute('postgres_db', %L);`, factTableCreationQuery);

  // logger.debug(`Creating initial fact table in cube using query:\n${createQuery}`);

  try {
    await quack.exec(createQuery);
  } catch (err) {
    logger.error(err, `Failed to create fact table in cube`);
    await quack.close();
    throw new FactTableValidationException(
      (err as Error).message,
      FactTableValidationExceptionType.FactTableCreationFailed,
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

  try {
    await loadTableDataIntoFactTableFromPostgres(quack, factTableDef, FACT_TABLE_NAME, dataTable.id);
    await validateNoteCodesColumn(quack, validatedSourceAssignment.noteCodes, FACT_TABLE_NAME);
  } catch (err) {
    let error = err as FactTableValidationException;
    logger.error(error, 'Failed to load data table into fact table');
    // Attempt to augment the error with details of where the errors in the data table are
    if (error.type === FactTableValidationExceptionType.EmptyValue) {
      error = await identifyIncompleteFacts(quack, primaryKeyDef, error);
    } else if (error.type === FactTableValidationExceptionType.DuplicateFact) {
      error = await identifyDuplicateFacts(quack, primaryKeyDef, error);
    }
    throw error;
  } finally {
    logger.debug('Closing duckdb database');
    await safelyCloseDuckDb(quack);
    logger.debug('Duckdb Closed');
  }

  return duckdbSaveFile;
};

async function validateNoteCodesColumn(
  quack: Database,
  noteCodeColumn: SourceAssignmentDTO | null,
  factTableName: string
) {
  let notesCodes: TableData;
  try {
    notesCodes = await quack.all(`
      SELECT DISTINCT "${noteCodeColumn?.column_name}" as codes
      FROM ${factTableName}
      WHERE "${noteCodeColumn?.column_name}" IS NOT NULL;
    `);
  } catch (error) {
    logger.error(error, 'Failed to extract or validate validate note codes');
    throw new FactTableValidationException(
      'Failed to extract note codes column',
      FactTableValidationExceptionType.NoNoteCodes,
      500
    );
  }
  const allCodes = NoteCodes.map((noteCode) => noteCode.code);
  const badCodes: string[] = [];
  for (const noteCode of notesCodes) {
    noteCode.codes.split(',').forEach((code: string) => {
      const trimmedCode = code.trim().toLowerCase();
      if (!allCodes.includes(trimmedCode)) {
        logger.error(`Note code ${trimmedCode} is not a valid note code`);
        badCodes.push(code.trim());
      }
    });
  }
  if (badCodes.length === 0) {
    return;
  }
  const error = new FactTableValidationException(
    'Bad note codes found in note codes column',
    FactTableValidationExceptionType.BadNoteCodes,
    400
  );
  try {
    const badCodesString = badCodes.map((code) => `codes LIKE '%${code.toLowerCase()}%'`).join(' or ');
    const brokeNoteCodeLines = await quack.all(`
    SELECT * exclude(codes) FROM (
      SELECT row_number() OVER () as line_number, *, lower(${noteCodeColumn?.column_name}) as codes FROM ${factTableName} WHERE ${badCodesString}
    ) LIMIT 500;
  `);
    const { headers, data } = tableDataToViewTable(brokeNoteCodeLines);
    error.data = data;
    error.headers = headers;
  } catch (extractionErr) {
    logger.error(extractionErr, 'Failed to extract data from data table.');
  }
  throw error;
}

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
    const duplicateQuery = `
        SELECT  *
        FROM (SELECT row_number() OVER () as line_number, * FROM data_table)
        WHERE (${primaryKeyDef.join(', ')}) IN
        (
            SELECT ${primaryKeyDef.join(', ')}
            FROM (
                SELECT ${primaryKeyDef.join(', ')}, count(*) as fact_count
                FROM data_table GROUP BY ${primaryKeyDef.join(', ')} HAVING fact_count > 1
            )
        ) LIMIT 500;`;

    // logger.debug(`Running query to find duplicates:\n${duplicateQuery}`);

    const brokenFacts = await quack.all(duplicateQuery);
    const { headers, data } = tableDataToViewTable(brokenFacts);
    error.data = data;
    error.headers = headers;
  } catch (extractionErr) {
    logger.error(extractionErr, 'Failed to extract data from data table.');
  }
  return error;
}
