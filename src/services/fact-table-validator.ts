import { format as pgformat } from '@scaleleap/pg-format';
import { ValidatedSourceAssignment } from './dimension-processor';
import { Dataset } from '../entities/dataset/dataset';
import { FactTableColumn } from '../entities/dataset/fact-table-column';
import { FactTableColumnType } from '../enums/fact-table-column-type';
import { logger } from '../utils/logger';
import { FACT_TABLE_NAME, NoteCodes } from './cube-handler';
import { FactTableValidationException } from '../exceptions/fact-table-validation-exception';
import { FactTableValidationExceptionType } from '../enums/fact-table-validation-exception-type';
import { SourceAssignmentDTO } from '../dtos/source-assignment-dto';
import { tableDataToViewTable } from '../utils/table-data-to-view-table';
import { getCubeDB } from '../db/cube-db';
import { PoolClient, QueryResult } from 'pg';

interface FactTableDefinition {
  factTableColumn: FactTableColumn;
  factTableColumnType: FactTableColumnType;
  sourceAssignment?: SourceAssignmentDTO;
}

export const factTableValidatorFromSource = async (
  dataset: Dataset,
  validatedSourceAssignment: ValidatedSourceAssignment
): Promise<void> => {
  const revision = dataset.draftRevision;

  if (!revision) {
    throw new FactTableValidationException(
      'Unable to find draft revision',
      FactTableValidationExceptionType.NoDraftRevision,
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

  const primaryKeyDef = primaryKeyColumns.map((def) => def.factTableColumn.columnName);

  const dataTable = revision.dataTable;
  if (!dataTable) {
    throw new FactTableValidationException(
      'Unable to find data on revision',
      FactTableValidationExceptionType.NoDataTable,
      500
    );
  }
  const connection = await getCubeDB().connect();
  try {
    await connection.query(pgformat(`SET search_path TO %I;`, revision.id));
  } catch (error) {
    logger.error(error, 'Unable to connect to postgres schema for revision.');
    throw new FactTableValidationException(
      'Unable to find data on revision cube in database',
      FactTableValidationExceptionType.UnknownError,
      500
    );
  }
  try {
    await connection.query(pgformat('ALTER TABLE %I ADD PRIMARY KEY (%I)', FACT_TABLE_NAME, primaryKeyDef));
    await validateNoteCodesColumn(connection, validatedSourceAssignment.noteCodes, FACT_TABLE_NAME);
  } catch (err) {
    logger.error(err, 'Failed to apply primary key to fact table.');
    if ((err as Error).message.includes('could not create unique index')) {
      let error: FactTableValidationException | undefined;
      error = await identifyDuplicateFacts(connection, primaryKeyDef);
      if (error) throw error;
      error = await identifyIncompleteFacts(connection, primaryKeyDef);
      if (error) throw error;
    } else if ((err as Error).message.includes('contains null values')) {
      const error = await identifyIncompleteFacts(connection, primaryKeyDef);
      if (error) throw error;
      throw new FactTableValidationException(
        'Incomplete facts found in fact table.',
        FactTableValidationExceptionType.IncompleteFact,
        400
      );
    }
    throw new FactTableValidationException(
      'Something went wrong trying to add primary key to fact table',
      FactTableValidationExceptionType.UnknownError,
      500
    );
  } finally {
    connection.release();
  }
};

async function validateNoteCodesColumn(
  connection: PoolClient,
  noteCodeColumn: SourceAssignmentDTO | null,
  factTableName: string
): Promise<void> {
  let notesCodes: QueryResult<{ codes: string }>;
  try {
    const findNoteCodesQuery = pgformat(
      'SELECT DISTINCT %I as codes FROM %I WHERE %I IS NOT NULL;',
      noteCodeColumn?.column_name,
      factTableName,
      noteCodeColumn?.column_name
    );
    notesCodes = await connection.query(findNoteCodesQuery);
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
  for (const noteCode of notesCodes.rows) {
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
    const columns: QueryResult<{ column_name: string }> = await connection.query(
      pgformat('SELECT column_name FROM information_schema.columns WHERE table_name = %L', factTableName)
    );
    const selectColumns = columns.rows
      .filter((row) => row.column_name != noteCodeColumn?.column_name)
      .map((col) => col.column_name);
    selectColumns.push('line_number');
    const brokeNoteCodeLinesQuery = pgformat(
      'SELECT %I FROM (SELECT row_number() OVER () as line_number, *, lower(%I) as codes FROM %I WHERE %L) LIMIT 500',
      selectColumns,
      noteCodeColumn?.column_name,
      factTableName,
      badCodesString
    );
    const brokeNoteCodeLines = await connection.query(brokeNoteCodeLinesQuery);
    const { headers, data } = tableDataToViewTable(brokeNoteCodeLines.rows);
    error.data = data;
    error.headers = headers;
  } catch (extractionErr) {
    logger.error(extractionErr, 'Failed to extract data from data table.');
  }
  throw error;
}

async function identifyIncompleteFacts(
  connection: PoolClient,
  primaryKeyDef: string[]
): Promise<FactTableValidationException | undefined> {
  const pkeyDef = primaryKeyDef.map((key) => pgformat('%I IS NULL', key));
  try {
    const incompleteFactQuery = pgformat(
      `SELECT * FROM (SELECT row_number() OVER () as line_number, * FROM fact_table) WHERE %s LIMIT 500;`,
      pkeyDef.join(' OR ')
    );
    const brokenFacts = await connection.query(incompleteFactQuery);
    if (brokenFacts.rows.length > 0) {
      logger.debug(`${brokenFacts.rows.length} incomplete facts found in fact table`);
      const { headers, data } = tableDataToViewTable(brokenFacts.rows);
      const err = new FactTableValidationException(
        'Incomplete facts found in the data table',
        FactTableValidationExceptionType.IncompleteFact,
        400
      );
      err.data = data;
      err.headers = headers;
      return err;
    }
  } catch (extractionErr) {
    logger.error(extractionErr, 'Failed to run query to identify incomplete facts.');
    return new FactTableValidationException(
      'Could not run the check for incomplete facts',
      FactTableValidationExceptionType.UnknownError,
      400
    );
  }
  return undefined;
}

async function identifyDuplicateFacts(
  connection: PoolClient,
  primaryKeyDef: string[]
): Promise<FactTableValidationException | undefined> {
  const pkeyDef = primaryKeyDef.map((key) => pgformat('%I', key));
  const duplicateFactQuery = pgformat(
    `
        SELECT * FROM (SELECT row_number() OVER () as line_number, * FROM fact_table)
        WHERE (%s) IN (
          SELECT %s FROM (
            SELECT %s, count(*) as fact_count FROM fact_table
            GROUP BY %s
          ) WHERE fact_count > 1
        ) LIMIT 500;`,
    pkeyDef.join(', '),
    pkeyDef.join(', '),
    pkeyDef.join(', '),
    pkeyDef.join(', ')
  );
  try {
    logger.debug(`Running query to find duplicates:\n${duplicateFactQuery}`);
    const brokenFacts = await connection.query(duplicateFactQuery);
    if (brokenFacts.rows.length > 0) {
      const { headers, data } = tableDataToViewTable(brokenFacts.rows);
      const err = new FactTableValidationException(
        'Duplicate facts found in the data table',
        FactTableValidationExceptionType.DuplicateFact,
        400
      );
      err.data = data;
      err.headers = headers;
      return err;
    }
  } catch (extractionErr) {
    logger.error(extractionErr, 'Failed to run query to identify duplicate facts.');
    return new FactTableValidationException(
      'Could not run the check for duplicate facts',
      FactTableValidationExceptionType.UnknownError,
      400
    );
  }
  return undefined;
}
