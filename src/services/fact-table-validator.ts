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
    let error = err as FactTableValidationException;
    logger.error(error, 'Failed to load data table into fact table');
    // Attempt to augment the error with details of where the errors in the data table are
    if (error.type === FactTableValidationExceptionType.EmptyValue) {
      error = await identifyIncompleteFacts(connection, primaryKeyDef, error);
    } else if (error.type === FactTableValidationExceptionType.DuplicateFact) {
      error = await identifyDuplicateFacts(connection, primaryKeyDef, error);
    }
    throw error;
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
    notesCodes = await connection.query(`
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
  primaryKeyDef: string[],
  error: FactTableValidationException
): Promise<FactTableValidationException> {
  try {
    const brokenFacts = await connection.query(
      `SELECT * FROM (SELECT row_number() OVER () as line_number, * FROM fact_table) WHERE ${primaryKeyDef.join('IS NULL OR ')} IS NULL LIMIT 500;`
    );
    const { headers, data } = tableDataToViewTable(brokenFacts.rows);
    error.data = data;
    error.headers = headers;
  } catch (extractionErr) {
    logger.error(extractionErr, 'Failed to extract data from data table.');
  }
  return error;
}

async function identifyDuplicateFacts(
  connection: PoolClient,
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

    const brokenFacts = await connection.query(duplicateQuery);
    const { headers, data } = tableDataToViewTable(brokenFacts.rows);
    error.data = data;
    error.headers = headers;
  } catch (extractionErr) {
    logger.error(extractionErr, 'Failed to extract data from data table.');
  }
  return error;
}
