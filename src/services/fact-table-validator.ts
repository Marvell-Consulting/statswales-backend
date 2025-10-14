import { QueryRunner } from 'typeorm';
import { format as pgformat } from '@scaleleap/pg-format';

import { ValidatedSourceAssignment } from './dimension-processor';
import { Dataset } from '../entities/dataset/dataset';
import { FactTableColumn } from '../entities/dataset/fact-table-column';
import { FactTableColumnType } from '../enums/fact-table-column-type';
import { logger } from '../utils/logger';
import { FACT_TABLE_NAME } from './cube-builder';
import { FactTableValidationException } from '../exceptions/fact-table-validation-exception';
import { FactTableValidationExceptionType } from '../enums/fact-table-validation-exception-type';
import { SourceAssignmentDTO } from '../dtos/source-assignment-dto';
import { tableDataToViewTable } from '../utils/table-data-to-view-table';
import { dbManager } from '../db/database-manager';
import { NoteCodes } from '../enums/note-code';

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

  logger.debug(`Validating fact table for revision ${revision.id}`);

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
  const cubeDB = dbManager.getCubeDataSource().createQueryRunner();

  try {
    await cubeDB.query(pgformat(`SET search_path TO %I;`, revision.id));
  } catch (error) {
    logger.error(error, 'Unable to connect to postgres schema for revision.');
    cubeDB.release();
    throw new FactTableValidationException(
      'Unable to find data on revision cube in database',
      FactTableValidationExceptionType.UnknownError,
      500
    );
  }

  try {
    logger.debug(`Dropping primary key if it exists on fact_table`);
    const dropPKQuery = pgformat(
      `ALTER TABLE %I DROP CONSTRAINT IF EXISTS %I;`,
      FACT_TABLE_NAME,
      `${FACT_TABLE_NAME}_pkey`
    );
    await cubeDB.query(dropPKQuery);
    logger.debug(`Adding primary key to fact_table with columns: ${primaryKeyDef.join(', ')}`);
    const pkQuery = pgformat('ALTER TABLE %I ADD PRIMARY KEY (%I)', FACT_TABLE_NAME, primaryKeyDef);
    await cubeDB.query(pkQuery);
  } catch (err) {
    cubeDB.release();
    logger.error(err, 'Failed to apply primary key to fact table.');
    if ((err as Error).message.includes('could not create unique index')) {
      let error: FactTableValidationException | undefined;
      error = await identifyDuplicateFacts(cubeDB, primaryKeyDef);
      if (error) throw error;
      error = await identifyIncompleteFacts(cubeDB, primaryKeyDef);
      if (error) throw error;
    } else if ((err as Error).message.includes('contains null values')) {
      const error = await identifyIncompleteFacts(cubeDB, primaryKeyDef);
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
  }

  try {
    await validateNoteCodesColumn(cubeDB, validatedSourceAssignment.noteCodes, revision.id, FACT_TABLE_NAME);
  } catch (err) {
    logger.error(err, 'Note code validation failed.');
    throw err;
  } finally {
    cubeDB.release();
  }
};

async function validateNoteCodesColumn(
  cubeDB: QueryRunner,
  noteCodeColumn: SourceAssignmentDTO | null,
  revisionId: string,
  factTableName: string
): Promise<void> {
  let notesCodes: { codes: string }[];
  try {
    const findNoteCodesQuery = pgformat(
      'SELECT DISTINCT %I as codes FROM %I.%I WHERE %I IS NOT NULL;',
      noteCodeColumn?.column_name,
      revisionId,
      factTableName,
      noteCodeColumn?.column_name
    );
    notesCodes = await cubeDB.query(findNoteCodesQuery);
  } catch (error) {
    logger.error(error, 'Failed to extract or validate validate note codes');
    throw new FactTableValidationException(
      'Failed to extract note codes column',
      FactTableValidationExceptionType.NoNoteCodes,
      500
    );
  }
  const validCodes = NoteCodes.map((noteCode) => noteCode.code);

  const badCodes: string[] = notesCodes
    .flatMap((noteCode: { codes: string }) => {
      return noteCode.codes
        .replace(/\s/g, '') // strip all whitespace
        .toLowerCase()
        .split(','); // split into individual notecodes for validation
    })
    .reduce((bad: string[], code: string) => {
      if (code && !validCodes.includes(code)) {
        logger.error(`Value "${code}" is not a valid note code`);
        bad.push(code);
      }
      return bad;
    }, []);

  if (badCodes.length === 0) {
    return;
  }

  const error = new FactTableValidationException(
    'Bad note codes found in note codes column',
    FactTableValidationExceptionType.BadNoteCodes,
    400
  );
  try {
    const badCodesString = badCodes
      .map((code) => pgformat(`LOWER(%I) LIKE %L`, noteCodeColumn?.column_name, `%${code.toLowerCase()}%`))
      .join(' or ');
    const columnNamesQuery = pgformat(
      'SELECT column_name FROM information_schema.columns WHERE table_schema = %L AND table_name = %L',
      revisionId,
      factTableName
    );
    const columns: { column_name: string }[] = await cubeDB.query(columnNamesQuery);
    const selectColumns = columns.map((col) => col.column_name);
    const brokeNoteCodeLinesQuery = pgformat(
      'SELECT line_number,%I FROM (SELECT row_number() OVER () as line_number, %I FROM %I.%I) WHERE %s LIMIT 500',
      selectColumns,
      selectColumns,
      revisionId,
      factTableName,
      badCodesString
    );
    const brokeNoteCodeLines = await cubeDB.query(brokeNoteCodeLinesQuery);
    const { headers, data } = tableDataToViewTable(brokeNoteCodeLines);
    error.data = data;
    error.headers = headers;
  } catch (extractionErr) {
    logger.error(extractionErr, 'Failed to extract data from data table.');
  }
  throw error;
}

async function identifyIncompleteFacts(
  cubeDB: QueryRunner,
  primaryKeyDef: string[]
): Promise<FactTableValidationException | undefined> {
  const pkeyDef = primaryKeyDef.map((key) => pgformat('%I IS NULL', key));
  try {
    const incompleteFactQuery = pgformat(
      `SELECT * FROM (SELECT row_number() OVER () as line_number, * FROM fact_table) WHERE %s LIMIT 500;`,
      pkeyDef.join(' OR ')
    );
    const brokenFacts = await cubeDB.query(incompleteFactQuery);
    if (brokenFacts.length > 0) {
      logger.debug(`${brokenFacts.length} incomplete facts found in fact table`);
      const { headers, data } = tableDataToViewTable(brokenFacts);
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
  cubeDB: QueryRunner,
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
    logger.debug(`Running query to find duplicate facts...`);
    logger.trace(`duplicate fact query: ${duplicateFactQuery}`);
    const brokenFacts = await cubeDB.query(duplicateFactQuery);
    if (brokenFacts.length > 0) {
      const { headers, data } = tableDataToViewTable(brokenFacts);
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
