import { format as pgformat } from '@scaleleap/pg-format';
import { FACT_TABLE_NAME, VALIDATION_TABLE_NAME } from '../services/cube-builder';
import { logger } from './logger';
import { runQueryBlock } from './run-query-block';

export async function createPostgresValidationSchema(
  schemaId: string,
  revisionId: string,
  factTableColumnName: string,
  type: 'lookup' | 'reference',
  lookupTableName?: string
): Promise<void> {
  const statements = [
    'BEGIN TRANSACTION;',
    pgformat('CREATE SCHEMA IF NOT EXISTS %I;', schemaId),
    pgformat(
      'CREATE TABLE %I.%I AS SELECT * FROM %I.%I;',
      schemaId,
      VALIDATION_TABLE_NAME,
      revisionId,
      VALIDATION_TABLE_NAME
    ),
    pgformat('CREATE INDEX ON %I.%I (reference);', schemaId, VALIDATION_TABLE_NAME),
    pgformat('CREATE INDEX ON %I.%I (fact_table_column);', schemaId, VALIDATION_TABLE_NAME),
    pgformat(
      'CREATE TABLE %I.%I AS SELECT reference AS %I FROM %I.%I WHERE fact_table_column = %L;',
      schemaId,
      FACT_TABLE_NAME,
      factTableColumnName,
      schemaId,
      VALIDATION_TABLE_NAME,
      factTableColumnName
    ),
    pgformat('CREATE INDEX ON %I.%I (%I);', schemaId, FACT_TABLE_NAME, factTableColumnName),
    type === 'lookup'
      ? pgformat(
          'CREATE TABLE %I.%I AS SELECT * FROM %I.%I;',
          schemaId,
          'lookup_table',
          'lookup_tables',
          lookupTableName
        )
      : '',
    'COMMIT;'
  ];
  logger.debug('Attempting to create mock cube for validation and lookup table processing');
  return runQueryBlock(statements);
}

export async function cleanUpPostgresValidationSchema(schemaId: string, lookupTableId: string): Promise<void> {
  const statements = [
    'BEGIN TRANSACTION;',
    pgformat('DROP SCHEMA IF EXISTS %I CASCADE;', schemaId),
    pgformat('DROP TABLE IF EXISTS %I.%I;', 'lookup_tables', `${lookupTableId}_tmp`),
    'COMMIT;'
  ];
  logger.debug('Dropping mock cube schema from database');
  return runQueryBlock(statements);
}

export async function saveValidatedLookupTableToDatabase(mockCubeId: string, lookupTableId: string): Promise<void> {
  const statements = [
    'BEGIN TRANSACTION;',
    pgformat('CREATE TABLE %I.%I AS SELECT * FROM %I.%I;', 'lookup_tables', lookupTableId, mockCubeId, lookupTableId),
    pgformat('DROP SCHEMA IF EXISTS %I CASCADE;', mockCubeId),
    pgformat('DROP TABLE IF EXISTS %I.%I;', 'lookup_tables', `${lookupTableId}_tmp`),
    'COMMIT;'
  ];
  logger.debug('Copying validated lookup table to database and cleaning up mock cube');
  return runQueryBlock(statements);
}
