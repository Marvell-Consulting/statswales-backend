import { DuckDBConnection, DuckDBInstance } from '@duckdb/node-api';
import { format as pgformat } from '@scaleleap/pg-format';

import { logger as parentLogger } from '../utils/logger';
import { config } from '../config';
import path from 'node:path';
import os from 'node:os';

export enum DuckDBDatabases {
  CubeDb = 'cube_db',
  LookupTables = 'lookup_tables_db',
  DataTables = 'data_tables_db',
  Memory = 'memory'
}

enum PostgresSchemas {
  LookupTables = 'lookup_tables',
  DataTables = 'data_tables'
}

const logger = parentLogger.child({ module: 'DuckDB' });

let duckDBInstance: DuckDBInstance | undefined;

export const duckdb = async (): Promise<DuckDBConnection> => {
  const { threads, memory } = config.duckdb;

  if (!duckDBInstance) {
    logger.debug(`Creating DuckDB instance with ${threads} thread(s) and ${memory} memory limit.`);
    duckDBInstance = await DuckDBInstance.create(':memory:', {
      threads: threads.toString(),
      memory_limit: memory,
      default_block_size: '16384',
      temp_directory: path.resolve(os.tmpdir(), 'duckdb_temp'),
      preserve_insertion_order: 'false'
    });

    const setupConn = await duckDBInstance.connect();
    logger.debug('Establishing connections between duckdb and postgres...');
    try {
      await setupConn.run(`LOAD 'postgres';`);
      await setupConn.run(`
          CREATE OR REPLACE SECRET (
            TYPE postgres,
            HOST '${config.database.host}',
            PORT ${config.database.port},
            DATABASE '${config.database.database}',
            USER '${config.database.username}',
            PASSWORD '${config.database.password}'
          );`);
      await setupConn.run(pgformat("ATTACH OR REPLACE '' AS %I (TYPE postgres);", DuckDBDatabases.CubeDb));
      await setupConn.run(
        pgformat(
          `ATTACH OR REPLACE '' AS %I (TYPE postgres, SCHEMA %L);`,
          DuckDBDatabases.DataTables,
          PostgresSchemas.DataTables
        )
      );
      await setupConn.run(
        pgformat(
          `ATTACH OR REPLACE '' AS %I (TYPE postgres, SCHEMA %L);`,
          DuckDBDatabases.LookupTables,
          PostgresSchemas.LookupTables
        )
      );
    } catch (error) {
      logger.fatal(error, 'Something went wrong trying to setup DuckDB postgres connections');
      throw error;
    } finally {
      setupConn.disconnectSync();
    }
    logger.debug('Successfully set up duckDB instance');
  } else {
    logger.debug('Using existing duckdb instance');
  }

  const duckdb = await duckDBInstance.connect();
  logger.debug('Successfully connected to duckDB');
  return duckdb;
};
