import { DuckDBConnection, DuckDBInstance } from '@duckdb/node-api';
import { format as pgformat } from '@scaleleap/pg-format';

import { logger as parentLogger } from '../utils/logger';
import { config } from '../config';
import path from 'node:path';
import os from 'node:os';

const logger = parentLogger.child({ module: 'DuckDB' });

let duckDBInstance: DuckDBInstance | undefined;

export const duckdb = async (cubeFile = ':memory:'): Promise<DuckDBConnection> => {
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
    try {
      await linkToPostgresSchema(setupConn, 'data_tables');
      await linkToPostgresSchema(setupConn, 'lookup_tables');
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

  if (cubeFile !== ':memory:') {
    await duckdb.run(pgformat('ATTACH %L AS cube_file;', cubeFile));
    await duckdb.run('USE cube_file;');
  }
  logger.debug('Successfully connected to duckDB');
  return duckdb;
};

async function linkToPostgresSchema(quack: DuckDBConnection, schema: 'lookup_tables' | 'data_tables'): Promise<void> {
  logger.debug(`Linking to postgres ${schema} schema`);
  await quack.run(`LOAD 'postgres';`);
  await quack.run(`
    CREATE OR REPLACE SECRET (
      TYPE postgres,
      HOST '${config.database.host}',
      PORT ${config.database.port},
      DATABASE '${config.database.database}',
      USER '${config.database.username}',
      PASSWORD '${config.database.password}'
    );
  `);
  await quack.run(pgformat(`ATTACH OR REPLACE '' AS %I (TYPE postgres, SCHEMA %L);`, `${schema}_db`, schema));
}
