import { Database } from 'duckdb-async';
import { format as pgformat } from '@scaleleap/pg-format';

import { logger as parentLogger } from '../utils/logger';
import { appConfig } from '../config';

const logger = parentLogger.child({ module: 'DuckDB' });

const config = appConfig();

export const DUCKDB_WRITE_TIMEOUT = config.duckdb.writeTimeOut;

export const safelyCloseDuckDb = async (quack: Database): Promise<void> => {
  await quack.exec(`CHECKPOINT;`);
  await quack.close();
  return new Promise((f) => setTimeout(f, DUCKDB_WRITE_TIMEOUT));
};

export const duckdb = async (cubeFile = ':memory:'): Promise<Database> => {
  const { threads, memory } = config.duckdb;

  logger.debug(`Creating DuckDB instance with ${threads} thread(s) and ${memory} memory limit.`);

  const duckdb = await Database.create(':memory:');

  await duckdb.exec(pgformat('SET threads = %L;', threads));
  await duckdb.exec(pgformat('SET memory_limit = %L;', memory));
  await duckdb.exec("SET default_block_size = '16384';");
  await duckdb.exec("SET temp_directory='/tmp/duckdb_temp';");
  await duckdb.exec('SET preserve_insertion_order=false;');

  if (cubeFile !== ':memory:') {
    await duckdb.exec(pgformat('ATTACH %L AS cube_file;', cubeFile));
    await duckdb.exec('USE cube_file;');
  }

  return duckdb;
};

export const linkToPostgresSchema = async (quack: Database, schema: 'lookup_tables' | 'data_tables'): Promise<void> => {
  logger.debug(`Linking to postgres ${schema} schema`);
  await quack.exec(`LOAD 'postgres';`);
  await quack.exec(`
    CREATE OR REPLACE SECRET (
      TYPE postgres,
      HOST '${config.database.host}',
      PORT ${config.database.port},
      DATABASE '${config.database.database}',
      USER '${config.database.username}',
      PASSWORD '${config.database.password}'
    );
  `);
  await quack.exec(pgformat(`ATTACH '' AS %I (TYPE postgres, SCHEMA %L);`, `${schema}_db`, schema));
  await quack.exec(pgformat(`USE %I;`, `${schema}_db`));
};
