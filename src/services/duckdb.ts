import { Database } from 'duckdb-async';

import { logger as parentLogger } from '../utils/logger';
import { appConfig } from '../config';

const logger = parentLogger.child({ module: 'DuckDB' });

const config = appConfig();

export const DUCKDB_WRITE_TIMEOUT = config.duckdb.writeTimeOut;

export const safelyCloseDuckDb = async (quack: Database) => {
  await quack.close();
  return new Promise((f) => setTimeout(f, DUCKDB_WRITE_TIMEOUT));
};

export const duckdb = async (cubeFile = ':memory:') => {
  logger.info(
    `Creating DuckDB instance with ${config.duckdb.threads} thread(s) and ${config.duckdb.memory} memory limit.`
  );
  const duckdb = await Database.create(cubeFile);
  await duckdb.exec(`SET threads = ${config.duckdb.threads};`);
  await duckdb.exec(`SET memory_limit = '${config.duckdb.memory}';`);
  await duckdb.exec("SET default_block_size = '16384';");
  await duckdb.exec("SET temp_directory='/tmp/duckdb_temp';");
  await duckdb.exec('SET preserve_insertion_order=false;');
  return duckdb;
};
