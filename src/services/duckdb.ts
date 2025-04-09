import { Database } from 'duckdb-async';

import { logger as parentLogger } from '../utils/logger';
import { appConfig } from '../config';

const logger = parentLogger.child({ module: 'DuckDB' });

const config = appConfig();

export const duckdb = async (cubeFile = ':memory:') => {
  logger.info(
    `Creating DuckDB instance with ${config.duckdb.threads} thread(s) and ${config.duckdb.memory} memory limit.`
  );
  const duckdb = await Database.create(cubeFile);
  await duckdb.exec(`SET threads = ${config.duckdb.threads};`);
  await duckdb.exec(`SET memory_limit = '${config.duckdb.memory}';`);
  await duckdb.exec("SET temp_directory='/tmp/duckdb_temp';");
  await duckdb.exec('SET preserve_insertion_order=false;');
  return duckdb;
};
