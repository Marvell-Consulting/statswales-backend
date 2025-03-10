import { Database } from 'duckdb-async';

import { logger as parentLogger } from '../utils/logger';
import { appConfig } from '../config';

const logger = parentLogger.child({ module: 'DataLakeService' });

const config = appConfig();

export const duckdb = async (cubeFile = ':memory:') => {
  logger.info(
    `Creating DuckDB instance with ${config.duckdb.threads} thread(s) and ${config.duckdb.memory} memory limit.`
  );
  const duckdb = await Database.create(cubeFile);
  await duckdb.exec(`SET threads = ${config.duckdb.threads};`);
  await duckdb.exec(`SET memory_limit = '${config.duckdb.memory}';`);
  return duckdb;
};
