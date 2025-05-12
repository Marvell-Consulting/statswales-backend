import { Database } from 'duckdb-async';
import { format as sqlFormat, escape } from 'sqlstring';

import { logger as parentLogger } from '../utils/logger';
import { appConfig } from '../config';

const logger = parentLogger.child({ module: 'DuckDB' });

const config = appConfig();

export const DUCKDB_WRITE_TIMEOUT = config.duckdb.writeTimeOut;

export const safelyCloseDuckDb = async (quack: Database) => {
  await quack.exec(`CHECKPOINT;`);
  await quack.close();
  return new Promise((f) => setTimeout(f, DUCKDB_WRITE_TIMEOUT));
};

export const duckdb = async (cubeFile = ':memory:') => {
  const { threads, memory } = config.duckdb;

  logger.debug(`Creating DuckDB instance with ${threads} thread(s) and ${memory} memory limit.`);

  const duckdb = await Database.create(cubeFile);

  await duckdb.exec(`SET threads = ${threads};`);
  await duckdb.exec(`SET memory_limit = '${memory}';`);
  await duckdb.exec("SET default_block_size = '16384';");
  await duckdb.exec("SET temp_directory='/tmp/duckdb_temp';");
  await duckdb.exec('SET preserve_insertion_order=false;');

  return duckdb;
};

export const duckDBFormat = (sql: string, args?: object | any[], stringifyObjects?: boolean, timeZone?: string) => {
  return sqlFormat(sql, args, stringifyObjects, timeZone).replaceAll('`', '"');
};

export const duckDBEscape = (value: string): string => {
  return escape(value).replaceAll('`', '"');
};
