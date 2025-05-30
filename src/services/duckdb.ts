import { Database } from 'duckdb-async';
import { format as pgformat } from '@scaleleap/pg-format';

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

export const linkToPostgres = async (quack: Database, revisionId: string, recreate: boolean) => {
  await quack.exec(`LOAD 'postgres';`);
  const secret = `CREATE OR REPLACE SECRET (
           TYPE postgres,
           HOST '${config.database.host}',
           PORT ${config.database.port},
           DATABASE '${config.database.database}',
           USER '${config.database.username}',
           PASSWORD '${config.database.password}'
       );`;
  logger.debug(`Creating secret for postgres connection: ${secret}`);
  await quack.exec(secret);
  if (recreate) {
    logger.debug(`Recreating empty schema for revision ${revisionId}`);
    await quack.exec(`ATTACH '' AS postgres_db (TYPE postgres);`);
    await quack.exec(`USE postgres_db;`);
    await quack.exec(pgformat(`DROP SCHEMA IF EXISTS %I CASCADE;`, revisionId));
    await quack.exec(pgformat(`CREATE SCHEMA %I;`, revisionId));
    await quack.exec('USE memory;');
    await quack.exec('DETACH postgres_db;');
  }
  await quack.exec(`ATTACH '' AS data_tables_db (TYPE postgres, SCHEMA data_tables);`);
  await quack.exec(pgformat(`ATTACH '' AS postgres_db (TYPE postgres, SCHEMA %I);`, revisionId));
  await quack.exec(`USE postgres_db;`);
};

export const linkToPostgresDataTables = async (quack: Database) => {
  logger.debug('Linking to postgres data tables schema');
  await quack.exec(`LOAD 'postgres';`);
  await quack.exec(`CREATE OR REPLACE SECRET (
           TYPE postgres,
           HOST '${config.database.host}',
           PORT ${config.database.port},
           DATABASE '${config.database.database}',
           USER '${config.database.username}',
           PASSWORD '${config.database.password}'
       );`);
  await quack.exec(`ATTACH '' AS data_tables_db (TYPE postgres, SCHEMA data_tables);`);
  await quack.exec(`USE data_tables_db;`);
};
