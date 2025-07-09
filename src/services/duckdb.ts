import { performance } from 'node:perf_hooks';

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

export const linkToExistingSchema = async (quack: Database, schemaID: string): Promise<void> => {
  const start = performance.now();
  await quack.exec(`LOAD 'postgres';`);

  const secret = `CREATE OR REPLACE SECRET (
    TYPE postgres,
    HOST '${config.database.host}',
    PORT ${config.database.port},
    DATABASE '${config.database.database}',
    USER '${config.database.username}',
    PASSWORD '${config.database.password}'
  );`;

  await quack.exec(secret);

  logger.debug(`Recreating empty schema for revision ${schemaID}`);
  await quack.exec(`ATTACH '' AS data_tables_db (TYPE postgres, SCHEMA data_tables);`);
  await quack.exec(pgformat(`ATTACH '' AS postgres_db (TYPE postgres, SCHEMA %I);`, schemaID));
  await quack.exec(`USE postgres_db;`);
  logger.debug('Linking to postgres schema successful');
  const end = performance.now();
  const timing = Math.round(end - start);
  logger.debug(`linkToPostgres: ${timing}ms`);
};

export const createNewBuildSchema = async (quack: Database): Promise<string> => {
  const start = performance.now();
  const buildID = crypto.randomUUID();
  await quack.exec(`LOAD 'postgres';`);

  const secret = `CREATE OR REPLACE SECRET (
    TYPE postgres,
    HOST '${config.database.host}',
    PORT ${config.database.port},
    DATABASE '${config.database.database}',
    USER '${config.database.username}',
    PASSWORD '${config.database.password}'
  );`;

  await quack.exec(secret);

  logger.debug(`Creating empty schema for build ID ${buildID}`);
  await quack.exec(`ATTACH '' AS postgres_db (TYPE postgres);`);
  await quack.exec(`USE postgres_db;`);
  await quack.exec(pgformat(`CREATE SCHEMA %I;`, buildID));
  await quack.exec('USE memory;');
  await quack.exec('DETACH postgres_db;');
  await quack.exec(`ATTACH '' AS data_tables_db (TYPE postgres, SCHEMA data_tables);`);
  await quack.exec(pgformat(`ATTACH '' AS postgres_db (TYPE postgres, SCHEMA %I);`, buildID));
  await quack.exec(`USE postgres_db;`);
  logger.debug('Linking to postgres schema successful');
  const end = performance.now();
  const timing = Math.round(end - start);
  logger.debug(`linkToPostgres: ${timing}ms`);
  return buildID;
};

export const replaceSchema = async (quack: Database, buildID: string, revisionId: string): Promise<void> => {
  logger.info(`Replacing schema for revision ${revisionId} using build ID ${buildID}`);
  await quack.exec(pgformat(`DROP SCHEMA IF EXISTS %I CASCADE;`, revisionId));
  const alterSchemaQuery = pgformat('ALTER SCHEMA %I RENAME TO %I;', buildID, revisionId);
  await quack.exec(pgformat(`CALL postgres_execute('postgres_db', %L);`, alterSchemaQuery));
  await quack.exec('USE memory;');
  await quack.exec('DETACH postgres_db;');
  await quack.exec(pgformat(`ATTACH '' AS postgres_db (TYPE postgres, SCHEMA %I);`, revisionId));
  await quack.exec(`USE postgres_db;`);
};

export const dropBuildSchema = async (quack: Database, buildID: string): Promise<void> => {
  logger.info(`Droping schema for build ${buildID}`);
  await quack.exec(pgformat(`DROP SCHEMA IF EXISTS %I CASCADE;`, buildID));
  await quack.exec('USE memory;');
  await quack.exec('DETACH postgres_db;');
};

export const linkToPostgresDataTables = async (quack: Database): Promise<void> => {
  logger.debug('Linking to postgres data tables schema');
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
  await quack.exec(`ATTACH '' AS data_tables_db (TYPE postgres, SCHEMA data_tables);`);
  await quack.exec(`USE data_tables_db;`);
};

export const linkToPostgresLookupTables = async (quack: Database): Promise<void> => {
  logger.debug('Linking to postgres lookup tables schema');
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
  await quack.exec(`ATTACH '' AS lookup_table_db (TYPE postgres, SCHEMA lookup_tables);`);
  await quack.exec(`USE lookup_table_db;`);
};
