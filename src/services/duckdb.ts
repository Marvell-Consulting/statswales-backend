import { DuckDBConnection, DuckDBInstance } from '@duckdb/node-api';
import { format as pgformat } from '@scaleleap/pg-format';
import { Mutex, Semaphore } from 'async-mutex';

import { logger as parentLogger } from '../utils/logger';
import { config } from '../config';
import path from 'node:path';
import os from 'node:os';
import { PostgresSchemas } from '../enums/postgres-schemas';

export enum DuckDBDatabases {
  CubeDb = 'cube_db',
  LookupTables = 'lookup_tables_db',
  DataTables = 'data_tables_db',
  Memory = 'memory'
}

const logger = parentLogger.child({ module: 'DuckDB' });

let duckDBInstance: DuckDBInstance | undefined;
const initMutex = new Mutex();

async function ensureInstance(): Promise<DuckDBInstance> {
  return initMutex.runExclusive(async () => {
    if (duckDBInstance) {
      return duckDBInstance;
    }

    const { threads, memory } = config.duckdb;
    logger.debug(
      `Creating DuckDB instance with ${threads} thread(s), ${memory} memory limit, and attaching postgres...`
    );
    const instance = await DuckDBInstance.create(':memory:', {
      threads: threads.toString(),
      memory_limit: memory,
      default_block_size: '16384',
      temp_directory: path.resolve(os.tmpdir(), 'duckdb_temp'),
      preserve_insertion_order: 'false'
    });

    const setupConn = await instance.connect();
    try {
      await setupConn.run(`LOAD 'postgres';`);
      await setupConn.run(
        pgformat(
          `CREATE OR REPLACE SECRET (TYPE postgres, HOST %L, PORT %s, DATABASE %L, USER %L, PASSWORD %L);`,
          config.database.host,
          config.database.port,
          config.database.database,
          config.database.username,
          config.database.password
        )
      );
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

    duckDBInstance = instance;
    logger.debug('DuckDB instance ready');
    return instance;
  });
}

const duckdb = async (): Promise<DuckDBConnection> => {
  const instance = await ensureInstance();
  return instance.connect();
};

export interface DuckDBHandle {
  duckdb: DuckDBConnection;
  duckRelease: () => void;
}

const semaphore = new Semaphore(config.duckdb.maxConcurrency);

export async function acquireDuckDB(): Promise<DuckDBHandle> {
  logger.debug(
    `Acquiring DuckDB connection (${config.duckdb.maxConcurrency - semaphore.getValue()}/${config.duckdb.maxConcurrency} in use)`
  );
  const [, release] = await semaphore.acquire();
  let conn: DuckDBConnection;
  try {
    conn = await duckdb();
  } catch (err) {
    release();
    throw err;
  }
  let released = false;
  return {
    duckdb: conn,
    duckRelease(): void {
      if (released) return;
      released = true;
      conn.disconnectSync();
      release();
      logger.debug(
        `Released DuckDB connection (${config.duckdb.maxConcurrency - semaphore.getValue()}/${config.duckdb.maxConcurrency} in use)`
      );
    }
  };
}
