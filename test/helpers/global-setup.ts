import { mkdirSync, writeFileSync } from 'node:fs';
import { Client } from 'pg';
import { DuckDBInstance } from '@duckdb/node-api';

interface GlobalConfig {
  maxWorkers: number;
}

const BASE_DB = 'statswales-backend-test';

// Pre-install the DuckDB `postgres` extension once, in this single setup process, before any
// Jest workers spawn. The app only ever calls `LOAD 'postgres'` (src/services/duckdb.ts), which
// otherwise triggers a per-worker autoinstall: every worker races to download the extension into
// the shared ~/.duckdb cache, and a network blip or concurrent partial write surfaces as
// `IO Error: Extension "...postgres_scanner.duckdb_extension" not found`. Installing once here
// populates that cache so each worker's LOAD is a local, offline operation.
async function installPostgresExtension(): Promise<void> {
  const attempts = 3;
  for (let attempt = 1; attempt <= attempts; attempt++) {
    const instance = await DuckDBInstance.create(':memory:');
    const conn = await instance.connect();
    try {
      await conn.run(`INSTALL postgres;`);
      return;
    } catch (err) {
      if (attempt === attempts) {
        throw new Error(`Failed to install DuckDB postgres extension after ${attempts} attempts: ${err}`);
      }
      await new Promise((resolve) => setTimeout(resolve, 1000 * attempt));
    } finally {
      conn.disconnectSync();
      instance.closeSync();
    }
  }
}

// Read connection params with the same defaults as jest-setup.ts
const host = process.env.TEST_DB_HOST ?? 'localhost';
const port = parseInt(process.env.TEST_DB_PORT ?? '5433', 10);
const user = process.env.TEST_DB_USERNAME ?? 'postgres';
const password = process.env.TEST_DB_PASSWORD ?? 'postgres';

export default async function globalSetup(globalConfig: GlobalConfig): Promise<void> {
  const workerCount = globalConfig.maxWorkers as number;

  mkdirSync('coverage', { recursive: true });
  writeFileSync('coverage/.jest-workers', String(workerCount));

  await installPostgresExtension();

  const client = new Client({ host, port, user, password, database: 'postgres' });
  await client.connect();

  for (let i = 1; i <= workerCount; i++) {
    const dbName = `${BASE_DB}-${i}`;
    // Terminate any open connections left over from a previous aborted run
    await client.query(`SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1`, [dbName]);
    await client.query(`DROP DATABASE IF EXISTS "${dbName}"`);
    await client.query(`CREATE DATABASE "${dbName}"`);
    mkdirSync(`/tmp/statswales-test-worker-${i}`, { recursive: true });
  }

  await client.end();
}
