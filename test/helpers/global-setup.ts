import { mkdirSync, writeFileSync } from 'node:fs';
import { Client } from 'pg';

interface GlobalConfig {
  maxWorkers: number;
}

const BASE_DB = 'statswales-backend-test';

// Read connection params with the same defaults as jest-setup.ts
const host = process.env.TEST_DB_HOST ?? 'localhost';
const port = parseInt(process.env.TEST_DB_PORT ?? '5433', 10);
const user = process.env.TEST_DB_USERNAME ?? 'postgres';
const password = process.env.TEST_DB_PASSWORD ?? 'postgres';

export default async function globalSetup(globalConfig: GlobalConfig): Promise<void> {
  const workerCount = globalConfig.maxWorkers as number;

  mkdirSync('coverage', { recursive: true });
  writeFileSync('coverage/.jest-workers', String(workerCount));

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
