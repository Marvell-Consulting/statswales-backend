import { readFileSync, rmSync } from 'node:fs';
import { Client } from 'pg';

const BASE_DB = 'statswales-backend-test';

const host = process.env.TEST_DB_HOST ?? 'localhost';
const port = parseInt(process.env.TEST_DB_PORT ?? '5433', 10);
const user = process.env.TEST_DB_USERNAME ?? 'postgres';
const password = process.env.TEST_DB_PASSWORD ?? 'postgres';

export default async function globalTeardown(): Promise<void> {
  let workerCount: number;
  try {
    workerCount = parseInt(readFileSync('coverage/.jest-workers', 'utf-8'), 10);
  } catch {
    workerCount = 1;
  }

  const client = new Client({ host, port, user, password, database: 'postgres' });
  await client.connect();

  for (let i = 1; i <= workerCount; i++) {
    const dbName = `${BASE_DB}-${i}`;
    await client.query(`SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1`, [dbName]);
    await client.query(`DROP DATABASE IF EXISTS "${dbName}"`);
    try {
      rmSync(`/tmp/statswales-test-worker-${i}`, { recursive: true, force: true });
    } catch {
      // best-effort cleanup
    }
  }

  await client.end();
}
