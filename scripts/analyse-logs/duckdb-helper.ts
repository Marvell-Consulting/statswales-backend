import { DuckDBConnection, DuckDBInstance } from '@duckdb/node-api';

let instance: DuckDBInstance | undefined;
let connection: DuckDBConnection | undefined;

export async function initDuckDB(): Promise<DuckDBConnection> {
  instance = await DuckDBInstance.create(':memory:', {
    threads: '4',
    memory_limit: '512MB'
  });
  connection = await instance.connect();
  return connection;
}

export async function query(sql: string): Promise<Record<string, unknown>[]> {
  if (!connection) throw new Error('DuckDB not initialised — call initDuckDB() first');
  const result = await connection.runAndReadAll(sql);
  return result.getRowObjectsJson() as Record<string, unknown>[];
}

export async function run(sql: string): Promise<void> {
  if (!connection) throw new Error('DuckDB not initialised — call initDuckDB() first');
  await connection.run(sql);
}

export async function closeDuckDB(): Promise<void> {
  if (connection) {
    connection.disconnectSync();
    connection = undefined;
  }
  instance = undefined;
}
