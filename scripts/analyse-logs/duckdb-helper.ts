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

export async function query(sql: string, ...params: unknown[]): Promise<Record<string, unknown>[]> {
  if (!connection) throw new Error('DuckDB not initialised — call initDuckDB() first');
  const prepared = await connection.prepare(sql);
  for (let i = 0; i < params.length; i++) {
    prepared.bindVarchar(i + 1, String(params[i]));
  }
  const result = await prepared.runAndReadAll();
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
