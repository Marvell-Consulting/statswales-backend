import 'dotenv/config';
import 'reflect-metadata';
import { DataSource, DataSourceOptions } from 'typeorm';

import { config } from '../config';

/**
 * Data source for migrations and seeders. Identical to the app data source
 * but without statement_timeout, so long-running DDL and bulk inserts are
 * not killed.
 */
const dataSourceOpts: DataSourceOptions = {
  type: 'postgres',
  host: config.database.host,
  port: config.database.port,
  username: config.database.username,
  password: config.database.password,
  database: config.database.database,
  ssl: config.database.ssl,
  synchronize: config.database.synchronize,
  logging: false,
  entities: [`${__dirname}/../entities/**/*.{ts,js}`],
  migrations: [`${__dirname}/../migrations/*.{ts,js}`],
  applicationName: 'sw3-backend-migrations',
  extra: {
    max: config.database.poolSize,
    maxUses: config.database.maxUses,
    idleTimeoutMillis: config.database.idleTimeoutMs,
    connectionTimeoutMillis: config.database.connectionTimeoutMs
  }
};

export const dataSource = new DataSource(dataSourceOpts);
