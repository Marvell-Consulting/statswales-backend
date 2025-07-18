import 'dotenv/config';
import 'reflect-metadata';
import { DataSource, DataSourceOptions } from 'typeorm';

import { appConfig } from '../config';

const config = appConfig().database;

/**
 * Data source configuration for TypeORM.
 *
 * This is the cube database configuration. For the application database, see data-source.ts.
 */
const dataSourceOpts: DataSourceOptions = {
  type: 'postgres',
  host: config.host,
  port: config.port,
  username: config.username,
  password: config.password,
  database: config.database,
  ssl: config.ssl,
  synchronize: false,
  logging: false,
  entities: [],
  migrations: [],
  applicationName: 'sw3-backend-cube',
  extra: {
    max: config.poolSize,
    maxUses: config.maxUses,
    idleTimeoutMillis: config.idleTimeoutMs,
    connectionTimeoutMillis: config.connectionTimeoutMs
  }
};

export const cubeDataSource = new DataSource(dataSourceOpts);
