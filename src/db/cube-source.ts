import 'dotenv/config';
import 'reflect-metadata';
import { DataSource, DataSourceOptions } from 'typeorm';

import { config } from '../config';

const dbConfig = config.database;

/**
 * Data source configuration for TypeORM.
 *
 * This is the cube database configuration. For the application database, see data-source.ts.
 */
const dataSourceOpts: DataSourceOptions = {
  type: 'postgres',
  host: dbConfig.host,
  port: dbConfig.port,
  username: dbConfig.username,
  password: dbConfig.password,
  database: dbConfig.database,
  ssl: dbConfig.ssl,
  synchronize: false,
  logging: false,
  entities: [],
  migrations: [],
  applicationName: 'sw3-backend-cube',
  extra: {
    max: dbConfig.poolSize,
    maxUses: dbConfig.maxUses,
    idleTimeoutMillis: dbConfig.idleTimeoutMs,
    connectionTimeoutMillis: dbConfig.connectionTimeoutMs
  }
};

export const cubeDataSource = new DataSource(dataSourceOpts);
