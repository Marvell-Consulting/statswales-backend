import 'dotenv/config';
import 'reflect-metadata';
import { DataSource, DataSourceOptions } from 'typeorm';

import { config } from '../config';

const dbConfig = config.database;

/**
 * Publisher DataSource — backs every authenticated editor route (`/dataset`, `/provider`, `/topic`,
 * `/admin`, …) and their repos. Isolated from the consumer pool so a saturated consumer pool cannot
 * starve publisher requests inside the same backend process. See SW-1265 / SW-1252.
 *
 * This is the source the migration CLI uses (see package.json migration scripts) — both pools point
 * at the same Postgres so migrations can run via either, but pinning to publisher keeps schema
 * changes off the consumer connection pool that's serving public traffic.
 */
const dataSourceOpts: DataSourceOptions = {
  type: 'postgres',
  host: dbConfig.host,
  port: dbConfig.port,
  username: dbConfig.username,
  password: dbConfig.password,
  database: dbConfig.database,
  ssl: dbConfig.ssl,
  synchronize: dbConfig.synchronize,
  logging: false,
  entities: [`${__dirname}/../entities/**/*.{ts,js}`],
  migrations: [`${__dirname}/../migrations/*.{ts,js}`],
  applicationName: 'sw3-backend-publisher',
  extra: {
    max: dbConfig.publisherPoolSize ?? dbConfig.poolSize,
    maxUses: dbConfig.maxUses,
    idleTimeoutMillis: dbConfig.idleTimeoutMs,
    connectionTimeoutMillis: dbConfig.connectionTimeoutMs
  }
};

export const publisherDataSource = new DataSource(dataSourceOpts);
