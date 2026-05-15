import 'dotenv/config';
import 'reflect-metadata';
import { DataSource, DataSourceOptions } from 'typeorm';

import { config } from '../config';

const dbConfig = config.database;

/**
 * Consumer DataSource — backs the unauthenticated public API (`/v1/*`, `/v2/*`) and the read-side
 * shared repos (PublishedDataset, PublishedRevision, PublishedTopic, QueryStore, SearchLog).
 * Isolated from the publisher pool so consumer-side traffic spikes can't starve editor requests.
 * For the publisher pool, see publisher-source.ts. For the cube DB, see cube-source.ts.
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
  applicationName: 'sw3-backend-consumer',
  extra: {
    max: dbConfig.consumerPoolSize ?? dbConfig.poolSize,
    maxUses: dbConfig.maxUses,
    idleTimeoutMillis: dbConfig.idleTimeoutMs,
    connectionTimeoutMillis: dbConfig.connectionTimeoutMs
  }
};

export const consumerDataSource = new DataSource(dataSourceOpts);
