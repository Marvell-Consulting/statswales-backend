import 'dotenv/config';
import 'reflect-metadata';
import { DataSource, DataSourceOptions } from 'typeorm';

const { NODE_ENV, DB_HOST, DB_PORT, DB_USERNAME, DB_PASSWORD, DB_DATABASE, DB_SSL } = process.env;

const dataSourceOpts: DataSourceOptions = {
    type: 'postgres',
    host: DB_HOST,
    port: parseInt(DB_PORT || '5432', 10),
    username: DB_USERNAME,
    password: DB_PASSWORD,
    database: DB_DATABASE,
    ssl: DB_SSL !== 'false' ?? true,
    synchronize: false,
    logging: false,
    entities: [`${__dirname}/../entities/*.{ts,js}`],
    migrations: [`${__dirname}/../migrations/*.{ts,js}`]
};

const { TEST_DB_HOST, TEST_DB_PORT, TEST_DB_USERNAME, TEST_DB_PASSWORD, TEST_DB_DATABASE } = process.env;

const testDataSourceOpts: DataSourceOptions = {
    type: 'postgres',
    host: TEST_DB_HOST,
    port: parseInt(TEST_DB_PORT || '5433', 10),
    username: TEST_DB_USERNAME,
    password: TEST_DB_PASSWORD,
    database: TEST_DB_DATABASE,
    ssl: false,
    synchronize: true, // auto-sync test db schema
    logging: false,
    entities: [`${__dirname}/../entities/*.{ts,js}`],
    migrations: [`${__dirname}/../migrations/*.{ts,js}`]
};

export const dataSource = new DataSource(NODE_ENV === 'test' ? testDataSourceOpts : dataSourceOpts);
