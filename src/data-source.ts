import 'reflect-metadata';
import { DataSource, DataSourceOptions } from 'typeorm';
import * as dotenv from 'dotenv';

dotenv.config();

const { DB_HOST, DB_PORT, DB_USERNAME, DB_PASSWORD, DB_DATABASE, DB_SSL } = process.env;

export const datasourceOptions: DataSourceOptions = {
    type: 'postgres',
    host: DB_HOST,
    port: parseInt(DB_PORT || '5432', 10),
    username: DB_USERNAME,
    password: DB_PASSWORD,
    database: DB_DATABASE,
    ssl: DB_SSL !== 'false' ?? true,
    synchronize: false,
    logging: false,
    entities: [`${__dirname}/entities/*{.ts,.js}`],
    migrations: [`${__dirname}/migration/*{.ts,.js}`],
    subscribers: []
};

export const dataSource = new DataSource(datasourceOptions);
