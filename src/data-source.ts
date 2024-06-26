import 'reflect-metadata';
import { DataSource, DataSourceOptions } from 'typeorm';
import * as dotenv from 'dotenv';

dotenv.config();

const { DB_HOST, DB_PORT, DB_USERNAME, DB_PASSWORD, DB_DATABASE } = process.env;

console.log(DB_HOST);

export const datasourceOptions: DataSourceOptions = {
    type: 'postgres',
    host: DB_HOST,
    port: parseInt(DB_PORT || '5432', 10),
    username: DB_USERNAME,
    password: DB_PASSWORD,
    database: DB_DATABASE,
    ssl: true,
    synchronize: false,
    logging: false,
    entities: [`${__dirname}/entity/*.ts`],
    migrations: [`${__dirname}/migration/*.ts`],
    subscribers: []
};

export const dataSource = new DataSource(datasourceOptions);
