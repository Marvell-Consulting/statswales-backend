import 'reflect-metadata';
import { DataSource, DataSourceOptions } from 'typeorm';
import * as dotenv from 'dotenv';

dotenv.config();

export const datasourceOptions: DataSourceOptions = {
    name: 'default',
    type: 'better-sqlite3',
    database: ':memory:',
    synchronize: true,
    logging: false,
    entities: [`${__dirname}../src/entities/*{.ts,.js}`],
    subscribers: []
};

export const testDataSource = new DataSource(datasourceOptions);
