import 'dotenv/config';
import 'reflect-metadata';
import { DataSource, DataSourceOptions } from 'typeorm';

import { appConfig } from '../config';

const config = appConfig();

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
    entities: [`${__dirname}/../entities/*.{ts,js}`],
    migrations: [`${__dirname}/../migrations/*.{ts,js}`]
};

export const dataSource = new DataSource(dataSourceOpts);
