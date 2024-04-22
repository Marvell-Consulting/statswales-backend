import 'reflect-metadata';
import { DataSourceOptions } from 'typeorm';
import * as dotenv from 'dotenv';

import { Datafile } from '../src/entity/Datafile';

dotenv.config();

export const datasourceOptions: DataSourceOptions = {
    name: 'default',
    type: 'better-sqlite3',
    database: ':memory:',
    synchronize: true,
    logging: false,
    entities: [Datafile],
    // entities: [`${__dirname}/entity/*.ts`],
    // migrations: [`${__dirname}/migration/*.ts`],
    subscribers: []
};
