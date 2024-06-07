import 'reflect-metadata';
import { DataSourceOptions } from 'typeorm';
import * as dotenv from 'dotenv';

import { Dataset } from '../src/entity/dataset';
import { Datafile } from '../src/entity/datafile';
import { LookupTable } from '../src/entity/lookuptable';
import { DatasetTitle } from '../src/entity/dataset_title';
import { DatasetColumn } from '../src/entity/dataset_column';
import { DatasetDescription } from '../src/entity/dataset_description';
import { ColumnTitle } from '../src/entity/column_title';

dotenv.config();

export const datasourceOptions: DataSourceOptions = {
    name: 'default',
    type: 'better-sqlite3',
    database: ':memory:',
    synchronize: true,
    logging: false,
    entities: [Dataset, Datafile, LookupTable, DatasetTitle, DatasetDescription, DatasetColumn, ColumnTitle],
    subscribers: []
};
