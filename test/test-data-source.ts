import 'reflect-metadata';
import { DataSourceOptions } from 'typeorm';
import * as dotenv from 'dotenv';

import { Dataset } from '../src/entity2/dataset';
import { DatasetInfo } from '../src/entity2/dataset_info';
import { Revision } from '../src/entity2/revision';
import { Import } from '../src/entity2/import';
import { CsvInfo } from '../src/entity2/csv_info';
import { Source } from '../src/entity2/source';
import { Dimension } from '../src/entity2/dimension';
import { DimensionInfo } from '../src/entity2/dimension_info';
import { Users } from '../src/entity2/users';

dotenv.config();

export const datasourceOptions: DataSourceOptions = {
    name: 'default',
    type: 'better-sqlite3',
    database: ':memory:',
    synchronize: true,
    logging: false,
    entities: [Dataset, DatasetInfo, Revision, Import, CsvInfo, Source, Dimension, DimensionInfo, Users],
    subscribers: []
};
