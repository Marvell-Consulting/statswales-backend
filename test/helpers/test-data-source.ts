import 'reflect-metadata';
import { DataSourceOptions } from 'typeorm';
import * as dotenv from 'dotenv';

import { Dataset } from '../../src/entities/dataset';
import { DatasetInfo } from '../../src/entities/dataset_info';
import { Revision } from '../../src/entities/revision';
import { FileImport } from '../../src/entities/import-file';
import { CsvInfo } from '../../src/entities/csv_info';
import { Source } from '../../src/entities/source';
import { Dimension } from '../../src/entities/dimension';
import { DimensionInfo } from '../../src/entities/dimension_info';
import { User } from '../../src/entities/user';

dotenv.config();

export const datasourceOptions: DataSourceOptions = {
    name: 'default',
    type: 'better-sqlite3',
    database: ':memory:',
    synchronize: true,
    logging: false,
    entities: [Dataset, DatasetInfo, Revision, FileImport, CsvInfo, Source, Dimension, DimensionInfo, User],
    subscribers: []
};
