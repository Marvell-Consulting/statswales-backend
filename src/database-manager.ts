import 'reflect-metadata';
import { DataSource, DataSourceOptions, EntityManager } from 'typeorm';
import { Logger } from 'pino';

import { Dataset } from './entities/dataset';
import { DatasetInfo } from './entities/dataset_info';
import { Revision } from './entities/revision';
import { FileImport } from './entities/import_file';
import { CsvInfo } from './entities/csv_info';
import { Source } from './entities/source';
import { Dimension } from './entities/dimension';
import { DimensionInfo } from './entities/dimension_info';
import { User } from './entities/user';

class DatabaseManager {
    private datasourceOptions: DataSourceOptions;
    private dataSource: DataSource;
    private entityManager: EntityManager;
    private logger: Logger;

    constructor(
        private config: DataSourceOptions,
        logger: Logger
    ) {
        this.datasourceOptions = config;
        this.logger = logger;
    }

    getDataSource() {
        return this.dataSource;
    }

    getEntityManager(): EntityManager {
        if (this.entityManager === undefined)
            Promise.resolve(this.initializeDataSource()).catch((error) => this.logger.error(error));
        return this.entityManager;
    }

    async initializeDataSource() {
        this.dataSource = new DataSource({
            ...this.datasourceOptions,
            entities: [Dataset, DatasetInfo, Revision, FileImport, CsvInfo, Source, Dimension, DimensionInfo, User]
        });

        await this.dataSource
            .initialize()
            .then(() => {
                this.logger.info('Data source initialized');
            })
            .catch((error) => this.logger.error(error));

        this.entityManager = this.dataSource.createEntityManager();
    }
}

export default DatabaseManager;
