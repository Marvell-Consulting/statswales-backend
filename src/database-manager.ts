import 'reflect-metadata';
import { DataSource, DataSourceOptions, EntityManager } from 'typeorm';
import { Logger } from 'pino';

import { Dataset } from './entity2/dataset';
import { DatasetInfo } from './entity2/dataset_info';
import { Revision } from './entity2/revision';
import { Import } from './entity2/import';
import { CsvInfo } from './entity2/csv_info';
import { Source } from './entity2/source';
import { Dimension } from './entity2/dimension';
import { DimensionInfo } from './entity2/dimension_info';
import { Users } from './entity2/users';

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
            entities: [Dataset, DatasetInfo, Revision, Import, CsvInfo, Source, Dimension, DimensionInfo, Users]
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
