/* eslint-disable import/no-cycle */
import 'reflect-metadata';
import { DataSource, DataSourceOptions, EntityManager } from 'typeorm';
import { Logger } from 'pino';

import { Dataset } from './entity/dataset';
import { Datafile } from './entity/datafile';
import { LookupTable } from './entity/lookuptable';
import { DatasetTitle } from './entity/dataset_title';
import { DatasetColumn } from './entity/dataset_column';
import { DatasetDescription } from './entity/dataset_description';
import { ColumnTitle } from './entity/column_title';

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
            entities: [Dataset, Datafile, LookupTable, DatasetTitle, DatasetDescription, DatasetColumn, ColumnTitle]
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
