import 'reflect-metadata';

import { Logger } from 'pino';
import { DataSource, EntityManager } from 'typeorm';

class DatabaseManager {
    private entityManager: EntityManager;
    private dataSource: DataSource;
    private logger: Logger;

    constructor(dataSource: DataSource, logger: Logger) {
        this.dataSource = dataSource;
        this.logger = logger;
    }

    getDataSource() {
        return this.dataSource;
    }

    getEntityManager(): EntityManager {
        if (this.entityManager === undefined) {
            Promise.resolve(this.initializeDataSource()).catch((error) => this.logger.error(error));
        }
        return this.entityManager;
    }

    async initializeDataSource() {
        await this.dataSource
            .initialize()
            .then(() => this.logger.info('Data source initialized'))
            .catch((error) => this.logger.error(error));

        this.entityManager = this.dataSource.createEntityManager();
    }
}

export default DatabaseManager;
