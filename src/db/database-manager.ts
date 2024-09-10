import 'reflect-metadata';

import { Logger } from 'pino';
import { DataSource, EntityManager } from 'typeorm';

import { dataSource } from './data-source';

class DatabaseManager {
    private entityManager: EntityManager;
    private dataSource: DataSource;
    private logger: Logger;

    constructor(logger: Logger) {
        this.logger = logger;
        this.dataSource = dataSource;
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
        console.log({
            NODE_ENV: process.env.NODE_ENV,
            TEST_DB_HOST: process.env.TEST_DB_HOST,
            TEST_DB_PORT: process.env.TEST_DB_PORT,
            TEST_DB_USERNAME: process.env.TEST_DB_USERNAME,
            TEST_DB_PASSWORD: process.env.TEST_DB_PASSWORD,
            TEST_DB_DATABASE: process.env.TEST_DB_DATABASE
        });

        this.logger.debug(`DB '${this.dataSource.options.database}' initializing...`);

        if (!this.dataSource.isInitialized) {
            try {
                await this.dataSource.initialize();
            } catch (error) {
                this.logger.error(error);
                return;
            }
        }

        this.logger.info(`DB '${this.dataSource.options.database}' initialized`);
        this.entityManager = this.dataSource.createEntityManager();
    }
}

export default DatabaseManager;
