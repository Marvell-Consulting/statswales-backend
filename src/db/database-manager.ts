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

  getDataSource(): DataSource {
    return this.dataSource;
  }

  getEntityManager(): EntityManager {
    if (this.entityManager === undefined) {
      Promise.resolve(this.initializeDataSource()).catch((error) => this.logger.error(error));
    }
    return this.entityManager;
  }

  async initializeDataSource(): Promise<void> {
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
