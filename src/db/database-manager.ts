import 'reflect-metadata';

import { Logger } from 'pino';
import { DataSource } from 'typeorm';

import { dataSource } from './data-source';
import { cubeDataSource } from './cube-source';
import { logger } from '../utils/logger';
import { EntitySubscriber } from './entity-subscriber';

export class DatabaseManager {
  private logger: Logger;
  private appDataSource: DataSource;
  private cubeDataSource: DataSource;

  constructor(logger: Logger) {
    this.logger = logger;
    this.appDataSource = dataSource;
    this.cubeDataSource = cubeDataSource;
  }

  getAppDataSource(): DataSource {
    return this.appDataSource;
  }

  getCubeDataSource(): DataSource {
    return this.cubeDataSource;
  }

  async initDataSources(): Promise<void> {
    await Promise.all([
      this.initializeDataSource(this.appDataSource, 'Application'),
      this.initializeDataSource(this.cubeDataSource, 'Cube')
    ]);
  }

  async destroyDataSources(): Promise<void> {
    await Promise.all([this.appDataSource.destroy(), this.cubeDataSource.destroy()]);
    this.logger.info('Datasources destroyed');
  }

  async initializeDataSource(dataSource: DataSource, name: string): Promise<void> {
    if (dataSource.isInitialized) {
      this.logger.info(`${name} datasource already initialized`);
      return;
    }

    this.logger.debug(`Initializing ${name} datasource...`);

    try {
      await dataSource.initialize();
    } catch (error) {
      this.logger.error(error);
      return;
    }

    this.logger.info(`${name} datasource '${dataSource.options.database}' ready`);
  }

  async initEntitySubscriber(): Promise<EntitySubscriber> {
    return new EntitySubscriber(this.appDataSource);
  }
}

export const dbManager = new DatabaseManager(logger);
