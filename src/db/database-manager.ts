import 'reflect-metadata';

import { Logger } from 'pino';
import { BaseEntity, DataSource } from 'typeorm';
import { Pool } from 'pg';

import { consumerDataSource } from './consumer-source';
import { publisherDataSource } from './publisher-source';
import { cubeDataSource } from './cube-source';
import { logger } from '../utils/logger';
import { EntitySubscriber } from './entity-subscriber';

import { BuildLog } from '../entities/dataset/build-log';
import { DataTable } from '../entities/dataset/data-table';
import { DataTableDescription } from '../entities/dataset/data-table-description';
import { Dataset } from '../entities/dataset/dataset';
import { Dimension } from '../entities/dataset/dimension';
import { DimensionMetadata } from '../entities/dataset/dimension-metadata';
import { FactTableColumn } from '../entities/dataset/fact-table-column';
import { LookupTable } from '../entities/dataset/lookup-table';
import { Measure } from '../entities/dataset/measure';
import { MeasureMetadata } from '../entities/dataset/measure-metadata';
import { MeasureRow } from '../entities/dataset/measure-row';
import { Provider } from '../entities/dataset/provider';
import { ProviderSource } from '../entities/dataset/provider-source';
import { Revision } from '../entities/dataset/revision';
import { RevisionMetadata } from '../entities/dataset/revision-metadata';
import { RevisionProvider } from '../entities/dataset/revision-provider';
import { RevisionTopic } from '../entities/dataset/revision-topic';
import { Topic } from '../entities/dataset/topic';
import { EventLog } from '../entities/event-log';
import { QueryStore } from '../entities/query-store';
import { SearchLog } from '../entities/search-log';
import { Task } from '../entities/task/task';
import { Organisation } from '../entities/user/organisation';
import { OrganisationMetadata } from '../entities/user/organisation-metadata';
import { User } from '../entities/user/user';
import { UserGroup } from '../entities/user/user-group';
import { UserGroupMetadata } from '../entities/user/user-group-metadata';
import { UserGroupRole } from '../entities/user/user-group-role';

// Entity → pool binding. Drives BaseEntity.useDataSource(...) at boot so any rogue static `.save()`
// routes to the correct pool. Production code uses repos directly, so this is defence-in-depth.
const PUBLISHER_ENTITIES: Array<typeof BaseEntity> = [
  BuildLog,
  DataTable,
  DataTableDescription,
  Dataset,
  Dimension,
  DimensionMetadata,
  EventLog, // audit writes route via event.manager regardless; binding here is arbitrary
  FactTableColumn,
  LookupTable,
  Measure,
  MeasureMetadata,
  MeasureRow,
  Organisation,
  OrganisationMetadata,
  Provider,
  ProviderSource,
  Revision,
  RevisionMetadata,
  RevisionProvider,
  RevisionTopic,
  Task,
  Topic,
  User,
  UserGroup,
  UserGroupMetadata,
  UserGroupRole
];

const CONSUMER_ENTITIES: Array<typeof BaseEntity> = [QueryStore, SearchLog];

export class DatabaseManager {
  private logger: Logger;
  private consumerDataSource: DataSource;
  private publisherDataSource: DataSource;
  private cubeDataSource: DataSource;

  constructor(logger: Logger) {
    this.logger = logger;
    this.consumerDataSource = consumerDataSource;
    this.publisherDataSource = publisherDataSource;
    this.cubeDataSource = cubeDataSource;
  }

  getConsumerDataSource(): DataSource {
    return this.consumerDataSource;
  }

  getPublisherDataSource(): DataSource {
    return this.publisherDataSource;
  }

  getCubeDataSource(): DataSource {
    return this.cubeDataSource;
  }

  async initDataSources(): Promise<void> {
    await Promise.all([
      this.initializeDataSource(this.consumerDataSource, 'Consumer'),
      this.initializeDataSource(this.publisherDataSource, 'Publisher'),
      this.initializeDataSource(this.cubeDataSource, 'Cube')
    ]);

    this.bindEntityDataSources();
  }

  async destroyDataSources(): Promise<void> {
    const sources = [this.consumerDataSource, this.publisherDataSource, this.cubeDataSource].filter(
      (ds) => ds.isInitialized
    );
    await Promise.all(sources.map((ds) => ds.destroy()));
    this.logger.info('Datasources destroyed');
  }

  async initializeDataSource(ds: DataSource, name: string): Promise<void> {
    if (ds.isInitialized) {
      this.logger.info(`${name} datasource already initialized`);
      return;
    }

    this.logger.debug(`Initializing ${name} datasource...`);

    try {
      await ds.initialize();
    } catch (error) {
      this.logger.error(error, `Failed to initialize ${name} datasource`);
      throw error;
    }

    this.logger.info(`${name} datasource '${ds.options.database}' ready`);
  }

  // Pin each BaseEntity class to the pool that owns its writes. Without this, two DataSources
  // registering the same entity classes leaves BaseEntity statics routing to whichever source
  // initialised last (a Promise.all race).
  private bindEntityDataSources(): void {
    PUBLISHER_ENTITIES.forEach((entity) => entity.useDataSource(this.publisherDataSource));
    CONSUMER_ENTITIES.forEach((entity) => entity.useDataSource(this.consumerDataSource));
  }

  async initEntitySubscriber(): Promise<EntitySubscriber> {
    // Only attach to the publisher pool — consumer routes are read-only and the few consumer-pool
    // writes (search_log, query_store) are in the audit ignore list anyway.
    return new EntitySubscriber(this.publisherDataSource);
  }

  getConsumerPool(): Pool {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const postgresDriver = this.getConsumerDataSource().driver as any;
    return postgresDriver.master as Pool;
  }

  getPublisherPool(): Pool {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const postgresDriver = this.getPublisherDataSource().driver as any;
    return postgresDriver.master as Pool;
  }

  getCubePool(): Pool {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const postgresDriver = this.getCubeDataSource().driver as any;
    return postgresDriver.master as Pool;
  }
}

export const dbManager = new DatabaseManager(logger);
