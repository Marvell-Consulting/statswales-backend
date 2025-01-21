import { DataSource } from 'typeorm';

import { logger } from '../utils/logger';
import { EntitySubscriber } from '../services/entity-subscriber';

import DatabaseManager from './database-manager';

export const initDb = async (): Promise<DatabaseManager> => {
    const dbManager = new DatabaseManager(logger);
    await dbManager.initializeDataSource();
    return dbManager;
};

export const initEntitySubscriber = async (dataSource: DataSource): Promise<void> => {
    const eventService = new EntitySubscriber(dataSource);
};
