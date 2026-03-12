import path from 'node:path';

import BlobStorage from '../../src/services/blob-storage';
import { dbManager } from '../../src/db/database-manager';
import { DataTable } from '../../src/entities/dataset/data-table';
import { FileType } from '../../src/enums/file-type';
import { validateFileAndExtractTableInfo } from '../../src/services/incoming-file-processor';
import { TempFile } from '../../src/interfaces/temp-file';
import { uuidV4 } from '../../src/utils/uuid';
import { logger } from '../../src/utils/logger';

jest.mock('../../src/services/blob-storage');

BlobStorage.prototype.listFiles = jest.fn().mockReturnValue([]);
BlobStorage.prototype.saveBuffer = jest.fn();

describe('DuckDB concurrency semaphore', () => {
  beforeAll(async () => {
    try {
      await dbManager.initDataSources();
      await dbManager.getAppDataSource().dropDatabase();
      await dbManager.getAppDataSource().runMigrations();
      const queryRunner = dbManager.getAppDataSource().createQueryRunner();
      try {
        await queryRunner.createSchema('data_tables', true);
      } finally {
        await queryRunner.release();
      }
    } catch (error) {
      logger.error(error, 'Could not initialise test database');
      await dbManager.getAppDataSource().dropDatabase();
      await dbManager.destroyDataSources();
      process.exit(1);
    }
  });

  afterAll(async () => {
    try {
      const queryRunner = dbManager.getAppDataSource().createQueryRunner();
      try {
        await queryRunner.dropSchema('data_tables', true, true);
      } finally {
        await queryRunner.release();
      }
      await dbManager.getAppDataSource().dropDatabase();
      await dbManager.destroyDataSources();
    } catch (error) {
      logger.error(error, 'Error during test teardown');
    }
  });

  test('20 concurrent imports succeed without OOM', async () => {
    const csvPath = path.resolve(__dirname, '../sample-files/csv/minimal/data.csv');
    const file: TempFile = {
      path: csvPath,
      originalname: 'data.csv',
      mimetype: 'text/csv'
    };

    const dataTables: DataTable[] = Array.from({ length: 20 }, () => {
      const dt = new DataTable();
      dt.id = uuidV4();
      dt.fileType = FileType.Csv;
      return dt;
    });

    const start = Date.now();
    const results = await Promise.all(dataTables.map((dt) => validateFileAndExtractTableInfo(file, dt, 'data_table')));
    const elapsed = Date.now() - start;
    logger.info(`20 concurrent imports completed in ${elapsed}ms`);

    for (const descriptions of results) {
      expect(descriptions).toHaveLength(4);
      expect(descriptions.map((d) => d.columnName)).toEqual(['date', 'data', 'measure', 'notes']);
    }
  }, 30_000);
});
