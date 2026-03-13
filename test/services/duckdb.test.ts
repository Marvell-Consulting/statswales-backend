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

describe('DuckDB concurrency integration', () => {
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

describe('acquireDuckDB', () => {
  let mockRun: jest.Mock;
  let mockCreate: jest.Mock;

  function createMockConnection() {
    return { run: mockRun, disconnectSync: jest.fn() };
  }

  beforeEach(() => {
    jest.resetModules();

    mockRun = jest.fn().mockResolvedValue(undefined);
    mockCreate = jest.fn().mockResolvedValue({
      connect: jest.fn().mockImplementation(() => Promise.resolve(createMockConnection()))
    });

    jest.doMock('@duckdb/node-api', () => ({
      DuckDBInstance: { create: mockCreate },
      DuckDBConnection: jest.fn()
    }));
  });

  afterEach(() => {
    jest.restoreAllMocks();
    jest.resetModules();
  });

  function loadModule() {
    // eslint-disable-next-line @typescript-eslint/no-require-imports
    return require('../../src/services/duckdb') as typeof import('../../src/services/duckdb');
  }

  test('returns a handle with duckdb connection and release function', async () => {
    const { acquireDuckDB } = loadModule();
    const handle = await acquireDuckDB();

    expect(handle.duckdb).toBeDefined();
    expect(handle.duckRelease).toBeInstanceOf(Function);

    const conn = handle.duckdb as unknown as { disconnectSync: jest.Mock };
    expect(conn.disconnectSync).not.toHaveBeenCalled();
    handle.duckRelease();
    expect(conn.disconnectSync).toHaveBeenCalledTimes(1);
  });

  test('semaphore is released when connection fails', async () => {
    // Set maxConcurrency to 1 so a leaked permit would deadlock
    const origEnv = process.env.DUCKDB_MAX_CONCURRENCY;
    process.env.DUCKDB_MAX_CONCURRENCY = '1';

    try {
      const setupConn = createMockConnection();
      const mockInstance = {
        connect: jest
          .fn()
          .mockResolvedValueOnce(setupConn) // setup connection succeeds
          .mockRejectedValueOnce(new Error('connection failed')) // user connection fails
          .mockImplementation(() => Promise.resolve(createMockConnection())) // subsequent calls succeed
      };
      mockCreate.mockResolvedValue(mockInstance);

      const { acquireDuckDB } = loadModule();

      await expect(acquireDuckDB()).rejects.toThrow('connection failed');

      // With maxConcurrency=1, this would hang if the permit leaked
      const handle = await acquireDuckDB();
      handle.duckRelease();
    } finally {
      if (origEnv === undefined) {
        delete process.env.DUCKDB_MAX_CONCURRENCY;
      } else {
        process.env.DUCKDB_MAX_CONCURRENCY = origEnv;
      }
    }
  }, 5_000);

  test('singleton is reset when setup ATTACH fails', async () => {
    mockRun
      .mockResolvedValueOnce(undefined) // LOAD postgres
      .mockResolvedValueOnce(undefined) // CREATE SECRET
      .mockRejectedValueOnce(new Error('ATTACH failed')); // first ATTACH

    const { acquireDuckDB } = loadModule();

    await expect(acquireDuckDB()).rejects.toThrow('ATTACH failed');

    // On retry, DuckDBInstance.create should be called again (singleton was reset)
    mockRun.mockResolvedValue(undefined);

    const handle = await acquireDuckDB();
    expect(mockCreate).toHaveBeenCalledTimes(2);
    handle.duckRelease();
  });

  test('reuses existing instance on subsequent calls', async () => {
    const { acquireDuckDB } = loadModule();

    const handle1 = await acquireDuckDB();
    handle1.duckRelease();

    const handle2 = await acquireDuckDB();
    handle2.duckRelease();

    expect(mockCreate).toHaveBeenCalledTimes(1);
  });

  test('duckRelease is idempotent', async () => {
    const { acquireDuckDB } = loadModule();
    const handle = await acquireDuckDB();

    const conn = handle.duckdb as unknown as { disconnectSync: jest.Mock };
    handle.duckRelease();
    handle.duckRelease();
    handle.duckRelease();

    expect(conn.disconnectSync).toHaveBeenCalledTimes(1);
  });
});
