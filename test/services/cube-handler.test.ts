import { format as pgformat } from '@scaleleap/pg-format';

import BlobStorage from '../../src/services/blob-storage';
import { User } from '../../src/entities/user/user';
import { getTestUser, getTestUserGroup } from '../helpers/get-test-user';
import { dbManager } from '../../src/db/database-manager';
import { initPassport } from '../../src/middleware/passport-auth';
import { UserGroup } from '../../src/entities/user/user-group';
import { UserGroupRole } from '../../src/entities/user/user-group-role';
import { GroupRole } from '../../src/enums/group-role';
import { createFullDataset } from '../helpers/test-helper';
import { logger } from '../../src/utils/logger';
import { Dataset } from '../../src/entities/dataset/dataset';
import { DatasetDTO } from '../../src/dtos/dataset-dto';
import { duckdb } from '../../src/services/duckdb';
import { createDataTableQuery, loadFileIntoCube, loadTableDataIntoFactTable } from '../../src/services/cube-handler';
import { FileType } from '../../src/enums/file-type';
import path from 'node:path';
import { FileImportInterface } from '../../src/entities/dataset/file-import.interface';
import { randomUUID } from 'node:crypto';
import { QueryRunner } from 'typeorm';

jest.mock('../../src/services/blob-storage');

BlobStorage.prototype.listFiles = jest
  .fn()
  .mockReturnValue([{ name: 'test-data-1.csv', path: 'test/test-data-1.csv', isDirectory: false }]);

BlobStorage.prototype.saveBuffer = jest.fn();

const dataset1Id = 'bdc40218-af89-424b-b86e-d21710bc92f1';
const revision1Id = '85f0e416-8bd1-4946-9e2c-1c958897c6ef';
const import1Id = 'fa07be9d-3495-432d-8c1f-d0fc6daae359';
const user: User = getTestUser('test', 'user');
let userGroup = getTestUserGroup('Test Group');

// let datasetService: DatasetService;
let queryRunner: QueryRunner;

describe('API Endpoints', () => {
  beforeAll(async () => {
    try {
      await dbManager.initDataSources();
      await initPassport(dbManager.getAppDataSource());
      queryRunner = dbManager.getAppDataSource().createQueryRunner();
      await queryRunner.dropSchema('data_tables', true, true);
      await queryRunner.dropSchema(revision1Id, true, true);
      await queryRunner.createSchema('data_tables', true);
      userGroup = await dbManager.getAppDataSource().getRepository(UserGroup).save(userGroup);
      user.groupRoles = [UserGroupRole.create({ group: userGroup, roles: [GroupRole.Editor] })];
      await user.save();
      await createFullDataset(dataset1Id, revision1Id, import1Id, user);
      // datasetService = new DatasetService(Locale.EnglishGb);
    } catch (error) {
      logger.error(error, 'Could not initialise test database');
      await dbManager.getAppDataSource().dropDatabase();
      await dbManager.destroyDataSources();
      process.exit(1);
    } finally {
      if (queryRunner) {
        await queryRunner.release();
      }
    }
  });

  test('Return true test', async () => {
    const dataset1 = await Dataset.findOneBy({ id: dataset1Id });
    if (!dataset1) {
      throw new Error('Dataset not found');
    }
    const dto = DatasetDTO.fromDataset(dataset1);
    expect(dto).toBeInstanceOf(DatasetDTO);
  });

  describe('Create Data Table SQL test', () => {
    test('for CSV type files', async () => {
      const quack = await duckdb();
      const val = await createDataTableQuery('test_table', 'my_temp_file.csv', FileType.Csv, quack);
      await quack.close();
      expect(val).toBe(
        "CREATE TABLE test_table AS SELECT * FROM read_csv('my_temp_file.csv', auto_type_candidates = ['BIGINT', 'DOUBLE', 'VARCHAR'], sample_size = -1);"
      );
    });
    test('for CSV Gzip type files', async () => {
      const quack = await duckdb();
      const val = await createDataTableQuery('test_table', 'my_temp_file.csv.gz', FileType.GzipCsv, quack);
      await quack.close();
      expect(val).toBe(
        "CREATE TABLE test_table AS SELECT * FROM read_csv('my_temp_file.csv.gz', auto_type_candidates = ['BIGINT', 'DOUBLE', 'VARCHAR'], sample_size = -1);"
      );
    });
    test('for Parquet type files', async () => {
      const quack = await duckdb();
      const val = await createDataTableQuery('test_table', 'my_temp_file.parquet', FileType.Parquet, quack);
      await quack.close();
      expect(val).toBe("CREATE TABLE test_table AS SELECT * FROM 'my_temp_file.parquet';");
    });
    test('for Gzip JSON type files', async () => {
      const quack = await duckdb();
      const val = await createDataTableQuery('test_table', 'my_temp_file.json.gz', FileType.GzipJson, quack);
      await quack.close();
      expect(val).toBe("CREATE TABLE test_table AS SELECT * FROM read_json_auto('my_temp_file.json.gz');");
    });
    test('for Excel type files', async () => {
      const quack = await duckdb();
      const val = await createDataTableQuery('test_table', 'my_temp_file.xlsx', FileType.Excel, quack);
      await quack.close();
      expect(val).toBe("CREATE TABLE test_table AS SELECT * FROM st_read('my_temp_file.xlsx');");
    });
    test('for unknown type files', async () => {
      const quack = await duckdb();
      try {
        await createDataTableQuery('test_table', 'my_temp_file.xlsx', FileType.Unknown, quack);
      } catch (error) {
        expect((error as Error).message).toBe('Unknown file type');
      }
      await quack.close();
    });
  });

  describe('Load Data Table SQL test', () => {
    test('for CSV type files', async () => {
      const quack = await duckdb();
      const tableName = 'data_table';
      const testFilePath = path.resolve(__dirname, `../sample-files/csv/sure-start-short.csv`);
      const testFileInterface: FileImportInterface = {
        id: randomUUID(),
        mimeType: 'text/csv',
        fileType: FileType.Csv,
        encoding: 'utf-8',
        filename: 'sure-start-short.csv',
        originalFilename: 'sure-start-short.csv',
        hash: '',
        uploadedAt: new Date()
      };
      await loadFileIntoCube(quack, testFileInterface.fileType, testFilePath, tableName);
      const tableData = await quack.all(`SELECT * FROM ${tableName}`);
      expect(tableData.length).toBe(24);
      expect(Object.keys(tableData[0]).length).toBe(6);
      await quack.close();
    });
  });

  describe('Load data into fact table SQL test', () => {
    test('Basic load of a small of file', async () => {
      const quack = await duckdb();
      const originalTableName = 'data_table';
      const factTableName = 'fact_table';
      const testFilePath = path.resolve(__dirname, `../sample-files/csv/sure-start-short.csv`);
      const testFileInterface: FileImportInterface = {
        id: randomUUID(),
        mimeType: 'text/csv',
        fileType: FileType.Csv,
        encoding: 'utf-8',
        filename: 'sure-start-short.csv',
        originalFilename: 'sure-start-short.csv',
        hash: '',
        uploadedAt: new Date()
      };
      const primaryKey = ['YearCode', 'AreaCode', 'RowRef', 'Measure'];
      const factTableDef = ['YearCode', 'AreaCode', 'Data', 'RowRef', 'Measure', 'NoteCodes'];
      const factTableCols = [
        '"YearCode" VARCHAR',
        '"AreaCode" VARCHAR',
        '"Data" DECIMAL',
        '"RowRef" INTEGER',
        '"Measure" INTEGER',
        '"NoteCodes" VARCHAR'
      ];
      const factTableQuery = pgformat(
        'CREATE TABLE %I (%s, PRIMARY KEY (%I));',
        'fact_table',
        factTableCols.join(','),
        primaryKey
      );
      await quack.exec(factTableQuery);
      await loadFileIntoCube(quack, testFileInterface.fileType, testFilePath, originalTableName);
      await loadTableDataIntoFactTable(quack, factTableDef, factTableName, originalTableName);
      const tableData = await quack.all(`SELECT * FROM ${factTableName}`);
      expect(tableData.length).toBe(24);
      await quack.close();
    });
  });

  afterAll(async () => {
    queryRunner = dbManager.getAppDataSource().createQueryRunner();
    await queryRunner.dropSchema('data_tables', true, true);
    await queryRunner.dropSchema(revision1Id, true, true);
    await queryRunner.release();
    await dbManager.getAppDataSource().dropDatabase();
    await dbManager.destroyDataSources();
  });
});
