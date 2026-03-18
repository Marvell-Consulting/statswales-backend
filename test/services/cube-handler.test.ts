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
import { acquireDuckDB } from '../../src/services/duckdb';
import { FileType } from '../../src/enums/file-type';
import path from 'node:path';
import { FileImportInterface } from '../../src/entities/dataset/file-import.interface';
import { QueryRunner } from 'typeorm';
import { loadFileIntoCube, loadFileIntoLookupTablesSchema } from '../../src/utils/file-utils';
import { uuidV4 } from '../../src/utils/uuid';
import { LookupTable } from '../../src/entities/dataset/lookup-table';
import { LookupTableExtractor } from '../../src/extractors/lookup-table-extractor';
import { FactTableColumn } from '../../src/entities/dataset/fact-table-column';
import { Locale } from '../../src/enums/locale';

jest.mock('../../src/services/blob-storage');

BlobStorage.prototype.listFiles = jest
  .fn()
  .mockReturnValue([{ name: 'test-data-1.csv', path: 'test/test-data-1.csv', isDirectory: false }]);

BlobStorage.prototype.saveBuffer = jest.fn();

const dataset1Id = 'bdc40218-af89-424b-b86e-d21710bc92f1';
const revision1Id = '85f0e416-8bd1-4946-9e2c-1c958897c6ef';
const import1Id = 'fa07be9d-3495-432d-8c1f-d0fc6daae359';
const user: User = getTestUser('test user');
let userGroup = getTestUserGroup('Test Group');

// let datasetService: DatasetService;
let queryRunner: QueryRunner;

describe('API Endpoints', () => {
  beforeAll(async () => {
    try {
      await dbManager.initDataSources();
      await dbManager.getAppDataSource().dropDatabase();
      await dbManager.getAppDataSource().runMigrations();
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

  describe('Load Data Table SQL test', () => {
    test('for CSV type files', async () => {
      const { duckdb, releaseDuckDB } = await acquireDuckDB();
      try {
        const tableName = 'data_table';
        const testFilePath = path.resolve(__dirname, `../sample-files/csv/minimal/data.csv`);
        const testFileInterface: FileImportInterface = {
          id: uuidV4(),
          mimeType: 'text/csv',
          fileType: FileType.Csv,
          encoding: 'utf-8',
          filename: 'data.csv',
          originalFilename: 'data.csv',
          hash: '',
          uploadedAt: new Date()
        };
        await loadFileIntoCube(duckdb, testFileInterface.fileType, testFilePath, tableName, 'memory');
        const tableData = await duckdb.run(`SELECT * FROM ${tableName}`);
        expect(tableData.rowCount).toBe(2);
        const rowsJson = await tableData.getRowsJson();
        expect(Object.keys(rowsJson[0]).length).toBe(4);
      } finally {
        releaseDuckDB();
      }
    });
  });

  describe('Load lookup table with empty hierarchy column', () => {
    test('should handle empty hierarchy values without type cast errors', async () => {
      const dataset = await Dataset.findOneBy({ id: dataset1Id });
      if (!dataset) {
        throw new Error('Dataset not found');
      }

      const factTableColumn = await FactTableColumn.findOneByOrFail({
        id: dataset1Id,
        columnName: 'AreaCode'
      });

      const lookupTable = new LookupTable();
      lookupTable.id = uuidV4();
      lookupTable.filename = 'area-lookup-empty-hierarchy.csv';
      lookupTable.originalFilename = 'area-lookup-empty-hierarchy.csv';
      lookupTable.fileType = FileType.Csv;
      lookupTable.isStatsWales2Format = true;
      lookupTable.mimeType = 'text/csv';
      lookupTable.hash = 'test-hash';

      const extractor: LookupTableExtractor = {
        tableLanguage: Locale.EnglishGb,
        descriptionColumns: [
          { lang: 'en-gb', name: 'Description_en' },
          { lang: 'cy-gb', name: 'Description_cy' }
        ],
        sortColumn: 'SortOrder',
        hierarchyColumn: 'Hierarchy',
        notesColumns: [
          { lang: 'en-gb', name: 'Notes_en' },
          { lang: 'cy-gb', name: 'Notes_cy' }
        ],
        isSW2Format: true
      };

      const testFilePath = path.resolve(__dirname, '../sample-files/csv/realistic/area-lookup-empty-hierarchy.csv');

      await expect(
        loadFileIntoLookupTablesSchema(dataset, lookupTable, extractor, factTableColumn, 'AreaCode', testFilePath)
      ).resolves.not.toThrow();

      // Verify that hierarchy values are NULL, not empty strings
      const { duckdb, releaseDuckDB } = await acquireDuckDB();
      try {
        const result = await duckdb.run(`SELECT DISTINCT hierarchy FROM lookup_tables_db."${lookupTable.id}"`);
        const rows = await result.getRowsJson();
        const hierarchyValues = rows.map((r: any) => r.hierarchy);
        expect(hierarchyValues).toEqual([undefined]);
      } finally {
        releaseDuckDB();
      }
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
