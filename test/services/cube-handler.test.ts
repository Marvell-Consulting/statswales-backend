import BlobStorage from '../../src/services/blob-storage';
import { User } from '../../src/entities/user/user';
import { getTestUser, getTestUserGroup } from '../helpers/get-test-user';
import { dbManager } from '../../src/db/database-manager';
import { initPassport } from '../../src/middleware/passport-auth';
import { UserGroup } from '../../src/entities/user/user-group';
import { UserGroupRole } from '../../src/entities/user/user-group-role';
import { GroupRole } from '../../src/enums/group-role';
import { createFullDataset } from '../helpers/test-helper';
import { ensureWorkerDataSources, resetDatabase } from '../helpers/reset-database';
import { Dataset } from '../../src/entities/dataset/dataset';
import { DatasetDTO } from '../../src/dtos/dataset-dto';
import { acquireDuckDB } from '../../src/services/duckdb';
import { FileType } from '../../src/enums/file-type';
import path from 'node:path';
import { FileImportInterface } from '../../src/entities/dataset/file-import.interface';
import { loadFileIntoCube, convertLookupTableToSW3Format } from '../../src/utils/file-utils';
import { uuidV4 } from '../../src/utils/uuid';
import { LookupTable } from '../../src/entities/dataset/lookup-table';
import { LookupTableExtractor } from '../../src/extractors/lookup-table-extractor';
import { FactTableColumn } from '../../src/entities/dataset/fact-table-column';
import { Locale } from '../../src/enums/locale';
import { format as pgformat } from '@scaleleap/pg-format';

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

describe('API Endpoints', () => {
  beforeAll(async () => {
    await ensureWorkerDataSources();
    await resetDatabase();
    await initPassport(dbManager.getAppDataSource());
    userGroup = await dbManager.getAppDataSource().getRepository(UserGroup).save(userGroup);
    user.groupRoles = [UserGroupRole.create({ group: userGroup, roles: [GroupRole.Editor] })];
    await user.save();
    await createFullDataset(dataset1Id, revision1Id, import1Id, user);
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
    test('should handle empty hierarchy values without type cast errors in Postgres', async () => {
      const factTableColumn = await FactTableColumn.findOneByOrFail({
        id: dataset1Id,
        columnName: 'AreaCode'
      });

      const mockCubeId = uuidV4();
      const lookupTable = new LookupTable();
      lookupTable.id = uuidV4();
      lookupTable.filename = 'test-lookup.csv';
      lookupTable.originalFilename = 'test-lookup.csv';
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

      // Mimic how the file processor loads CSV data into Postgres. Numeric columns
      // get their detected types, but Hierarchy (all empty) ends up as VARCHAR.
      const cubeRunner = dbManager.getCubeDataSource().createQueryRunner();
      try {
        await cubeRunner.query(pgformat('CREATE SCHEMA IF NOT EXISTS %I;', mockCubeId));
        await cubeRunner.query(
          pgformat(
            `CREATE TABLE %I.lookup_table (
              "AreaCode" BIGINT,
              "Description_en" TEXT,
              "Description_cy" TEXT,
              "Hierarchy" VARCHAR,
              "SortOrder" INTEGER,
              "Notes_en" TEXT,
              "Notes_cy" TEXT
            );`,
            mockCubeId
          )
        );
        // Insert rows with empty hierarchy values (empty strings, as CSV parser would produce)
        await cubeRunner.query(
          pgformat(
            `INSERT INTO %I.lookup_table VALUES
              (512, 'Isle of Anglesey', 'Ynys Môn', '', 1, '', ''),
              (514, 'Gwynedd', 'Gwynedd', '', 2, '', ''),
              (596, 'Wales', 'Cymru', '', 0, '', '');`,
            mockCubeId
          )
        );

        // This should not throw — before the fix, Postgres rejected the INSERT because
        // the hierarchy column was typed as BIGINT but received VARCHAR empty strings
        await convertLookupTableToSW3Format(mockCubeId, lookupTable, extractor, factTableColumn, 'AreaCode');

        // Verify hierarchy values are NULL, not empty strings
        const result = await cubeRunner.query(
          pgformat('SELECT DISTINCT hierarchy FROM %I.%I;', mockCubeId, lookupTable.id)
        );
        expect(result.map((r: any) => r.hierarchy)).toEqual([null]);
      } finally {
        await cubeRunner.query(pgformat('DROP SCHEMA IF EXISTS %I CASCADE;', mockCubeId));
        await cubeRunner.release();
      }
    });
  });
});
