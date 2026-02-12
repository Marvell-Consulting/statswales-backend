import { EntityNotFoundError } from 'typeorm';

import { dbManager } from '../../src/db/database-manager';
import { Dataset } from '../../src/entities/dataset/dataset';
import { Dimension } from '../../src/entities/dataset/dimension';
import { DimensionMetadata } from '../../src/entities/dataset/dimension-metadata';
import { LookupTable } from '../../src/entities/dataset/lookup-table';
import { DimensionRepository } from '../../src/repositories/dimension';
import { DimensionType } from '../../src/enums/dimension-type';
import { FileType } from '../../src/enums/file-type';
import { getTestUser } from '../helpers/get-test-user';
import { User } from '../../src/entities/user/user';
import { uuidV4 } from '../../src/utils/uuid';

jest.mock('../../src/services/blob-storage', () => {
  return function BlobStorage() {
    return {
      getContainerClient: jest.fn().mockReturnValue({
        createIfNotExists: jest.fn().mockResolvedValue(true)
      })
    };
  };
});

const user: User = getTestUser('dim-repo-test');

async function createDataset(createdBy: User): Promise<Dataset> {
  const ds = new Dataset();
  ds.id = uuidV4();
  ds.createdBy = createdBy;
  return ds.save();
}

async function createDimension(dataset: Dataset, overrides: Partial<Dimension> = {}): Promise<Dimension> {
  const dim = new Dimension();
  dim.id = uuidV4();
  dim.datasetId = dataset.id;
  dim.type = DimensionType.Raw;
  dim.factTableColumn = `col_${dim.id.slice(0, 8)}`;
  dim.isSliceDimension = false;
  dim.extractor = null;
  dim.joinColumn = null;
  Object.assign(dim, overrides);
  return dim.save();
}

async function createDimensionMetadata(
  dimension: Dimension,
  language: string,
  name: string
): Promise<DimensionMetadata> {
  const meta = new DimensionMetadata();
  meta.id = dimension.id;
  meta.language = language;
  meta.name = name;
  meta.dimension = dimension;
  return meta.save();
}

async function createLookupTable(): Promise<LookupTable> {
  const lt = new LookupTable();
  lt.id = uuidV4();
  lt.filename = `${lt.id}.csv`;
  lt.originalFilename = 'test-lookup.csv';
  lt.mimeType = 'text/csv';
  lt.fileType = FileType.Csv;
  lt.hash = 'abc123';
  lt.isStatsWales2Format = false;
  return lt.save();
}

describe('DimensionRepository', () => {
  beforeAll(async () => {
    try {
      await dbManager.initDataSources();
      await dbManager.getAppDataSource().dropDatabase();
      await dbManager.getAppDataSource().runMigrations();
      await user.save();
    } catch (err) {
      // eslint-disable-next-line no-console
      console.error('Failed to initialise test database', err);
      await dbManager.getAppDataSource().dropDatabase();
      await dbManager.destroyDataSources();
      process.exit(1);
    }
  });

  afterAll(async () => {
    await dbManager.getAppDataSource().dropDatabase();
    await dbManager.destroyDataSources();
  });

  describe('getById', () => {
    let dataset: Dataset;
    let dimensionWithMeta: Dimension;
    let dimensionPlain: Dimension;

    beforeAll(async () => {
      dataset = await createDataset(user);

      dimensionWithMeta = await createDimension(dataset);
      await createDimensionMetadata(dimensionWithMeta, 'en-GB', 'English Name');
      await createDimensionMetadata(dimensionWithMeta, 'cy-GB', 'Welsh Name');

      const lookupTable = await createLookupTable();
      await createDimension(dataset, { lookupTable });

      dimensionPlain = await createDimension(dataset);
    });

    it('should return dimension with metadata and lookupTable relations', async () => {
      const result = await DimensionRepository.getById(dimensionWithMeta.id);
      expect(result.id).toBe(dimensionWithMeta.id);
      expect(result.metadata).toHaveLength(2);
      expect(result.metadata.map((m: DimensionMetadata) => m.language).sort()).toEqual(['cy-GB', 'en-GB']);
    });

    it('should return dimension without lookup table (null)', async () => {
      const result = await DimensionRepository.getById(dimensionPlain.id);
      expect(result.id).toBe(dimensionPlain.id);
      expect(result.lookupTable).toBeNull();
    });

    it('should throw EntityNotFoundError for non-existent id', async () => {
      await expect(DimensionRepository.getById(uuidV4())).rejects.toThrow(EntityNotFoundError);
    });
  });

  describe('getByDatasetId', () => {
    let datasetA: Dataset;
    let datasetB: Dataset;
    let emptyDataset: Dataset;

    beforeAll(async () => {
      datasetA = await createDataset(user);
      const dimA1 = await createDimension(datasetA);
      await createDimensionMetadata(dimA1, 'en-GB', 'Dim A1');
      const dimA2 = await createDimension(datasetA);
      await createDimensionMetadata(dimA2, 'en-GB', 'Dim A2');

      datasetB = await createDataset(user);
      await createDimension(datasetB);

      emptyDataset = await createDataset(user);
    });

    it('should return all dimensions for a dataset with metadata', async () => {
      const result = await DimensionRepository.getByDatasetId(datasetA.id);
      expect(result).toHaveLength(2);
      result.forEach((dim: Dimension) => {
        expect(dim.datasetId).toBe(datasetA.id);
        expect(dim.metadata).toBeDefined();
        expect(dim.metadata.length).toBeGreaterThanOrEqual(1);
      });
    });

    it('should return empty array when dataset has no dimensions', async () => {
      const result = await DimensionRepository.getByDatasetId(emptyDataset.id);
      expect(result).toEqual([]);
    });

    it('should not return dimensions from other datasets', async () => {
      const resultB = await DimensionRepository.getByDatasetId(datasetB.id);
      expect(resultB).toHaveLength(1);
      resultB.forEach((dim: Dimension) => {
        expect(dim.datasetId).toBe(datasetB.id);
      });
    });
  });
});
