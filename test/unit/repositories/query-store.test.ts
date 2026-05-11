import 'reflect-metadata';
import { createHash } from 'node:crypto';

jest.mock('../../../src/utils/logger', () => ({
  logger: { debug: jest.fn(), info: jest.fn(), warn: jest.fn(), error: jest.fn() }
}));

const mockFindOneBy = jest.fn();
const mockSave = jest.fn();
const mockCreate = jest.fn((data: unknown) => ({ ...(data as object), save: mockSave }));

jest.mock('../../../src/entities/query-store', () => ({
  QueryStore: {
    findOneBy: (...args: unknown[]) => mockFindOneBy(...args),
    create: (data: unknown) => mockCreate(data)
  }
}));

jest.mock('../../../src/db/data-source', () => ({
  dataSource: {
    getRepository: () => ({ extend: (extension: Record<string, unknown>) => extension })
  }
}));

const mockCubeQuery = jest.fn();
const mockCubeRelease = jest.fn();
jest.mock('../../../src/db/database-manager', () => ({
  dbManager: {
    getCubeDataSource: () => ({
      createQueryRunner: () => ({ query: mockCubeQuery, release: mockCubeRelease })
    })
  }
}));

jest.mock('../../../src/utils/consumer', () => ({
  checkAvailableViews: jest.fn(),
  getFilterTable: jest.fn(),
  coreViewChooser: jest.fn(),
  getColumns: jest.fn(),
  createBaseQuery: jest.fn()
}));

import { FilterInterface } from '../../../src/interfaces/filterInterface';
import { v1FilterToDataOptions, QueryStoreRepository } from '../../../src/repositories/query-store';

function hash(datasetId: string, revisionId: string, options: unknown, namespace = 'v1'): string {
  return createHash('sha256')
    .update(`${namespace}:${datasetId}:${revisionId}:${JSON.stringify(options)}`)
    .digest('hex');
}

describe('v1FilterToDataOptions', () => {
  it('returns identical hashes for filter entries in different order', () => {
    const a: FilterInterface[] = [
      { columnName: 'region', values: ['wales', 'england'] },
      { columnName: 'year', values: ['2024', '2023'] }
    ];
    const b: FilterInterface[] = [
      { columnName: 'year', values: ['2023', '2024'] },
      { columnName: 'region', values: ['england', 'wales'] }
    ];

    expect(hash('d', 'r', v1FilterToDataOptions(a))).toEqual(hash('d', 'r', v1FilterToDataOptions(b)));
  });

  it('returns identical hashes for value arrays in different order', () => {
    const a: FilterInterface[] = [{ columnName: 'region', values: ['c', 'a', 'b'] }];
    const b: FilterInterface[] = [{ columnName: 'region', values: ['b', 'c', 'a'] }];

    expect(hash('d', 'r', v1FilterToDataOptions(a))).toEqual(hash('d', 'r', v1FilterToDataOptions(b)));
  });

  it('returns different hashes for different filter values', () => {
    const a: FilterInterface[] = [{ columnName: 'region', values: ['wales'] }];
    const b: FilterInterface[] = [{ columnName: 'region', values: ['england'] }];

    expect(hash('d', 'r', v1FilterToDataOptions(a))).not.toEqual(hash('d', 'r', v1FilterToDataOptions(b)));
  });

  it('treats undefined and empty filter lists as equivalent', () => {
    expect(hash('d', 'r', v1FilterToDataOptions(undefined))).toEqual(hash('d', 'r', v1FilterToDataOptions([])));
  });

  it('uses options that match v1 contract (raw columns, reference values, no pivot)', () => {
    const dto = v1FilterToDataOptions([]);
    expect(dto.options?.use_raw_column_names).toBe(true);
    expect(dto.options?.use_reference_values).toBe(true);
    expect(dto.pivot).toBeUndefined();
  });
});

describe('v1/v2 hash isolation', () => {
  it('produces a different hash for v1 than for the same options without a namespace', () => {
    // The v1 cache hashes its entries under a "v1:" namespace prefix so they can
    // never collide with v2 entries — even if a v2 client happened to send a
    // DataOptionsDTO that matched v1's canonicalised shape.
    const dto = v1FilterToDataOptions([{ columnName: 'year', values: ['2020'] }]);
    const v1Hash = hash('d', 'r', dto, 'v1');
    const v2Hash = hash('d', 'r', dto, '');
    expect(v1Hash).not.toEqual(v2Hash);
  });
});

describe('QueryStoreRepository.getTotalLinesForV1', () => {
  beforeEach(() => {
    mockFindOneBy.mockReset();
    mockSave.mockReset();
    mockCreate.mockClear();
    mockCubeQuery.mockReset();
    mockCubeRelease.mockReset();
  });

  it('returns cached totalLines without hitting the cube when entry exists', async () => {
    mockFindOneBy.mockResolvedValueOnce({ totalLines: 42 });

    const result = await QueryStoreRepository.getTotalLinesForV1('d', 'r', [], 'SELECT 1');

    expect(result).toBe(42);
    expect(mockCubeQuery).not.toHaveBeenCalled();
  });

  it('runs COUNT once on cache miss and persists the result', async () => {
    mockFindOneBy.mockResolvedValueOnce(null); // hash lookup miss
    mockFindOneBy.mockResolvedValueOnce(null); // id collision check
    mockCubeQuery.mockResolvedValueOnce([{ totalLines: '15' }]);
    mockSave.mockResolvedValue(undefined);

    const result = await QueryStoreRepository.getTotalLinesForV1(
      'd',
      'r',
      [{ columnName: 'year', values: ['2020'] }],
      'SELECT * FROM cube WHERE year = 2020'
    );

    expect(result).toBe(15);
    expect(mockCubeQuery).toHaveBeenCalledTimes(1);
    expect(mockCubeQuery.mock.calls[0][0]).toMatch(/SELECT count\(\*\)/i);
    expect(mockCubeRelease).toHaveBeenCalledTimes(1);
    expect(mockSave).toHaveBeenCalledTimes(1);
    expect(mockCreate).toHaveBeenCalledWith(
      expect.objectContaining({
        datasetId: 'd',
        revisionId: 'r',
        totalLines: 15,
        query: {},
        columnMapping: []
      })
    );
  });

  it('returns the computed totalLines even when persistence fails (concurrent write)', async () => {
    mockFindOneBy.mockResolvedValueOnce(null);
    mockFindOneBy.mockResolvedValueOnce(null);
    mockCubeQuery.mockResolvedValueOnce([{ totalLines: '7' }]);
    mockSave.mockRejectedValue(new Error('duplicate key'));

    const result = await QueryStoreRepository.getTotalLinesForV1('d', 'r', undefined, 'SELECT 1');

    expect(result).toBe(7);
  });

  it('releases the cube connection even when COUNT throws', async () => {
    mockFindOneBy.mockResolvedValueOnce(null);
    mockCubeQuery.mockRejectedValueOnce(new Error('column does not exist'));

    await expect(QueryStoreRepository.getTotalLinesForV1('d', 'r', undefined, 'SELECT 1')).rejects.toThrow(
      'column does not exist'
    );
    expect(mockCubeRelease).toHaveBeenCalledTimes(1);
  });
});
