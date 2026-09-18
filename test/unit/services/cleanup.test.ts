// --- Mock setup (must come before imports) ---

jest.mock('../../../src/utils/logger', () => ({
  logger: {
    debug: jest.fn(),
    info: jest.fn(),
    warn: jest.fn(),
    error: jest.fn()
  }
}));

const mockQuery = jest.fn();
const mockRelease = jest.fn().mockResolvedValue(undefined);

jest.mock('../../../src/db/database-manager', () => ({
  dbManager: {
    getCubeDataSource: jest.fn(() => ({
      createQueryRunner: jest.fn(() => ({ query: mockQuery, release: mockRelease }))
    })),
    getPublisherDataSource: jest.fn()
  }
}));

// the lock's own acquire/release behaviour is covered by test/unit/utils/advisory-lock.test.ts;
// here we only care that the wrapped work runs
jest.mock('../../../src/utils/advisory-lock', () => ({
  withAdvisoryLock: (_dataSource: unknown, _lockKey: number, fn: () => Promise<unknown>) => fn()
}));

jest.mock('../../../src/repositories/dataset', () => ({
  DatasetRepository: {
    getActiveRevisionIds: jest.fn()
  }
}));

// --- Imports after mocks ---

import { cleanupSupersededMaterializedViews } from '../../../src/services/cleanup';
import { DatasetRepository } from '../../../src/repositories/dataset';

describe('cleanupSupersededMaterializedViews', () => {
  afterEach(() => jest.clearAllMocks());

  it('drops materialized views for schemas that are not a current draft or published revision', async () => {
    (DatasetRepository.getActiveRevisionIds as jest.Mock).mockResolvedValue(['draft-rev', 'published-rev']);
    mockQuery.mockResolvedValueOnce([
      { schemaname: 'draft-rev', matviewname: 'core_view_mat_en' },
      { schemaname: 'published-rev', matviewname: 'core_view_mat_en' },
      { schemaname: 'old-rev', matviewname: 'core_view_mat_en' },
      { schemaname: 'old-rev', matviewname: 'core_view_mat_cy' }
    ]);

    await cleanupSupersededMaterializedViews();

    const dropCalls = mockQuery.mock.calls.filter(([sql]) => sql.startsWith('DROP MATERIALIZED VIEW'));
    expect(dropCalls).toHaveLength(2);
    expect(dropCalls.map(([sql]) => sql)).toEqual([
      expect.stringContaining('old-rev'),
      expect.stringContaining('old-rev')
    ]);
    expect(dropCalls.some(([sql]) => sql.includes('draft-rev'))).toBe(false);
    expect(dropCalls.some(([sql]) => sql.includes('published-rev'))).toBe(false);
  });

  it('does nothing when every materialized view belongs to a current draft or published revision', async () => {
    (DatasetRepository.getActiveRevisionIds as jest.Mock).mockResolvedValue(['draft-rev']);
    mockQuery.mockResolvedValueOnce([{ schemaname: 'draft-rev', matviewname: 'core_view_mat_en' }]);

    await cleanupSupersededMaterializedViews();

    expect(mockQuery.mock.calls.some(([sql]) => sql.startsWith('DROP MATERIALIZED VIEW'))).toBe(false);
  });

  it('releases the cube query runner even when no stale views are found', async () => {
    (DatasetRepository.getActiveRevisionIds as jest.Mock).mockResolvedValue([]);
    mockQuery.mockResolvedValueOnce([]);

    await cleanupSupersededMaterializedViews();

    expect(mockRelease).toHaveBeenCalledTimes(1);
  });

  it('throws once every drop has been attempted if one or more materialized views failed to drop', async () => {
    (DatasetRepository.getActiveRevisionIds as jest.Mock).mockResolvedValue([]);
    mockQuery
      .mockResolvedValueOnce([
        { schemaname: 'old-rev-1', matviewname: 'core_view_mat_en' },
        { schemaname: 'old-rev-2', matviewname: 'core_view_mat_en' }
      ])
      .mockRejectedValueOnce(new Error('drop failed')) // old-rev-1 drop
      .mockResolvedValueOnce(undefined); // old-rev-2 drop

    await expect(cleanupSupersededMaterializedViews()).rejects.toThrow(/failed to drop 1 of 2/);

    const dropCalls = mockQuery.mock.calls.filter(([sql]) => sql.startsWith('DROP MATERIALIZED VIEW'));
    expect(dropCalls).toHaveLength(2); // both attempted despite the first failing
    expect(mockRelease).toHaveBeenCalledTimes(1); // runner still released
  });
});
