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
const mockRelease = jest.fn();

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

jest.mock('../../../src/repositories/build-log', () => ({
  BuildLogRepository: {
    getStuckBuilds: jest.fn()
  }
}));

jest.mock('../../../src/repositories/dataset', () => ({
  DatasetRepository: {
    getActiveRevisionIds: jest.fn()
  }
}));

jest.mock('node:fs/promises', () => ({
  readdir: jest.fn(),
  stat: jest.fn(),
  unlink: jest.fn()
}));

jest.mock('node:os', () => ({
  tmpdir: jest.fn(() => '/tmp')
}));

// --- Imports after mocks ---

import { readdir, stat, unlink } from 'node:fs/promises';

import {
  cleanupOrphanedCubeBuilds,
  cleanupStaleTempFiles,
  cleanupSupersededMaterializedViews
} from '../../../src/services/cleanup';
import { BuildLogRepository } from '../../../src/repositories/build-log';
import { DatasetRepository } from '../../../src/repositories/dataset';
import { CubeBuildStatus } from '../../../src/enums/cube-build-status';
import { CubeBuildType } from '../../../src/enums/cube-build-type';

describe('cleanupOrphanedCubeBuilds', () => {
  afterEach(() => jest.clearAllMocks());

  const makeBuild = (overrides: Partial<{ id: string; type: CubeBuildType; status: CubeBuildStatus }> = {}) => ({
    id: 'build-id',
    type: CubeBuildType.FullCube,
    status: CubeBuildStatus.Building,
    completeBuild: jest.fn(),
    save: jest.fn(),
    ...overrides
  });

  it('does nothing when there are no stuck builds', async () => {
    (BuildLogRepository.getStuckBuilds as jest.Mock).mockResolvedValue([]);

    await cleanupOrphanedCubeBuilds(1000);

    expect(mockQuery).not.toHaveBeenCalled();
  });

  it('drops the schema and fails the build when a schema-owning build is stuck before rename', async () => {
    const build = makeBuild({ type: CubeBuildType.FullCube, status: CubeBuildStatus.Building });
    (BuildLogRepository.getStuckBuilds as jest.Mock).mockResolvedValue([build]);

    await cleanupOrphanedCubeBuilds(1000);

    expect(mockQuery).toHaveBeenCalledWith(expect.stringContaining('DROP SCHEMA IF EXISTS'));
    expect(mockQuery.mock.calls[0][0]).toContain('build-id');
    expect(build.completeBuild).toHaveBeenCalledWith(CubeBuildStatus.Failed, undefined, expect.any(String));
    expect(build.save).toHaveBeenCalled();
  });

  it('does not drop a schema for a build stuck after the schema was already renamed', async () => {
    const build = makeBuild({ type: CubeBuildType.FullCube, status: CubeBuildStatus.Materializing });
    (BuildLogRepository.getStuckBuilds as jest.Mock).mockResolvedValue([build]);

    await cleanupOrphanedCubeBuilds(1000);

    expect(mockQuery).not.toHaveBeenCalled();
    expect(build.completeBuild).toHaveBeenCalledWith(CubeBuildStatus.Failed, undefined, expect.any(String));
  });

  it('does not attempt a schema drop for bulk orchestrator build types', async () => {
    const build = makeBuild({ type: CubeBuildType.AllCubes, status: CubeBuildStatus.Building });
    (BuildLogRepository.getStuckBuilds as jest.Mock).mockResolvedValue([build]);

    await cleanupOrphanedCubeBuilds(1000);

    expect(mockQuery).not.toHaveBeenCalled();
    expect(build.completeBuild).toHaveBeenCalledWith(CubeBuildStatus.Failed, undefined, expect.any(String));
  });
});

describe('cleanupStaleTempFiles', () => {
  afterEach(() => jest.clearAllMocks());

  it('only removes files matching the AV-scanner naming pattern that are older than the cutoff', async () => {
    (readdir as jest.Mock).mockResolvedValue([
      'a1b2c3d4e5f60718293a4b5c6d7e8f90', // matches pattern, stale
      'a1b2c3d4e5f60718293a4b5c6d7e8f91', // matches pattern, fresh
      'not-a-hex-name.txt', // does not match pattern
      'duckdb_temp' // does not match pattern
    ]);

    const now = Date.now();
    (stat as jest.Mock).mockImplementation((filePath: string) => {
      if (filePath.endsWith('f90')) {
        return Promise.resolve({ isFile: () => true, mtimeMs: now - 1000 });
      }
      return Promise.resolve({ isFile: () => true, mtimeMs: now });
    });

    await cleanupStaleTempFiles(500);

    expect(unlink).toHaveBeenCalledTimes(1);
    expect((unlink as jest.Mock).mock.calls[0][0]).toContain('f90');
  });

  it('does not throw when the temp directory cannot be read', async () => {
    (readdir as jest.Mock).mockRejectedValue(new Error('permission denied'));

    await expect(cleanupStaleTempFiles(500)).resolves.not.toThrow();
    expect(unlink).not.toHaveBeenCalled();
  });
});

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
});
