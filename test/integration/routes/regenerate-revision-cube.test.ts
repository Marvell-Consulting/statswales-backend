import request from 'supertest';

import app from '../../../src/app';
import { dbManager } from '../../../src/db/database-manager';
import { initPassport } from '../../../src/middleware/passport-auth';
import { BuildLog } from '../../../src/entities/dataset/build-log';
import { Revision } from '../../../src/entities/dataset/revision';
import { CubeBuildStatus } from '../../../src/enums/cube-build-status';
import { CubeBuildType } from '../../../src/enums/cube-build-type';
import { GroupRole } from '../../../src/enums/group-role';
import { User } from '../../../src/entities/user/user';
import { UserGroup } from '../../../src/entities/user/user-group';
import { UserGroupRole } from '../../../src/entities/user/user-group-role';
import { ensureWorkerDataSources, resetDatabase } from '../../helpers/reset-database';
import { createSmallDataset } from '../../helpers/test-helper';
import { getTestUser, getTestUserGroup } from '../../helpers/get-test-user';
import { getAuthHeader } from '../../helpers/auth-header';
import BlobStorage from '../../../src/services/blob-storage';

jest.mock('../../../src/services/blob-storage');

jest.mock('../../../src/utils/lookup-table-utils', () => ({
  ...jest.requireActual('../../../src/utils/lookup-table-utils'),
  bootstrapCubeBuildProcess: jest.fn().mockResolvedValue(undefined)
}));

jest.mock('../../../src/services/cube-builder', () => ({
  ...jest.requireActual('../../../src/services/cube-builder'),
  createAllCubeFiles: jest.fn().mockResolvedValue(undefined)
}));

import { bootstrapCubeBuildProcess } from '../../../src/utils/lookup-table-utils';
import { createAllCubeFiles } from '../../../src/services/cube-builder';

BlobStorage.prototype.listFiles = jest
  .fn()
  .mockReturnValue([{ name: 'test-data-1.csv', path: 'test/test-data-1.csv', isDirectory: false }]);
BlobStorage.prototype.saveBuffer = jest.fn();

const datasetId = '11111111-1111-4111-8111-111111111111';
const revisionId = '22222222-2222-4222-8222-222222222222';
const dataTableId = '33333333-3333-4333-8333-333333333333';
const user: User = getTestUser('cube test user');
let userGroup = getTestUserGroup('Cube Test Group');

const endpoint = (dsId = datasetId, rvId = revisionId) => `/dataset/${dsId}/revision/by-id/${rvId}`;

const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;

describe('POST /dataset/:dataset_id/revision/by-id/:revision_id', () => {
  beforeAll(async () => {
    await ensureWorkerDataSources();
    await resetDatabase();
    await initPassport(dbManager.getAppDataSource());
    userGroup = await dbManager.getAppDataSource().getRepository(UserGroup).save(userGroup);
    user.groupRoles = [UserGroupRole.create({ group: userGroup, roles: [GroupRole.Editor] })];
    await user.save();
    await createSmallDataset(datasetId, revisionId, dataTableId, user);
  });

  beforeEach(() => {
    jest.clearAllMocks();
    jest.mocked(bootstrapCubeBuildProcess).mockResolvedValue(undefined);
    jest.mocked(createAllCubeFiles).mockResolvedValue(undefined);
  });

  test('returns 401 when no auth header is sent', async () => {
    const res = await request(app).post(endpoint());
    expect(res.status).toBe(401);
  });

  test('returns 404 for an unknown dataset_id', async () => {
    const res = await request(app).post(endpoint('aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaaaaa')).set(getAuthHeader(user));
    expect(res.status).toBe(404);
  });

  test('returns 404 for an unknown revision_id', async () => {
    const res = await request(app)
      .post(endpoint(datasetId, 'aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaaaaa'))
      .set(getAuthHeader(user));
    expect(res.status).toBe(404);
  });

  test('returns 500 when bootstrapCubeBuildProcess throws', async () => {
    jest.mocked(bootstrapCubeBuildProcess).mockRejectedValue(new Error('Schema not found'));
    const res = await request(app).post(endpoint()).set(getAuthHeader(user));
    expect(res.status).toBe(500);
  });

  test('returns 202 with a build_id UUID', async () => {
    const res = await request(app).post(endpoint()).set(getAuthHeader(user));
    expect(res.status).toBe(202);
    expect(res.body).toEqual({ build_id: expect.stringMatching(UUID_PATTERN) });
  });

  test('fires createAllCubeFiles with the returned build_id', async () => {
    const res = await request(app).post(endpoint()).set(getAuthHeader(user));
    expect(res.status).toBe(202);
    expect(createAllCubeFiles).toHaveBeenCalledWith(
      datasetId,
      revisionId,
      expect.anything(),
      CubeBuildType.FullCube,
      expect.objectContaining({ id: res.body.build_id })
    );
  });

  test('creates a BuildLog row in the database for the returned build_id', async () => {
    let createAllCubeFilesPromise: Promise<void> | undefined;

    jest.mocked(createAllCubeFiles).mockImplementation((_datasetId, _revisionId, userId, buildType, _build) => {
      createAllCubeFilesPromise = (async () => {
        const revision = await Revision.findOneBy({ id: _revisionId });
        await BuildLog.startBuild(revision, buildType!, userId);
      })();

      return createAllCubeFilesPromise;
    });

    const res = await request(app).post(endpoint()).set(getAuthHeader(user));
    expect(res.status).toBe(202);

    expect(createAllCubeFilesPromise).toBeDefined();
    await createAllCubeFilesPromise;

    const buildLog = await BuildLog.findOneBy({ id: res.body.build_id });
    expect(buildLog).not.toBeNull();
    expect(buildLog!.status).toBe(CubeBuildStatus.Queued);
    expect(buildLog!.type).toBe(CubeBuildType.FullCube);
    expect(buildLog!.revisionId).toBe(revisionId);
  });
});
