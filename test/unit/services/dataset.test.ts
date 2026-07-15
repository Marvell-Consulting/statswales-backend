import { QueryFailedError } from 'typeorm';

import { Locale } from '../../../src/enums/locale';
import { TaskAction } from '../../../src/enums/task-action';
import { TaskStatus } from '../../../src/enums/task-status';
import { PublishingStatus } from '../../../src/enums/publishing-status';
import { CubeBuildStatus } from '../../../src/enums/cube-build-status';
import { BadRequestException } from '../../../src/exceptions/bad-request.exception';
import { uuidV4 } from '../../../src/utils/uuid';
import { User } from '../../../src/entities/user/user';
import { StorageService } from '../../../src/interfaces/storage-service';

jest.mock('../../../src/utils/logger', () => ({
  logger: { debug: jest.fn(), info: jest.fn(), error: jest.fn(), warn: jest.fn(), trace: jest.fn() }
}));

// --- DatasetRepository ---
const mockDatasetCreate = jest.fn();
const mockDatasetSave = jest.fn();
const mockDatasetGetById = jest.fn();
const mockDatasetFindOneOrFail = jest.fn();
const mockDatasetFindOneByOrFail = jest.fn();
const mockDatasetReplaceFactTable = jest.fn();
const mockDatasetPublish = jest.fn();
jest.mock('../../../src/repositories/dataset', () => ({
  DatasetRepository: {
    create: (...a: unknown[]) => mockDatasetCreate(...a),
    save: (...a: unknown[]) => mockDatasetSave(...a),
    getById: (...a: unknown[]) => mockDatasetGetById(...a),
    findOneOrFail: (...a: unknown[]) => mockDatasetFindOneOrFail(...a),
    findOneByOrFail: (...a: unknown[]) => mockDatasetFindOneByOrFail(...a),
    replaceFactTable: (...a: unknown[]) => mockDatasetReplaceFactTable(...a),
    publish: (...a: unknown[]) => mockDatasetPublish(...a)
  },
  withDraftAndMetadata: {},
  withDraftAndProviders: {},
  withDraftAndTopics: {}
}));

// --- RevisionRepository ---
const mockRevCreate = jest.fn();
const mockRevSave = jest.fn();
const mockRevCreateMetadata = jest.fn();
const mockRevUpdateMetadata = jest.fn();
const mockRevReplaceDataTable = jest.fn();
const mockRevRevertToDraft = jest.fn();
const mockRevApprovePublication = jest.fn();
const mockRevDeepClone = jest.fn();
const mockRevGetById = jest.fn();
jest.mock('../../../src/repositories/revision', () => ({
  RevisionRepository: {
    create: (...a: unknown[]) => mockRevCreate(...a),
    save: (...a: unknown[]) => mockRevSave(...a),
    createMetadata: (...a: unknown[]) => mockRevCreateMetadata(...a),
    updateMetadata: (...a: unknown[]) => mockRevUpdateMetadata(...a),
    replaceDataTable: (...a: unknown[]) => mockRevReplaceDataTable(...a),
    revertToDraft: (...a: unknown[]) => mockRevRevertToDraft(...a),
    approvePublication: (...a: unknown[]) => mockRevApprovePublication(...a),
    deepCloneRevision: (...a: unknown[]) => mockRevDeepClone(...a),
    getById: (...a: unknown[]) => mockRevGetById(...a)
  }
}));

const mockDimensionSave = jest.fn();
jest.mock('../../../src/repositories/dimension', () => ({
  DimensionRepository: { save: (...a: unknown[]) => mockDimensionSave(...a) }
}));

const mockUserGroupFindOneByOrFail = jest.fn();
jest.mock('../../../src/repositories/user-group', () => ({
  UserGroupRepository: { findOneByOrFail: (...a: unknown[]) => mockUserGroupFindOneByOrFail(...a) }
}));

// --- TaskService (instantiated in the constructor) ---
const mockTaskCreate = jest.fn();
const mockTaskUpdate = jest.fn();
const mockTaskCloseOpenPublishTasks = jest.fn();
const mockTaskWithdrawApproved = jest.fn();
const mockTaskGetTasksForDataset = jest.fn();
jest.mock('../../../src/services/task', () => ({
  TaskService: jest.fn().mockImplementation(() => ({
    create: (...a: unknown[]) => mockTaskCreate(...a),
    update: (...a: unknown[]) => mockTaskUpdate(...a),
    closeOpenPublishTasks: (...a: unknown[]) => mockTaskCloseOpenPublishTasks(...a),
    withdrawApproved: (...a: unknown[]) => mockTaskWithdrawApproved(...a),
    getTasksForDataset: (...a: unknown[]) => mockTaskGetTasksForDataset(...a)
  }))
}));

// --- Entity repositories with getRepository() ---
const mockProviderSave = jest.fn();
const mockProviderRemove = jest.fn();
jest.mock('../../../src/entities/dataset/revision-provider', () => ({
  RevisionProvider: {
    getRepository: () => ({
      save: (...a: unknown[]) => mockProviderSave(...a),
      remove: (...a: unknown[]) => mockProviderRemove(...a)
    })
  }
}));

const mockTopicCreate = jest.fn((...args: unknown[]) => args[0]);
const mockTopicSave = jest.fn();
const mockTopicRemove = jest.fn();
jest.mock('../../../src/entities/dataset/revision-topic', () => ({
  RevisionTopic: {
    getRepository: () => ({
      create: (...a: unknown[]) => mockTopicCreate(...a),
      save: (...a: unknown[]) => mockTopicSave(...a),
      remove: (...a: unknown[]) => mockTopicRemove(...a)
    })
  }
}));

const mockRevMetadataSave = jest.fn();
jest.mock('../../../src/entities/dataset/revision-metadata', () => ({
  RevisionMetadata: { getRepository: () => ({ save: (...a: unknown[]) => mockRevMetadataSave(...a) }) }
}));

const mockEventLogFind = jest.fn();
const mockEventLogRepoFind = jest.fn();
jest.mock('../../../src/entities/event-log', () => ({
  EventLog: {
    find: (...a: unknown[]) => mockEventLogFind(...a),
    getRepository: () => ({ find: (...a: unknown[]) => mockEventLogRepoFind(...a) })
  }
}));

// --- DTOs / helpers ---
jest.mock('../../../src/dtos/revision-provider-dto', () => ({
  RevisionProviderDTO: { toRevisionProvider: (p: unknown) => ({ ...(p as object) }) }
}));

const mockTasklistFromDataset = jest.fn();
jest.mock('../../../src/dtos/tasklist-state-dto', () => ({
  TasklistStateDTO: { fromDataset: (...a: unknown[]) => mockTasklistFromDataset(...a) }
}));

const mockCreateAllCubeFiles = jest.fn();
jest.mock('../../../src/services/cube-builder', () => ({
  createAllCubeFiles: (...a: unknown[]) => mockCreateAllCubeFiles(...a)
}));

const mockBuildLogStartBuild = jest.fn();
jest.mock('../../../src/entities/dataset/build-log', () => ({
  BuildLog: {
    startBuild: (...a: unknown[]) => mockBuildLogStartBuild(...a)
  }
}));

const mockValidateAndUpload = jest.fn();
jest.mock('../../../src/services/incoming-file-processor', () => ({
  validateAndUpload: (...a: unknown[]) => mockValidateAndUpload(...a)
}));

const mockRemoveAllDimensions = jest.fn();
const mockRemoveMeasure = jest.fn();
jest.mock('../../../src/services/dimension-processor', () => ({
  removeAllDimensions: (...a: unknown[]) => mockRemoveAllDimensions(...a),
  removeMeasure: (...a: unknown[]) => mockRemoveMeasure(...a)
}));

const mockIsPublished = jest.fn();
jest.mock('../../../src/utils/revision', () => ({
  isPublished: (...a: unknown[]) => mockIsPublished(...a)
}));

const mockGetPublishingStatus = jest.fn();
jest.mock('../../../src/utils/dataset-status', () => ({
  getPublishingStatus: (...a: unknown[]) => mockGetPublishingStatus(...a)
}));

const mockBootstrapCubeBuildProcess = jest.fn();
jest.mock('../../../src/utils/lookup-table-utils', () => ({
  bootstrapCubeBuildProcess: (...a: unknown[]) => mockBootstrapCubeBuildProcess(...a)
}));

const mockQuery = jest.fn();
const mockRelease = jest.fn();
jest.mock('../../../src/db/database-manager', () => ({
  dbManager: {
    getCubeDataSource: jest.fn().mockReturnValue({
      createQueryRunner: jest.fn().mockReturnValue({
        query: (...a: unknown[]) => mockQuery(...a),
        release: (...a: unknown[]) => mockRelease(...a)
      })
    })
  }
}));

jest.mock('../../../src/utils/dataset-history', () => ({
  omitDatasetUpdates: () => true,
  omitRevisionUpdates: () => true,
  flagUpdateTask: (_ds: unknown, e: unknown) => e,
  generateSimulatedEvents: () => []
}));

// Import after mocks
import { DatasetService } from '../../../src/services/dataset';

// --- Helpers ---
function makeUser(): User {
  return { id: uuidV4(), name: 'test-user' } as unknown as User;
}

function withSave<T extends object>(obj: T): T & { save: jest.Mock } {
  const o = obj as T & { save: jest.Mock };
  o.save = jest.fn().mockResolvedValue(o);
  return o;
}

function makeBuildLog() {
  const build = {
    id: 'build-1',
    status: CubeBuildStatus.Queued as CubeBuildStatus,
    completeBuild: jest.fn(function (this: { status: CubeBuildStatus }, status: CubeBuildStatus) {
      this.status = status;
    }),
    save: jest.fn().mockResolvedValue(undefined)
  };
  return build;
}

describe('DatasetService', () => {
  let service: DatasetService;
  let fileService: StorageService & { delete: jest.Mock };
  let user: User;

  beforeEach(() => {
    jest.clearAllMocks();
    fileService = { delete: jest.fn() } as unknown as StorageService & { delete: jest.Mock };
    service = new DatasetService(Locale.EnglishGb, fileService);
    user = makeUser();
  });

  describe('createNew', () => {
    it('creates a dataset, first revision and metadata then returns the loaded dataset', async () => {
      const dataset = { id: 'ds-1' };
      const firstRev = { id: 'rev-1' };
      mockDatasetCreate.mockReturnValue({ save: jest.fn().mockResolvedValue(dataset) });
      mockRevCreate.mockReturnValue({ save: jest.fn().mockResolvedValue(firstRev) });
      mockRevCreateMetadata.mockResolvedValue(undefined);
      mockDatasetSave.mockResolvedValue(undefined);
      mockDatasetGetById.mockResolvedValue({ id: 'ds-1', loaded: true });

      const result = await service.createNew('My Title', 'group-1', user);

      expect(mockRevCreate).toHaveBeenCalledWith({ dataset, createdBy: user, revisionIndex: 1 });
      expect(mockRevCreateMetadata).toHaveBeenCalledWith(firstRev, 'My Title', Locale.EnglishGb);
      expect(mockDatasetSave).toHaveBeenCalledWith(
        expect.objectContaining({ draftRevision: firstRev, startRevision: firstRev, endRevision: firstRev })
      );
      expect(result).toEqual({ id: 'ds-1', loaded: true });
    });
  });

  describe('getDatasetOverview', () => {
    it('loads the dataset with overview relations', async () => {
      mockDatasetGetById.mockResolvedValue({ id: 'ds-1' });
      const result = await service.getDatasetOverview('ds-1');
      expect(mockDatasetGetById).toHaveBeenCalledWith('ds-1', expect.objectContaining({ tasks: expect.anything() }));
      expect(result).toEqual({ id: 'ds-1' });
    });
  });

  describe('updateMetadata', () => {
    it('updates the draft revision metadata', async () => {
      mockDatasetGetById.mockResolvedValueOnce({ id: 'ds-1', draftRevision: { id: 'rev-1' } });
      mockDatasetGetById.mockResolvedValueOnce({ id: 'ds-1' });

      await service.updateMetadata('ds-1', { title: 'X' } as never);

      expect(mockRevUpdateMetadata).toHaveBeenCalledWith({ id: 'rev-1' }, { title: 'X' });
    });
  });

  describe('updateFactTable', () => {
    it('uploads, replaces and rebuilds the cube for the first revision', async () => {
      mockDatasetGetById.mockResolvedValueOnce({
        id: 'ds-1',
        draftRevision: { id: 'rev-1', revisionIndex: 1 }
      });
      mockDatasetGetById.mockResolvedValueOnce({ id: 'ds-1' });
      mockValidateAndUpload.mockResolvedValue({
        dataTableDescriptions: [{ columnName: 'a', factTableColumn: undefined }]
      });

      await service.updateFactTable('ds-1', { originalname: 'f.csv' } as never, user.id);

      expect(mockRemoveAllDimensions).toHaveBeenCalled();
      expect(mockRemoveMeasure).toHaveBeenCalled();
      expect(mockRevReplaceDataTable).toHaveBeenCalled();
      expect(mockDatasetReplaceFactTable).toHaveBeenCalled();
      expect(mockCreateAllCubeFiles).toHaveBeenCalledWith('ds-1', 'rev-1', user.id);
    });

    it('throws when there is no draft revision', async () => {
      mockDatasetGetById.mockResolvedValueOnce({ id: 'ds-1', draftRevision: null });
      const err = await captureError(service.updateFactTable('ds-1', {} as never));
      expect(err).toBeInstanceOf(BadRequestException);
    });

    it('throws when the draft revision is not the first revision', async () => {
      mockDatasetGetById.mockResolvedValueOnce({ id: 'ds-1', draftRevision: { id: 'rev-1', revisionIndex: 2 } });
      const err = await captureError(service.updateFactTable('ds-1', {} as never));
      expect(err).toBeInstanceOf(BadRequestException);
    });
  });

  describe('addDataProvider', () => {
    it('saves the provider in both languages', async () => {
      mockDatasetGetById.mockResolvedValue({ id: 'ds-1' });

      await service.addDataProvider('ds-1', { language: 'en-gb', groupId: 'g1' } as never);

      const saved = mockProviderSave.mock.calls[0][0];
      expect(saved).toHaveLength(2);
      expect(saved[1].language).toBe('cy-gb');
    });
  });

  describe('updateDataProviders', () => {
    it('removes providers no longer present and updates the rest', async () => {
      mockDatasetGetById.mockResolvedValue({
        id: 'ds-1',
        draftRevision: {
          revisionProviders: [
            { groupId: 'keep', providerId: 'old', providerSourceId: 'old' },
            { groupId: 'remove', providerId: 'x', providerSourceId: 'y' }
          ]
        }
      });

      await service.updateDataProviders('ds-1', [
        { groupId: 'keep', providerId: 'new', providerSourceId: 'newSrc' } as never
      ]);

      expect(mockProviderRemove).toHaveBeenCalledWith([expect.objectContaining({ groupId: 'remove' })]);
      expect(mockProviderSave).toHaveBeenCalledWith([
        expect.objectContaining({ groupId: 'keep', providerId: 'new', providerSourceId: 'newSrc' })
      ]);
    });
  });

  describe('updateTopics', () => {
    it('replaces the existing topic relations', async () => {
      mockDatasetGetById.mockResolvedValue({
        id: 'ds-1',
        draftRevision: { id: 'rev-1', revisionTopics: [{ id: 1 }] }
      });

      await service.updateTopics('ds-1', ['10', '20']);

      expect(mockTopicRemove).toHaveBeenCalledWith([{ id: 1 }]);
      expect(mockTopicSave).toHaveBeenCalledWith([
        { revisionId: 'rev-1', topicId: 10 },
        { revisionId: 'rev-1', topicId: 20 }
      ]);
    });
  });

  describe('updateTranslations', () => {
    it('applies dimension, metadata and link translations', async () => {
      mockDatasetGetById.mockResolvedValue({
        id: 'ds-1',
        draftRevision: {
          id: 'rev-1',
          metadata: [{ language: Locale.EnglishGb }, { language: Locale.WelshGb }],
          relatedLinks: [{ id: 'link-1' }]
        },
        dimensions: [
          {
            factTableColumn: 'col1',
            metadata: [{ language: 'en-GB' }, { language: 'cy-GB' }]
          }
        ],
        measure: { metadata: [] }
      });

      const translations = [
        { type: 'dimension', key: 'col1', english: 'EN', cymraeg: 'CY' },
        { type: 'metadata', key: 'title', english: 'Title', cymraeg: 'Teitl' },
        { type: 'link', key: 'link-1', english: 'Link', cymraeg: 'Dolen' }
      ];

      await service.updateTranslations('ds-1', translations as never);

      expect(mockDimensionSave).toHaveBeenCalled();
      expect(mockRevMetadataSave).toHaveBeenCalled();
      expect(mockRevSave).toHaveBeenCalled();
    });
  });

  describe('submitForPublication', () => {
    it('creates a publish task on first submission', async () => {
      mockDatasetGetById.mockResolvedValue({ id: 'ds-1', draftRevision: { id: 'rev-1' } });
      mockTaskGetTasksForDataset.mockResolvedValue([]); // no rejected task

      await service.submitForPublication('ds-1', 'rev-1', user);

      expect(mockTaskCreate).toHaveBeenCalledWith('ds-1', TaskAction.Publish, user, undefined, { revisionId: 'rev-1' });
    });

    it('reopens a previously rejected publish task', async () => {
      mockDatasetGetById.mockResolvedValue({ id: 'ds-1', draftRevision: { id: 'rev-1' } });
      mockTaskGetTasksForDataset.mockResolvedValue([
        { id: 'task-1', action: TaskAction.Publish, status: TaskStatus.Rejected }
      ]);

      await service.submitForPublication('ds-1', 'rev-1', user);

      expect(mockTaskUpdate).toHaveBeenCalledWith('task-1', TaskStatus.Requested, true, user, null);
      expect(mockTaskCreate).not.toHaveBeenCalled();
    });

    it('is a no-op when a pending publish task already exists (duplicate submission)', async () => {
      mockDatasetGetById.mockResolvedValue({ id: 'ds-1', draftRevision: { id: 'rev-1' } });
      mockTaskGetTasksForDataset.mockResolvedValue([
        { id: 'task-1', action: TaskAction.Publish, status: TaskStatus.Requested }
      ]);

      await service.submitForPublication('ds-1', 'rev-1', user);

      expect(mockTaskCreate).not.toHaveBeenCalled();
      expect(mockTaskUpdate).not.toHaveBeenCalled();
    });

    it('swallows a unique-constraint violation from a concurrent submission', async () => {
      mockDatasetGetById.mockResolvedValue({ id: 'ds-1', draftRevision: { id: 'rev-1' } });
      mockTaskGetTasksForDataset.mockResolvedValue([]);
      mockTaskCreate.mockRejectedValueOnce(
        new QueryFailedError('INSERT', undefined, new Error('duplicate key value violates unique constraint'))
      );

      await expect(service.submitForPublication('ds-1', 'rev-1', user)).resolves.toBeUndefined();
    });

    it('rethrows errors from task creation that are not unique-constraint violations', async () => {
      mockDatasetGetById.mockResolvedValue({ id: 'ds-1', draftRevision: { id: 'rev-1' } });
      mockTaskGetTasksForDataset.mockResolvedValue([]);
      mockTaskCreate.mockRejectedValueOnce(new Error('database exploded'));

      const err = await captureError(service.submitForPublication('ds-1', 'rev-1', user));
      expect(err).toBeInstanceOf(Error);
      expect((err as Error).message).toBe('database exploded');
    });

    it('throws when the revision id does not match the draft revision', async () => {
      mockDatasetGetById.mockResolvedValue({ id: 'ds-1', draftRevision: { id: 'other' } });
      const err = await captureError(service.submitForPublication('ds-1', 'rev-1', user));
      expect(err).toBeInstanceOf(BadRequestException);
    });
  });

  describe('withdrawFromPublication', () => {
    it('closes every open publish task and deletes the online cube file', async () => {
      mockDatasetGetById.mockResolvedValue({ id: 'ds-1', endRevision: {}, tasks: [] });
      mockGetPublishingStatus.mockReturnValue(PublishingStatus.PendingApproval);
      mockRevRevertToDraft.mockResolvedValue({ id: 'rev-1', onlineCubeFilename: 'cube.duckdb' });
      mockTaskGetTasksForDataset.mockResolvedValue([
        { id: 'task-1', action: TaskAction.Publish, status: TaskStatus.Requested },
        { id: 'task-2', action: TaskAction.Publish, status: TaskStatus.Requested }
      ]);

      await service.withdrawFromPublication('ds-1', 'rev-1', user);

      expect(fileService.delete).toHaveBeenCalledWith('cube.duckdb', 'ds-1');
      expect(mockTaskCloseOpenPublishTasks).toHaveBeenCalledWith('ds-1', user);
      expect(mockTaskWithdrawApproved).not.toHaveBeenCalled();
    });

    it('withdraws an approved (but unpublished) publication when there is no open publish task', async () => {
      mockDatasetGetById.mockResolvedValue({ id: 'ds-1', endRevision: {}, tasks: [] });
      mockGetPublishingStatus.mockReturnValue(PublishingStatus.Scheduled);
      mockRevRevertToDraft.mockResolvedValue({ id: 'rev-1' });
      mockTaskGetTasksForDataset.mockResolvedValue([]);

      await service.withdrawFromPublication('ds-1', 'rev-1', user);

      expect(mockTaskWithdrawApproved).toHaveBeenCalledWith('ds-1', 'rev-1', user);
      expect(mockTaskCloseOpenPublishTasks).not.toHaveBeenCalled();
    });

    it('throws when there is no pending publication to withdraw', async () => {
      mockDatasetGetById.mockResolvedValue({ id: 'ds-1', endRevision: {}, tasks: [] });
      mockGetPublishingStatus.mockReturnValue(PublishingStatus.Published);
      const err = await captureError(service.withdrawFromPublication('ds-1', 'rev-1', user));
      expect(err).toBeInstanceOf(BadRequestException);
    });
  });

  describe('approvePublication', () => {
    let build: ReturnType<typeof makeBuildLog>;

    beforeEach(() => {
      build = makeBuildLog();
      mockBuildLogStartBuild.mockResolvedValue(build);
    });

    it('bootstraps, builds, approves and publishes', async () => {
      mockRevApprovePublication.mockResolvedValue({ id: 'rev-1' });
      mockDatasetPublish.mockResolvedValue({ id: 'ds-1', published: true });

      const result = await service.approvePublication('ds-1', 'rev-1', user);

      expect(mockBootstrapCubeBuildProcess).toHaveBeenCalledWith('ds-1', 'rev-1');
      expect(mockCreateAllCubeFiles).toHaveBeenCalled();
      expect(result).toEqual({ id: 'ds-1', published: true });
    });

    it('marks the build failed and throws a generic 500 when the cube build fails, without publishing', async () => {
      mockCreateAllCubeFiles.mockRejectedValueOnce(new Error('duplicate key value violates unique constraint'));

      await expect(service.approvePublication('ds-1', 'rev-1', user)).rejects.toMatchObject({
        message: 'errors.cube_builder.cube_build_failed',
        status: 500
      });

      expect(build.completeBuild).toHaveBeenCalledWith(CubeBuildStatus.Failed, undefined, expect.any(String));
      expect(build.save).toHaveBeenCalled();
      // the raw Postgres error must not leak to the publisher
      expect(mockRevApprovePublication).not.toHaveBeenCalled();
      expect(mockDatasetPublish).not.toHaveBeenCalled();
    });

    it('records a failed build when the bootstrap step fails (before the build proper)', async () => {
      mockBootstrapCubeBuildProcess.mockRejectedValueOnce(new Error('invalid input syntax for type bigint'));

      await expect(service.approvePublication('ds-1', 'rev-1', user)).rejects.toMatchObject({
        message: 'errors.cube_builder.cube_build_failed',
        status: 500
      });

      expect(mockCreateAllCubeFiles).not.toHaveBeenCalled();
      expect(build.completeBuild).toHaveBeenCalledWith(CubeBuildStatus.Failed, undefined, expect.any(String));
      expect(build.save).toHaveBeenCalled();
    });
  });

  describe('rejectPublication', () => {
    it('reverts to draft and deletes the online cube file when present', async () => {
      mockRevRevertToDraft.mockResolvedValue({ id: 'rev-1', onlineCubeFilename: 'cube.duckdb' });
      await service.rejectPublication('ds-1', 'rev-1');
      expect(fileService.delete).toHaveBeenCalledWith('cube.duckdb', 'ds-1');
    });

    it('reverts to draft without deleting when no online cube file', async () => {
      mockRevRevertToDraft.mockResolvedValue({ id: 'rev-1' });
      await service.rejectPublication('ds-1', 'rev-1');
      expect(fileService.delete).not.toHaveBeenCalled();
    });
  });

  describe('createRevision', () => {
    it('clones the published revision into a new draft', async () => {
      mockDatasetFindOneOrFail.mockResolvedValue({
        revisions: [{ id: 'r1' }],
        publishedRevision: { id: 'pub-1' }
      });
      mockIsPublished.mockReturnValue(true);
      mockRevDeepClone.mockResolvedValue({ id: 'new-rev' });
      mockDatasetGetById.mockResolvedValue({ id: 'ds-1' });

      await service.createRevision('ds-1', user);

      expect(mockRevDeepClone).toHaveBeenCalledWith('pub-1', user);
      expect(mockDatasetSave).toHaveBeenCalledWith(
        expect.objectContaining({ draftRevision: { id: 'new-rev' }, endRevision: { id: 'new-rev' } })
      );
    });

    it('throws when an unpublished (draft) revision already exists', async () => {
      mockDatasetFindOneOrFail.mockResolvedValue({ revisions: [{ id: 'r1' }], publishedRevision: { id: 'pub-1' } });
      mockIsPublished.mockReturnValue(false);
      const err = await captureError(service.createRevision('ds-1', user));
      expect(err).toBeInstanceOf(BadRequestException);
    });
  });

  describe('deleteDraftRevision', () => {
    it('drops the schema, deletes the data table and removes the revision', async () => {
      const draft = {
        id: 'rev-1',
        dataTable: { id: 'dt-1' },
        previousRevision: { id: 'prev' },
        remove: jest.fn()
      };
      const dataset = withSave({ id: 'ds-1', draftRevision: draft, endRevision: undefined });
      mockDatasetGetById.mockResolvedValue(dataset);

      await service.deleteDraftRevision('ds-1', 'rev-1');

      expect(mockQuery).toHaveBeenCalledWith(expect.stringContaining('DROP SCHEMA'));
      expect(mockQuery).toHaveBeenCalledWith(expect.stringContaining('DROP TABLE'));
      expect(fileService.delete).toHaveBeenCalledWith('dt-1', 'ds-1');
      expect(mockRelease).toHaveBeenCalled();
      expect(dataset.save).toHaveBeenCalled();
      expect(draft.remove).toHaveBeenCalled();
    });

    it('throws when the draft revision id does not match', async () => {
      mockDatasetGetById.mockResolvedValue({ id: 'ds-1', draftRevision: null });
      const err = await captureError(service.deleteDraftRevision('ds-1', 'rev-1'));
      expect(err).toBeInstanceOf(BadRequestException);
    });
  });

  describe('getTasklistState', () => {
    it('builds the tasklist state including the previous revision', async () => {
      mockDatasetGetById.mockResolvedValue({ id: 'ds-1', draftRevisionId: 'rev-1' });
      mockRevGetById
        .mockResolvedValueOnce({ id: 'rev-1', previousRevisionId: 'prev' })
        .mockResolvedValueOnce({ id: 'prev' });
      mockEventLogRepoFind.mockResolvedValue([]);
      mockTasklistFromDataset.mockReturnValue({ state: 'ok' });

      const result = await service.getTasklistState('ds-1', Locale.EnglishGb);

      expect(mockTasklistFromDataset).toHaveBeenCalled();
      expect(result).toEqual({ state: 'ok' });
    });

    it('throws when the draft revision cannot be loaded', async () => {
      mockDatasetGetById.mockResolvedValue({ id: 'ds-1', draftRevisionId: 'rev-1' });
      mockRevGetById.mockResolvedValueOnce(null);
      const err = await captureError(service.getTasklistState('ds-1', Locale.EnglishGb));
      expect(err).toBeInstanceOf(BadRequestException);
    });
  });

  describe('updateDatasetGroup', () => {
    it('assigns the dataset to the new user group', async () => {
      const dataset = withSave({ id: 'ds-1', userGroupId: 'old' });
      mockDatasetFindOneByOrFail.mockResolvedValue(dataset);
      mockUserGroupFindOneByOrFail.mockResolvedValue({ id: 'group-2' });

      await service.updateDatasetGroup('ds-1', 'group-2');

      expect(dataset.userGroupId).toBe('group-2');
      expect(dataset.save).toHaveBeenCalled();
    });
  });

  describe('task queries', () => {
    it('getOpenTasks requests only open tasks', async () => {
      mockTaskGetTasksForDataset.mockResolvedValue([]);
      await service.getOpenTasks('ds-1');
      expect(mockTaskGetTasksForDataset).toHaveBeenCalledWith('ds-1', true);
    });

    it('getAllTasks requests all tasks', async () => {
      mockTaskGetTasksForDataset.mockResolvedValue([]);
      await service.getAllTasks('ds-1');
      expect(mockTaskGetTasksForDataset).toHaveBeenCalledWith('ds-1');
    });

    it('getPendingPublishTask finds the requested publish task', async () => {
      mockTaskGetTasksForDataset.mockResolvedValue([
        { action: TaskAction.Publish, status: TaskStatus.Requested, id: 'task-1' }
      ]);
      const result = await service.getPendingPublishTask('ds-1');
      expect(result).toEqual(expect.objectContaining({ id: 'task-1' }));
    });

    it('getRejectedPublishTask finds the rejected publish task', async () => {
      mockTaskGetTasksForDataset.mockResolvedValue([
        { action: TaskAction.Publish, status: TaskStatus.Rejected, id: 'task-2' }
      ]);
      const result = await service.getRejectedPublishTask('ds-1');
      expect(result).toEqual(expect.objectContaining({ id: 'task-2' }));
    });
  });

  describe('getHistory', () => {
    it('combines event log and simulated events sorted descending', async () => {
      mockDatasetGetById.mockResolvedValue({ id: 'ds-1', revisions: [{ id: 'r1' }, { id: 'r2' }] });
      mockEventLogFind.mockResolvedValue([
        { entity: 'task', createdAt: new Date('2024-01-01') },
        { entity: 'revision', createdAt: new Date('2024-02-01') }
      ]);

      const result = await service.getHistory('ds-1');

      expect(mockEventLogFind).toHaveBeenCalled();
      expect(result).toHaveLength(2);
      // sorted descending: Feb before Jan
      expect(result[0].createdAt).toEqual(new Date('2024-02-01'));
    });
  });

  describe('approveUnpublish', () => {
    it('marks the published revision unpublished and creates a fresh draft', async () => {
      mockDatasetGetById
        .mockResolvedValueOnce({ id: 'ds-1', publishedRevision: { id: 'pub-1' } }) // initial load
        .mockResolvedValueOnce({ id: 'ds-1', draftRevision: { id: 'new-rev' } }); // from createRevision
      mockDatasetFindOneOrFail.mockResolvedValue({ revisions: [{ id: 'r1' }], publishedRevision: { id: 'pub-1' } });
      mockIsPublished.mockReturnValue(true);
      mockRevDeepClone.mockResolvedValue({ id: 'new-rev' });

      await service.approveUnpublish('ds-1', user);

      expect(mockRevSave).toHaveBeenCalledWith(
        expect.objectContaining({ id: 'pub-1', unpublishedAt: expect.any(Date) })
      );
      expect(mockCreateAllCubeFiles).toHaveBeenCalledWith('ds-1', 'new-rev', user.id);
    });

    it('throws when the dataset has no published revision', async () => {
      mockDatasetGetById.mockResolvedValue({ id: 'ds-1', publishedRevision: null });
      const err = await captureError(service.approveUnpublish('ds-1', user));
      expect(err).toBeInstanceOf(Error);
    });
  });
});

async function captureError(p: Promise<unknown>): Promise<unknown> {
  try {
    await p;
    return undefined;
  } catch (e) {
    return e;
  }
}
