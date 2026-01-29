import { Dataset } from '../../src/entities/dataset/dataset';
import { Revision } from '../../src/entities/dataset/revision';
import { DatasetStatus } from '../../src/enums/dataset-status';
import { PublishingStatus } from '../../src/enums/publishing-status';
import { TaskAction } from '../../src/enums/task-action';
import { TaskStatus } from '../../src/enums/task-status';
import { getDatasetStatus, getPublishingStatus } from '../../src/utils/dataset-status';

describe('dataset-status', () => {
  beforeEach(() => {
    jest.useFakeTimers().setSystemTime(new Date('2025-06-01T00:00:00Z'));
  });

  afterEach(() => {
    jest.useRealTimers();
  });

  describe('getDatasetStatus', () => {
    it('should return Archived when archivedAt is in the past', () => {
      const dataset = {
        archivedAt: new Date('2025-05-01'),
        firstPublishedAt: new Date('2025-04-01'),
        publishedRevision: null
      } as unknown as Dataset;

      expect(getDatasetStatus(dataset)).toBe(DatasetStatus.Archived);
    });

    it('should return Archived even when publishedRevision has unpublishedAt', () => {
      const dataset = {
        archivedAt: new Date('2025-05-01'),
        firstPublishedAt: new Date('2025-04-01'),
        publishedRevision: { unpublishedAt: new Date('2025-05-15') }
      } as unknown as Dataset;

      expect(getDatasetStatus(dataset)).toBe(DatasetStatus.Archived);
    });

    it('should return Offline when publishedRevision has unpublishedAt and not archived', () => {
      const dataset = {
        archivedAt: null,
        publishedRevision: { unpublishedAt: new Date('2025-05-01') },
        firstPublishedAt: new Date('2025-04-01')
      } as unknown as Dataset;

      expect(getDatasetStatus(dataset)).toBe(DatasetStatus.Offline);
    });

    it('should return Live when firstPublishedAt is in the past', () => {
      const dataset = {
        archivedAt: null,
        publishedRevision: null,
        firstPublishedAt: new Date('2025-04-01')
      } as unknown as Dataset;

      expect(getDatasetStatus(dataset)).toBe(DatasetStatus.Live);
    });

    it('should return New when firstPublishedAt is in the future', () => {
      const dataset = {
        archivedAt: null,
        publishedRevision: null,
        firstPublishedAt: new Date('2025-07-01')
      } as unknown as Dataset;

      expect(getDatasetStatus(dataset)).toBe(DatasetStatus.New);
    });

    it('should return New when firstPublishedAt is null', () => {
      const dataset = {
        archivedAt: null,
        publishedRevision: null,
        firstPublishedAt: null
      } as unknown as Dataset;

      expect(getDatasetStatus(dataset)).toBe(DatasetStatus.New);
    });

    it('should not return Archived when archivedAt is in the future', () => {
      const dataset = {
        archivedAt: new Date('2025-07-01'),
        publishedRevision: null,
        firstPublishedAt: new Date('2025-04-01')
      } as unknown as Dataset;

      expect(getDatasetStatus(dataset)).toBe(DatasetStatus.Live);
    });
  });

  describe('getPublishingStatus', () => {
    it('should return PendingApproval for a new dataset with open publish request', () => {
      const dataset = {
        archivedAt: null,
        publishedRevision: null,
        firstPublishedAt: null,
        tasks: [{ open: true, action: TaskAction.Publish, status: TaskStatus.Requested }]
      } as unknown as Dataset;
      const revision = {} as unknown as Revision;

      expect(getPublishingStatus(dataset, revision)).toBe(PublishingStatus.PendingApproval);
    });

    it('should return UpdatePendingApproval for a live dataset with open publish request', () => {
      const dataset = {
        archivedAt: null,
        publishedRevision: null,
        firstPublishedAt: new Date('2025-04-01'),
        tasks: [{ open: true, action: TaskAction.Publish, status: TaskStatus.Requested }]
      } as unknown as Dataset;
      const revision = {} as unknown as Revision;

      expect(getPublishingStatus(dataset, revision)).toBe(PublishingStatus.UpdatePendingApproval);
    });

    it('should return ChangesRequested when publish task is rejected', () => {
      const dataset = {
        archivedAt: null,
        publishedRevision: null,
        firstPublishedAt: null,
        tasks: [{ open: true, action: TaskAction.Publish, status: TaskStatus.Rejected }]
      } as unknown as Dataset;
      const revision = {} as unknown as Revision;

      expect(getPublishingStatus(dataset, revision)).toBe(PublishingStatus.ChangesRequested);
    });

    it('should return UnpublishRequested when unpublish task is open', () => {
      const dataset = {
        archivedAt: null,
        publishedRevision: null,
        firstPublishedAt: new Date('2025-04-01'),
        tasks: [{ open: true, action: TaskAction.Unpublish, status: TaskStatus.Requested }]
      } as unknown as Dataset;
      const revision = {} as unknown as Revision;

      expect(getPublishingStatus(dataset, revision)).toBe(PublishingStatus.UnpublishRequested);
    });

    it('should return ArchiveRequested when archive task is open', () => {
      const dataset = {
        archivedAt: null,
        publishedRevision: null,
        firstPublishedAt: new Date('2025-04-01'),
        tasks: [{ open: true, action: TaskAction.Archive, status: TaskStatus.Requested }]
      } as unknown as Dataset;
      const revision = {} as unknown as Revision;

      expect(getPublishingStatus(dataset, revision)).toBe(PublishingStatus.ArchiveRequested);
    });

    it('should return UnarchiveRequested when unarchive task is open', () => {
      const dataset = {
        archivedAt: new Date('2025-05-01'),
        publishedRevision: null,
        firstPublishedAt: new Date('2025-04-01'),
        tasks: [{ open: true, action: TaskAction.Unarchive, status: TaskStatus.Requested }]
      } as unknown as Dataset;
      const revision = {} as unknown as Revision;

      expect(getPublishingStatus(dataset, revision)).toBe(PublishingStatus.UnarchiveRequested);
    });

    it('should return Unpublished for offline dataset with approved revision', () => {
      const dataset = {
        archivedAt: null,
        publishedRevision: { unpublishedAt: new Date('2025-05-01') },
        firstPublishedAt: new Date('2025-04-01'),
        tasks: []
      } as unknown as Dataset;
      const revision = { approvedAt: new Date('2025-05-15') } as unknown as Revision;

      expect(getPublishingStatus(dataset, revision)).toBe(PublishingStatus.UpdateScheduled);
    });

    it('should return Unpublished for offline dataset without approved revision', () => {
      const dataset = {
        archivedAt: null,
        publishedRevision: { unpublishedAt: new Date('2025-05-01') },
        firstPublishedAt: new Date('2025-04-01'),
        tasks: []
      } as unknown as Dataset;
      const revision = { approvedAt: null } as unknown as Revision;

      expect(getPublishingStatus(dataset, revision)).toBe(PublishingStatus.Unpublished);
    });

    it('should return Incomplete for new dataset without approved revision', () => {
      const dataset = {
        archivedAt: null,
        publishedRevision: null,
        firstPublishedAt: null,
        tasks: []
      } as unknown as Dataset;
      const revision = { approvedAt: null } as unknown as Revision;

      expect(getPublishingStatus(dataset, revision)).toBe(PublishingStatus.Incomplete);
    });

    it('should return Scheduled for new dataset with approved revision', () => {
      const dataset = {
        archivedAt: null,
        publishedRevision: null,
        firstPublishedAt: null,
        tasks: []
      } as unknown as Dataset;
      const revision = { approvedAt: new Date('2025-05-15') } as unknown as Revision;

      expect(getPublishingStatus(dataset, revision)).toBe(PublishingStatus.Scheduled);
    });

    it('should return Published for live dataset with approved and published revision', () => {
      const dataset = {
        archivedAt: null,
        publishedRevision: null,
        firstPublishedAt: new Date('2025-04-01'),
        tasks: []
      } as unknown as Dataset;
      const revision = {
        approvedAt: new Date('2025-04-01'),
        publishAt: new Date('2025-05-01')
      } as unknown as Revision;

      expect(getPublishingStatus(dataset, revision)).toBe(PublishingStatus.Published);
    });

    it('should return UpdateScheduled for live dataset with approved but future publishAt', () => {
      const dataset = {
        archivedAt: null,
        publishedRevision: null,
        firstPublishedAt: new Date('2025-04-01'),
        tasks: []
      } as unknown as Dataset;
      const revision = {
        approvedAt: new Date('2025-05-15'),
        publishAt: new Date('2025-07-01')
      } as unknown as Revision;

      expect(getPublishingStatus(dataset, revision)).toBe(PublishingStatus.UpdateScheduled);
    });

    it('should return UpdateIncomplete for live dataset without approved revision', () => {
      const dataset = {
        archivedAt: null,
        publishedRevision: null,
        firstPublishedAt: new Date('2025-04-01'),
        tasks: []
      } as unknown as Dataset;
      const revision = { approvedAt: null } as unknown as Revision;

      expect(getPublishingStatus(dataset, revision)).toBe(PublishingStatus.UpdateIncomplete);
    });

    it('should ignore closed tasks', () => {
      const dataset = {
        archivedAt: null,
        publishedRevision: null,
        firstPublishedAt: null,
        tasks: [{ open: false, action: TaskAction.Publish, status: TaskStatus.Requested }]
      } as unknown as Dataset;
      const revision = { approvedAt: null } as unknown as Revision;

      expect(getPublishingStatus(dataset, revision)).toBe(PublishingStatus.Incomplete);
    });

    it('should handle undefined tasks array', () => {
      const dataset = {
        archivedAt: null,
        publishedRevision: null,
        firstPublishedAt: null,
        tasks: undefined
      } as unknown as Dataset;
      const revision = { approvedAt: null } as unknown as Revision;

      expect(getPublishingStatus(dataset, revision)).toBe(PublishingStatus.Incomplete);
    });
  });
});
