import { Dataset } from '../../src/entities/dataset/dataset';
import { EventLog } from '../../src/entities/event-log';
import {
  flagUpdateTask,
  generateSimulatedEvents,
  omitDatasetUpdates,
  omitRevisionUpdates
} from '../../src/utils/dataset-history';

jest.mock('../../src/utils/uuid', () => ({
  uuidV4: jest.fn().mockReturnValue('mock-uuid')
}));

// Mock EventLog.create since it requires a TypeORM DataSource
jest.spyOn(EventLog, 'create').mockImplementation((props: Partial<EventLog>) => {
  return { ...props } as EventLog;
});

describe('dataset-history', () => {
  beforeEach(() => {
    jest.useFakeTimers().setSystemTime(new Date('2025-06-01T00:00:00Z'));
    jest.clearAllMocks();
  });

  afterEach(() => {
    jest.useRealTimers();
  });

  describe('flagUpdateTask', () => {
    it('should set isUpdate to false when revisionId matches startRevisionId', () => {
      const dataset = { startRevisionId: 'rev-1' } as unknown as Dataset;
      const event = {
        entity: 'task',
        data: { metadata: { revisionId: 'rev-1' } }
      } as unknown as EventLog;

      const result = flagUpdateTask(dataset, event);
      expect(result.data?.isUpdate).toBe(false);
    });

    it('should set isUpdate to true when revisionId differs from startRevisionId', () => {
      const dataset = { startRevisionId: 'rev-1' } as unknown as Dataset;
      const event = {
        entity: 'task',
        data: { metadata: { revisionId: 'rev-2' } }
      } as unknown as EventLog;

      const result = flagUpdateTask(dataset, event);
      expect(result.data?.isUpdate).toBe(true);
    });

    it('should not modify non-task entities', () => {
      const dataset = { startRevisionId: 'rev-1' } as unknown as Dataset;
      const event = {
        entity: 'dataset',
        data: { metadata: { revisionId: 'rev-2' } }
      } as unknown as EventLog;

      const result = flagUpdateTask(dataset, event);
      expect(result.data?.isUpdate).toBeUndefined();
    });

    it('should handle missing data.metadata gracefully', () => {
      const dataset = { startRevisionId: 'rev-1' } as unknown as Dataset;
      const event = {
        entity: 'task',
        data: {}
      } as unknown as EventLog;

      const result = flagUpdateTask(dataset, event);
      expect(result.data?.isUpdate).toBe(true);
    });
  });

  describe('omitDatasetUpdates', () => {
    it('should return false for dataset update events', () => {
      const event = { entity: 'dataset', action: 'update' } as unknown as EventLog;
      expect(omitDatasetUpdates(event)).toBe(false);
    });

    it('should return true for dataset insert events', () => {
      const event = { entity: 'dataset', action: 'insert' } as unknown as EventLog;
      expect(omitDatasetUpdates(event)).toBe(true);
    });

    it('should return true for non-dataset events', () => {
      const event = { entity: 'revision', action: 'update' } as unknown as EventLog;
      expect(omitDatasetUpdates(event)).toBe(true);
    });
  });

  describe('omitRevisionUpdates', () => {
    it('should return false for first revision insert', () => {
      const event = { entity: 'revision', action: 'insert', data: { revisionIndex: 1 } } as unknown as EventLog;
      expect(omitRevisionUpdates(event)).toBe(false);
    });

    it('should return true for non-first revision insert', () => {
      const event = { entity: 'revision', action: 'insert', data: { revisionIndex: 2 } } as unknown as EventLog;
      expect(omitRevisionUpdates(event)).toBe(true);
    });

    it('should return false for revision update events', () => {
      const event = { entity: 'revision', action: 'update' } as unknown as EventLog;
      expect(omitRevisionUpdates(event)).toBe(false);
    });

    it('should return true for other events', () => {
      const event = { entity: 'task', action: 'insert' } as unknown as EventLog;
      expect(omitRevisionUpdates(event)).toBe(true);
    });
  });

  describe('generateSimulatedEvents', () => {
    it('should generate a dataset publish event when firstPublishedAt is in the past', () => {
      const dataset = {
        firstPublishedAt: new Date('2025-04-01'),
        startRevisionId: 'rev-1',
        revisions: [
          {
            id: 'rev-1',
            revisionIndex: 1,
            approvedAt: new Date('2025-03-15'),
            publishAt: new Date('2025-04-01')
          }
        ]
      } as unknown as Dataset;

      const events = generateSimulatedEvents(dataset);

      expect(events).toHaveLength(1);
      expect(events[0].entity).toBe('dataset');
      expect(events[0].action).toBe('publish');
      expect(events[0].id).toBe('simulated-mock-uuid');
    });

    it('should not generate publish event when firstPublishedAt is in the future', () => {
      const dataset = {
        firstPublishedAt: new Date('2025-07-01'),
        startRevisionId: 'rev-1',
        revisions: [{ id: 'rev-1', revisionIndex: 1, approvedAt: new Date('2025-03-15') }]
      } as unknown as Dataset;

      const events = generateSimulatedEvents(dataset);

      expect(events).toHaveLength(0);
    });

    it('should not generate publish event when firstPublishedAt is null', () => {
      const dataset = {
        firstPublishedAt: null,
        startRevisionId: 'rev-1',
        revisions: [{ id: 'rev-1', revisionIndex: 1 }]
      } as unknown as Dataset;

      const events = generateSimulatedEvents(dataset);

      expect(events).toHaveLength(0);
    });

    it('should generate revision publish events for revisionIndex > 1 with past publishAt', () => {
      const dataset = {
        firstPublishedAt: new Date('2025-03-01'),
        startRevisionId: 'rev-1',
        revisions: [
          {
            id: 'rev-1',
            revisionIndex: 1,
            approvedAt: new Date('2025-02-15'),
            publishAt: new Date('2025-03-01')
          },
          {
            id: 'rev-2',
            revisionIndex: 2,
            approvedAt: new Date('2025-04-01'),
            publishAt: new Date('2025-05-01')
          }
        ]
      } as unknown as Dataset;

      const events = generateSimulatedEvents(dataset);

      const revisionEvents = events.filter((e) => e.entity === 'revision');
      expect(revisionEvents).toHaveLength(1);
      expect(revisionEvents[0].action).toBe('publish');
      expect(revisionEvents[0].data?.revisionIndex).toBe(2);
    });

    it('should not generate revision publish events for revisionIndex === 1', () => {
      const dataset = {
        firstPublishedAt: new Date('2025-03-01'),
        startRevisionId: 'rev-1',
        revisions: [
          {
            id: 'rev-1',
            revisionIndex: 1,
            approvedAt: new Date('2025-02-15'),
            publishAt: new Date('2025-03-01')
          }
        ]
      } as unknown as Dataset;

      const events = generateSimulatedEvents(dataset);

      const revisionEvents = events.filter((e) => e.entity === 'revision');
      expect(revisionEvents).toHaveLength(0);
    });

    it('should not generate revision publish events when publishAt is in the future', () => {
      const dataset = {
        firstPublishedAt: new Date('2025-03-01'),
        startRevisionId: 'rev-1',
        revisions: [
          {
            id: 'rev-1',
            revisionIndex: 1,
            approvedAt: new Date('2025-02-15'),
            publishAt: new Date('2025-03-01')
          },
          {
            id: 'rev-2',
            revisionIndex: 2,
            approvedAt: new Date('2025-05-01'),
            publishAt: new Date('2025-07-01')
          }
        ]
      } as unknown as Dataset;

      const events = generateSimulatedEvents(dataset);

      const revisionEvents = events.filter((e) => e.entity === 'revision');
      expect(revisionEvents).toHaveLength(0);
    });
  });
});
