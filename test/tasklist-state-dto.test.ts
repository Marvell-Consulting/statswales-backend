import { TasklistStateDTO } from '../src/dtos/tasklist-state-dto';
import { TaskListStatus } from '../src/enums/task-list-status';
import { DimensionType } from '../src/enums/dimension-type';
import { Dataset } from '../src/entities/dataset/dataset';
import { Revision } from '../src/entities/dataset/revision';
import { EventLog } from '../src/entities/event-log';

// Mock dependencies
jest.mock('../src/utils/collect-translations');

const mockCollectTranslations = jest.requireMock('../src/utils/collect-translations');

describe('TasklistStateDTO', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    mockCollectTranslations.collectTranslations = jest.fn();
  });

  describe('dataTableStatus', () => {
    it('should return NotStarted when revision has no dataTable', () => {
      const dataset = {} as Dataset;
      const revision = { previousRevisionId: null, dataTable: null } as unknown as Revision;

      const result = TasklistStateDTO.dataTableStatus(dataset, revision);

      expect(result).toBe(TaskListStatus.NotStarted);
    });

    it('should return Incomplete when dataset has no dimensions', () => {
      const dataset = { dimensions: [] } as unknown as Dataset;
      const revision = {
        previousRevisionId: null,
        dataTable: { uploadedAt: new Date() }
      } as unknown as Revision;

      const result = TasklistStateDTO.dataTableStatus(dataset, revision);

      expect(result).toBe(TaskListStatus.Incomplete);
    });

    it('should return Completed when dataset has dimensions', () => {
      const dataset = {
        dimensions: [{ id: '1', type: DimensionType.Text }]
      } as Dataset;
      const revision = {
        previousRevisionId: null,
        dataTable: { uploadedAt: new Date() }
      } as unknown as Revision;

      const result = TasklistStateDTO.dataTableStatus(dataset, revision);

      expect(result).toBe(TaskListStatus.Completed);
    });

    it('should return Updated when revision is update and dataTable uploaded after revision creation', () => {
      const dataset = {} as Dataset;
      const revisionCreatedAt = new Date('2023-01-01');
      const dataTableUploadedAt = new Date('2023-01-02');
      const revision = {
        previousRevisionId: 'prev-123',
        createdAt: revisionCreatedAt,
        dataTable: { uploadedAt: dataTableUploadedAt }
      } as Revision;

      const result = TasklistStateDTO.dataTableStatus(dataset, revision);

      expect(result).toBe(TaskListStatus.Updated);
    });

    it('should return Unchanged when revision is update and dataTable uploaded before revision creation', () => {
      const dataset = {} as Dataset;
      const revisionCreatedAt = new Date('2023-01-02');
      const dataTableUploadedAt = new Date('2023-01-01');
      const revision = {
        previousRevisionId: 'prev-123',
        createdAt: revisionCreatedAt,
        dataTable: { uploadedAt: dataTableUploadedAt }
      } as Revision;

      const result = TasklistStateDTO.dataTableStatus(dataset, revision);

      expect(result).toBe(TaskListStatus.Unchanged);
    });

    it('should return Unchanged when revision is update and no dataTable uploadedAt', () => {
      const dataset = {} as Dataset;
      const revision = {
        previousRevisionId: 'prev-123',
        createdAt: new Date(),
        dataTable: {}
      } as Revision;

      const result = TasklistStateDTO.dataTableStatus(dataset, revision);

      expect(result).toBe(TaskListStatus.Unchanged);
    });
  });

  describe('measureStatus', () => {
    it('should return undefined when dataset has no measure', () => {
      const dataset = { measure: null } as unknown as Dataset;
      const revision = {} as Revision;

      const result = TasklistStateDTO.measureStatus(dataset, revision, 'en');

      expect(result).toBeUndefined();
    });

    it('should return status with measure name from metadata', () => {
      const dataset = {
        measure: {
          id: 'measure-1',
          factTableColumn: 'value',
          joinColumn: 'measure_col',
          metadata: [{ language: 'en', name: 'Test Measure' }]
        }
      } as Dataset;
      const revision = { previousRevisionId: null } as unknown as Revision;

      const result = TasklistStateDTO.measureStatus(dataset, revision, 'en');

      expect(result).toEqual({
        type: 'measure',
        id: 'measure-1',
        name: 'Test Measure',
        status: TaskListStatus.Completed
      });
    });

    it('should use factTableColumn as name when no metadata found', () => {
      const dataset = {
        measure: {
          id: 'measure-1',
          factTableColumn: 'value',
          joinColumn: 'measure_col',
          metadata: []
        }
      } as unknown as Dataset;
      const revision = { previousRevisionId: null } as unknown as Revision;

      const result = TasklistStateDTO.measureStatus(dataset, revision, 'en');

      expect(result).toEqual({
        type: 'measure',
        id: 'measure-1',
        name: 'value',
        status: TaskListStatus.Completed
      });
    });

    it('should return NotStarted status when no joinColumn', () => {
      const dataset = {
        measure: {
          id: 'measure-1',
          factTableColumn: 'value',
          joinColumn: null,
          metadata: []
        }
      } as unknown as Dataset;
      const revision = { previousRevisionId: null } as unknown as Revision;

      const result = TasklistStateDTO.measureStatus(dataset, revision, 'en');

      expect(result).toEqual({
        type: 'measure',
        id: 'measure-1',
        name: 'value',
        status: TaskListStatus.NotStarted
      });
    });

    it('should return Unchanged status for updates', () => {
      const dataset = {
        measure: {
          id: 'measure-1',
          factTableColumn: 'value',
          joinColumn: null,
          metadata: []
        }
      } as unknown as Dataset;
      const revision = { previousRevisionId: 'prev-123' } as Revision;

      const result = TasklistStateDTO.measureStatus(dataset, revision, 'en');

      expect(result).toEqual({
        type: 'measure',
        id: 'measure-1',
        name: 'value',
        status: TaskListStatus.Unchanged
      });
    });
  });

  describe('dimensionStatus', () => {
    it('should filter out NoteCodes dimensions', () => {
      const dataset = {
        dimensions: [
          {
            id: 'dim-1',
            type: DimensionType.NoteCodes,
            factTableColumn: 'notes',
            metadata: []
          },
          {
            id: 'dim-2',
            type: DimensionType.Text,
            factTableColumn: 'category',
            extractor: { id: 'ext-1' },
            metadata: [{ language: 'en', name: 'Category' }]
          }
        ]
      } as Dataset;
      const revision = { previousRevisionId: null } as unknown as Revision;

      const result = TasklistStateDTO.dimensionStatus(dataset, revision, 'en');

      expect(result).toHaveLength(1);
      expect(result[0].id).toBe('dim-2');
    });

    it('should return Completed status when dimension has extractor', () => {
      const dataset = {
        dimensions: [
          {
            id: 'dim-1',
            type: DimensionType.Text,
            factTableColumn: 'category',
            extractor: { id: 'ext-1' },
            metadata: [{ language: 'en', name: 'Category' }]
          }
        ]
      } as Dataset;
      const revision = { previousRevisionId: null } as unknown as Revision;

      const result = TasklistStateDTO.dimensionStatus(dataset, revision, 'en');

      expect(result[0]).toEqual({
        id: 'dim-1',
        name: 'Category',
        status: TaskListStatus.Completed,
        type: DimensionType.Text
      });
    });

    it('should return NotStarted status when dimension has no extractor', () => {
      const dataset = {
        dimensions: [
          {
            id: 'dim-1',
            type: DimensionType.Text,
            factTableColumn: 'category',
            extractor: null,
            metadata: [{ language: 'en', name: 'Category' }]
          }
        ]
      } as Dataset;
      const revision = { previousRevisionId: null } as unknown as Revision;

      const result = TasklistStateDTO.dimensionStatus(dataset, revision, 'en');

      expect(result[0].status).toBe(TaskListStatus.NotStarted);
    });

    it('should use factTableColumn when no metadata name found', () => {
      const dataset = {
        dimensions: [
          {
            id: 'dim-1',
            type: DimensionType.Text,
            factTableColumn: 'category',
            extractor: { id: 'ext-1' },
            metadata: []
          }
        ]
      } as unknown as Dataset;
      const revision = { previousRevisionId: null } as unknown as Revision;

      const result = TasklistStateDTO.dimensionStatus(dataset, revision, 'en');

      expect(result[0].name).toBe('category');
    });

    it('should handle update scenarios with lookup table updates', () => {
      const dataset = {
        dimensions: [
          {
            id: 'dim-1',
            type: DimensionType.Text,
            factTableColumn: 'category',
            extractor: { id: 'ext-1' },
            metadata: [{ language: 'en', name: 'Category' }]
          }
        ]
      } as Dataset;
      const revision = {
        previousRevisionId: 'prev-123',
        tasks: {
          dimensions: [{ id: 'dim-1', lookupTableUpdated: true }]
        }
      } as Revision;

      const result = TasklistStateDTO.dimensionStatus(dataset, revision, 'en');

      expect(result[0].status).toBe(TaskListStatus.Updated);
    });

    it('should return NotStarted for update tasks with no lookup table update', () => {
      const dataset = {
        dimensions: [
          {
            id: 'dim-1',
            type: DimensionType.Text,
            factTableColumn: 'category',
            extractor: { id: 'ext-1' },
            metadata: [{ language: 'en', name: 'Category' }]
          }
        ]
      } as Dataset;
      const revision = {
        previousRevisionId: 'prev-123',
        tasks: {
          dimensions: [{ id: 'dim-1', lookupTableUpdated: false }]
        }
      } as Revision;

      const result = TasklistStateDTO.dimensionStatus(dataset, revision, 'en');

      expect(result[0].status).toBe(TaskListStatus.NotStarted);
    });
  });

  describe('metadataStatus', () => {
    it('should throw error when metadata not found for language', () => {
      const revision = {
        metadata: [{ language: 'cy', title: 'Welsh Title' }]
      } as Revision;

      expect(() => {
        TasklistStateDTO.metadataStatus(revision, 'en');
      }).toThrow('Cannot generate tasklist state - metadata not found for language en');
    });

    it('should return completed status for all fields when metadata exists', () => {
      const revision = {
        previousRevisionId: null,
        metadata: [
          {
            language: 'en',
            title: 'Test Title',
            summary: 'Test Summary',
            quality: 'Test Quality',
            collection: 'Test Collection'
          }
        ],
        updateFrequency: { id: 'freq-1' },
        designation: 'official',
        revisionProviders: [{ id: 'prov-1' }],
        revisionTopics: [{ topicId: 'topic-1' }],
        relatedLinks: [{ id: 'link-1' }]
      } as unknown as Revision;

      const result = TasklistStateDTO.metadataStatus(revision, 'en');

      expect(result).toEqual({
        title: TaskListStatus.Completed,
        summary: TaskListStatus.Completed,
        quality: TaskListStatus.Completed,
        collection: TaskListStatus.Completed,
        frequency: TaskListStatus.Completed,
        designation: TaskListStatus.Completed,
        sources: TaskListStatus.Completed,
        topics: TaskListStatus.Completed,
        related: TaskListStatus.Completed
      });
    });

    it('should return not started status for missing fields', () => {
      const revision = {
        previousRevisionId: null,
        metadata: [
          {
            language: 'en',
            title: null,
            summary: null,
            quality: null,
            collection: null
          }
        ],
        updateFrequency: null,
        designation: null,
        revisionProviders: [],
        revisionTopics: [],
        relatedLinks: []
      } as unknown as Revision;

      const result = TasklistStateDTO.metadataStatus(revision, 'en');

      expect(result).toEqual({
        title: TaskListStatus.NotStarted,
        summary: TaskListStatus.NotStarted,
        quality: TaskListStatus.NotStarted,
        collection: TaskListStatus.NotStarted,
        frequency: TaskListStatus.NotStarted,
        designation: TaskListStatus.NotStarted,
        sources: TaskListStatus.NotStarted,
        topics: TaskListStatus.NotStarted,
        related: TaskListStatus.NotStarted
      });
    });

    it('should handle update scenarios and compare with previous revision', () => {
      const previousRevision = {
        metadata: [
          {
            language: 'en',
            title: 'Old Title',
            summary: 'Old Summary',
            quality: 'Old Quality',
            collection: 'Old Collection'
          }
        ],
        updateFrequency: { id: 'old-freq' },
        designation: 'old-designation',
        revisionProviders: [{ providerId: 'prov-1', providerSourceId: 'source-1', language: 'en' }],
        revisionTopics: [{ topicId: 'topic-1' }],
        relatedLinks: [{ id: 'link-1' }]
      };

      const revision = {
        previousRevisionId: 'prev-123',
        previousRevision,
        metadata: [
          {
            language: 'en',
            title: 'New Title',
            summary: 'Old Summary',
            quality: 'Old Quality',
            collection: 'Old Collection'
          }
        ],
        updateFrequency: { id: 'old-freq' },
        designation: 'old-designation',
        revisionProviders: [{ providerId: 'prov-1', providerSourceId: 'source-1', language: 'en' }],
        revisionTopics: [{ topicId: 'topic-1' }],
        relatedLinks: [{ id: 'link-1' }]
      } as unknown as Revision;

      const result = TasklistStateDTO.metadataStatus(revision, 'en');

      expect(result.title).toBe(TaskListStatus.Updated);
      expect(result.summary).toBe(TaskListStatus.Unchanged);
      expect(result.quality).toBe(TaskListStatus.Unchanged);
      expect(result.collection).toBe(TaskListStatus.Unchanged);
      expect(result.frequency).toBe(TaskListStatus.Unchanged);
      expect(result.designation).toBe(TaskListStatus.Unchanged);
      expect(result.sources).toBe(TaskListStatus.Unchanged);
      expect(result.topics).toBe(TaskListStatus.Unchanged);
      expect(result.related).toBe(TaskListStatus.Unchanged);
    });

    it('should throw error when previous metadata not found for updates', () => {
      const revision = {
        previousRevisionId: 'prev-123',
        previousRevision: {
          metadata: [{ language: 'cy', title: 'Welsh Title' }]
        },
        metadata: [{ language: 'en', title: 'English Title' }]
      } as Revision;

      expect(() => {
        TasklistStateDTO.metadataStatus(revision, 'en');
      }).toThrow('Cannot generate tasklist state - previous metadata not found for language en');
    });
  });

  describe('publishingStatus', () => {
    it('should return Completed when publishAt is set', () => {
      const dataset = {} as Dataset;
      const revision = { publishAt: new Date() } as Revision;

      const result = TasklistStateDTO.publishingStatus(dataset, revision);

      expect(result).toEqual({
        when: TaskListStatus.Completed
      });
    });

    it('should return NotStarted when publishAt is not set', () => {
      const dataset = {} as Dataset;
      const revision = { publishAt: null } as unknown as Revision;

      const result = TasklistStateDTO.publishingStatus(dataset, revision);

      expect(result).toEqual({
        when: TaskListStatus.NotStarted
      });
    });
  });

  describe('translationStatus', () => {
    beforeEach(() => {
      mockCollectTranslations.collectTranslations.mockReturnValue([{ key: 'test.key', english: 'Test Value' }]);
    });

    it('should throw error when metadata missing', () => {
      const dataset = {} as Dataset;
      const revision = {
        metadata: [{ language: 'en', updatedAt: new Date() }]
      } as Revision;

      expect(() => {
        TasklistStateDTO.translationStatus(dataset, revision);
      }).toThrow('Cannot generate tasklist state - metadata missing');
    });

    it('should return Unchanged for updates when no metadata has changed', () => {
      const dataset = {} as Dataset;
      const previousRevision = {} as Revision;
      const revision = {
        previousRevisionId: 'prev-123',
        previousRevision,
        metadata: [
          { language: 'en', updatedAt: new Date() },
          { language: 'cy', updatedAt: new Date() }
        ]
      } as Revision;

      mockCollectTranslations.collectTranslations
        .mockReturnValueOnce([{ key: 'test.key', english: 'Test Value' }])
        .mockReturnValueOnce([{ key: 'test.key', english: 'Test Value' }]);

      const result = TasklistStateDTO.translationStatus(dataset, revision);

      expect(result).toEqual({
        import: TaskListStatus.Unchanged,
        export: TaskListStatus.Unchanged
      });
    });

    it('should return an export status of NotStarted if never exported', () => {
      const dataset = {} as Dataset;
      const revision = {
        metadata: [
          { language: 'en', updatedAt: new Date() },
          { language: 'cy', updatedAt: new Date() }
        ]
      } as unknown as Revision;

      mockCollectTranslations.collectTranslations.mockReturnValue([{ key: 'test.key', english: 'Test Value' }]);

      // No translation events provided
      const result = TasklistStateDTO.translationStatus(dataset, revision);

      expect(result.export).toBe(TaskListStatus.NotStarted);
    });

    it('should return an export status of Completed if an export has been made since the last change to the dataset', () => {
      const dataset = {} as Dataset;
      const metaUpdateTime = new Date('2023-01-01T10:00:00');
      const exportTime = new Date('2023-01-01T12:00:00'); // Export after metadata update

      const revision = {
        metadata: [
          { language: 'en', updatedAt: metaUpdateTime },
          { language: 'cy', updatedAt: metaUpdateTime }
        ]
      } as unknown as Revision;

      const translationEvents = [
        {
          action: 'export',
          createdAt: exportTime,
          data: { translations: [{ key: 'test.key', english: 'Test Value' }] }
        }
      ] as unknown as EventLog[];

      mockCollectTranslations.collectTranslations.mockReturnValue([{ key: 'test.key', english: 'Test Value' }]);

      const result = TasklistStateDTO.translationStatus(dataset, revision, translationEvents);

      expect(result.export).toBe(TaskListStatus.Completed);
    });

    it('should return an import status of NotStarted if never imported', () => {
      const dataset = {} as Dataset;
      const revision = {
        metadata: [
          { language: 'en', updatedAt: new Date() },
          { language: 'cy', updatedAt: new Date() }
        ]
      } as unknown as Revision;

      const translationEvents = [
        {
          action: 'export',
          createdAt: new Date(),
          data: { translations: [{ key: 'test.key', english: 'Test Value' }] }
        }
      ] as unknown as EventLog[];

      mockCollectTranslations.collectTranslations.mockReturnValue([{ key: 'test.key', english: 'Test Value' }]);

      const result = TasklistStateDTO.translationStatus(dataset, revision, translationEvents);

      expect(result.import).toBe(TaskListStatus.NotStarted);
    });

    it('should return an import status of Completed if an import has updated all the necessary fields', () => {
      const dataset = {} as Dataset;
      const revision = {
        metadata: [
          { language: 'en', updatedAt: new Date('2023-01-01T10:00:00') },
          { language: 'cy', updatedAt: new Date('2023-01-01T10:00:00') }
        ]
      } as unknown as Revision;

      const translationEvents = [
        {
          action: 'export',
          createdAt: new Date('2023-01-01T11:00:00'),
          data: { translations: [{ key: 'test.key', english: 'Test Value' }] }
        },
        {
          action: 'import',
          createdAt: new Date('2023-01-01T12:00:00'),
          data: [{ key: 'test.key', english: 'Test Value', cymraeg: 'Gwerth Prawf' }]
        }
      ] as unknown as EventLog[];

      mockCollectTranslations.collectTranslations.mockReturnValue([
        { key: 'test.key', english: 'Test Value', cymraeg: 'Gwerth Prawf' }
      ]);

      const result = TasklistStateDTO.translationStatus(dataset, revision, translationEvents);

      expect(result.import).toBe(TaskListStatus.Completed);
    });

    it('should return export status of Incomplete when export is stale', () => {
      const dataset = {} as Dataset;
      const metaUpdateTime = new Date('2023-01-01T12:00:00'); // Metadata updated after export
      const exportTime = new Date('2023-01-01T10:00:00');

      const revision = {
        metadata: [
          { language: 'en', updatedAt: metaUpdateTime },
          { language: 'cy', updatedAt: metaUpdateTime }
        ]
      } as unknown as Revision;

      const translationEvents = [
        {
          action: 'export',
          createdAt: exportTime,
          data: { translations: [{ key: 'test.key', english: 'Test Value' }] }
        }
      ] as unknown as EventLog[];

      mockCollectTranslations.collectTranslations.mockReturnValue([
        { key: 'test.key', english: 'Updated Value' } // Different from exported value
      ]);

      const result = TasklistStateDTO.translationStatus(dataset, revision, translationEvents);

      expect(result.export).toBe(TaskListStatus.Incomplete);
    });

    it('should return import status of Incomplete when import is stale', () => {
      const dataset = {} as Dataset;
      const revision = {
        metadata: [
          { language: 'en', updatedAt: new Date('2023-01-01T10:00:00') },
          { language: 'cy', updatedAt: new Date('2023-01-01T10:00:00') }
        ]
      } as unknown as Revision;

      const translationEvents = [
        {
          action: 'export',
          createdAt: new Date('2023-01-01T11:00:00'),
          data: { translations: [{ key: 'test.key', english: 'Test Value' }] }
        },
        {
          action: 'import',
          createdAt: new Date('2023-01-01T12:00:00'),
          data: [{ key: 'test.key', english: 'Old Value', cymraeg: 'Hen Werth' }] // Different from current
        }
      ] as unknown as EventLog[];

      mockCollectTranslations.collectTranslations.mockReturnValue([
        { key: 'test.key', english: 'Test Value', cymraeg: 'Gwerth Prawf' } // Current translations
      ]);

      const result = TasklistStateDTO.translationStatus(dataset, revision, translationEvents);

      expect(result.import).toBe(TaskListStatus.Incomplete);
    });
  });

  describe('fromDataset', () => {
    it('should create complete TasklistStateDTO for new dataset', () => {
      const dataset = {
        dimensions: [
          {
            id: 'dim-1',
            type: DimensionType.Text,
            factTableColumn: 'category',
            extractor: { id: 'ext-1' },
            metadata: [{ language: 'en', name: 'Category' }]
          }
        ],
        measure: {
          id: 'measure-1',
          factTableColumn: 'value',
          joinColumn: 'measure_col',
          metadata: [{ language: 'en', name: 'Value' }]
        }
      } as Dataset;

      const revision = {
        previousRevisionId: null,
        dataTable: { uploadedAt: new Date() },
        metadata: [
          {
            language: 'en',
            title: 'Test Dataset',
            summary: 'Test Summary',
            quality: 'Test Quality',
            collection: 'Test Collection',
            updatedAt: new Date()
          },
          {
            language: 'cy',
            title: 'Set Ddata Prawf',
            summary: 'Crynodeb Prawf',
            quality: 'Ansawdd Prawf',
            collection: 'Casgliad Prawf',
            updatedAt: new Date()
          }
        ],
        updateFrequency: { id: 'freq-1' },
        designation: 'official',
        revisionProviders: [{ id: 'prov-1' }],
        revisionTopics: [{ topicId: 'topic-1' }],
        relatedLinks: [{ id: 'link-1' }],
        publishAt: new Date(),
        roundingApplied: false
      } as unknown as Revision;

      mockCollectTranslations.collectTranslations.mockReturnValue([{ key: 'test.key', english: 'Test Value' }]);

      const result = TasklistStateDTO.fromDataset(dataset, revision, 'en');

      expect(result.isUpdate).toBe(false);
      expect(result.datatable).toBe(TaskListStatus.Completed);
      expect(result.measure).toBeDefined();
      expect(result.dimensions).toHaveLength(1);
      expect(result.metadata).toBeDefined();
      expect(result.publishing.when).toBe(TaskListStatus.Completed);
      expect(result.translation).toBeDefined();
    });

    it('should set canPublish to false when translation import is incomplete', () => {
      const dataset = {
        dimensions: [
          {
            id: 'dim-1',
            type: DimensionType.Text,
            factTableColumn: 'category',
            extractor: { id: 'ext-1' },
            metadata: [{ language: 'en', name: 'Category' }]
          }
        ]
      } as unknown as Dataset;

      const updatedAt = new Date();
      const revision = {
        previousRevisionId: null,
        dataTable: { uploadedAt: new Date() },
        metadata: [
          {
            language: 'en',
            title: 'Test Dataset',
            summary: 'Test Summary',
            quality: 'Test Quality',
            collection: 'Test Collection',
            updatedAt
          },
          {
            language: 'cy',
            title: 'Set Ddata Prawf',
            summary: 'Crynodeb Prawf',
            quality: 'Ansawdd Prawf',
            collection: 'Casgliad Prawf',
            updatedAt: new Date(updatedAt.getTime() + 1000) // Different timestamp
          }
        ],
        updateFrequency: { id: 'freq-1' },
        designation: 'official',
        revisionProviders: [{ id: 'prov-1' }],
        revisionTopics: [{ topicId: 'topic-1' }],
        relatedLinks: [{ id: 'link-1', labelEN: 'Link', labelCY: 'Cyswllt' }],
        publishAt: new Date(),
        roundingApplied: false
      } as unknown as Revision;

      // Mock the translation system to return different translations
      mockCollectTranslations.collectTranslations.mockReturnValue([{ key: 'test.key', english: 'Test Value' }]);

      // No translation events, so translation import should be NotStarted
      const result = TasklistStateDTO.fromDataset(dataset, revision, 'en');

      expect(result.canPublish).toBe(false);
      expect(result.translation.import).toBe(TaskListStatus.NotStarted);
    });

    it('should require a reason for updates', () => {
      const dataset = {
        dimensions: []
      } as unknown as Dataset;

      const revision = {
        previousRevisionId: 'prev-123',
        previousRevision: {
          metadata: [
            { language: 'en', title: 'Old Title' },
            { language: 'cy', title: 'Hen Deitl' }
          ],
          revisionTopics: [],
          revisionProviders: [],
          relatedLinks: [],
          updateFrequency: null,
          designation: null
        },
        dataTable: null,
        metadata: [
          {
            language: 'en',
            title: null,
            updatedAt: new Date()
          },
          {
            language: 'cy',
            title: null,
            updatedAt: new Date()
          }
        ],
        revisionTopics: [],
        revisionProviders: [],
        relatedLinks: [],
        updateFrequency: null,
        designation: null,
        publishAt: new Date()
      } as unknown as Revision;

      mockCollectTranslations.collectTranslations.mockReturnValue([]);

      const translationEvents = [
        {
          action: 'import',
          createdAt: new Date(),
          data: []
        }
      ] as unknown as EventLog[];

      const result = TasklistStateDTO.fromDataset(dataset, revision, 'en', translationEvents);

      expect(result.isUpdate).toBe(true);
      expect(result.canPublish).toBe(false);
    });

    it('should allow publishing for updates even with incomplete sections', () => {
      const dataset = {
        dimensions: []
      } as unknown as Dataset;

      const revision = {
        previousRevisionId: 'prev-123',
        previousRevision: {
          metadata: [
            { language: 'en', title: 'Old Title' },
            { language: 'cy', title: 'Hen Deitl' }
          ],
          revisionTopics: [],
          revisionProviders: [],
          relatedLinks: [],
          updateFrequency: null,
          designation: null
        },
        dataTable: null,
        metadata: [
          {
            language: 'en',
            title: null,
            updatedAt: new Date(),
            reason: 'a reason for the update'
          },
          {
            language: 'cy',
            title: null,
            updatedAt: new Date()
          }
        ],
        revisionTopics: [],
        revisionProviders: [],
        relatedLinks: [],
        updateFrequency: null,
        designation: null,
        publishAt: new Date()
      } as unknown as Revision;

      mockCollectTranslations.collectTranslations.mockReturnValue([]);

      const translationEvents = [
        {
          action: 'import',
          createdAt: new Date(),
          data: []
        }
      ] as unknown as EventLog[];

      const result = TasklistStateDTO.fromDataset(dataset, revision, 'en', translationEvents);

      expect(result.isUpdate).toBe(true);
      expect(result.canPublish).toBe(true);
    });
  });
});
