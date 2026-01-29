jest.mock('../../src/dtos/data-table-dto', () => ({
  DataTableDto: { fromDataTable: jest.fn().mockReturnValue({ id: 'dt-stub' }) }
}));

jest.mock('../../src/dtos/revistion-metadata-dto', () => ({
  RevisionMetadataDTO: { fromRevisionMetadata: jest.fn().mockReturnValue({ id: 'meta-stub' }) }
}));

jest.mock('../../src/dtos/revision-provider-dto', () => ({
  RevisionProviderDTO: { fromRevisionProvider: jest.fn().mockReturnValue({ id: 'provider-stub' }) }
}));

jest.mock('../../src/dtos/topic-dto', () => ({
  TopicDTO: { fromTopic: jest.fn().mockReturnValue({ id: 'topic-stub' }) }
}));

jest.mock('../../src/dtos/related-link-dto', () => ({
  RelatedLinkDTO: { fromRelatedLink: jest.fn().mockReturnValue({ id: 'link-stub' }) }
}));

import { Revision } from '../../src/entities/dataset/revision';
import { RevisionDTO } from '../../src/dtos/revision-dto';
import { DataTableDto } from '../../src/dtos/data-table-dto';
import { RevisionMetadataDTO } from '../../src/dtos/revistion-metadata-dto';
import { RevisionProviderDTO } from '../../src/dtos/revision-provider-dto';
import { TopicDTO } from '../../src/dtos/topic-dto';
import { RelatedLinkDTO } from '../../src/dtos/related-link-dto';

describe('RevisionDTO', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  const makeRevision = (overrides = {}): Revision => {
    return {
      id: 'rev-1',
      revisionIndex: 3,
      dataset: { id: 'ds-1' },
      dataTableId: 'dt-1',
      createdAt: new Date('2025-01-10T08:00:00Z'),
      updatedAt: new Date('2025-01-12T10:00:00Z'),
      previousRevisionId: 'rev-0',
      onlineCubeFilename: 'cube.duckdb',
      publishAt: new Date('2025-02-01T00:00:00Z'),
      unpublishedAt: null,
      approvedAt: new Date('2025-01-20T12:00:00Z'),
      approvedBy: { name: 'Approver Name' },
      createdBy: { name: 'Creator Name' },
      startDate: new Date('2020-01-01'),
      endDate: new Date('2024-12-31'),
      dataTable: null,
      roundingApplied: true,
      updateFrequency: { frequency: 'monthly' },
      designation: 'official',
      relatedLinks: null,
      metadata: null,
      revisionProviders: null,
      revisionTopics: null,
      ...overrides
    } as unknown as Revision;
  };

  describe('fromRevision', () => {
    it('should map scalar fields correctly', () => {
      const dto = RevisionDTO.fromRevision(makeRevision());

      expect(dto.id).toBe('rev-1');
      expect(dto.revision_index).toBe(3);
      expect(dto.dataset_id).toBe('ds-1');
      expect(dto.data_table_id).toBe('dt-1');
      expect(dto.previous_revision_id).toBe('rev-0');
      expect(dto.online_cube_filename).toBe('cube.duckdb');
      expect(dto.rounding_applied).toBe(true);
      expect(dto.designation).toBe('official');
    });

    it('should convert dates to ISO strings', () => {
      const dto = RevisionDTO.fromRevision(makeRevision());

      expect(dto.created_at).toBe('2025-01-10T08:00:00.000Z');
      expect(dto.updated_at).toBe('2025-01-12T10:00:00.000Z');
      expect(dto.approved_at).toBe('2025-01-20T12:00:00.000Z');
      expect(dto.publish_at).toBe('2025-02-01T00:00:00.000Z');
    });

    it('should handle null optional dates', () => {
      const dto = RevisionDTO.fromRevision(makeRevision({ publishAt: null, unpublishedAt: null, approvedAt: null }));

      expect(dto.publish_at).toBeUndefined();
      expect(dto.unpublished_at).toBeUndefined();
      expect(dto.approved_at).toBeUndefined();
    });

    it('should map approvedBy and createdBy names', () => {
      const dto = RevisionDTO.fromRevision(makeRevision());

      expect(dto.approved_by).toBe('Approver Name');
      expect(dto.created_by).toBe('Creator Name');
    });

    it('should handle null approvedBy and createdBy', () => {
      const dto = RevisionDTO.fromRevision(makeRevision({ approvedBy: null, createdBy: null }));

      expect(dto.approved_by).toBeUndefined();
      expect(dto.created_by).toBeUndefined();
    });

    it('should set online_cube_filename to undefined when falsy', () => {
      const dto = RevisionDTO.fromRevision(makeRevision({ onlineCubeFilename: null }));

      expect(dto.online_cube_filename).toBeUndefined();
    });

    it('should delegate dataTable to DataTableDto when present', () => {
      const dt = { id: 'dt-1' };
      const dto = RevisionDTO.fromRevision(makeRevision({ dataTable: dt }));

      expect(DataTableDto.fromDataTable).toHaveBeenCalledWith(dt);
      expect(dto.data_table).toEqual({ id: 'dt-stub' });
    });

    it('should leave data_table undefined when dataTable is null', () => {
      const dto = RevisionDTO.fromRevision(makeRevision({ dataTable: null }));

      expect(dto.data_table).toBeUndefined();
    });

    it('should delegate metadata mapping to RevisionMetadataDTO', () => {
      const meta = [{ id: 'm1' }];
      const dto = RevisionDTO.fromRevision(makeRevision({ metadata: meta }));

      expect(RevisionMetadataDTO.fromRevisionMetadata).toHaveBeenCalledWith({ id: 'm1' });
      expect(dto.metadata).toEqual([{ id: 'meta-stub' }]);
    });

    it('should leave metadata undefined when not present', () => {
      const dto = RevisionDTO.fromRevision(makeRevision({ metadata: null }));

      expect(dto.metadata).toBeUndefined();
    });

    it('should delegate providers to RevisionProviderDTO', () => {
      const providers = [{ id: 'p1' }];
      const dto = RevisionDTO.fromRevision(makeRevision({ revisionProviders: providers }));

      expect(RevisionProviderDTO.fromRevisionProvider).toHaveBeenCalledWith({ id: 'p1' });
      expect(dto.providers).toEqual([{ id: 'provider-stub' }]);
    });

    it('should unwrap revisionTopics to TopicDTO via topic field', () => {
      const topics = [{ topic: { id: 't1' } }, { topic: { id: 't2' } }];
      const dto = RevisionDTO.fromRevision(makeRevision({ revisionTopics: topics }));

      expect(TopicDTO.fromTopic).toHaveBeenCalledWith({ id: 't1' });
      expect(TopicDTO.fromTopic).toHaveBeenCalledWith({ id: 't2' });
      expect(dto.topics).toEqual([{ id: 'topic-stub' }, { id: 'topic-stub' }]);
    });

    it('should delegate relatedLinks to RelatedLinkDTO', () => {
      const links = [{ id: 'rl-1' }];
      const dto = RevisionDTO.fromRevision(makeRevision({ relatedLinks: links }));

      expect(RelatedLinkDTO.fromRelatedLink).toHaveBeenCalledWith({ id: 'rl-1' });
      expect(dto.related_links).toEqual([{ id: 'link-stub' }]);
    });

    it('should map coverage start and end dates', () => {
      const start = new Date('2020-01-01');
      const end = new Date('2024-12-31');
      const dto = RevisionDTO.fromRevision(makeRevision({ startDate: start, endDate: end }));

      expect(dto.coverage_start_date).toEqual(start);
      expect(dto.coverage_end_date).toEqual(end);
    });
  });
});
