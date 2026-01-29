jest.mock('../../src/dtos/dimension-dto', () => ({
  DimensionDTO: { fromDimension: jest.fn().mockReturnValue({ id: 'dim-stub' }) }
}));

jest.mock('../../src/dtos/revision-dto', () => ({
  RevisionDTO: { fromRevision: jest.fn().mockReturnValue({ id: 'rev-stub' }) }
}));

jest.mock('../../src/dtos/measure-dto', () => ({
  MeasureDTO: { fromMeasure: jest.fn().mockReturnValue({ id: 'measure-stub' }) }
}));

jest.mock('../../src/dtos/fact-table-column-dto', () => ({
  FactTableColumnDto: { fromFactTableColumn: jest.fn().mockReturnValue({ name: 'col-stub' }) }
}));

jest.mock('../../src/dtos/task-dto', () => ({
  TaskDTO: { fromTask: jest.fn().mockReturnValue({ id: 'task-stub' }) }
}));

import { Dataset } from '../../src/entities/dataset/dataset';
import { DatasetDTO } from '../../src/dtos/dataset-dto';
import { DimensionDTO } from '../../src/dtos/dimension-dto';
import { RevisionDTO } from '../../src/dtos/revision-dto';
import { MeasureDTO } from '../../src/dtos/measure-dto';
import { FactTableColumnDto } from '../../src/dtos/fact-table-column-dto';
import { TaskDTO } from '../../src/dtos/task-dto';

describe('DatasetDTO', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  const makeDataset = (overrides = {}): Dataset => {
    return {
      id: 'ds-1',
      createdAt: new Date('2025-01-15T10:00:00Z'),
      createdById: 'user-1',
      firstPublishedAt: new Date('2025-02-01T12:00:00Z'),
      archivedAt: null,
      dimensions: null,
      revisions: null,
      draftRevisionId: 'draft-rev-1',
      startRevisionId: 'start-rev-1',
      endRevisionId: 'end-rev-1',
      publishedRevisionId: 'pub-rev-1',
      draftRevision: null,
      startRevision: null,
      endRevision: null,
      publishedRevision: null,
      measure: null,
      factTable: null,
      startDate: null,
      endDate: null,
      userGroupId: 'ug-1',
      tasks: null,
      ...overrides
    } as unknown as Dataset;
  };

  describe('fromDataset', () => {
    it('should map scalar fields correctly', () => {
      const dto = DatasetDTO.fromDataset(makeDataset());

      expect(dto.id).toBe('ds-1');
      expect(dto.created_by_id).toBe('user-1');
      expect(dto.draft_revision_id).toBe('draft-rev-1');
      expect(dto.start_revision_id).toBe('start-rev-1');
      expect(dto.end_revision_id).toBe('end-rev-1');
      expect(dto.published_revision_id).toBe('pub-rev-1');
      expect(dto.user_group_id).toBe('ug-1');
    });

    it('should convert dates to ISO strings', () => {
      const dto = DatasetDTO.fromDataset(makeDataset());

      expect(dto.created_at).toBe('2025-01-15T10:00:00.000Z');
      expect(dto.first_published_at).toBe('2025-02-01T12:00:00.000Z');
    });

    it('should handle null firstPublishedAt and archivedAt', () => {
      const dto = DatasetDTO.fromDataset(makeDataset({ firstPublishedAt: null, archivedAt: null }));

      expect(dto.first_published_at).toBeUndefined();
      expect(dto.archived_at).toBeUndefined();
    });

    it('should delegate dimensions mapping to DimensionDTO', () => {
      const dims = [{ id: 'd1' }, { id: 'd2' }];
      const dto = DatasetDTO.fromDataset(makeDataset({ dimensions: dims }));

      expect(DimensionDTO.fromDimension).toHaveBeenCalledTimes(2);
      expect(dto.dimensions).toEqual([{ id: 'dim-stub' }, { id: 'dim-stub' }]);
    });

    it('should delegate revisions mapping to RevisionDTO', () => {
      const revs = [{ id: 'r1' }];
      const dto = DatasetDTO.fromDataset(makeDataset({ revisions: revs }));

      expect(RevisionDTO.fromRevision).toHaveBeenCalledTimes(1);
      expect(dto.revisions).toEqual([{ id: 'rev-stub' }]);
    });

    it('should delegate draftRevision to RevisionDTO when present', () => {
      const dto = DatasetDTO.fromDataset(makeDataset({ draftRevision: { id: 'dr-1' } }));

      expect(RevisionDTO.fromRevision).toHaveBeenCalledWith({ id: 'dr-1' });
      expect(dto.draft_revision).toEqual({ id: 'rev-stub' });
    });

    it('should leave draft_revision undefined when draftRevision is null', () => {
      const dto = DatasetDTO.fromDataset(makeDataset({ draftRevision: null }));

      expect(dto.draft_revision).toBeUndefined();
    });

    it('should delegate measure mapping to MeasureDTO when present', () => {
      const dto = DatasetDTO.fromDataset(makeDataset({ measure: { id: 'm1' } }));

      expect(MeasureDTO.fromMeasure).toHaveBeenCalledWith({ id: 'm1' });
      expect(dto.measure).toEqual({ id: 'measure-stub' });
    });

    it('should delegate factTable mapping to FactTableColumnDto', () => {
      const dto = DatasetDTO.fromDataset(makeDataset({ factTable: [{ name: 'c1' }] }));

      expect(FactTableColumnDto.fromFactTableColumn).toHaveBeenCalledTimes(1);
      expect(dto.fact_table).toEqual([{ name: 'col-stub' }]);
    });

    it('should delegate tasks mapping to TaskDTO', () => {
      const dto = DatasetDTO.fromDataset(makeDataset({ tasks: [{ id: 't1' }] }));

      expect(TaskDTO.fromTask).toHaveBeenCalledTimes(1);
      expect(dto.tasks).toEqual([{ id: 'task-stub' }]);
    });

    it('should default tasks to empty array when tasks is null', () => {
      const dto = DatasetDTO.fromDataset(makeDataset({ tasks: null }));

      expect(dto.tasks).toEqual([]);
    });

    it('should use endRevision dates for start_date and end_date when present', () => {
      const startDate = new Date('2020-01-01');
      const endDate = new Date('2024-12-31');
      const dto = DatasetDTO.fromDataset(
        makeDataset({
          endRevision: { startDate, endDate },
          startDate: new Date('2019-01-01'),
          endDate: new Date('2023-12-31')
        })
      );

      expect(dto.start_date).toEqual(startDate);
      expect(dto.end_date).toEqual(endDate);
    });

    it('should fall back to dataset dates when endRevision is null', () => {
      const startDate = new Date('2019-01-01');
      const endDate = new Date('2023-12-31');
      const dto = DatasetDTO.fromDataset(
        makeDataset({
          endRevision: null,
          startDate,
          endDate
        })
      );

      expect(dto.start_date).toEqual(startDate);
      expect(dto.end_date).toEqual(endDate);
    });

    it('should handle null dimensions and revisions arrays', () => {
      const dto = DatasetDTO.fromDataset(makeDataset({ dimensions: null, revisions: null }));

      expect(dto.dimensions).toBeUndefined();
      expect(dto.revisions).toBeUndefined();
    });

    it('should convert archivedAt to ISO string when present', () => {
      const dto = DatasetDTO.fromDataset(makeDataset({ archivedAt: new Date('2025-06-01T00:00:00Z') }));

      expect(dto.archived_at).toBe('2025-06-01T00:00:00.000Z');
    });
  });
});
