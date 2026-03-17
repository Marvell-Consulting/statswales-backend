jest.mock('../../src/dtos/dimension-dto', () => ({
  DimensionDTO: { fromDimension: jest.fn().mockReturnValue({ id: 'dim-stub' }) }
}));

jest.mock('../../src/dtos/consumer-revision-dto', () => ({
  ConsumerRevisionDTO: { fromRevision: jest.fn().mockReturnValue({ id: 'rev-stub' }) }
}));

jest.mock('../../src/dtos/publisher-dto', () => ({
  PublisherDTO: { fromUserGroup: jest.fn().mockReturnValue({ name: 'pub-stub' }) }
}));

import { Dataset } from '../../src/entities/dataset/dataset';
import { ConsumerDatasetDTO } from '../../src/dtos/consumer-dataset-dto';
import { ConsumerRevisionDTO } from '../../src/dtos/consumer-revision-dto';
import { DimensionDTO } from '../../src/dtos/dimension-dto';

describe('ConsumerDatasetDTO', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  const makeDataset = (overrides = {}): Dataset => {
    return {
      id: 'ds-1',
      firstPublishedAt: new Date('2025-02-01T12:00:00Z'),
      archivedAt: null,
      replacementDatasetId: null,
      replacementAutoRedirect: false,
      replacementDataset: null,
      dimensions: null,
      revisions: null,
      publishedRevision: null,
      startDate: null,
      endDate: null,
      ...overrides
    } as unknown as Dataset;
  };

  describe('fromDataset', () => {
    it('should map scalar fields correctly', () => {
      const dto = ConsumerDatasetDTO.fromDataset(makeDataset());

      expect(dto.id).toBe('ds-1');
      expect(dto.first_published_at).toBe('2025-02-01T12:00:00.000Z');
    });

    it('should omit replaced_by when replacementDatasetId is not set', () => {
      const dto = ConsumerDatasetDTO.fromDataset(makeDataset());

      expect(dto.replaced_by).toBeUndefined();
    });

    it('should populate replaced_by when replacementDatasetId is set', () => {
      const dto = ConsumerDatasetDTO.fromDataset(
        makeDataset({
          replacementDatasetId: 'rep-ds-1',
          replacementAutoRedirect: true,
          replacementDataset: {
            publishedRevision: { metadata: [{ title: 'Replacement Title' }] }
          }
        })
      );

      expect(dto.replaced_by).toEqual({
        dataset_id: 'rep-ds-1',
        dataset_title: 'Replacement Title',
        auto_redirect: true
      });
    });

    it('should default auto_redirect to false when replacementAutoRedirect is falsy', () => {
      const dto = ConsumerDatasetDTO.fromDataset(
        makeDataset({
          replacementDatasetId: 'rep-ds-1',
          replacementAutoRedirect: undefined,
          replacementDataset: null
        })
      );

      expect(dto.replaced_by).toEqual({
        dataset_id: 'rep-ds-1',
        dataset_title: undefined,
        auto_redirect: false
      });
    });

    it('should only include published revisions', () => {
      const past = new Date('2025-01-01T00:00:00Z');
      const future = new Date('2099-01-01T00:00:00Z');

      const dto = ConsumerDatasetDTO.fromDataset(
        makeDataset({
          revisions: [
            { id: 'r1', approvedAt: past, publishAt: past },
            { id: 'r2', approvedAt: null, publishAt: null },
            { id: 'r3', approvedAt: past, publishAt: future }
          ]
        })
      );

      expect(ConsumerRevisionDTO.fromRevision).toHaveBeenCalledTimes(1);
      expect(dto.revisions).toEqual([{ id: 'rev-stub' }]);
    });

    it('should delegate dimensions mapping to DimensionDTO', () => {
      const dto = ConsumerDatasetDTO.fromDataset(makeDataset({ dimensions: [{ id: 'd1' }] }));

      expect(DimensionDTO.fromDimension).toHaveBeenCalledTimes(1);
      expect(dto.dimensions).toEqual([{ id: 'dim-stub' }]);
    });

    it('should map published_revision when present', () => {
      const dto = ConsumerDatasetDTO.fromDataset(makeDataset({ publishedRevision: { id: 'pr-1' } }));

      expect(ConsumerRevisionDTO.fromRevision).toHaveBeenCalledWith({ id: 'pr-1' });
      expect(dto.published_revision).toEqual({ id: 'rev-stub' });
    });

    it('should leave published_revision undefined when not present', () => {
      const dto = ConsumerDatasetDTO.fromDataset(makeDataset());

      expect(dto.published_revision).toBeUndefined();
    });
  });
});
