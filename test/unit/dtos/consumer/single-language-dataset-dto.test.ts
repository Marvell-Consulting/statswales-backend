jest.mock('../../../../src/dtos/consumer/single-language-dimension-dto', () => ({
  SingleLanguageDimensionDTO: { fromDimension: jest.fn().mockReturnValue({ id: 'dim-stub' }) }
}));

jest.mock('../../../../src/dtos/consumer/single-language-revision-dto', () => ({
  SingleLanguageRevisionDTO: { fromRevision: jest.fn().mockReturnValue({ id: 'rev-stub' }) }
}));

jest.mock('../../../../src/dtos/consumer/single-language-measure-dto', () => ({
  SingleLanguageMeasureDTO: { fromMeasure: jest.fn().mockReturnValue({ id: 'measure-stub' }) }
}));

jest.mock('../../../../src/dtos/publisher-dto', () => ({
  PublisherDTO: { fromUserGroup: jest.fn().mockReturnValue({ name: 'pub-stub' }) }
}));

jest.mock('../../../../src/utils/revision', () => ({
  isPublished: jest.fn((rev) => !!rev.approvedAt && !!rev.publishAt)
}));

import { Dataset } from '../../../../src/entities/dataset/dataset';
import { SingleLanguageDatasetDTO } from '../../../../src/dtos/consumer/single-language-dataset-dto';
import { SingleLanguageRevisionDTO } from '../../../../src/dtos/consumer/single-language-revision-dto';
import { SingleLanguageDimensionDTO } from '../../../../src/dtos/consumer/single-language-dimension-dto';
import { SingleLanguageMeasureDTO } from '../../../../src/dtos/consumer/single-language-measure-dto';
import { PublisherDTO } from '../../../../src/dtos/publisher-dto';
import { Locale } from '../../../../src/enums/locale';

describe('SingleLanguageDatasetDTO', () => {
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
      userGroup: null,
      measure: null,
      ...overrides
    } as unknown as Dataset;
  };

  describe('fromDataset', () => {
    it('should map scalar fields correctly', () => {
      const dto = SingleLanguageDatasetDTO.fromDataset(makeDataset(), Locale.English);

      expect(dto.id).toBe('ds-1');
      expect(dto.first_published_at).toBe('2025-02-01T12:00:00.000Z');
    });

    it('should omit replaced_by when replacementDatasetId is not set', () => {
      const dto = SingleLanguageDatasetDTO.fromDataset(makeDataset(), Locale.English);

      expect(dto.replaced_by).toBeUndefined();
    });

    it('should populate replaced_by with the correct language title', () => {
      const dto = SingleLanguageDatasetDTO.fromDataset(
        makeDataset({
          replacementDatasetId: 'rep-ds-1',
          replacementAutoRedirect: true,
          replacementDataset: {
            publishedRevision: {
              metadata: [
                { language: Locale.English, title: 'English Title' },
                { language: Locale.Welsh, title: 'Teitl Cymraeg' }
              ]
            }
          }
        }),
        Locale.Welsh
      );

      expect(dto.replaced_by).toEqual({
        dataset_id: 'rep-ds-1',
        dataset_title: 'Teitl Cymraeg',
        auto_redirect: true
      });
    });

    it('should select English title when lang is English', () => {
      const dto = SingleLanguageDatasetDTO.fromDataset(
        makeDataset({
          replacementDatasetId: 'rep-ds-1',
          replacementAutoRedirect: false,
          replacementDataset: {
            publishedRevision: {
              metadata: [
                { language: Locale.English, title: 'English Title' },
                { language: Locale.Welsh, title: 'Teitl Cymraeg' }
              ]
            }
          }
        }),
        Locale.English
      );

      expect(dto.replaced_by?.dataset_title).toBe('English Title');
    });

    it('should default auto_redirect to false when replacementAutoRedirect is falsy', () => {
      const dto = SingleLanguageDatasetDTO.fromDataset(
        makeDataset({
          replacementDatasetId: 'rep-ds-1',
          replacementAutoRedirect: undefined,
          replacementDataset: null
        }),
        Locale.English
      );

      expect(dto.replaced_by).toEqual({
        dataset_id: 'rep-ds-1',
        dataset_title: undefined,
        auto_redirect: false
      });
    });

    it('should only include published revisions', () => {
      const past = new Date('2025-01-01T00:00:00Z');

      const dto = SingleLanguageDatasetDTO.fromDataset(
        makeDataset({
          revisions: [
            { id: 'r1', approvedAt: past, publishAt: past },
            { id: 'r2', approvedAt: null, publishAt: null }
          ]
        }),
        Locale.English
      );

      expect(SingleLanguageRevisionDTO.fromRevision).toHaveBeenCalledTimes(1);
      expect(dto.revisions).toEqual([{ id: 'rev-stub' }]);
    });

    it('should delegate dimensions mapping with language', () => {
      SingleLanguageDatasetDTO.fromDataset(makeDataset({ dimensions: [{ id: 'd1' }] }), Locale.Welsh);

      expect(SingleLanguageDimensionDTO.fromDimension).toHaveBeenCalledWith({ id: 'd1' }, Locale.Welsh);
    });

    it('should map published_revision when present', () => {
      const dto = SingleLanguageDatasetDTO.fromDataset(
        makeDataset({ publishedRevision: { id: 'pr-1' } }),
        Locale.English
      );

      expect(SingleLanguageRevisionDTO.fromRevision).toHaveBeenCalledWith({ id: 'pr-1' }, Locale.English);
      expect(dto.published_revision).toEqual({ id: 'rev-stub' });
    });

    it('should map publisher when userGroup is present', () => {
      const dto = SingleLanguageDatasetDTO.fromDataset(makeDataset({ userGroup: { id: 'ug-1' } }), Locale.Welsh);

      expect(PublisherDTO.fromUserGroup).toHaveBeenCalledWith({ id: 'ug-1' }, Locale.Welsh);
      expect(dto.publisher).toEqual({ name: 'pub-stub' });
    });

    it('should map data_description when measure is present', () => {
      const dto = SingleLanguageDatasetDTO.fromDataset(makeDataset({ measure: { id: 'm1' } }), Locale.English);

      expect(SingleLanguageMeasureDTO.fromMeasure).toHaveBeenCalledWith({ id: 'm1' }, Locale.English);
      expect(dto.data_description).toEqual({ id: 'measure-stub' });
    });

    it('should leave data_description undefined when measure is null', () => {
      const dto = SingleLanguageDatasetDTO.fromDataset(makeDataset(), Locale.English);

      expect(dto.data_description).toBeUndefined();
    });
  });
});
