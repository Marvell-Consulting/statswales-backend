import { ConsumerRevisionDTO } from '../../../src/dtos/consumer-revision-dto';
import { Dimension } from '../../../src/entities/dataset/dimension';
import { Revision } from '../../../src/entities/dataset/revision';
import { DimensionType } from '../../../src/enums/dimension-type';
import { isPublished, revisionStartAndEndDateFinder, widenCoverageRange } from '../../../src/utils/revision';

describe('revision utils', () => {
  beforeEach(() => {
    jest.useFakeTimers().setSystemTime(new Date('2025-06-01T00:00:00Z'));
  });

  afterEach(() => {
    jest.useRealTimers();
  });

  describe('isPublished', () => {
    it('should return true for a Revision with approvedAt and publishAt in the past', () => {
      const rev = {
        approvedAt: new Date('2025-05-01'),
        publishAt: new Date('2025-05-15')
      } as unknown as Revision;

      expect(isPublished(rev)).toBe(true);
    });

    it('should return false for a Revision with publishAt in the future', () => {
      const rev = {
        approvedAt: new Date('2025-05-01'),
        publishAt: new Date('2025-07-01')
      } as unknown as Revision;

      expect(isPublished(rev)).toBe(false);
    });

    it('should return false for a Revision with null approvedAt', () => {
      const rev = {
        approvedAt: null,
        publishAt: new Date('2025-05-15')
      } as unknown as Revision;

      expect(isPublished(rev)).toBe(false);
    });

    it('should return false for a Revision with null publishAt', () => {
      const rev = {
        approvedAt: new Date('2025-05-01'),
        publishAt: null
      } as unknown as Revision;

      expect(isPublished(rev)).toBe(false);
    });

    it('should return true for a ConsumerRevisionDTO with past dates', () => {
      const dto = new ConsumerRevisionDTO();
      dto.approved_at = '2025-05-01T00:00:00Z';
      dto.publish_at = '2025-05-15T00:00:00Z';

      expect(isPublished(dto)).toBe(true);
    });

    it('should return false for a ConsumerRevisionDTO with future publishAt', () => {
      const dto = new ConsumerRevisionDTO();
      dto.approved_at = '2025-05-01T00:00:00Z';
      dto.publish_at = '2025-07-01T00:00:00Z';

      expect(isPublished(dto)).toBe(false);
    });

    it('should return false for a ConsumerRevisionDTO with null approved_at', () => {
      const dto = new ConsumerRevisionDTO();
      dto.approved_at = undefined;
      dto.publish_at = '2025-05-15T00:00:00Z';

      expect(isPublished(dto)).toBe(false);
    });
  });

  describe('widenCoverageRange', () => {
    it('should set start and end when current has no existing dates', () => {
      const result = widenCoverageRange(
        { startDate: null, endDate: null },
        { startDate: new Date('2020-01-01'), endDate: new Date('2023-12-31') }
      );

      expect(result.startDate).toEqual(new Date('2020-01-01'));
      expect(result.endDate).toEqual(new Date('2023-12-31'));
    });

    it('should widen start date when incoming starts earlier', () => {
      const result = widenCoverageRange(
        { startDate: new Date('2021-01-01'), endDate: new Date('2023-12-31') },
        { startDate: new Date('2020-01-01'), endDate: new Date('2023-12-31') }
      );

      expect(result.startDate).toEqual(new Date('2020-01-01'));
    });

    it('should widen end date when incoming ends later', () => {
      const result = widenCoverageRange(
        { startDate: new Date('2020-01-01'), endDate: new Date('2023-12-31') },
        { startDate: new Date('2020-01-01'), endDate: new Date('2024-06-30') }
      );

      expect(result.endDate).toEqual(new Date('2024-06-30'));
    });

    it('should NOT narrow start date when incoming starts later', () => {
      const result = widenCoverageRange(
        { startDate: new Date('2020-01-01'), endDate: new Date('2023-12-31') },
        { startDate: new Date('2021-06-01'), endDate: new Date('2023-12-31') }
      );

      expect(result.startDate).toEqual(new Date('2020-01-01'));
    });

    it('should NOT narrow end date when incoming ends earlier', () => {
      const result = widenCoverageRange(
        { startDate: new Date('2020-01-01'), endDate: new Date('2023-12-31') },
        { startDate: new Date('2020-01-01'), endDate: new Date('2022-06-30') }
      );

      expect(result.endDate).toEqual(new Date('2023-12-31'));
    });

    it('should handle null incoming dates gracefully (no change)', () => {
      const result = widenCoverageRange(
        { startDate: new Date('2020-01-01'), endDate: new Date('2023-12-31') },
        { startDate: null, endDate: null }
      );

      expect(result.startDate).toEqual(new Date('2020-01-01'));
      expect(result.endDate).toEqual(new Date('2023-12-31'));
    });

    it('should widen correctly across multiple sequential calls (multiple dimensions)', () => {
      let range = widenCoverageRange(
        { startDate: null, endDate: null },
        { startDate: new Date('2021-01-01'), endDate: new Date('2022-12-31') }
      );
      range = widenCoverageRange(range, { startDate: new Date('2019-06-01'), endDate: new Date('2024-06-30') });
      range = widenCoverageRange(range, { startDate: new Date('2020-01-01'), endDate: new Date('2023-12-31') });

      expect(range.startDate).toEqual(new Date('2019-06-01'));
      expect(range.endDate).toEqual(new Date('2024-06-30'));
    });

    it('regression: should NOT narrow range when incoming is a subset', () => {
      const result = widenCoverageRange(
        { startDate: new Date('2020-01-01'), endDate: new Date('2023-12-31') },
        { startDate: new Date('2021-01-01'), endDate: new Date('2022-12-31') }
      );

      // Range must stay 2020-2023, not narrow to 2021-2022
      expect(result.startDate).toEqual(new Date('2020-01-01'));
      expect(result.endDate).toEqual(new Date('2023-12-31'));
    });
  });

  describe('revisionStartAndEndDateFinder', () => {
    it('should return null dates when there are no date dimensions', () => {
      const dimensions = [{ type: DimensionType.Text, extractor: {} }] as unknown as Dimension[];

      const result = revisionStartAndEndDateFinder(dimensions);
      expect(result).toEqual({ startDate: null, endDate: null });
    });

    it('should find start and end dates from a single date dimension', () => {
      const start = new Date('2020-01-01');
      const end = new Date('2024-12-31');
      const dimensions = [
        {
          type: DimensionType.DatePeriod,
          extractor: { lookupTableStart: start, lookupTableEnd: end }
        }
      ] as unknown as Dimension[];

      const result = revisionStartAndEndDateFinder(dimensions);
      expect(result).toEqual({ startDate: start, endDate: end });
    });

    it('should pick the earliest start and latest end across multiple date dimensions', () => {
      const dim1Start = new Date('2020-01-01');
      const dim1End = new Date('2023-12-31');
      const dim2Start = new Date('2019-06-01');
      const dim2End = new Date('2024-06-30');

      const dimensions = [
        {
          type: DimensionType.DatePeriod,
          extractor: { lookupTableStart: dim1Start, lookupTableEnd: dim1End }
        },
        {
          type: DimensionType.Date,
          extractor: { lookupTableStart: dim2Start, lookupTableEnd: dim2End }
        }
      ] as unknown as Dimension[];

      const result = revisionStartAndEndDateFinder(dimensions);
      expect(result.startDate).toEqual(dim2Start);
      expect(result.endDate).toEqual(dim2End);
    });

    it('should handle missing extractor fields gracefully', () => {
      const dimensions = [
        {
          type: DimensionType.TimePeriod,
          extractor: {}
        }
      ] as unknown as Dimension[];

      const result = revisionStartAndEndDateFinder(dimensions);
      expect(result).toEqual({ startDate: null, endDate: null });
    });
  });
});
