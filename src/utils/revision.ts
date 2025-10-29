import { ConsumerRevisionDTO } from '../dtos/consumer-revision-dto';
import { Revision } from '../entities/dataset/revision';
import { Dimension } from '../entities/dataset/dimension';
import { DateExtractor } from '../extractors/date-extractor';
import { DateDimensionTypes } from '../enums/dimension-type';

export const isPublished = (rev: Revision | ConsumerRevisionDTO): boolean => {
  const now = new Date();
  if (rev instanceof ConsumerRevisionDTO) {
    return !!(rev.approved_at && rev.publish_at && new Date(rev.approved_at) < now && new Date(rev.publish_at) < now);
  }
  return !!(rev.approvedAt && rev.publishAt && rev.approvedAt < now && rev.publishAt < now);
};

export const revisionStartAndEndDateFinder = (
  dimensions: Dimension[]
): { startDate: Date | null; endDate: Date | null } => {
  let startDate: Date | null = null;
  let endDate: Date | null = null;
  dimensions
    .filter((dim) => DateDimensionTypes.includes(dim.type))
    .forEach((dim) => {
      const extractor = dim.extractor as DateExtractor;
      if (extractor.lookupTableStart) {
        if (!startDate) {
          startDate = extractor.lookupTableStart;
        } else if (extractor.lookupTableStart < startDate) {
          startDate = extractor.lookupTableStart;
        }
      }
      if (extractor.lookupTableEnd) {
        if (!endDate) {
          endDate = extractor.lookupTableEnd;
        } else if (extractor.lookupTableEnd > endDate) {
          endDate = extractor.lookupTableEnd;
        }
      }
    });
  return { startDate, endDate };
};
