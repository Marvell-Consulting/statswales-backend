import { ConsumerRevisionDTO } from '../dtos/consumer-revision-dto';
import { Dataset } from '../entities/dataset/dataset';
import { Revision } from '../entities/dataset/revision';
import { Dimension } from '../entities/dataset/dimension';
import { DateExtractor } from '../extractors/date-extractor';
import { DateDimensionTypes } from '../enums/dimension-type';

export interface CoverageRange {
  startDate: Date | null;
  endDate: Date | null;
}

// Fresh draft update revisions (cloned from the published revision via deepCloneRevision) have no data table id
// yet, so anything that queries the per-revision schema must use the previously-published revision id until the draft
// has its own data. Use this helper to decide which revision id to query; it returns undefined only when neither side
// has a cube.
export const resolvePreviewRevisionId = (
  revision: Pick<Revision, 'id' | 'dataTableId'> | null | undefined,
  dataset: Pick<Dataset, 'publishedRevisionId'> | null | undefined
): string | undefined => {
  if (revision?.dataTableId) return revision.id;
  return dataset?.publishedRevisionId;
};

export const isPublished = (rev: Revision | ConsumerRevisionDTO): boolean => {
  const now = new Date();
  if (rev instanceof ConsumerRevisionDTO) {
    return !!(rev.approved_at && rev.publish_at && new Date(rev.approved_at) < now && new Date(rev.publish_at) < now);
  }
  return !!(rev.approvedAt && rev.publishAt && rev.approvedAt < now && rev.publishAt < now);
};

export const widenCoverageRange = (current: CoverageRange, incoming: CoverageRange): CoverageRange => {
  const startDate =
    incoming.startDate && (!current.startDate || incoming.startDate < current.startDate)
      ? incoming.startDate
      : current.startDate;
  const endDate =
    incoming.endDate && (!current.endDate || incoming.endDate > current.endDate) ? incoming.endDate : current.endDate;
  return { startDate, endDate };
};

export const revisionStartAndEndDateFinder = (dimensions: Dimension[]): CoverageRange => {
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
