import { isBefore } from 'date-fns';

import { Dataset } from '../entities/dataset/dataset';
import { Dimension } from '../entities/dataset/dimension';
import { Revision } from '../entities/dataset/revision';

import { DimensionDTO } from './dimension-dto';
import { ConsumerRevisionDTO } from './consumer-revision-dto';
import { PublisherDTO } from './publisher-dto';

// WARNING: Make sure to filter any props the consumer side should not have access to
export class ConsumerDatasetDTO {
  id: string;
  live?: string | null;
  dimensions?: DimensionDTO[];
  revisions: ConsumerRevisionDTO[];
  published_revision?: ConsumerRevisionDTO;
  start_date?: Date | null;
  end_date?: Date | null;
  publisher?: PublisherDTO;

  static fromDataset(dataset: Dataset): ConsumerDatasetDTO {
    const dto = new ConsumerDatasetDTO();
    dto.id = dataset.id;
    dto.live = dataset.live?.toISOString();

    // only return published revisions
    dto.revisions = dataset.revisions
      ?.filter((rev: Revision) => rev.approvedAt && rev.publishAt && isBefore(rev.publishAt, new Date()))
      .map((rev: Revision) => ConsumerRevisionDTO.fromRevision(rev));

    dto.dimensions = dataset.dimensions?.map((dimension: Dimension) => DimensionDTO.fromDimension(dimension));

    if (dataset.publishedRevision) {
      dto.published_revision = ConsumerRevisionDTO.fromRevision(dataset.publishedRevision);
    }

    dto.start_date = dataset.startDate;
    dto.end_date = dataset.endDate;

    return dto;
  }
}
