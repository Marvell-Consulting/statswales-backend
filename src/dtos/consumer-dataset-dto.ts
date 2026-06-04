import { isBefore } from 'date-fns';

import { Dataset } from '../entities/dataset/dataset';
import { Dimension } from '../entities/dataset/dimension';
import { Revision } from '../entities/dataset/revision';

import { DimensionDTO } from './dimension-dto';
import { ConsumerRevisionDTO } from './consumer-revision-dto';
import { PublisherDTO } from './publisher-dto';
import { Locale } from '../enums/locale';

// WARNING: Make sure to filter any props the consumer side should not have access to
export class ConsumerDatasetDTO {
  id: string;
  first_published_at?: string | null;
  archived_at?: string;
  replaced_by?: { dataset_id: string; dataset_title?: string; auto_redirect: boolean };
  dimensions?: DimensionDTO[];
  revisions: ConsumerRevisionDTO[];
  published_revision?: ConsumerRevisionDTO;
  start_date?: string;
  end_date?: string;
  publisher?: PublisherDTO;

  static fromDataset(dataset: Dataset, lang: Locale = Locale.English): ConsumerDatasetDTO {
    const dto = new ConsumerDatasetDTO();
    dto.id = dataset.id;
    dto.first_published_at = dataset.firstPublishedAt?.toISOString();
    dto.archived_at = dataset.archivedAt?.toISOString();

    if (dataset.replacementDatasetId) {
      const metadata = dataset.replacementDataset?.publishedRevision?.metadata;
      const langCode = lang.toLowerCase();
      dto.replaced_by = {
        dataset_id: dataset.replacementDatasetId,
        dataset_title: metadata?.find((m) => m.language.toLowerCase().includes(langCode))?.title ?? undefined,
        auto_redirect: dataset.replacementAutoRedirect ?? false
      };
    }

    // only return published revisions
    dto.revisions = dataset.revisions
      ?.filter((rev: Revision) => rev.approvedAt && rev.publishAt && isBefore(rev.publishAt, new Date()))
      .map((rev: Revision) => ConsumerRevisionDTO.fromRevision(rev));

    dto.dimensions = dataset.dimensions?.map((dimension: Dimension) => DimensionDTO.fromDimension(dimension));

    if (dataset.publishedRevision) {
      dto.published_revision = ConsumerRevisionDTO.fromRevision(dataset.publishedRevision);
    }

    dto.start_date = dataset.startDate?.toISOString().split('T')[0];
    dto.end_date = dataset.endDate?.toISOString().split('T')[0];

    return dto;
  }
}
