import { Dataset } from '../../entities/dataset/dataset';
import { Revision } from '../../entities/dataset/revision';
import { Dimension } from '../../entities/dataset/dimension';
import { Locale } from '../../enums/locale';
import { PublisherDTO } from '../publisher-dto';
import { SingleLanguageRevisionDTO } from './single-language-revision-dto';
import { SingleLanguageDimensionDTO } from './single-language-dimension-dto';
import { isPublished } from '../../utils/revision';
import { SingleLanguageMeasureDTO } from './single-language-measure-dto';

export class SingleLanguageDatasetDTO {
  id: string;
  first_published_at?: string;
  archived_at?: string;
  start_date?: string;
  end_date?: string;
  data_description?: SingleLanguageMeasureDTO;
  dimensions?: SingleLanguageDimensionDTO[];
  revisions: SingleLanguageRevisionDTO[];
  published_revision?: SingleLanguageRevisionDTO;
  publisher?: PublisherDTO;

  static fromDataset(dataset: Dataset, lang: Locale): SingleLanguageDatasetDTO {
    const dto = new SingleLanguageDatasetDTO();
    dto.id = dataset.id;
    dto.first_published_at = dataset.firstPublishedAt?.toISOString();
    dto.archived_at = dataset.archivedAt?.toISOString();
    dto.start_date = dataset.startDate?.toISOString();
    dto.end_date = dataset.endDate?.toISOString();

    dto.dimensions = dataset.dimensions?.map((dimension: Dimension) =>
      SingleLanguageDimensionDTO.fromDimension(dimension, lang)
    );

    dto.revisions = dataset.revisions
      ?.filter((rev: Revision) => isPublished(rev)) // safety fallback in case unpublished revisions are included
      .map((rev: Revision) => SingleLanguageRevisionDTO.fromRevision(rev, lang));

    if (dataset.publishedRevision) {
      dto.published_revision = SingleLanguageRevisionDTO.fromRevision(dataset.publishedRevision, lang);
    }

    if (dataset.userGroup) {
      dto.publisher = PublisherDTO.fromUserGroup(dataset.userGroup, lang);
    }

    dto.data_description = dataset.measure ? SingleLanguageMeasureDTO.fromMeasure(dataset.measure, lang) : undefined;

    return dto;
  }
}
