import { isBefore } from 'date-fns';

import { Dataset } from '../entities/dataset/dataset';
import { Dimension } from '../entities/dataset/dimension';
import { Revision } from '../entities/dataset/revision';
import { RevisionProvider } from '../entities/dataset/revision-provider';
import { RevisionTopic } from '../entities/dataset/revision-topic';
import { SUPPORTED_LOCALES } from '../middleware/translation';

import { DimensionDTO } from './dimension-dto';
import { RevisionDTO } from './revision-dto';
import { RevisionMetadataDTO } from './revistion-metadata-dto';
import { RevisionProviderDTO } from './revision-provider-dto';
import { TeamDTO } from './team-dto';
import { TopicDTO } from './topic-dto';

// TODO: make sure to filter any props the consumer side should not have access to
export class ConsumerDatasetDTO {
    id: string;
    live?: string | null;
    dimensions?: DimensionDTO[];
    revisions: RevisionDTO[];
    datasetInfo: RevisionMetadataDTO[];
    providers: RevisionProviderDTO[];
    topics: TopicDTO[];
    team?: TeamDTO[];
    start_date?: Date | null;
    end_date?: Date | null;

    static fromDataset(dataset: Dataset): ConsumerDatasetDTO {
        const dto = new ConsumerDatasetDTO();
        dto.id = dataset.id;
        dto.live = dataset.live?.toISOString();

        // only return published revisions
        dto.revisions = dataset.revisions
            ?.filter((rev: Revision) => rev.approvedAt && rev.publishAt && isBefore(rev.publishAt, new Date()))
            .map((rev: Revision) => RevisionDTO.fromRevision(rev));

        dto.dimensions = dataset.dimensions?.map((dimension: Dimension) => DimensionDTO.fromDimension(dimension));

        const publishedRevision = dataset.publishedRevision;

        dto.providers = publishedRevision.revisionProviders?.map((revProvider: RevisionProvider) =>
            RevisionProviderDTO.fromRevisionProvider(revProvider)
        );

        dto.topics = publishedRevision.revisionTopics?.map((revTopic: RevisionTopic) =>
            TopicDTO.fromTopic(revTopic.topic)
        );

        if (dataset.team) {
            dto.team = SUPPORTED_LOCALES.map((locale) => {
                return TeamDTO.fromTeam(dataset.team, locale);
            });
        }

        dto.start_date = dataset.startDate;
        dto.end_date = dataset.endDate;

        return dto;
    }
}
