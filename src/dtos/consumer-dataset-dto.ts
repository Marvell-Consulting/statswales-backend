import { isBefore } from 'date-fns';

import { Dataset } from '../entities/dataset/dataset';
import { Dimension } from '../entities/dataset/dimension';
import { Revision } from '../entities/dataset/revision';
import { DatasetInfo } from '../entities/dataset/dataset-info';
import { DatasetProvider } from '../entities/dataset/dataset-provider';
import { DatasetTopic } from '../entities/dataset/dataset-topic';
import { SUPPORTED_LOCALES } from '../middleware/translation';

import { DimensionDTO } from './dimension-dto';
import { RevisionDTO } from './revision-dto';
import { DatasetInfoDTO } from './dataset-info-dto';
import { DatasetProviderDTO } from './dataset-provider-dto';
import { TeamDTO } from './team-dto';
import { TopicDTO } from './topic-dto';

export class ConsumerDatasetDTO {
    id: string;
    live?: string | null;
    dimensions?: DimensionDTO[];
    revisions: RevisionDTO[];
    datasetInfo: DatasetInfoDTO[];
    providers: DatasetProviderDTO[];
    topics: TopicDTO[];
    team?: TeamDTO[];
    start_date?: Date | null;
    end_date?: Date | null;

    // Make sure to filter any props the consumer side should not have access to
    static fromDataset(dataset: Dataset): ConsumerDatasetDTO {
        const dto = new ConsumerDatasetDTO();
        dto.id = dataset.id;
        dto.live = dataset.live?.toISOString();

        // only return published revisions
        dto.revisions = dataset.revisions
            ?.filter((rev: Revision) => rev.approvedAt && rev.publishAt && isBefore(rev.publishAt, new Date()))
            .map((rev: Revision) => RevisionDTO.fromRevision(rev));

        dto.datasetInfo = dataset.datasetInfo?.map((info: DatasetInfo) => DatasetInfoDTO.fromDatasetInfo(info));
        dto.dimensions = dataset.dimensions?.map((dimension: Dimension) => DimensionDTO.fromDimension(dimension));
        dto.providers = dataset.datasetProviders?.map((datasetProvider: DatasetProvider) =>
            DatasetProviderDTO.fromDatasetProvider(datasetProvider)
        );

        dto.topics = dataset.datasetTopics?.map((datasetTopic: DatasetTopic) => TopicDTO.fromTopic(datasetTopic.topic));

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
