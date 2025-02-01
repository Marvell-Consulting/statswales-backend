import { Dataset } from '../entities/dataset/dataset';
import { Dimension } from '../entities/dataset/dimension';
import { Revision } from '../entities/dataset/revision';
import { DatasetMetadata } from '../entities/dataset/dataset-metadata';
import { DatasetProvider } from '../entities/dataset/dataset-provider';
import { DatasetTopic } from '../entities/dataset/dataset-topic';
import { SUPPORTED_LOCALES } from '../middleware/translation';

import { DimensionDTO } from './dimension-dto';
import { RevisionDTO } from './revision-dto';
import { DatasetInfoDTO } from './dataset-info-dto';
import { DatasetProviderDTO } from './dataset-provider-dto';
import { MeasureDTO } from './measure-dto';
import { TeamDTO } from './team-dto';
import { TopicDTO } from './topic-dto';

export class DatasetDTO {
    id: string;
    created_at: string;
    created_by: string;
    live?: string | null;
    archive?: string;
    dimensions?: DimensionDTO[];
    revisions: RevisionDTO[];
    measure?: MeasureDTO;
    datasetInfo: DatasetInfoDTO[];
    providers: DatasetProviderDTO[];
    topics: TopicDTO[];
    team?: TeamDTO[];
    start_date?: Date | null;
    end_date?: Date | null;

    static fromDataset(dataset: Dataset): DatasetDTO {
        const dto = new DatasetDTO();
        dto.id = dataset.id;
        dto.created_at = dataset.createdAt.toISOString();
        dto.created_by = dataset.createdBy?.name;
        dto.live = dataset.live?.toISOString();
        dto.archive = dataset.archive?.toISOString();

        dto.datasetInfo = dataset.metadata?.map((info: DatasetMetadata) => DatasetInfoDTO.fromDatasetInfo(info));
        dto.dimensions = dataset.dimensions?.map((dimension: Dimension) => DimensionDTO.fromDimension(dimension));
        dto.revisions = dataset.revisions?.map((revision: Revision) => RevisionDTO.fromRevision(revision));
        dto.measure = dataset.measure ? MeasureDTO.fromMeasure(dataset.measure) : undefined;
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
