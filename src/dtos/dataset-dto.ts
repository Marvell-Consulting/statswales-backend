import { Dataset } from '../entities/dataset/dataset';
import { Dimension } from '../entities/dataset/dimension';
import { Revision } from '../entities/dataset/revision';
import { DatasetInfo } from '../entities/dataset/dataset-info';
import { DatasetProvider } from '../entities/dataset/dataset-provider';
import { DatasetTopic } from '../entities/dataset/dataset-topic';

import { DimensionDTO } from './dimension-dto';
import { RevisionDTO } from './revision-dto';
import { DatasetInfoDTO } from './dataset-info-dto';
import { DatasetProviderDTO } from './dataset-provider-dto';
import { DatasetTopicDTO } from './dataset-topic-dto';
import { MeasureDTO } from './measure-dto';

export class DatasetDTO {
    id: string;
    created_at: string;
    created_by: string;
    live?: string;
    archive?: string;
    dimensions?: DimensionDTO[];
    revisions: RevisionDTO[];
    measure?: MeasureDTO;
    datasetInfo: DatasetInfoDTO[];
    providers: DatasetProviderDTO[];
    topics: DatasetTopicDTO[];
    team_id?: string;

    static fromDataset(dataset: Dataset): DatasetDTO {
        const dto = new DatasetDTO();
        dto.id = dataset.id;
        dto.created_at = dataset.createdAt.toISOString();
        dto.created_by = dataset.createdBy?.name;
        dto.live = dataset.live?.toISOString();
        dto.archive = dataset.archive?.toISOString();

        dto.datasetInfo = dataset.datasetInfo?.map((info: DatasetInfo) => DatasetInfoDTO.fromDatasetInfo(info));
        dto.dimensions = dataset.dimensions?.map((dimension: Dimension) => DimensionDTO.fromDimension(dimension));
        dto.revisions = dataset.revisions?.map((revision: Revision) => RevisionDTO.fromRevision(revision));
        dto.measure = dataset.measure ? MeasureDTO.fromMeasure(dataset.measure) : undefined;
        dto.providers = dataset.datasetProviders?.map((datasetProvider: DatasetProvider) =>
            DatasetProviderDTO.fromDatasetProvider(datasetProvider)
        );

        dto.topics = dataset.datasetTopics?.map((datasetTopic: DatasetTopic) =>
            DatasetTopicDTO.fromDatasetTopic(datasetTopic)
        );

        dto.team_id = dataset.team?.id;

        return dto;
    }
}
