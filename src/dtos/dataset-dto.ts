import { Dataset } from '../entities/dataset/dataset';
import { Dimension } from '../entities/dataset/dimension';
import { Revision } from '../entities/dataset/revision';
import { DatasetInfo } from '../entities/dataset/dataset-info';
import { DatasetProvider } from '../entities/dataset/dataset-provider';

import { DimensionDTO } from './dimension-dto';
import { RevisionDTO } from './revision-dto';
import { DatasetInfoDTO } from './dataset-info-dto';
import { DatasetProviderDTO } from './dataset-provider-dto';

export class DatasetDTO {
    id: string;
    created_at: string;
    created_by: string;
    live?: string;
    archive?: string;
    dimensions?: DimensionDTO[];
    revisions: RevisionDTO[];
    datasetInfo: DatasetInfoDTO[];
    providers: DatasetProviderDTO[];

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

        dto.providers = dataset.datasetProviders?.map((datasetProvider: DatasetProvider) =>
            DatasetProviderDTO.fromDatasetProvider(datasetProvider)
        );

        return dto;
    }
}
