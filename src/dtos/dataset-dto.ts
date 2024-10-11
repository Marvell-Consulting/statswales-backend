import { Dataset } from '../entities/dataset/dataset';
import { Dimension } from '../entities/dataset/dimension';
import { Revision } from '../entities/dataset/revision';
import { DatasetInfo } from '../entities/dataset/dataset-info';

import { DimensionDTO } from './dimension-dto';
import { RevisionDTO } from './revision-dto';

export class DatasetInfoDTO {
    language?: string;
    title?: string;
    description?: string;

    static fromDatasetInfo(datasetInfo: DatasetInfo): DatasetInfoDTO {
        const dto = new DatasetInfoDTO();
        dto.language = datasetInfo.language;
        dto.title = datasetInfo.title;
        dto.description = datasetInfo.description;
        return dto;
    }
}

export class DatasetDTO {
    id: string;
    created_at: string;
    created_by: string;
    live?: string;
    archive?: string;
    dimensions?: DimensionDTO[];
    revisions: RevisionDTO[];
    datasetInfo: DatasetInfoDTO[];

    static async fromDatasetShallow(dataset: Dataset): Promise<DatasetDTO> {
        const dto = new DatasetDTO();
        dto.id = dataset.id;
        dto.created_at = dataset.createdAt.toISOString();
        dto.created_by = (await dataset.createdBy).name;
        dto.live = dataset.live?.toISOString();
        dto.archive = dataset.archive?.toISOString();
        dto.datasetInfo = (await dataset.datasetInfo).map((datasetInfo: DatasetInfo) => {
            const infoDto = DatasetInfoDTO.fromDatasetInfo(datasetInfo);
            return infoDto;
        });
        dto.dimensions = [];
        dto.revisions = [];
        return dto;
    }

    static async fromDatasetComplete(dataset: Dataset): Promise<DatasetDTO> {
        const dto = await DatasetDTO.fromDatasetShallow(dataset);
        dto.dimensions = await Promise.all(
            (await dataset.dimensions).map(async (dimension: Dimension) => {
                const dimDto = await DimensionDTO.fromDimensionWithSources(dimension);
                return dimDto;
            })
        );
        dto.revisions = await Promise.all(
            (await dataset.revisions).map(async (revision: Revision) => {
                const revDto = await RevisionDTO.fromRevisionWithImportsAndSources(revision);
                return revDto;
            })
        );
        return dto;
    }

    static async fromDatasetWithRevisions(dataset: Dataset): Promise<DatasetDTO> {
        const dto = await DatasetDTO.fromDatasetShallow(dataset);
        dto.revisions = await Promise.all(
            (await dataset.revisions).map(async (revision: Revision) => {
                const revDto = await RevisionDTO.fromRevision(revision);
                return revDto;
            })
        );
        return dto;
    }

    static async fromDatasetWithRevisionsAndImports(dataset: Dataset): Promise<DatasetDTO> {
        const dto = await DatasetDTO.fromDatasetShallow(dataset);
        dto.dimensions = [];
        dto.revisions = await Promise.all(
            (await dataset.revisions).map(async (revision: Revision) => {
                const revDto = await RevisionDTO.fromRevisionWithImports(revision);
                return revDto;
            })
        );
        return dto;
    }

    static async fromDatasetWithShallowDimensionsAndRevisions(dataset: Dataset): Promise<DatasetDTO> {
        const dto = await DatasetDTO.fromDatasetShallow(dataset);
        dto.dimensions = await Promise.all(
            (await dataset.dimensions).map(async (dimension: Dimension) => {
                const dimDto = await DimensionDTO.fromDimension(dimension);
                return dimDto;
            })
        );
        dto.revisions = await Promise.all(
            (await dataset.revisions).map(async (revision: Revision) => {
                const revDto = await RevisionDTO.fromRevision(revision);
                return revDto;
            })
        );
        return dto;
    }
}
