import { DatasetInfo } from '../entities/dataset/dataset-info';

export class DatasetInfoDTO {
    language: string;
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
