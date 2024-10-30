import { IsNotEmpty, IsOptional, IsString } from 'class-validator';

import { DatasetInfo } from '../entities/dataset/dataset-info';

export class DatasetInfoDTO {
    @IsString()
    @IsNotEmpty()
    language: string;

    @IsString()
    @IsOptional()
    title?: string;

    @IsString()
    @IsOptional()
    description?: string;

    @IsString()
    @IsOptional()
    collection?: string;

    @IsString()
    @IsOptional()
    quality?: string;

    static fromDatasetInfo(datasetInfo: DatasetInfo): DatasetInfoDTO {
        const dto = new DatasetInfoDTO();
        dto.language = datasetInfo.language;
        dto.title = datasetInfo.title;
        dto.description = datasetInfo.description;
        dto.collection = datasetInfo.collection;
        dto.quality = datasetInfo.quality;

        return dto;
    }
}
