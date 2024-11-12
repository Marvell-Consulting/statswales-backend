import { IsNotEmpty, IsOptional, IsString, IsUUID } from 'class-validator';

import { DatasetProvider } from '../entities/dataset/dataset-provider';

export class DatasetProviderDTO {
    id?: string;

    @IsUUID(4)
    @IsNotEmpty()
    dataset_id: string;

    @IsString()
    @IsNotEmpty()
    language: string;

    @IsUUID(4)
    @IsNotEmpty()
    provider_id: string;

    provider_name?: string;

    @IsUUID(4)
    @IsOptional()
    source_id?: string;

    source_name?: string;

    static fromDatasetProvider(datasetProvider: DatasetProvider): DatasetProviderDTO {
        const dto = new DatasetProviderDTO();
        dto.id = datasetProvider.id;
        dto.dataset_id = datasetProvider.datasetId;
        dto.language = datasetProvider.language;
        dto.provider_id = datasetProvider.providerId;
        dto.provider_name = datasetProvider.provider?.name;
        dto.source_id = datasetProvider.providerSourceId;
        dto.source_name = datasetProvider.providerSource?.name;

        return dto;
    }
}
