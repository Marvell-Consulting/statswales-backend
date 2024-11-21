import { IsISO8601, IsNotEmpty, IsOptional, IsString, IsUUID } from 'class-validator';
import { v4 as uuid } from 'uuid';

import { DatasetProvider } from '../entities/dataset/dataset-provider';

export class DatasetProviderDTO {
    @IsUUID(4)
    @IsOptional()
    id?: string;

    @IsUUID(4)
    @IsOptional()
    group_id?: string;

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

    @IsISO8601()
    @IsOptional()
    created_at?: string;

    static fromDatasetProvider(datasetProvider: DatasetProvider): DatasetProviderDTO {
        const dto = new DatasetProviderDTO();
        dto.id = datasetProvider.id;
        dto.group_id = datasetProvider.groupId;
        dto.dataset_id = datasetProvider.datasetId;
        dto.language = datasetProvider.language;
        dto.provider_id = datasetProvider.providerId;
        dto.provider_name = datasetProvider.provider?.name;
        dto.source_id = datasetProvider.providerSourceId;
        dto.source_name = datasetProvider.providerSource?.name;
        dto.created_at = datasetProvider.createdAt.toISOString();

        return dto;
    }

    static toDatsetProvider(dto: DatasetProviderDTO): DatasetProvider {
        const datasetProvider = new DatasetProvider();
        datasetProvider.id = dto.id || uuid();
        datasetProvider.groupId = dto.group_id || uuid();
        datasetProvider.datasetId = dto.dataset_id;
        datasetProvider.language = dto.language?.toLowerCase();
        datasetProvider.providerId = dto.provider_id;
        datasetProvider.providerSourceId = dto.source_id;
        datasetProvider.createdAt = dto.created_at ? new Date(dto.created_at) : new Date();

        return datasetProvider;
    }
}
