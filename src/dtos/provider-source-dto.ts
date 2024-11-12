import { ProviderSource } from '../entities/dataset/provider-source';

export class ProviderSourceDTO {
    id: string;
    language: string;
    name: string;
    provider_id: string;

    static fromProviderSource(providerSource: ProviderSource): ProviderSourceDTO {
        const dto = new ProviderSourceDTO();
        dto.id = providerSource.id;
        dto.language = providerSource.language;
        dto.name = providerSource.name;
        dto.provider_id = providerSource.providerId;

        return dto;
    }
}
