import { DimensionMetadata } from '../entities/dataset/dimension-metadata';

export class DimensionMetadataDTO {
    language: string;
    name: string;
    description?: string;
    notes?: string;

    static fromDimensionMetadata(dimensionInfo: DimensionMetadata): DimensionMetadataDTO {
        const dto = new DimensionMetadataDTO();
        dto.language = dimensionInfo.language;
        dto.name = dimensionInfo.name;
        dto.description = dimensionInfo.description;
        dto.notes = dimensionInfo.notes;

        return dto;
    }
}
