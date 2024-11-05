import { DimensionInfo } from '../entities/dataset/dimension-info';

export class DimensionInfoDTO {
    language?: string;
    name: string;
    description?: string;
    notes?: string;

    static fromDimensionInfo(dimensionInfo: DimensionInfo): DimensionInfoDTO {
        const dto = new DimensionInfoDTO();
        dto.language = dimensionInfo.language;
        dto.name = dimensionInfo.name;
        dto.description = dimensionInfo.description;
        dto.notes = dimensionInfo.notes;

        return dto;
    }
}
