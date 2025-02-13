import { MeasureItem } from '../entities/dataset/measure-item';

export class MeasureInfoDTO {
    measure_id: string;
    language: string;
    reference: string;
    sort_order?: number;
    description: string;
    notes?: string;
    format: string;
    decimals?: number;
    measure_type?: string;
    hierarchy?: string;

    static fromMeasureInfo(info: MeasureItem) {
        const dto = new MeasureInfoDTO();
        dto.measure_id = info.id;
        dto.language = info.language;
        dto.reference = info.reference;
        dto.sort_order = info.sortOrder || undefined;
        dto.description = info.description;
        dto.notes = info.notes || undefined;
        dto.format = info.format;
        dto.decimals = info.decimal || undefined;
        dto.measure_type = info.measureType || undefined;
        dto.hierarchy = info.hierarchy || undefined;
        return dto;
    }
}
