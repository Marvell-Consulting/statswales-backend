import { MeasureInfo } from '../entities/dataset/measure-info';

export class MeasureInfoDTO {
    measure_id: string;
    language: string;
    reference_id: string;
    sort_order: number;
    description: string;
    notes: string;
    display_type: string;

    static fromMeasureInfo(info: MeasureInfo) {
        const dto = new MeasureInfoDTO();
        dto.measure_id = info.id;
        dto.language = info.language;
        dto.reference_id = info.reference;
        dto.sort_order = info.sortOrder;
        dto.description = info.description;
        dto.notes = info.notes;
        dto.display_type = info.displayType;
        return dto;
    }
}
