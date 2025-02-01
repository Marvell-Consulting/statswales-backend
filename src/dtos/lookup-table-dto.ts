import { LookupTable } from '../entities/dataset/lookup-table';

export class LookupTableDTO {
    id: string;
    dimension_id?: string;
    measure_id?: string;
    mime_type: string;
    filename: string;
    file_type: string;
    hash: string;
    uploaded_at?: string;

    static fromLookupTable(lookupTable: LookupTable): LookupTableDTO {
        const dto = new LookupTableDTO();
        dto.id = lookupTable.id;
        dto.dimension_id = lookupTable.dimension?.id || undefined;
        dto.measure_id = lookupTable.measure?.id || undefined;
        dto.mime_type = lookupTable.mimeType;
        dto.filename = lookupTable.filename;
        dto.file_type = lookupTable.fileType;
        dto.hash = lookupTable.hash;
        dto.uploaded_at = lookupTable.uploadedAt?.toISOString();
        return dto;
    }
}
