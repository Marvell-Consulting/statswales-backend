import { DataTable } from '../entities/dataset/data-table';
import { DataTableDescription } from '../entities/dataset/data-table-description';

import { DataTableDescriptionDto } from './data-table-description-dto';

export class DataTableDto {
    id: string;
    mime_type: string;
    filename: string;
    original_filename: string;
    file_type: string;
    hash: string;
    uploaded_at?: string;
    fact_table_info: DataTableDescriptionDto[];

    static fromDataTable(fileImport: DataTable): DataTableDto {
        const dto = new DataTableDto();
        dto.id = fileImport.id;
        dto.mime_type = fileImport.mimeType;
        dto.filename = fileImport.filename;
        dto.original_filename = fileImport.originalFilename;
        dto.file_type = fileImport.fileType;
        dto.hash = fileImport.hash;
        dto.uploaded_at = fileImport.uploadedAt?.toISOString();
        dto.fact_table_info = [];

        dto.fact_table_info = fileImport.dataTableDescriptions?.map((factTableInfo: DataTableDescription) =>
            DataTableDescriptionDto.fromDataTableDescription(factTableInfo)
        );
        return dto;
    }
}
