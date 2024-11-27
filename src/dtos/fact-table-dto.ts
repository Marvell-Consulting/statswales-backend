import { FactTable } from '../entities/dataset/fact-table';
import { FactTableInfo } from '../entities/dataset/fact-table-info';

import { FactTableInfoDto } from './fact-table-info-dto';

export class FactTableDTO {
    id: string;
    revision_id: string;
    mime_type: string;
    filename: string;
    file_type: string;
    hash: string;
    uploaded_at?: string;
    delimiter: string;
    quote: string;
    linebreak: string;
    fact_table_info?: FactTableInfoDto[];

    static fromFactTable(fileImport: FactTable): FactTableDTO {
        const dto = new FactTableDTO();
        dto.id = fileImport.id;
        dto.revision_id = fileImport.revision?.id;
        dto.mime_type = fileImport.mimeType;
        dto.filename = fileImport.filename;
        dto.file_type = fileImport.fileType;
        dto.hash = fileImport.hash;
        dto.uploaded_at = fileImport.uploadedAt?.toISOString();
        dto.quote = fileImport.quote;
        dto.linebreak = fileImport.linebreak;
        dto.delimiter = fileImport.delimiter;
        dto.fact_table_info = [];

        dto.fact_table_info = fileImport.factTableInfo?.map((factTableInfo: FactTableInfo) =>
            FactTableInfoDto.fromFactTableInfo(factTableInfo)
        );
        return dto;
    }
}
