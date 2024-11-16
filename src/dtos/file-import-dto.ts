import { FileImport } from '../entities/dataset/file-import';
import { Source } from '../entities/dataset/source';

import { SourceDTO } from './source-dto';

export class FileImportDTO {
    id: string;
    revision_id: string;
    mime_type: string;
    filename: string;
    hash: string;
    uploaded_at?: string;
    type: string;
    sources?: SourceDTO[];

    static fromImport(fileImport: FileImport): FileImportDTO {
        const dto = new FileImportDTO();
        dto.id = fileImport.id;
        dto.revision_id = fileImport.revision?.id;
        dto.mime_type = fileImport.mimeType;
        dto.filename = fileImport.filename;
        dto.hash = fileImport.hash;
        dto.uploaded_at = fileImport.uploadedAt?.toISOString();
        dto.type = fileImport.type;
        dto.sources = [];

        dto.sources = fileImport.sources?.map((source: Source) => SourceDTO.fromSource(source));

        return dto;
    }
}
