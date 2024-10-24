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
    location: string;
    sources?: SourceDTO[];

    static fromImport(importEntity: FileImport): FileImportDTO {
        const dto = new FileImportDTO();
        dto.id = importEntity.id;
        dto.revision_id = importEntity.revision?.id;
        dto.mime_type = importEntity.mimeType;
        dto.filename = importEntity.filename;
        dto.hash = importEntity.hash;
        dto.uploaded_at = importEntity.uploadedAt?.toISOString();
        dto.type = importEntity.type;
        dto.location = importEntity.location;
        dto.sources = [];

        dto.sources = importEntity.sources.map((source: Source) => SourceDTO.fromSource(source));

        return dto;
    }
}
