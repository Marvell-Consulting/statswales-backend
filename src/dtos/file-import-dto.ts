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

    static async fromImport(importEntity: FileImport): Promise<FileImportDTO> {
        const dto = new FileImportDTO();
        dto.id = importEntity.id;
        const revision = await importEntity.revision;
        dto.revision_id = revision.id;
        dto.mime_type = importEntity.mimeType;
        dto.filename = importEntity.filename;
        dto.hash = importEntity.hash;
        dto.uploaded_at = importEntity.uploadedAt?.toISOString();
        dto.type = importEntity.type;
        dto.location = importEntity.location;
        dto.sources = [];
        return dto;
    }

    static async fromImportWithSources(importEntity: FileImport): Promise<FileImportDTO> {
        const dto = await FileImportDTO.fromImport(importEntity);
        dto.sources = await Promise.all(
            (await importEntity.sources).map(async (source: Source) => {
                const sourceDto = await SourceDTO.fromSource(source);
                return sourceDto;
            })
        );
        return dto;
    }
}
