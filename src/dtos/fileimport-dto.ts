import { FileImport } from '../entities/file-import';
import { Source } from '../entities/source';

import { SourceDTO } from './source-dto';

export class ImportDTO {
    id: string;
    revision_id: string;
    mime_type: string;
    filename: string;
    hash: string;
    uploaded_at?: string;
    type: string;
    location: string;
    sources?: SourceDTO[];

    static async fromImport(importEntity: FileImport): Promise<ImportDTO> {
        const dto = new ImportDTO();
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

    static async fromImportWithSources(importEntity: FileImport): Promise<ImportDTO> {
        const dto = await ImportDTO.fromImport(importEntity);
        dto.sources = await Promise.all(
            (await importEntity.sources).map(async (source: Source) => {
                const sourceDto = await SourceDTO.fromSource(source);
                return sourceDto;
            })
        );
        return dto;
    }
}
