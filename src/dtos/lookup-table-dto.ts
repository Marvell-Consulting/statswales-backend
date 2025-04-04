import { LookupTable } from '../entities/dataset/lookup-table';

export class LookupTableDTO {
  id: string;
  mime_type: string;
  filename: string;
  original_filename: string | null;
  file_type: string;
  hash: string;
  uploaded_at?: string;

  static fromLookupTable(lookupTable: LookupTable): LookupTableDTO {
    const dto = new LookupTableDTO();
    dto.id = lookupTable.id;
    dto.mime_type = lookupTable.mimeType;
    dto.filename = lookupTable.filename;
    dto.original_filename = lookupTable.originalFilename;
    dto.file_type = lookupTable.fileType;
    dto.hash = lookupTable.hash;
    dto.uploaded_at = lookupTable.uploadedAt?.toISOString();
    return dto;
  }
}
