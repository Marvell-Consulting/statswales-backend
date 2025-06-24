import { FileImportInterface } from '../entities/dataset/file-import.interface';
import { FileImportType } from '../enums/file-import-type';

export class FileImportDto {
  filename: string;
  mime_type: string;
  file_type: string;
  hash: string;
  uploaded_at?: string;
  type: FileImportType;
  parent_id?: string;

  static fromFileImport(fileImport: FileImportInterface): FileImportDto {
    const dto = new FileImportDto();
    dto.filename = fileImport.originalFilename || fileImport.filename;
    dto.mime_type = fileImport.mimeType;
    dto.file_type = fileImport.fileType;
    dto.hash = fileImport.hash;
    dto.uploaded_at = fileImport.uploadedAt?.toISOString();
    dto.type = FileImportType.Unknown;
    return dto;
  }
}
