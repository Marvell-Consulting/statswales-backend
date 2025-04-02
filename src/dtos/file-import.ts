import { FileImportInterface } from '../entities/dataset/file-import.interface';

export class FileImportDto {
  filename: string;
  mime_type: string;
  file_type: string;
  hash: string;
  uploaded_at?: string;

  static fromFileImport(fileImport: FileImportInterface) {
    const dto = new FileImportDto();
    dto.filename = fileImport.originalFilename || fileImport.filename;
    dto.mime_type = fileImport.mimeType;
    dto.file_type = fileImport.fileType;
    dto.hash = fileImport.hash;
    dto.uploaded_at = fileImport.uploadedAt?.toISOString();
    return dto;
  }
}
