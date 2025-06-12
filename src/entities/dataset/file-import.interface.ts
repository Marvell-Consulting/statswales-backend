import { FileType } from '../../enums/file-type';

export interface FileImportInterface {
  id: string;
  mimeType: string;
  fileType: FileType;
  filename: string;
  encoding: 'utf-8' | 'latin-1' | null;
  originalFilename: string | null;
  hash: string;
  uploadedAt: Date;
}
