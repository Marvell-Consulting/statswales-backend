import { FileType } from '../../enums/file-type';

export interface FileImportInterface {
  id: string;
  mimeType: string;
  fileType: FileType;
  filename: string;
  originalFilename: string | null;
  hash: string;
  uploadedAt: Date;
}
