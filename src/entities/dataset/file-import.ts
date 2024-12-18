import { FileType } from '../../enums/file-type';

export interface FileImport {
    id: string;
    mimeType: string;
    fileType: FileType;
    filename: string;
    hash: string;
    uploadedAt: Date;
    delimiter: string;
    quote: string;
    linebreak: string;
}
