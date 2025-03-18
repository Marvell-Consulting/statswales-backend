import { Readable } from 'node:stream';

import { BlobServiceClient, BlobUploadCommonResponse } from '@azure/storage-blob';
import { DataLakeServiceClient, FileUploadResponse } from '@azure/storage-file-datalake';
import { FileStore } from '../config/file-store.enum';

export interface StorageService {
  getType(): FileStore;
  getServiceClient(): BlobServiceClient | DataLakeServiceClient;
  saveBuffer(
    filename: string,
    directory: string,
    content: Buffer
  ): Promise<BlobUploadCommonResponse | FileUploadResponse>;
  loadBuffer(filename: string, directory: string): Promise<Buffer>;
  saveStream(
    filename: string,
    directory: string,
    content: Readable
  ): Promise<BlobUploadCommonResponse | FileUploadResponse>;
  loadStream(filename: string, directory: string): Promise<Readable>;
  delete(filename: string, directory: string): Promise<any>;
  deleteDirectory(directory: string): Promise<any>;
  listFiles(directory: string): Promise<any>;
}
