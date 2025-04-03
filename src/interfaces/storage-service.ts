import { Readable } from 'node:stream';

import { BlobDeleteIfExistsResponse, BlobServiceClient, BlobUploadCommonResponse } from '@azure/storage-blob';
import { DataLakeServiceClient, FileUploadResponse, PathDeleteIfExistsResponse } from '@azure/storage-file-datalake';
import { FileStore } from '../config/file-store.enum';
import { DataLakeFileEntry } from './datalake-file-entry';

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
  delete(filename: string, directory: string): Promise<BlobDeleteIfExistsResponse | PathDeleteIfExistsResponse>;
  deleteDirectory(directory: string): Promise<void | PathDeleteIfExistsResponse>;
  listFiles(directory: string): Promise<string[] | DataLakeFileEntry[]>;
}
