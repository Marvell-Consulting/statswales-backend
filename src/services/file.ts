import { Readable } from 'node:stream';

import { FileStore } from '../config/file-store.enum';
import { AppConfig } from '../config/app-config.interface';
import { BlobStorageService } from './blob';
import { DataLakeService } from './datalake';
import { BlobDeleteIfExistsResponse, BlobUploadCommonResponse } from '@azure/storage-blob';
import { FileUploadResponse, PathDeleteIfExistsResponse } from '@azure/storage-file-datalake';

export class FileService {
  public store: FileStore;

  private blobService: BlobStorageService;
  private datalakeService: DataLakeService;

  public constructor(config: AppConfig) {
    this.store = config.storage.store;

    switch (this.store) {
      case FileStore.Blob:
        this.blobService = new BlobStorageService(config.storage.blob);
        break;

      case FileStore.DataLake:
        this.datalakeService = new DataLakeService(config.storage.datalake);
        break;

      default:
        throw new Error('Could not determine file storage client');
    }
  }

  public getStore(): FileStore {
    return this.store;
  }

  public getStorageService(): BlobStorageService | DataLakeService {
    return this.store === FileStore.Blob ? this.blobService : this.datalakeService;
  }

  public getServiceClient() {
    return this.getStorageService().getServiceClient();
  }

  public async saveBuffer(
    filename: string,
    directory: string,
    content: Buffer
  ): Promise<BlobUploadCommonResponse | FileUploadResponse> {
    return this.getStorageService().saveBuffer(filename, directory, content);
  }

  public loadBuffer(filename: string, directory: string): Promise<Buffer> {
    return this.getStorageService().loadBuffer(filename, directory);
  }

  public async saveStream(
    filename: string,
    directory: string,
    content: Readable
  ): Promise<BlobUploadCommonResponse | FileUploadResponse> {
    return this.getStorageService().saveStream(filename, directory, content);
  }

  public async loadStream(filename: string, directory: string): Promise<NodeJS.ReadableStream | undefined> {
    return this.getStorageService().loadStream(filename, directory);
  }

  public async delete(
    filename: string,
    directory: string
  ): Promise<BlobDeleteIfExistsResponse | PathDeleteIfExistsResponse> {
    return this.getStorageService().delete(filename, directory);
  }

  public async deleteDirectory(directory: string): Promise<PathDeleteIfExistsResponse | void> {
    return this.getStorageService().deleteDirectory(directory);
  }
}
