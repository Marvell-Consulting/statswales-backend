import { Readable } from 'node:stream';

import {
  BlobDeleteIfExistsResponse,
  BlobItem,
  BlobServiceClient,
  BlobUploadCommonResponse,
  BlockBlobClient,
  ContainerClient,
  StorageSharedKeyCredential
} from '@azure/storage-blob';

import { FileStore } from '../config/file-store.enum';
import { StorageService } from '../interfaces/storage-service';
import { logger as parentLogger } from '../utils/logger';

const logger = parentLogger.child({ module: 'BlobStorage' });

interface BlobStorageConfig {
  url: string;
  accountName: string;
  accountKey: string;
  containerName: string;
}

export default class BlobStorage implements StorageService {
  private readonly serviceClient: BlobServiceClient;
  private readonly containerClient: ContainerClient;
  private readonly delimiter = '/'; // used for virtual directories

  public constructor(config: BlobStorageConfig) {
    const { url, accountName, accountKey, containerName } = config;
    const sharedKeyCredential = new StorageSharedKeyCredential(accountName, accountKey);
    this.serviceClient = new BlobServiceClient(url, sharedKeyCredential);
    this.containerClient = this.serviceClient.getContainerClient(containerName);

    if (this.containerClient) {
      this.containerClient.createIfNotExists();
    }
  }

  public getType(): FileStore {
    return FileStore.Blob;
  }

  public getServiceClient(): BlobServiceClient {
    return this.serviceClient;
  }

  public getContainerClient(): ContainerClient {
    return this.containerClient;
  }

  private getBlockBlobClient(filename: string): BlockBlobClient {
    return this.getContainerClient().getBlockBlobClient(filename);
  }

  private getNamespacedFilename(filename: string, directory: string): string {
    return `${directory}${this.delimiter}${filename}`;
  }

  public async saveBuffer(filename: string, directory: string, content: Buffer): Promise<BlobUploadCommonResponse> {
    const namespacedFilename = this.getNamespacedFilename(filename, directory);
    logger.debug(`Uploading file '${namespacedFilename}' to blob storage as buffer`);
    return this.getBlockBlobClient(namespacedFilename).uploadData(content);
  }

  public async loadBuffer(filename: string, directory: string): Promise<Buffer> {
    const namespacedFilename = this.getNamespacedFilename(filename, directory);
    logger.debug(`Fetching file '${namespacedFilename}' from blob storage as buffer`);
    return this.getBlockBlobClient(namespacedFilename).downloadToBuffer();
  }

  public async saveStream(filename: string, directory: string, content: Readable): Promise<BlobUploadCommonResponse> {
    const namespacedFilename = this.getNamespacedFilename(filename, directory);
    logger.debug(`Uploading file '${namespacedFilename}' to blob storage as stream`);
    return this.getBlockBlobClient(namespacedFilename).uploadStream(content);
  }

  public async loadStream(filename: string, directory: string): Promise<Readable> {
    const namespacedFilename = this.getNamespacedFilename(filename, directory);
    logger.debug(`Fetching file '${namespacedFilename}' from blob storage as stream`);
    const downloadResponse = await this.getBlockBlobClient(namespacedFilename).download();

    if (!downloadResponse.readableStreamBody) {
      throw new Error(`Failed to download file '${namespacedFilename}' from blob storage`);
    }

    return downloadResponse.readableStreamBody as Readable;
  }

  public async delete(filename: string, directory: string): Promise<BlobDeleteIfExistsResponse> {
    const namespacedFilename = this.getNamespacedFilename(filename, directory);
    logger.warn(`Deleting file '${namespacedFilename}' from blob storage`);
    return this.getBlockBlobClient(namespacedFilename).deleteIfExists();
  }

  private async listHierarchical(
    containerClient: ContainerClient,
    prefix: string,
    blobAction?: (blob: BlobItem) => Promise<any>
  ): Promise<void> {
    for await (const blob of containerClient.listBlobsByHierarchy(this.delimiter, { prefix })) {
      if (blob.kind === 'prefix') {
        await this.listHierarchical(containerClient, `${prefix}${blob.name}`, blobAction);
      } else {
        if (blobAction) {
          await blobAction(blob);
        }
      }
    }
  }

  public async deleteDirectory(directory: string): Promise<void> {
    const deleteBlobFn = async (blob: BlobItem) => {
      await this.getBlockBlobClient(blob.name).deleteIfExists();
    };
    await this.listHierarchical(this.containerClient, directory, deleteBlobFn);
  }

  public async listFiles(directory: string): Promise<string[]> {
    const files: string[] = [];
    const listBlobFn = async (blob: BlobItem) => Promise.resolve().then(() => files.push(blob.name));
    await this.listHierarchical(this.containerClient, `${directory}`, listBlobFn);

    return files;
  }
}
