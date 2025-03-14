import { Readable } from 'stream';

import {
  DataLakeDirectoryClient,
  DataLakeFileClient,
  DataLakeFileSystemClient,
  DataLakeServiceClient,
  FileUploadResponse,
  PathDeleteIfExistsResponse,
  StorageSharedKeyCredential
} from '@azure/storage-file-datalake';

import { logger as parentLogger } from '../utils/logger';

const logger = parentLogger.child({ module: 'DataLakeService' });

interface DataLakeConfig {
  url: string;
  accountName: string;
  accountKey: string;
  fileSystemName: string;
}

export class DataLakeService {
  private readonly serviceClient: DataLakeServiceClient;
  private readonly fileSystemClient: DataLakeFileSystemClient;

  public constructor(config: DataLakeConfig) {
    const { url, accountName, accountKey, fileSystemName } = config;

    const sharedKeyCredential = new StorageSharedKeyCredential(accountName, accountKey);
    this.serviceClient = new DataLakeServiceClient(url, sharedKeyCredential);
    this.fileSystemClient = this.serviceClient.getFileSystemClient(fileSystemName);
  }

  public getServiceClient(): DataLakeServiceClient {
    return this.serviceClient;
  }

  private getDirectoryClient(directory: string): DataLakeDirectoryClient {
    return this.fileSystemClient.getDirectoryClient(directory);
  }

  private getFileClient(filename: string, directory: string): DataLakeFileClient {
    return this.getDirectoryClient(directory).getFileClient(filename);
  }

  public async createDirectoryIfNotExists(directory: string) {
    return this.getDirectoryClient(directory).createIfNotExists();
  }

  public async saveBuffer(filename: string, directory: string, content: Buffer): Promise<FileUploadResponse> {
    logger.debug(`Uploading file '${filename}' to datalake as buffer`);
    await this.createDirectoryIfNotExists(directory);
    return this.getFileClient(filename, directory).upload(content);
  }

  public async loadBuffer(filename: string, directory: string): Promise<Buffer> {
    logger.debug(`Fetching file '${filename}' from datalake as buffer`);
    return this.getFileClient(filename, directory).readToBuffer();
  }

  public async saveStream(filename: string, directory: string, content: Readable): Promise<FileUploadResponse> {
    logger.debug(`Uploading file '${filename}' to datalake as stream`);
    await this.createDirectoryIfNotExists(directory);
    return this.getFileClient(filename, directory).uploadStream(content);
  }

  public async loadStream(filename: string, directory: string): Promise<NodeJS.ReadableStream | undefined> {
    logger.debug(`Fetching file '${filename}' from datalake as stream`);
    const downloadResponse = await this.getFileClient(filename, directory).read();
    return downloadResponse.readableStreamBody;
  }

  public async delete(filename: string, directory: string): Promise<PathDeleteIfExistsResponse> {
    return this.getFileClient(filename, directory).deleteIfExists();
  }

  public async deleteDirectory(directory: string): Promise<PathDeleteIfExistsResponse> {
    return this.getDirectoryClient(directory).deleteIfExists(true);
  }
}
