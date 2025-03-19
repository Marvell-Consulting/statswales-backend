import { basename } from 'node:path';
import { Readable } from 'node:stream';

import {
  DataLakeDirectoryClient,
  DataLakeFileClient,
  DataLakeFileSystemClient,
  DataLakeServiceClient,
  DirectoryCreateIfNotExistsResponse,
  FileUploadResponse,
  PathDeleteIfExistsResponse,
  StorageSharedKeyCredential
} from '@azure/storage-file-datalake';

import { FileStore } from '../config/file-store.enum';
import { StorageService } from '../interfaces/storage-service';
import { logger as parentLogger } from '../utils/logger';

const logger = parentLogger.child({ module: 'DataLake' });

interface DataLakeConfig {
  url: string;
  accountName: string;
  accountKey: string;
  fileSystemName: string;
}

export default class DataLakeStorage implements StorageService {
  private readonly serviceClient: DataLakeServiceClient;
  private readonly fileSystemClient: DataLakeFileSystemClient;

  public constructor(config: DataLakeConfig) {
    const { url, accountName, accountKey, fileSystemName } = config;

    const sharedKeyCredential = new StorageSharedKeyCredential(accountName, accountKey);
    this.serviceClient = new DataLakeServiceClient(url, sharedKeyCredential);
    this.fileSystemClient = this.serviceClient.getFileSystemClient(fileSystemName);
  }

  public getType(): FileStore {
    return FileStore.DataLake;
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

  public async createDirectoryIfNotExists(directory: string): Promise<DirectoryCreateIfNotExistsResponse> {
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

  public async loadStream(filename: string, directory: string): Promise<Readable> {
    logger.debug(`Fetching file '${filename}' from datalake as stream`);
    const downloadResponse = await this.getFileClient(filename, directory).read();

    if (!downloadResponse.readableStreamBody) {
      throw new Error(`Failed to download file '${filename}' from datalake`);
    }

    return downloadResponse.readableStreamBody as Readable;
  }

  public async delete(filename: string, directory: string): Promise<PathDeleteIfExistsResponse> {
    return this.getFileClient(filename, directory).deleteIfExists();
  }

  public async deleteDirectory(directory: string): Promise<PathDeleteIfExistsResponse> {
    return this.getDirectoryClient(directory).deleteIfExists(true);
  }

  public async listFiles(directory: string): Promise<Record<string, unknown>[]> {
    const files = await this.fileSystemClient.listPaths({ path: directory });
    const fileList = [];

    for await (const file of files) {
      if (file.name === undefined) {
        continue;
      }
      fileList.push({ name: basename(file.name), path: file.name, isDirectory: file.isDirectory });
    }

    return fileList;
  }
}
