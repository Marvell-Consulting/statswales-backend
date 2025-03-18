import { Readable } from 'node:stream';

import {
  DataLakeServiceClient,
  DataLakeFileSystemClient,
  DataLakeDirectoryClient,
  DataLakeFileClient,
  FileUploadResponse,
  PathDeleteIfExistsResponse,
  StorageSharedKeyCredential,
  DirectoryCreateIfNotExistsResponse,
  Path,
  FileSystemListPathsResponse
} from '@azure/storage-file-datalake';
import { PagedAsyncIterableIterator } from '@azure/core-paging';

import DataLakeStorage from '../../src/services/datalake-storage';
import { FileStore } from '../../src/config/file-store.enum';

jest.mock('@azure/storage-file-datalake');

describe('DataLakeStorage', () => {
  const config = {
    url: 'https://azure.example.com',
    accountName: 'accountName',
    accountKey: 'accountKey',
    fileSystemName: 'fileSystemName'
  };

  let dataLakeStorage: DataLakeStorage;
  let fsClientMock: jest.Mocked<DataLakeFileSystemClient>;
  let directoryClientMock: jest.Mocked<DataLakeDirectoryClient>;
  let fileClientMock: jest.Mocked<DataLakeFileClient>;

  beforeEach(() => {
    const credentials = new StorageSharedKeyCredential(config.accountName, config.accountKey);

    fsClientMock = new DataLakeFileSystemClient(config.url, credentials) as jest.Mocked<DataLakeFileSystemClient>;
    directoryClientMock = new DataLakeDirectoryClient(config.url) as jest.Mocked<DataLakeDirectoryClient>;
    fileClientMock = new DataLakeFileClient(config.url, credentials) as jest.Mocked<DataLakeFileClient>;

    (DataLakeServiceClient.prototype.getFileSystemClient as jest.Mock).mockReturnValue(fsClientMock);
    fsClientMock.getDirectoryClient.mockReturnValue(directoryClientMock);
    directoryClientMock.getFileClient.mockReturnValue(fileClientMock);

    dataLakeStorage = new DataLakeStorage(config);
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  it('should return the correct file store type', () => {
    expect(dataLakeStorage.getType()).toBe(FileStore.DataLake);
  });

  it('should create directory if not exists', async () => {
    const response = { succeeded: true } as unknown as DirectoryCreateIfNotExistsResponse;
    directoryClientMock.createIfNotExists.mockResolvedValueOnce(response);
    await dataLakeStorage.createDirectoryIfNotExists('test-directory');
    expect(directoryClientMock.createIfNotExists).toHaveBeenCalled();
  });

  it('should save buffer to datalake', async () => {
    const buffer = Buffer.from('test content');
    const response = { requestId: 'requestId', version: 'version', date: new Date() } as FileUploadResponse;
    fileClientMock.upload.mockResolvedValueOnce(response);

    const result = await dataLakeStorage.saveBuffer('test-file', 'test-directory', buffer);
    expect(fileClientMock.upload).toHaveBeenCalledWith(buffer);
    expect(result).toBe(response);
  });

  it('should load buffer from datalake', async () => {
    const buffer = Buffer.from('test content');
    fileClientMock.readToBuffer.mockResolvedValueOnce(buffer);

    const result = await dataLakeStorage.loadBuffer('test-file', 'test-directory');

    expect(fileClientMock.readToBuffer).toHaveBeenCalled();
    expect(result).toBe(buffer);
  });

  it('should save stream to datalake', async () => {
    const stream = Readable.from(['test content']);
    const response = { requestId: 'requestId', version: 'version', date: new Date() } as FileUploadResponse;
    fileClientMock.uploadStream.mockResolvedValueOnce(response);

    const result = await dataLakeStorage.saveStream('test-file', 'test-directory', stream);
    expect(fileClientMock.uploadStream).toHaveBeenCalledWith(stream);
    expect(result).toBe(response);
  });

  it('should load stream from datalake', async () => {
    const stream = Readable.from(['test content']);
    fileClientMock.read.mockResolvedValueOnce({ readableStreamBody: stream } as any);

    const result = await dataLakeStorage.loadStream('test-file', 'test-directory');

    expect(fileClientMock.read).toHaveBeenCalled();
    expect(result).toBe(stream);
  });

  it('should delete file from datalake', async () => {
    const response = { requestId: 'requestId', date: new Date(), succeeded: true } as PathDeleteIfExistsResponse;
    fileClientMock.deleteIfExists.mockResolvedValueOnce(response);

    const result = await dataLakeStorage.delete('test-file', 'test-directory');
    expect(fileClientMock.deleteIfExists).toHaveBeenCalled();
    expect(result).toBe(response);
  });

  it('should delete directory from datalake', async () => {
    const response = { requestId: 'requestId', date: new Date(), succeeded: true } as PathDeleteIfExistsResponse;
    directoryClientMock.deleteIfExists.mockResolvedValueOnce(response);

    const result = await dataLakeStorage.deleteDirectory('test-directory');
    expect(directoryClientMock.deleteIfExists).toHaveBeenCalledWith(true);
    expect(result).toBe(response);
  });

  it('should list files in directory', async () => {
    const files = [
      { name: 'file1.txt', isDirectory: false },
      { name: 'file2.txt', isDirectory: false }
    ];

    const fileIterator = {
      async *[Symbol.asyncIterator]() {
        yield* files;
      }
    } as unknown as PagedAsyncIterableIterator<Path, FileSystemListPathsResponse>;

    fsClientMock.listPaths.mockReturnValue(fileIterator);

    const result = await dataLakeStorage.listFiles('test-directory');
    expect(fsClientMock.listPaths).toHaveBeenCalledWith({ path: 'test-directory' });
    expect(result).toEqual([
      { name: 'file1.txt', path: 'file1.txt', isDirectory: false },
      { name: 'file2.txt', path: 'file2.txt', isDirectory: false }
    ]);
  });
});
