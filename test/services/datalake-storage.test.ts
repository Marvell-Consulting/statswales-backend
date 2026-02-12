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

  // --- Accessor methods ---

  it('should return the service client', () => {
    const serviceClient = dataLakeStorage.getServiceClient();
    expect(serviceClient).toBeInstanceOf(DataLakeServiceClient);
  });

  // --- Error handling ---

  it('should throw when loadStream response has no readableStreamBody', async () => {
    fileClientMock.read.mockResolvedValue({ readableStreamBody: undefined } as any);

    await expect(dataLakeStorage.loadStream('test-file', 'test-directory')).rejects.toThrow(
      "Failed to download file 'test-file' from datalake"
    );
  });

  it('should propagate error when saveBuffer fails', async () => {
    fileClientMock.upload.mockRejectedValue(new Error('Network error'));

    await expect(dataLakeStorage.saveBuffer('file.txt', 'dir', Buffer.from('data'))).rejects.toThrow('Network error');
  });

  it('should propagate error when loadBuffer fails', async () => {
    fileClientMock.readToBuffer.mockRejectedValue(new Error('Auth failure'));

    await expect(dataLakeStorage.loadBuffer('file.txt', 'dir')).rejects.toThrow('Auth failure');
  });

  it('should propagate error when saveStream fails', async () => {
    fileClientMock.uploadStream.mockRejectedValue(new Error('Timeout'));

    await expect(dataLakeStorage.saveStream('file.txt', 'dir', Readable.from(['x']))).rejects.toThrow('Timeout');
  });

  it('should propagate error when delete fails', async () => {
    fileClientMock.deleteIfExists.mockRejectedValue(new Error('Service unavailable'));

    await expect(dataLakeStorage.delete('file.txt', 'dir')).rejects.toThrow('Service unavailable');
  });

  it('should propagate error when deleteDirectory fails', async () => {
    directoryClientMock.deleteIfExists.mockRejectedValue(new Error('Forbidden'));

    await expect(dataLakeStorage.deleteDirectory('dir')).rejects.toThrow('Forbidden');
  });

  // --- Verifying directory creation before saves ---

  it('should create directory before saving buffer', async () => {
    const buffer = Buffer.from('test');
    const createResponse = { succeeded: true } as unknown as DirectoryCreateIfNotExistsResponse;
    const uploadResponse = { requestId: 'r1' } as FileUploadResponse;

    directoryClientMock.createIfNotExists.mockResolvedValueOnce(createResponse);
    fileClientMock.upload.mockResolvedValueOnce(uploadResponse);

    await dataLakeStorage.saveBuffer('file.txt', 'my-dir', buffer);

    expect(directoryClientMock.createIfNotExists).toHaveBeenCalled();
    expect(fileClientMock.upload).toHaveBeenCalledWith(buffer);
  });

  it('should create directory before saving stream', async () => {
    const stream = Readable.from(['data']);
    const createResponse = { succeeded: true } as unknown as DirectoryCreateIfNotExistsResponse;
    const uploadResponse = { requestId: 'r1' } as FileUploadResponse;

    directoryClientMock.createIfNotExists.mockResolvedValueOnce(createResponse);
    fileClientMock.uploadStream.mockResolvedValueOnce(uploadResponse);

    await dataLakeStorage.saveStream('file.txt', 'my-dir', stream);

    expect(directoryClientMock.createIfNotExists).toHaveBeenCalled();
    expect(fileClientMock.uploadStream).toHaveBeenCalledWith(stream);
  });

  // --- Edge cases ---

  it('should handle empty buffer upload', async () => {
    const emptyBuffer = Buffer.alloc(0);
    const response = { requestId: 'r1' } as FileUploadResponse;
    directoryClientMock.createIfNotExists.mockResolvedValueOnce({} as DirectoryCreateIfNotExistsResponse);
    fileClientMock.upload.mockResolvedValueOnce(response);

    const result = await dataLakeStorage.saveBuffer('empty.txt', 'dir', emptyBuffer);

    expect(fileClientMock.upload).toHaveBeenCalledWith(emptyBuffer);
    expect(result).toBe(response);
  });

  it('should return empty array when listing empty directory', async () => {
    const fileIterator = {
      async *[Symbol.asyncIterator]() {
        // yields nothing
      }
    } as unknown as PagedAsyncIterableIterator<Path, FileSystemListPathsResponse>;

    fsClientMock.listPaths.mockReturnValue(fileIterator);

    const result = await dataLakeStorage.listFiles('empty-dir');
    expect(result).toEqual([]);
  });

  it('should skip files with undefined name when listing', async () => {
    const files = [
      { name: 'file1.txt', isDirectory: false },
      { name: undefined, isDirectory: false },
      { name: 'file2.txt', isDirectory: true }
    ];

    const fileIterator = {
      async *[Symbol.asyncIterator]() {
        yield* files;
      }
    } as unknown as PagedAsyncIterableIterator<Path, FileSystemListPathsResponse>;

    fsClientMock.listPaths.mockReturnValue(fileIterator);

    const result = await dataLakeStorage.listFiles('dir');
    expect(result).toEqual([
      { name: 'file1.txt', path: 'file1.txt', isDirectory: false },
      { name: 'file2.txt', path: 'file2.txt', isDirectory: true }
    ]);
  });

  it('should default isDirectory to false when not set', async () => {
    const files = [{ name: 'file.txt' }];

    const fileIterator = {
      async *[Symbol.asyncIterator]() {
        yield* files;
      }
    } as unknown as PagedAsyncIterableIterator<Path, FileSystemListPathsResponse>;

    fsClientMock.listPaths.mockReturnValue(fileIterator);

    const result = await dataLakeStorage.listFiles('dir');
    expect(result).toEqual([{ name: 'file.txt', path: 'file.txt', isDirectory: false }]);
  });

  it('should extract basename from full path in listFiles', async () => {
    const files = [{ name: 'dir/subdir/deep-file.csv', isDirectory: false }];

    const fileIterator = {
      async *[Symbol.asyncIterator]() {
        yield* files;
      }
    } as unknown as PagedAsyncIterableIterator<Path, FileSystemListPathsResponse>;

    fsClientMock.listPaths.mockReturnValue(fileIterator);

    const result = await dataLakeStorage.listFiles('dir');
    expect(result).toEqual([{ name: 'deep-file.csv', path: 'dir/subdir/deep-file.csv', isDirectory: false }]);
  });
});
