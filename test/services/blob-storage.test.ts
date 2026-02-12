import { Readable } from 'node:stream';

import {
  BlobServiceClient,
  ContainerClient,
  BlockBlobClient,
  BlobUploadCommonResponse,
  BlobDeleteIfExistsResponse,
  StorageSharedKeyCredential
} from '@azure/storage-blob';

import BlobStorage from '../../src/services/blob-storage';
import { FileStore } from '../../src/config/file-store.enum';

jest.mock('@azure/storage-blob');

describe('BlobStorage', () => {
  const config = {
    url: 'https://azure.example.com',
    accountName: 'accountName',
    accountKey: 'accountKey',
    containerName: 'containerName'
  };

  let blobStorage: BlobStorage;
  let containerClientMock: jest.Mocked<ContainerClient>;
  let blockBlobClientMock: jest.Mocked<BlockBlobClient>;

  beforeEach(() => {
    const credentials = new StorageSharedKeyCredential(config.accountName, config.accountKey);

    blockBlobClientMock = new BlockBlobClient(config.url, credentials) as jest.Mocked<BlockBlobClient>;
    containerClientMock = new ContainerClient(config.url, config.containerName) as jest.Mocked<ContainerClient>;

    (BlobServiceClient.prototype.getContainerClient as jest.Mock).mockReturnValue(containerClientMock);
    (containerClientMock.getBlockBlobClient as jest.Mock).mockReturnValue(blockBlobClientMock);

    blobStorage = new BlobStorage(config);
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  it('should return the correct file store type', () => {
    expect(blobStorage.getType()).toBe(FileStore.Blob);
  });

  it('should save buffer to blob storage', async () => {
    const buffer = Buffer.from('test content');
    const response: BlobUploadCommonResponse = { _response: {} } as BlobUploadCommonResponse;
    blockBlobClientMock.uploadData.mockResolvedValue(response);

    const result = await blobStorage.saveBuffer('test.txt', 'directory', buffer);

    expect(blockBlobClientMock.uploadData).toHaveBeenCalledWith(buffer);
    expect(result).toBe(response);
  });

  it('should load buffer from blob storage', async () => {
    const buffer = Buffer.from('test content');
    blockBlobClientMock.downloadToBuffer.mockResolvedValue(buffer);

    const result = await blobStorage.loadBuffer('test.txt', 'directory');

    expect(blockBlobClientMock.downloadToBuffer).toHaveBeenCalled();
    expect(result).toBe(buffer);
  });

  it('should save stream to blob storage', async () => {
    const stream = Readable.from(['test content']);
    const response: BlobUploadCommonResponse = { _response: {} } as BlobUploadCommonResponse;
    blockBlobClientMock.uploadStream.mockResolvedValue(response);

    const result = await blobStorage.saveStream('test.txt', 'directory', stream);

    expect(blockBlobClientMock.uploadStream).toHaveBeenCalledWith(stream);
    expect(result).toBe(response);
  });

  it('should load stream from blob storage', async () => {
    const stream = Readable.from(['test content']);
    blockBlobClientMock.download.mockResolvedValue({ readableStreamBody: stream } as any);

    const result = await blobStorage.loadStream('test.txt', 'directory');

    expect(blockBlobClientMock.download).toHaveBeenCalled();
    expect(result).toBe(stream);
  });

  it('should delete file from blob storage', async () => {
    const response: BlobDeleteIfExistsResponse = { succeeded: true, _response: {} } as BlobDeleteIfExistsResponse;
    blockBlobClientMock.deleteIfExists.mockResolvedValue(response);

    const result = await blobStorage.delete('test.txt', 'directory');

    expect(blockBlobClientMock.deleteIfExists).toHaveBeenCalled();
    expect(result).toBe(response);
  });

  it('should list files in a directory', async () => {
    const files = [{ name: '1234/file1.csv' }, { name: '1234/file2.csv' }] as any;

    containerClientMock.listBlobsByHierarchy = jest.fn().mockImplementation(function* (_delimiter, _opts) {
      yield* files;
    }) as any;

    const result = await blobStorage.listFiles('1234');

    expect(result).toEqual(['1234/file1.csv', '1234/file2.csv']);
  });

  it('should delete all files in a directory', async () => {
    const files = [{ name: '1234/file1.csv' }, { name: '1234/file2.csv' }] as any;

    containerClientMock.listBlobsByHierarchy = jest.fn().mockImplementation(function* (_delimiter, _opts) {
      yield* files;
    }) as any;

    blockBlobClientMock.deleteIfExists.mockResolvedValue({
      succeeded: true,
      _response: {}
    } as BlobDeleteIfExistsResponse);

    await blobStorage.deleteDirectory('1234');

    expect(blockBlobClientMock.deleteIfExists).toHaveBeenCalledTimes(2);
  });

  // --- Accessor methods ---

  it('should return the service client', () => {
    const serviceClient = blobStorage.getServiceClient();
    expect(serviceClient).toBeInstanceOf(BlobServiceClient);
  });

  it('should return the container client', () => {
    const containerClient = blobStorage.getContainerClient();
    expect(containerClient).toBe(containerClientMock);
  });

  // --- Error handling ---

  it('should throw when loadStream response has no readableStreamBody', async () => {
    blockBlobClientMock.download.mockResolvedValue({ readableStreamBody: undefined } as any);

    await expect(blobStorage.loadStream('test.txt', 'directory')).rejects.toThrow(
      "Failed to download file 'directory/test.txt' from blob storage"
    );
  });

  it('should propagate error when saveBuffer fails', async () => {
    blockBlobClientMock.uploadData.mockRejectedValue(new Error('Network error'));

    await expect(blobStorage.saveBuffer('test.txt', 'dir', Buffer.from('data'))).rejects.toThrow('Network error');
  });

  it('should propagate error when loadBuffer fails', async () => {
    blockBlobClientMock.downloadToBuffer.mockRejectedValue(new Error('Auth failure'));

    await expect(blobStorage.loadBuffer('test.txt', 'dir')).rejects.toThrow('Auth failure');
  });

  it('should propagate error when saveStream fails', async () => {
    blockBlobClientMock.uploadStream.mockRejectedValue(new Error('Timeout'));

    await expect(blobStorage.saveStream('test.txt', 'dir', Readable.from(['x']))).rejects.toThrow('Timeout');
  });

  it('should propagate error when delete fails', async () => {
    blockBlobClientMock.deleteIfExists.mockRejectedValue(new Error('Not found'));

    await expect(blobStorage.delete('test.txt', 'dir')).rejects.toThrow('Not found');
  });

  // --- Edge cases ---

  it('should handle empty buffer upload', async () => {
    const emptyBuffer = Buffer.alloc(0);
    const response: BlobUploadCommonResponse = { _response: {} } as BlobUploadCommonResponse;
    blockBlobClientMock.uploadData.mockResolvedValue(response);

    const result = await blobStorage.saveBuffer('empty.txt', 'dir', emptyBuffer);

    expect(blockBlobClientMock.uploadData).toHaveBeenCalledWith(emptyBuffer);
    expect(result).toBe(response);
  });

  it('should return empty array when listing an empty directory', async () => {
    containerClientMock.listBlobsByHierarchy = jest.fn().mockImplementation(function* () {
      // yields nothing
    }) as any;

    const result = await blobStorage.listFiles('empty-dir');
    expect(result).toEqual([]);
  });

  it('should construct namespaced filenames with delimiter', async () => {
    const buffer = Buffer.from('content');
    const response: BlobUploadCommonResponse = { _response: {} } as BlobUploadCommonResponse;
    blockBlobClientMock.uploadData.mockResolvedValue(response);

    await blobStorage.saveBuffer('file.csv', 'my/nested/dir', buffer);

    expect(containerClientMock.getBlockBlobClient).toHaveBeenCalledWith('my/nested/dir/file.csv');
  });

  // --- Hierarchical listing with nested prefixes ---

  it('should recurse into virtual directories when listing files', async () => {
    // First call returns a prefix (virtual directory), second returns a blob inside it
    const topLevel = [{ kind: 'prefix', name: '1234/subdir/' }];
    const nested = [{ name: '1234/subdir/file.csv' }];

    const callArgs: { prefix: string }[] = [];
    containerClientMock.listBlobsByHierarchy = jest.fn().mockImplementation(function* (_delimiter, opts) {
      callArgs.push(opts);
      if (callArgs.length === 1) {
        yield* topLevel;
      } else {
        yield* nested;
      }
    }) as any;

    const result = await blobStorage.listFiles('1234');
    expect(result).toEqual(['1234/subdir/file.csv']);

    // Verify correct prefixes were used for top-level and recursive calls
    expect(callArgs).toHaveLength(2);
    expect(callArgs[0]).toEqual({ prefix: '1234' });
    expect(callArgs[1]).toEqual({ prefix: '1234/subdir/' });
  });

  it('should delete files inside nested virtual directories', async () => {
    const topLevel = [{ kind: 'prefix', name: '1234/subdir/' }];
    const nested = [{ name: '1234/subdir/file1.csv' }, { name: '1234/subdir/file2.csv' }];

    const callArgs: { prefix: string }[] = [];
    containerClientMock.listBlobsByHierarchy = jest.fn().mockImplementation(function* (_delimiter, opts) {
      callArgs.push(opts);
      if (callArgs.length === 1) {
        yield* topLevel;
      } else {
        yield* nested;
      }
    }) as any;

    blockBlobClientMock.deleteIfExists.mockResolvedValue({
      succeeded: true,
      _response: {}
    } as BlobDeleteIfExistsResponse);

    await blobStorage.deleteDirectory('1234');

    expect(blockBlobClientMock.deleteIfExists).toHaveBeenCalledTimes(2);

    // Verify correct prefixes were used for top-level and recursive calls
    expect(callArgs).toHaveLength(2);
    expect(callArgs[0]).toEqual({ prefix: '1234' });
    expect(callArgs[1]).toEqual({ prefix: '1234/subdir/' });
  });
});
