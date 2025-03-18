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
});
