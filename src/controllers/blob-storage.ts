import { Readable } from 'stream';

import {
    BlobServiceClient,
    BlobUploadCommonResponse,
    ContainerClient,
    StorageSharedKeyCredential
} from '@azure/storage-blob';
import * as dotenv from 'dotenv';
import pino from 'pino';

dotenv.config();

export const logger = pino({
    name: 'StatsWales-Alpha-App: BlobStorageService',
    level: 'debug'
});

const accountName = process.env.AZURE_BLOB_STORAGE_ACCOUNT_NAME || 'your-storage-account-name';
const accountKey = process.env.AZURE_BLOB_STORAGE_ACCOUNT_KEY || 'your-storage';
const containerName = process.env.AZURE_BLOB_STORAGE_CONTAINER_NAME || 'your-container-name';

/*
  Wrapper Class around the Azure Blob Storage API.
  Proper filename handling is assumed to be handled by the Azure API.
  Filenames coming to this class should be from database generated UUIDs.
*/
export class BlobStorageService {
    private readonly blobServiceClient: BlobServiceClient;
    private readonly containerClient: ContainerClient;

    public constructor() {
        logger.debug(
            `Creating BlobServiceClient and ContainerClient for blob storage with account name '${accountName}' and container name '${containerName}'`
        );
        const sharedKeyCredential = new StorageSharedKeyCredential(accountName, accountKey);
        this.blobServiceClient = new BlobServiceClient(
            `https://${accountName}.blob.core.windows.net`,
            sharedKeyCredential
        );
        this.containerClient = this.blobServiceClient.getContainerClient(containerName);
    }

    public getBlobServiceClient() {
        return this.blobServiceClient;
    }

    public getContainerClient() {
        return this.containerClient;
    }

    public async uploadFile(fileName: string | undefined, fileContent: Readable) {
        if (fileName === undefined) {
            throw new Error('File name is undefined');
        }
        if (fileContent === undefined) {
            throw new Error('File content is undefined');
        }

        const blockBlobClient = this.containerClient.getBlockBlobClient(fileName);

        const uploadBlobResponse: BlobUploadCommonResponse = await blockBlobClient.uploadStream(
            fileContent,
            fileContent.readableLength
        );
        return uploadBlobResponse;
    }

    public async deleteFile(fileName: string) {
        logger.warn(`Deleting file with file '${fileName}' from blob storage`);
        const blockBlobClient = this.containerClient.getBlockBlobClient(fileName);
        await blockBlobClient.delete();
    }

    public async listFiles() {
        const fileList: string[] = [];
        for await (const blob of this.containerClient.listBlobsFlat()) {
            fileList.push(blob.name);
        }
        return fileList;
    }

    public async readFile(fileName: string) {
        const blockBlobClient = this.containerClient.getBlockBlobClient(fileName);
        const downloadBlockBlobResponse = await blockBlobClient.download();
        const readableStreamBody = downloadBlockBlobResponse.readableStreamBody;

        if (!readableStreamBody) {
            throw new Error('Failed to get readable stream body from download response.');
        }

        const chunks: Buffer[] = [];
        for await (const chunk of readableStreamBody) {
            if (chunk instanceof Buffer) chunks.push(chunk);
            else chunks.push(Buffer.from(chunk));
        }
        return Buffer.concat(chunks);
    }

    public async getReadableStream(fileName: string) {
        const blockBlobClient = this.containerClient.getBlockBlobClient(fileName);
        const downloadBlockBlobResponse = await blockBlobClient.download();
        const readableStreamBody: ReadableStream | undefined = downloadBlockBlobResponse.readableStreamBody;

        if (!readableStreamBody) {
            throw new Error('Failed to get readable stream body from download response.');
        }
        return readableStreamBody;
    }

    public async readFileToBuffer(fileName: string) {
        const blockBlobClient = this.containerClient.getBlockBlobClient(fileName);
        const downloadBlockBlobResponse = await blockBlobClient.download();
        const readableStreamBody = downloadBlockBlobResponse.readableStreamBody;

        if (!readableStreamBody) {
            throw new Error('Failed to get readable stream body from download response.');
        }

        const chunks: Buffer[] = [];
        for await (const chunk of readableStreamBody) {
            if (chunk instanceof Buffer) chunks.push(chunk);
            else chunks.push(Buffer.from(chunk));
        }
        return Buffer.concat(chunks);
    }
}
