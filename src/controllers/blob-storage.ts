import { Readable } from 'stream';

import {
    BlobServiceClient,
    BlobUploadCommonResponse,
    ContainerClient,
    StorageSharedKeyCredential
} from '@azure/storage-blob';
import * as dotenv from 'dotenv';

import { logger } from '../utils/logger';

dotenv.config();

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
        logger.info(`Uploading file with file '${fileName}' to blob storage`);

        const blockBlobClient = this.containerClient.getBlockBlobClient(fileName);

        const uploadOptions = {
            bufferSize: 4 * 1024 * 1024, // 4MB buffer size
            maxBuffers: 5 // Parallelism of 5
        };
        const uploadBlobResponse: BlobUploadCommonResponse = await blockBlobClient.uploadStream(
            fileContent,
            uploadOptions.bufferSize,
            uploadOptions.maxBuffers
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
        logger.info(`Getting file with file '${fileName}' to blob storage`);
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
        return downloadBlockBlobResponse.readableStreamBody as Readable;
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
