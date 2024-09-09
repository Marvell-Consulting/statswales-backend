import { basename } from 'path';
import { Readable } from 'stream';

import { DataLakeServiceClient, StorageSharedKeyCredential } from '@azure/storage-file-datalake';

import { logger as parentLogger } from '../utils/logger';

const logger = parentLogger.child({ module: 'DataLakeService' });

const accountName = process.env.AZURE_DATALAKE_STORAGE_ACCOUNT_NAME || 'your-storage-account-name';
const accountKey = process.env.AZURE_DATALAKE_STORAGE_ACCOUNT_KEY || 'your-storage';
const defaultDirectoryName = process.env.AZURE_DATALAKE_STORAGE_DIRECTORY_NAME || 'your-directory-name';
// const fileSystemName = process.env.AZURE_STORAGE_FILESYSTEM_NAME || 'your-filesystem-name';
// const containerName = process.env.AZURE_STORAGE_CONTAINER_NAME || 'your-container-name';

const fileSystemName = 'swdlfs';

export class DataLakeService {
    private readonly serviceClient: DataLakeServiceClient;

    public constructor() {
        const sharedKeyCredential = new StorageSharedKeyCredential(accountName, accountKey);
        this.serviceClient = new DataLakeServiceClient(
            `https://${accountName}.dfs.core.windows.net`,
            sharedKeyCredential
        );
    }

    public getServiceClient() {
        return this.serviceClient;
    }

    public async createDirectory(directoryName: string) {
        const fileSystemClient = this.serviceClient.getFileSystemClient(fileSystemName);
        const directoryClient = fileSystemClient.getDirectoryClient(directoryName);

        await directoryClient.create();
    }

    // public async uploadFileStream(fileName: string | undefined, fileContent: Readable) {
    //     if (fileName === undefined) {
    //         throw new Error('File name is undefined');
    //     }
    //     if (fileContent === undefined) {
    //         throw new Error('File name is undefined');
    //     }
    //     logger.debug(`Uploading file with file '${fileName}' to datalake`);
    //     const fileSystemClient = this.serviceClient.getFileSystemClient(fileSystemName);
    //     const directoryClient = fileSystemClient.getDirectoryClient(defaultDirectoryName);
    //     const fileClient = directoryClient.getFileClient(fileName);
    //     await fileClient.create();
    //     await fileClient.uploadStream(fileContent, {
    //         chunkSize: 8 * 1024 * 1024,
    //         maxConcurrency: 5
    //     });
    //     await fileClient.flush(fileContent.readableLength);
    // }

    public async uploadFileStream(fileName: string | undefined, fileContent: Readable) {
        if (fileName === undefined) {
            throw new Error('File name is undefined');
        }
        if (fileContent === undefined) {
            throw new Error('File content is undefined');
        }
        logger.debug(`Uploading file with name '${fileName}' to datalake`);

        const fileSystemClient = this.serviceClient.getFileSystemClient(fileSystemName);
        const directoryClient = fileSystemClient.getDirectoryClient(defaultDirectoryName);
        const fileClient = directoryClient.getFileClient(fileName);
        // Create the file in the Data Lake
        await fileClient.create();
        let position = 0;
        for await (const chunk of fileContent) {
            const chunkSize = chunk.length;
            // Append the chunk at the current position
            await fileClient.append(chunk, position, chunkSize);
            position += chunkSize;
        }
        // Flush and commit the file
        await fileClient.flush(position);
    }

    public async uploadFile(fileName: string | undefined, fileContent: Buffer) {
        if (fileName === undefined) {
            throw new Error('File name is undefined');
        }
        if (fileContent === undefined) {
            throw new Error('File name is undefined');
        }
        logger.debug(`Uploading file with file '${fileName}' to datalake`);
        const fileSystemClient = this.serviceClient.getFileSystemClient(fileSystemName);
        const directoryClient = fileSystemClient.getDirectoryClient(defaultDirectoryName);
        const fileClient = directoryClient.getFileClient(fileName);
        const body = fileContent.toString();
        await fileClient.create();
        await fileClient.append(body, 0, fileContent.length);
        await fileClient.flush(fileContent.length);
    }

    public async deleteFile(fileName: string) {
        const fileSystemClient = this.serviceClient.getFileSystemClient(fileSystemName);
        const directoryClient = fileSystemClient.getDirectoryClient(defaultDirectoryName);
        const fileClient = directoryClient.getFileClient(fileName);

        await fileClient.delete();
    }

    public async listFiles() {
        const fileSystemClient = this.serviceClient.getFileSystemClient(fileSystemName);

        const files = await fileSystemClient.listPaths({ path: defaultDirectoryName });
        const fileList = [];
        for await (const file of files) {
            if (file.name === undefined) {
                continue;
            }
            fileList.push({ name: basename(file.name), path: file.name, isDirectory: file.isDirectory });
        }
        return fileList;
    }

    public async downloadFile(fileName: string) {
        const fileSystemClient = this.serviceClient.getFileSystemClient(fileSystemName);
        const directoryClient = fileSystemClient.getDirectoryClient(defaultDirectoryName);
        const fileClient = directoryClient.getFileClient(fileName);

        const downloadResponse = await fileClient.read();
        const body = downloadResponse.readableStreamBody;
        if (body === undefined) {
            throw new Error('ReadableStreamBody is undefined');
        }

        const downloaded = await streamToBuffer(body);

        function streamToBuffer(readableStream: NodeJS.ReadableStream): Promise<Buffer> {
            const chunks: Uint8Array[] = [];
            if (readableStream === undefined) {
                throw new Error('ReadableStream is undefined');
            }
            return new Promise((resolve, reject) => {
                readableStream.on('data', (data) => {
                    chunks.push(data);
                });
                readableStream.on('end', () => {
                    resolve(Buffer.concat(chunks));
                });
                readableStream.on('error', reject);
            });
        }

        return downloaded;
    }

    public async downloadFileStream(fileName: string) {
        const fileSystemClient = this.serviceClient.getFileSystemClient(fileSystemName);
        const directoryClient = fileSystemClient.getDirectoryClient(defaultDirectoryName);
        const fileClient = directoryClient.getFileClient(fileName);

        const downloadResponse = await fileClient.read();
        return downloadResponse.readableStreamBody as Readable;
    }
}
