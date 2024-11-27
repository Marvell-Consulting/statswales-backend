import { basename } from 'path';
import { Readable } from 'stream';

import { DataLakeServiceClient, StorageSharedKeyCredential } from '@azure/storage-file-datalake';

import { logger as parentLogger } from '../utils/logger';
import { appConfig } from '../config';

const logger = parentLogger.child({ module: 'DataLakeService' });

const config = appConfig();
const { accountName, accountKey, fileSystemName } = config.storage.datalake;

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

    public async createDirectory(dirName: string) {
        const fileSystemClient = this.serviceClient.getFileSystemClient(fileSystemName);
        const directoryClient = fileSystemClient.getDirectoryClient(dirName);

        await directoryClient.create();
    }

    public async uploadFileStream(fileName: string, directory: string, fileContent: Readable) {
        logger.debug(`Uploading file with name '${fileName}' to datalake`);

        const fileSystemClient = this.serviceClient.getFileSystemClient(fileSystemName);
        const directoryClient = fileSystemClient.getDirectoryClient(directory);
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

    public async uploadFileBuffer(fileName: string, directory: string, fileContent: Buffer) {
        logger.debug(`Uploading file with file '${fileName}' to datalake`);
        const fileSystemClient = this.serviceClient.getFileSystemClient(fileSystemName);
        const directoryClient = fileSystemClient.getDirectoryClient(directory);
        const fileClient = directoryClient.getFileClient(fileName);
        await fileClient.create();
        await fileClient.append(fileContent, 0, fileContent.length);
        await fileClient.flush(fileContent.length);
    }

    public async deleteFile(fileName: string, directory: string) {
        const fileSystemClient = this.serviceClient.getFileSystemClient(fileSystemName);
        const directoryClient = fileSystemClient.getDirectoryClient(directory);
        const fileClient = directoryClient.getFileClient(fileName);

        await fileClient.delete();
    }

    public async listFiles(directory: string) {
        const fileSystemClient = this.serviceClient.getFileSystemClient(fileSystemName);

        const files = await fileSystemClient.listPaths({ path: directory });
        const fileList = [];
        for await (const file of files) {
            if (file.name === undefined) {
                continue;
            }
            fileList.push({ name: basename(file.name), path: file.name, isDirectory: file.isDirectory });
        }
        return fileList;
    }

    public async getFileBuffer(fileName: string, directory: string) {
        const fileSystemClient = this.serviceClient.getFileSystemClient(fileSystemName);
        const directoryClient = fileSystemClient.getDirectoryClient(directory);
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

    public async getFileStream(fileName: string, directory: string) {
        const fileSystemClient = this.serviceClient.getFileSystemClient(fileSystemName);
        const directoryClient = fileSystemClient.getDirectoryClient(directory);
        const fileClient = directoryClient.getFileClient(fileName);

        const downloadResponse = await fileClient.read();
        return downloadResponse.readableStreamBody as Readable;
    }
}
