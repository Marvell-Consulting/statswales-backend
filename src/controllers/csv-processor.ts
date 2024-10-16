import { createHash, randomUUID } from 'node:crypto';
import { Readable } from 'stream';

import { parse } from 'csv';

import { i18next } from '../middleware/translation';
import { logger as parentLogger } from '../utils/logger';
import { DatasetDTO } from '../dtos/dataset-dto';
import { FileImportDTO } from '../dtos/file-import-dto';
import { Error } from '../dtos/error';
import { CSVHeader, ViewDTO, ViewErrDTO, ViewStream } from '../dtos/view-dto';
import { Dataset } from '../entities/dataset/dataset';
import { Revision } from '../entities/dataset/revision';
import { Source } from '../entities/dataset/source';
import { FileImport } from '../entities/dataset/file-import';
import { SourceAction } from '../enums/source-action';
import { ImportType } from '../enums/import-type';
import { DataLocation } from '../enums/data-location';
import { Locale } from '../enums/locale';

import { BlobStorageService } from './blob-storage';
import { DataLakeService } from './datalake';

export const MAX_PAGE_SIZE = 500;
export const MIN_PAGE_SIZE = 5;
export const DEFAULT_PAGE_SIZE = 100;

const t = i18next.t;
const logger = parentLogger.child({ module: 'CSVProcessor' });

function hashReadableStream(stream: Readable, algorithm = 'sha256'): Promise<string> {
    return new Promise((resolve, reject) => {
        const hash = createHash(algorithm);
        stream.on('data', (chunk) => {
            hash.update(chunk);
        });
        stream.on('end', () => {
            resolve(hash.digest('hex'));
        });
        stream.on('error', (err) => {
            reject(err);
        });
    });
}

function paginate<T>(array: T[], page_number: number, page_size: number): T[] {
    const page = array.slice((page_number - 1) * page_size, page_number * page_size);
    return page;
}

function validatePageSize(page_size: number): boolean {
    if (page_size > MAX_PAGE_SIZE || page_size < MIN_PAGE_SIZE) {
        return false;
    }
    return true;
}

function validatePageNumber(page_number: number): boolean {
    if (page_number < 1) {
        return false;
    }
    return true;
}

function validatMaxPageNumber(page_number: number, max_page_number: number): boolean {
    if (page_number > max_page_number) {
        return false;
    }
    return true;
}

function validateParams(page_number: number, max_page_number: number, page_size: number): Error[] {
    const errors: Error[] = [];
    if (!validatePageSize(page_size)) {
        errors.push({
            field: 'page_size',
            message: [
                {
                    lang: Locale.English,
                    message: t('errors.page_size', {
                        lng: Locale.English,
                        max_page_size: MAX_PAGE_SIZE,
                        min_page_size: MIN_PAGE_SIZE
                    })
                },
                {
                    lang: Locale.Welsh,
                    message: t('errors.page_size', {
                        lng: Locale.Welsh,
                        max_page_size: MAX_PAGE_SIZE,
                        min_page_size: MIN_PAGE_SIZE
                    })
                }
            ],
            tag: {
                name: 'errors.page_size',
                params: { max_page_size: MAX_PAGE_SIZE, min_page_size: MIN_PAGE_SIZE }
            }
        });
    }
    if (!validatMaxPageNumber(page_number, max_page_number)) {
        errors.push({
            field: 'page_number',
            message: [
                {
                    lang: Locale.English,
                    message: t('errors.page_number_to_high', { lng: Locale.English, page_number: max_page_number })
                },
                {
                    lang: Locale.Welsh,
                    message: t('errors.page_number_to_high', { lng: Locale.Welsh, page_number: max_page_number })
                }
            ],
            tag: {
                name: 'errors.page_number_to_high',
                params: { page_number: max_page_number }
            }
        });
    }
    if (!validatePageNumber(page_number)) {
        errors.push({
            field: 'page_number',
            message: [
                { lang: Locale.English, message: t('errors.page_number_to_low', { lng: Locale.English }) },
                { lang: Locale.Welsh, message: t('errors.page_number_to_low', { lng: Locale.Welsh }) }
            ],
            tag: { name: 'errors.page_number_to_low', params: {} }
        });
    }
    return errors;
}

export const uploadCSVToBlobStorage = async (fileStream: Readable, filetype: string): Promise<FileImport> => {
    const blobStorageService = new BlobStorageService();
    if (!fileStream) {
        logger.error('No buffer to upload to blob storage');
        throw new Error('No buffer to upload to blob storage');
    }
    const importRecord = new FileImport();
    importRecord.id = randomUUID().toLowerCase();
    importRecord.mimeType = filetype;
    const extension = filetype === 'text/csv' ? 'csv' : 'zip';
    importRecord.filename = `${importRecord.id}.${extension}`;
    try {
        const promisedHash = hashReadableStream(fileStream)
            .then((hash) => {
                return hash.toString();
            })
            .catch((error) => {
                throw new Error(`Error hashing stream: ${error}`);
            });
        await blobStorageService.uploadFile(`${importRecord.filename}`, fileStream);
        const resolvedHash = await promisedHash;
        if (resolvedHash) importRecord.hash = resolvedHash;
        importRecord.uploadedAt = new Date();
        importRecord.type = ImportType.Draft;
        importRecord.location = DataLocation.BlobStorage;
        return importRecord;
    } catch (err) {
        logger.error(err);
        throw new Error('Error processing file upload to blob storage');
    }
};

export const uploadCSVBufferToBlobStorage = async (fileBuffer: Buffer, filetype: string): Promise<FileImport> => {
    const fileStream = Readable.from(fileBuffer);
    const importRecord: FileImport = await uploadCSVToBlobStorage(fileStream, filetype);
    return importRecord;
};

function setupPagination(page: number, total_pages: number): (string | number)[] {
    const pages = [];
    if (page !== 1) pages.push('previous');
    if (page - 1 > 0) pages.push(page - 1);
    pages.push(page);
    if (page + 1 <= total_pages) pages.push(page + 1);
    if (page < total_pages) pages.push('next');
    return pages;
}

async function processCSVData(
    buffer: Buffer,
    page: number,
    size: number,
    dataset: Dataset,
    importObj: FileImport
): Promise<ViewDTO | ViewErrDTO> {
    const dataArray: string[][] = (await parse(buffer, {
        delimiter: ','
    }).toArray()) as string[][];
    const csvheaders = dataArray.shift();
    if (!csvheaders) {
        return {
            success: false,
            status: 400,
            errors: [
                {
                    field: 'csv',
                    message: [
                        { lang: Locale.English, message: t('errors.csv_headers', { lng: Locale.English }) },
                        { lang: Locale.Welsh, message: t('errors.csv_headers', { lng: Locale.Welsh }) }
                    ],
                    tag: { name: 'errors.csv_headers', params: {} }
                }
            ],
            dataset_id: dataset.id
        };
    }
    const headers: CSVHeader[] = [];
    for (let i = 0; i < csvheaders.length; i++) {
        headers.push({
            index: i,
            name: csvheaders[i]
        });
    }
    const totalPages = Math.ceil(dataArray.length / size);
    const errors = validateParams(page, totalPages, size);
    if (errors.length > 0) {
        return {
            success: false,
            status: 400,
            errors,
            dataset_id: dataset.id
        };
    }

    const csvdata = paginate(dataArray, page, size);
    const pages = setupPagination(page, totalPages);
    const end_record = () => {
        if (size > dataArray.length) {
            return dataArray.length;
        } else if (page === totalPages) {
            return dataArray.length;
        } else {
            return page * size;
        }
    };
    const currentDataset = await Dataset.findOneByOrFail({ id: dataset.id });
    const currentImport = await FileImport.findOneByOrFail({ id: importObj.id });
    return {
        success: true,
        dataset: await DatasetDTO.fromDatasetComplete(currentDataset),
        import: await FileImportDTO.fromImport(currentImport),
        current_page: page,
        page_info: {
            total_records: dataArray.length,
            start_record: (page - 1) * size + 1,
            end_record: end_record()
        },
        pages,
        page_size: size,
        total_pages: totalPages,
        headers,
        data: csvdata
    };
}

export const getFileFromDataLake = async (
    dataset: Dataset,
    importObj: FileImport
): Promise<ViewStream | ViewErrDTO> => {
    const datalakeService = new DataLakeService();
    let stream: Readable;
    try {
        stream = await datalakeService.downloadFileStream(importObj.filename);
    } catch (err) {
        logger.error(err);
        return {
            success: false,
            status: 500,
            errors: [
                {
                    field: 'csv',
                    message: [
                        { lang: Locale.English, message: t('errors.download_from_datalake', { lng: Locale.English }) },
                        { lang: Locale.Welsh, message: t('errors.download_from_datalake', { lng: Locale.Welsh }) }
                    ],
                    tag: { name: 'errors.download_from_datalake', params: {} }
                }
            ],
            dataset_id: dataset.id
        };
    }
    return { success: true, stream };
};

export const processCSVFromDatalake = async (
    dataset: Dataset,
    importObj: FileImport,
    page: number,
    size: number
): Promise<ViewErrDTO | ViewDTO> => {
    const datalakeService = new DataLakeService();
    let buff: Buffer;
    try {
        buff = await datalakeService.downloadFile(importObj.filename);
    } catch (err) {
        logger.error(err);
        return {
            success: false,
            status: 500,
            errors: [
                {
                    field: 'csv',
                    message: [
                        { lang: Locale.English, message: t('errors.download_from_datalake', { lng: Locale.English }) },
                        { lang: Locale.Welsh, message: t('errors.download_from_datalake', { lng: Locale.Welsh }) }
                    ],
                    tag: { name: 'errors.download_from_datalake', params: {} }
                }
            ],
            dataset_id: dataset.id
        };
    }
    return processCSVData(buff, page, size, dataset, importObj);
};

export const getFileFromBlobStorage = async (
    dataset: Dataset,
    importObj: FileImport
): Promise<ViewStream | ViewErrDTO> => {
    const blobStoageService = new BlobStorageService();
    let stream: Readable;
    try {
        stream = await blobStoageService.getReadableStream(importObj.filename);
    } catch (err) {
        logger.error(err);
        return {
            success: false,
            status: 500,
            errors: [
                {
                    field: 'csv',
                    message: [
                        {
                            lang: Locale.English,
                            message: t('errors.download_from_blobstorage', { lng: Locale.English })
                        },
                        { lang: Locale.Welsh, message: t('errors.download_from_blobstorage', { lng: Locale.Welsh }) }
                    ],
                    tag: { name: 'errors.download_from_blobstorage', params: {} }
                }
            ],
            dataset_id: dataset.id
        };
    }
    return {
        success: true,
        stream
    };
};

export const processCSVFromBlobStorage = async (
    dataset: Dataset,
    importObj: FileImport,
    page: number,
    size: number
): Promise<ViewErrDTO | ViewDTO> => {
    const blobStoageService = new BlobStorageService();
    let buff: Buffer;
    try {
        buff = await blobStoageService.readFile(importObj.filename);
    } catch (err) {
        logger.error(err);
        return {
            success: false,
            status: 500,
            errors: [
                {
                    field: 'csv',
                    message: [
                        {
                            lang: Locale.English,
                            message: t('errors.download_from_blobstorage', { lng: Locale.English })
                        },
                        { lang: Locale.Welsh, message: t('errors.download_from_blobstorage', { lng: Locale.Welsh }) }
                    ],
                    tag: { name: 'errors.download_from_blobstorage', params: {} }
                }
            ],
            dataset_id: dataset.id
        };
    }
    return processCSVData(buff, page, size, dataset, importObj);
};

export const moveFileToDataLake = async (importObj: FileImport) => {
    const blobStorageService = new BlobStorageService();
    const datalakeService = new DataLakeService();
    try {
        const fileStream = await blobStorageService.getReadableStream(importObj.filename);
        await datalakeService.uploadFileStream(importObj.filename, fileStream);
        await blobStorageService.deleteFile(importObj.filename);
    } catch (err) {
        logger.error(err);
        throw new Error('Error moving file to datalake');
    }
};

export const removeTempfileFromBlobStorage = async (importObj: FileImport) => {
    const blobStorageService = new BlobStorageService();
    try {
        await blobStorageService.deleteFile(importObj.filename);
    } catch (err) {
        logger.error(err);
        throw new Error('Unable to successfully remove file from Blob Storage');
    }
};

export const removeFileFromDatalake = async (importObj: FileImport) => {
    const datalakeService = new DataLakeService();
    try {
        await datalakeService.deleteFile(importObj.filename);
    } catch (err) {
        logger.error(err);
        throw new Error('Unable to successfully remove from from Datalake');
    }
};

export const createSources = async (importObj: FileImport): Promise<FileImportDTO> => {
    const revision: Revision = await importObj.revision;
    const dataset: Dataset = await revision.dataset;
    let fileView: ViewDTO | ViewErrDTO;
    try {
        fileView = await processCSVFromDatalake(dataset, importObj, 1, 5);
    } catch (err) {
        logger.error(err);
        throw new Error('Error getting file from datalake');
    }
    let fileData: ViewDTO;
    if (fileView.success) {
        fileData = fileView as ViewDTO;
    } else {
        throw new Error('Error processing file from datalake');
    }
    const sources: Source[] = fileData.headers.map((header) => {
        const source = new Source();
        source.id = crypto.randomUUID().toLowerCase();
        source.columnIndex = header.index;
        source.csvField = header.name;
        source.action = SourceAction.Unknown;
        source.revision = Promise.resolve(revision);
        source.import = Promise.resolve(importObj);
        return source;
    });
    const freshFileImport = await FileImport.findOneBy({ id: importObj.id });
    if (!freshFileImport) {
        throw new Error('Import not found');
    }
    try {
        freshFileImport.sources = Promise.resolve(sources);
        await freshFileImport.save();
    } catch (err) {
        logger.error(err);
        throw new Error('Error saving sources to import');
    }
    return FileImportDTO.fromImportWithSources(freshFileImport);
};
