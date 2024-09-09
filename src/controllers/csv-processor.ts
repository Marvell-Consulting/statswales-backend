/* eslint-disable import/no-cycle */
import { createHash, randomUUID } from 'crypto';
import { Readable } from 'stream';

import { parse } from 'csv';

import { ENGLISH, WELSH, logger, t } from '../app';
import { DatasetDTO, ImportDTO, RevisionDTO } from '../dtos/dataset-dto';
import { Error } from '../dtos/error';
import { CSVHeader, ViewStream, ViewDTO, ViewErrDTO } from '../dtos/view-dto';
import { Dataset } from '../entities/dataset';
import { Revision } from '../entities/revision';
import { Source } from '../entities/source';
import { Import } from '../entities/import';

import { BlobStorageService } from './blob-storage';
import { DataLakeService } from './datalake';

export const MAX_PAGE_SIZE = 500;
export const MIN_PAGE_SIZE = 5;
export const DEFAULT_PAGE_SIZE = 100;

function hashReadableStream(stream: Readable, algorithm: string = 'sha256'): Promise<string> {
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

function paginate<T>(array: Array<T>, page_number: number, page_size: number): Array<T> {
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

function validateParams(page_number: number, max_page_number: number, page_size: number): Array<Error> {
    const errors: Array<Error> = [];
    if (!validatePageSize(page_size)) {
        errors.push({
            field: 'page_size',
            message: [
                {
                    lang: ENGLISH,
                    message: t('errors.page_size', {
                        lng: ENGLISH,
                        max_page_size: MAX_PAGE_SIZE,
                        min_page_size: MIN_PAGE_SIZE
                    })
                },
                {
                    lang: WELSH,
                    message: t('errors.page_size', {
                        lng: WELSH,
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
                    lang: ENGLISH,
                    message: t('errors.page_number_to_high', { lng: ENGLISH, page_number: max_page_number })
                },
                { lang: WELSH, message: t('errors.page_number_to_high', { lng: WELSH, page_number: max_page_number }) }
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
                { lang: ENGLISH, message: t('errors.page_number_to_low', { lng: ENGLISH }) },
                { lang: WELSH, message: t('errors.page_number_to_low', { lng: WELSH }) }
            ],
            tag: { name: 'errors.page_number_to_low', params: {} }
        });
    }
    return errors;
}

export const uploadCSVToBlobStorage = async (fileStream: Readable, filetype: string): Promise<Import> => {
    const blobStorageService = new BlobStorageService();
    if (!fileStream) {
        logger.error('No buffer to upload to blob storage');
        throw new Error('No buffer to upload to blob storage');
    }
    const importRecord = new Import();
    importRecord.id = randomUUID();
    importRecord.mime_type = filetype;
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
        importRecord.uploaded_at = new Date(Date.now());
        importRecord.type = 'Draft';
        importRecord.location = 'BlobStorage';
        return importRecord;
    } catch (err) {
        logger.error(err);
        throw new Error('Error processing file upload to blob storage');
    }
};

export const uploadCSVBufferToBlobStorage = async (fileBuffer: Buffer, filetype: string): Promise<Import> => {
    const fileStream = Readable.from(fileBuffer);
    const importRecord: Import = await uploadCSVToBlobStorage(fileStream, filetype);
    return importRecord;
};

function setupPagination(page: number, total_pages: number): Array<string | number> {
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
    importObj: Import
): Promise<ViewDTO | ViewErrDTO> {
    const dataArray: Array<Array<string>> = (await parse(buffer, {
        delimiter: ','
    }).toArray()) as string[][];
    const csvheaders = dataArray.shift();
    if (!csvheaders) {
        return {
            success: false,
            errors: [
                {
                    field: 'csv',
                    message: [
                        { lang: ENGLISH, message: t('errors.csv_headers', { lng: ENGLISH }) },
                        { lang: WELSH, message: t('errors.csv_headers', { lng: WELSH }) }
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

    return {
        success: true,
        dataset: await DatasetDTO.fromDatasetShallow(dataset),
        import: await ImportDTO.fromImport(importObj),
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

export const getFileFromDataLake = async (dataset: Dataset, importObj: Import): Promise<ViewStream | ViewErrDTO> => {
    const datalakeService = new DataLakeService();
    let stream: Readable;
    try {
        stream = await datalakeService.downloadFileStream(importObj.filename);
    } catch (err) {
        logger.error(err);
        return {
            success: false,
            errors: [
                {
                    field: 'csv',
                    message: [
                        { lang: ENGLISH, message: t('errors.download_from_datalake', { lng: ENGLISH }) },
                        { lang: WELSH, message: t('errors.download_from_datalake', { lng: WELSH }) }
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
    importObj: Import,
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
            errors: [
                {
                    field: 'csv',
                    message: [
                        { lang: ENGLISH, message: t('errors.download_from_datalake', { lng: ENGLISH }) },
                        { lang: WELSH, message: t('errors.download_from_datalake', { lng: WELSH }) }
                    ],
                    tag: { name: 'errors.download_from_datalake', params: {} }
                }
            ],
            dataset_id: dataset.id
        };
    }
    return processCSVData(buff, page, size, dataset, importObj);
};

export const getFileFromBlobStorage = async (dataset: Dataset, importObj: Import): Promise<ViewStream | ViewErrDTO> => {
    const blobStoageService = new BlobStorageService();
    let stream: Readable;
    try {
        stream = await blobStoageService.getReadableStream(importObj.filename);
    } catch (err) {
        logger.error(err);
        return {
            success: false,
            errors: [
                {
                    field: 'csv',
                    message: [
                        { lang: ENGLISH, message: t('errors.download_from_blobstorage', { lng: ENGLISH }) },
                        { lang: WELSH, message: t('errors.download_from_blobstorage', { lng: WELSH }) }
                    ],
                    tag: { name: 'errors.download_from_datalake', params: {} }
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
    importObj: Import,
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
            errors: [
                {
                    field: 'csv',
                    message: [
                        { lang: ENGLISH, message: t('errors.download_from_blobstorage', { lng: ENGLISH }) },
                        { lang: WELSH, message: t('errors.download_from_blobstorage', { lng: WELSH }) }
                    ],
                    tag: { name: 'errors.download_from_datalake', params: {} }
                }
            ],
            dataset_id: dataset.id
        };
    }
    return processCSVData(buff, page, size, dataset, importObj);
};

export const moveFileToDataLake = async (importObj: Import) => {
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

export const createSources = async (importObj: Import): Promise<RevisionDTO> => {
    const revision: Revision = await importObj.revision;
    const dataset: Dataset = await revision.dataset;
    let fileView: ViewDTO | ViewErrDTO;
    try {
        fileView = await processCSVFromDatalake(dataset, importObj, 1, 5);
    } catch (err) {
        logger.error(err);
        throw new Error('Error moving file to datalake');
    }
    let fileData: ViewDTO;
    if (fileView.success) {
        fileData = fileView as ViewDTO;
    } else {
        throw new Error('Error processing file from datalake');
    }
    const headers = fileData.headers;
    const sources: Source[] = [];
    headers.forEach((header) => {
        const source = new Source();
        source.columnIndex = header.index;
        source.csvField = header.name;
        source.action = 'unknwon';
        source.revision = Promise.resolve(revision);
        source.import = Promise.resolve(importObj);
        sources.push(source);
        source.save();
    });
    const saveImport = await Import.findOneBy({ id: importObj.id });
    if (!saveImport) {
        throw new Error('Import not found');
    }
    saveImport.sources = Promise.resolve(sources);
    await importObj.save();
    const saveRevision = await Revision.findOneBy({ id: revision.id });
    if (!saveRevision) {
        throw new Error('Revision not found');
    }
    saveRevision.sources = Promise.resolve(sources);
    await saveRevision.save();
    return RevisionDTO.fromRevision(saveRevision);
};
