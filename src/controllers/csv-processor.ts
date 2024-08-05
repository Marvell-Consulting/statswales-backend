/* eslint-disable import/no-cycle */
import { createHash } from 'crypto';

import { parse } from 'csv';

import { UploadDTO, UploadErrDTO } from '../dtos/upload-dto';
import { Error } from '../models/error';
import { Datafile } from '../entity/datafile';
import { Dataset } from '../entity/dataset';
import { ViewDTO, ViewErrDTO } from '../dtos/view-dto';
import { datasetToDatasetDTO } from '../dtos/dataset-dto';
import { ENGLISH, WELSH, logger, t } from '../app';

import { DataLakeService } from './datalake';
import { BlobStorageService } from './blob-storage';

export const MAX_PAGE_SIZE = 500;
export const MIN_PAGE_SIZE = 5;
export const DEFAULT_PAGE_SIZE = 100;

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

export const moveCSVFromBlobStorageToDatalake = async (dataset: Dataset): Promise<UploadDTO | UploadErrDTO> => {
    const blobStorageService = new BlobStorageService();
    const dataLakeService = new DataLakeService();
    const datafiles = await dataset.datafiles;
    const datafile: Datafile | undefined = datafiles
        .filter((filterfile: Datafile) => filterfile.draft === false)
        .sort(
            (first: Datafile, second: Datafile) =>
                new Date(second.creationDate).getTime() - new Date(first.creationDate).getTime()
        )
        .shift();
    const dto = await datasetToDatasetDTO(dataset);
    if (datafile) {
        try {
            logger.debug(`Moving file ${datafile.id} from blob storage to datalake`);
            const buff: Buffer = await blobStorageService.readFileToBuffer(`${datafile.id}.csv`);
            await dataLakeService.uploadFile(`${datafile.id}.csv`, buff);
            datafile.draft = false;
            await datafile.save();
            await blobStorageService.deleteFile(`${datafile.id}.csv`);
            return {
                success: true,
                dataset: dto
            };
        } catch (err) {
            logger.error(err);
            return {
                success: false,
                dataset: dto,
                errors: [
                    {
                        field: 'csv',
                        message: [
                            { lang: ENGLISH, message: t('errors.move_to_datalake', { lng: ENGLISH }) },
                            { lang: WELSH, message: t('errors.move_to_datalake', { lng: WELSH }) }
                        ],
                        tag: { name: 'errors.move_to_datalake', params: {} }
                    }
                ]
            };
        }
    } else {
        return {
            success: false,
            dataset: dto,
            errors: [
                {
                    field: 'csv',
                    message: [
                        { lang: ENGLISH, message: t('errors.no_csv', { lng: ENGLISH }) },
                        { lang: WELSH, message: t('errors.no_csv', { lng: WELSH }) }
                    ],
                    tag: { name: 'errors.no_csv', params: {} }
                }
            ]
        };
    }
};

export const uploadCSVToBlobStorage = async (buff: Buffer, dataset: Dataset): Promise<UploadDTO | UploadErrDTO> => {
    const blobStorageService = new BlobStorageService();
    const hash = createHash('sha256').update(buff).digest('hex');
    const datafile = Datafile.createDatafile(dataset, hash.toString(), 'BetaUser');
    const savedDataFile = await datafile.save();
    const dto = await datasetToDatasetDTO(dataset);
    if (buff) {
        try {
            logger.debug(`Uploading file ${savedDataFile.id} to blob storage`);
            await blobStorageService.uploadFile(`${savedDataFile.id}.csv`, buff);
            return {
                success: true,
                dataset: dto
            };
        } catch (err) {
            logger.error(err);
            datafile.remove();
            return {
                success: false,
                dataset: dto,
                errors: [
                    {
                        field: 'csv',
                        message: [
                            { lang: ENGLISH, message: t('errors.upload_to_datalake', { lng: ENGLISH }) },
                            { lang: WELSH, message: t('errors.upload_to_datalake', { lng: WELSH }) }
                        ],
                        tag: { name: 'errors.upload_to_datalake', params: {} }
                    }
                ]
            };
        }
    } else {
        logger.debug('No buffer to upload to datalake');
        datafile.remove();
        return {
            success: false,
            dataset: dto,
            errors: [
                {
                    field: 'csv',
                    message: [
                        { lang: ENGLISH, message: t('errors.no_csv_data', { lng: ENGLISH }) },
                        { lang: WELSH, message: t('errors.no_csv_data', { lng: WELSH }) }
                    ],
                    tag: { name: 'errors.no_csv_data', params: {} }
                }
            ]
        };
    }
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

export const processCSVFromDatalake = async (
    dataset: Dataset,
    page: number,
    size: number
): Promise<ViewErrDTO | ViewDTO> => {
    const datalakeService = new DataLakeService();
    try {
        const datafiles = await dataset.datafiles;
        const datafile: Datafile | undefined = datafiles
            .filter((filterfile: Datafile) => filterfile.draft === false)
            .sort(
                (first: Datafile, second: Datafile) =>
                    new Date(second.creationDate).getTime() - new Date(first.creationDate).getTime()
            )
            .shift();
        if (datafile === undefined || datafile === null) {
            return {
                success: false,
                errors: [
                    {
                        field: 'dataset',
                        message: [
                            { lang: ENGLISH, message: t('errors.no_datafile', { lng: ENGLISH }) },
                            { lang: WELSH, message: t('errors.no_datafile', { lng: WELSH }) }
                        ],
                        tag: { name: 'erorors.no_datafile', params: {} }
                    }
                ],
                dataset_id: dataset.id
            };
        }
        const buff = await datalakeService.downloadFile(`${datafile.id}.csv`);

        const dataArray: Array<Array<string>> = (await parse(buff, {
            delimiter: ','
        }).toArray()) as string[][];
        const csvheaders = dataArray.shift();
        const total_pages = Math.ceil(dataArray.length / size);
        const errors = validateParams(page, total_pages, size);
        if (errors.length > 0) {
            return {
                success: false,
                errors,
                dataset_id: dataset.id
            };
        }

        const csvdata = paginate(dataArray, page, size);
        const pages = setupPagination(page, total_pages);
        const end_record = () => {
            if (size > dataArray.length) {
                return dataArray.length;
            } else if (page === total_pages) {
                return dataArray.length;
            } else {
                return page * size;
            }
        };
        const dto = await datasetToDatasetDTO(dataset);
        return {
            success: true,
            dataset: dto,
            current_page: page,
            page_info: {
                total_records: dataArray.length,
                start_record: (page - 1) * size + 1,
                end_record: end_record()
            },
            pages,
            page_size: size,
            total_pages,
            headers: csvheaders,
            data: csvdata
        };
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
};

export const processCSVFromBlobStorage = async (
    dataset: Dataset,
    page: number,
    size: number
): Promise<ViewErrDTO | ViewDTO> => {
    const blobStoageService = new BlobStorageService();
    try {
        const datafiles = await dataset.datafiles;
        const datafile: Datafile | undefined = datafiles
            .filter((filterfile: Datafile) => filterfile.draft === true)
            .sort(
                (first: Datafile, second: Datafile) =>
                    new Date(second.creationDate).getTime() - new Date(first.creationDate).getTime()
            )
            .shift();
        if (datafile === undefined || datafile === null) {
            return {
                success: false,
                errors: [
                    {
                        field: 'dataset',
                        message: [
                            { lang: ENGLISH, message: t('errors.no_datafile', { lng: ENGLISH }) },
                            { lang: WELSH, message: t('errors.no_datafile', { lng: WELSH }) }
                        ],
                        tag: { name: 'erorors.no_datafile', params: {} }
                    }
                ],
                dataset_id: dataset.id
            };
        }
        const buff = await blobStoageService.readFile(`${datafile.id}.csv`);

        const dataArray: Array<Array<string>> = (await parse(buff, {
            delimiter: ','
        }).toArray()) as string[][];
        const csvheaders = dataArray.shift();
        const total_pages = Math.ceil(dataArray.length / size);
        const errors = validateParams(page, total_pages, size);
        if (errors.length > 0) {
            return {
                success: false,
                errors,
                dataset_id: dataset.id
            };
        }

        const csvdata = paginate(dataArray, page, size);
        const pages = setupPagination(page, total_pages);
        const end_record = () => {
            if (size > dataArray.length) {
                return dataArray.length;
            } else if (page === total_pages) {
                return dataArray.length;
            } else {
                return page * size;
            }
        };
        const dto = await datasetToDatasetDTO(dataset);
        return {
            success: true,
            dataset: dto,
            current_page: page,
            page_info: {
                total_records: dataArray.length,
                start_record: (page - 1) * size + 1,
                end_record: end_record()
            },
            pages,
            page_size: size,
            total_pages,
            headers: csvheaders,
            data: csvdata
        };
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
};
