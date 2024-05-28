/* eslint-disable import/no-cycle */
import { createHash } from 'crypto';

import { parse } from 'csv';
import pino from 'pino';

import { UploadDTO, UploadErrDTO } from '../dtos/upload-dto';
import { Error } from '../models/error';
import { Datafile } from '../entity/datafile';
import { Dataset } from '../entity/dataset';
import { ViewDTO, ViewErrDTO } from '../dtos/view-dto';
import { datasetToDatasetDTO } from '../dtos/dataset-dto';

import { DataLakeService } from './datalake';

const MAX_PAGE_SIZE = 500;
const MIN_PAGE_SIZE = 5;
export const DEFAULT_PAGE_SIZE = 100;

export const logger = pino({
    name: 'StatsWales-Alpha-App',
    level: 'debug'
});

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
        errors.push({ field: 'page_size', message: `Page size must be between ${MIN_PAGE_SIZE} and ${MAX_PAGE_SIZE}` });
    }
    if (!validatMaxPageNumber(page_number, max_page_number)) {
        errors.push({ field: 'page_number', message: `Page number must be less than or equal to ${max_page_number}` });
    }
    if (!validatePageNumber(page_number)) {
        errors.push({ field: 'page_number', message: 'Page number must be greater than 0' });
    }
    return errors;
}

export const uploadCSV = async (buff: Buffer, dataset: Dataset): Promise<UploadDTO | UploadErrDTO> => {
    const dataLakeService = new DataLakeService();
    const hash = createHash('sha256').update(buff).digest('hex');
    const datafile = Datafile.createDatafile(dataset, hash.toString(), 'BetaUser');
    const savedDataFile = await datafile.save();
    const dto = await datasetToDatasetDTO(dataset);
    if (buff) {
        try {
            logger.debug(`Uploading file ${savedDataFile.id} to datalake`);
            await dataLakeService.uploadFile(`${savedDataFile.id}.csv`, buff);
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
                errors: [{ field: 'csv', message: 'Error uploading file to datalake' }]
            };
        }
    } else {
        logger.debug('No buffer to upload to datalake');
        datafile.remove();
        return {
            success: false,
            dataset: dto,
            errors: [{ field: 'csv', message: 'No CSV data available' }]
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

export const processCSV = async (dataset: Dataset, page: number, size: number): Promise<ViewErrDTO | ViewDTO> => {
    const datalakeService = new DataLakeService();
    try {
        const datafiles = await dataset.datafiles;
        const datafile: Datafile | undefined = datafiles
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
                        message: 'No datafile attached to Dataset'
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
            errors: [{ field: 'csv', message: 'Error downloading file from datalake' }],
            dataset_id: dataset.id
        };
    }
};
