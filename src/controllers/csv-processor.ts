import { parse } from 'csv';
import pino from 'pino';

import { ProcessedCSV } from '../models/processedcsv';
import { Error } from '../models/error';
import { Datafile } from '../entity/Datafile';

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

export const uploadCSV = async (buff: Buffer, datafile: Datafile): Promise<ProcessedCSV> => {
    const dataLateService = new DataLakeService();
    if (buff) {
        try {
            logger.debug(`Uploading file ${datafile} to datalake`);
            await dataLateService.uploadFile(`${datafile.id}.csv`, buff);
            return {
                success: true,
                datafile_id: datafile.id,
                datafile_name: datafile.name,
                datafile_description: datafile.description,
                page_size: undefined,
                page_info: undefined,
                pages: undefined,
                current_page: undefined,
                total_pages: undefined,
                headers: undefined,
                data: undefined,
                errors: undefined
            };
        } catch (err) {
            logger.error(err);
            datafile.remove();
            return {
                success: false,
                datafile_id: undefined,
                datafile_name: undefined,
                datafile_description: undefined,
                page_size: undefined,
                page_info: undefined,
                pages: undefined,
                current_page: undefined,
                total_pages: undefined,
                headers: undefined,
                data: undefined,
                errors: [{ field: 'csv', message: 'Error uploading file to datalake' }]
            };
        }
    } else {
        logger.debug('No buffer to upload to datalake');
        datafile.remove();
        return {
            success: false,
            datafile_id: datafile.id,
            datafile_name: undefined,
            datafile_description: undefined,
            page_size: undefined,
            page_info: undefined,
            pages: undefined,
            current_page: undefined,
            total_pages: undefined,
            headers: undefined,
            data: undefined,
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

export const processCSV = async (filename: string, page: number, size: number): Promise<ProcessedCSV> => {
    const dataLateService = new DataLakeService();
    try {
        const buff = await dataLateService.downloadFile(`${filename}.csv`);
        const dataArray: Array<Array<string>> = (await parse(buff, {
            delimiter: ','
        }).toArray()) as string[][];
        const csvheaders = dataArray.shift();
        const total_pages = Math.ceil(dataArray.length / size);
        const errors = validateParams(page, total_pages, size);
        if (errors.length > 0) {
            return {
                success: false,
                datafile_id: filename,
                datafile_name: undefined,
                datafile_description: undefined,
                page_size: undefined,
                page_info: undefined,
                pages: undefined,
                current_page: undefined,
                total_pages: undefined,
                headers: undefined,
                data: undefined,
                errors
            };
        }
        const datafile = await Datafile.findOneBy({ id: filename });
        if (datafile === null) {
            return {
                success: false,
                datafile_id: filename,
                datafile_name: undefined,
                datafile_description: undefined,
                page_size: undefined,
                page_info: undefined,
                pages: undefined,
                current_page: undefined,
                total_pages: undefined,
                headers: undefined,
                data: undefined,
                errors: [{ field: 'csv', message: 'unable to find datafile in database' }]
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
        return {
            success: true,
            datafile_id: filename,
            datafile_name: datafile.name,
            datafile_description: datafile.description,
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
            data: csvdata,
            errors: undefined
        };
    } catch (err) {
        logger.error(err);
        return {
            success: false,
            datafile_id: filename,
            datafile_name: undefined,
            datafile_description: undefined,
            page_size: undefined,
            page_info: undefined,
            pages: undefined,
            current_page: undefined,
            total_pages: undefined,
            headers: undefined,
            data: undefined,
            errors: [{ field: 'csv', message: 'Error downloading file from datalake' }]
        };
    }
};
