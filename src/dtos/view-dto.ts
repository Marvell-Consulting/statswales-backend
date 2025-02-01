import { Readable } from 'stream';

import { FactTableColumnType } from '../enums/fact-table-column-type';

import { Error } from './error';
import { DatasetDTO } from './dataset-dto';
import { DataTableDto } from './data-table-dto';

export interface CSVHeader {
    index: number;
    name: string;
    source_type?: FactTableColumnType;
}

export interface PageInfo {
    total_records: number | undefined;
    start_record: number | undefined;
    end_record: number | undefined;
}

export interface ViewErrDTO {
    status: number;
    errors: Error[];
    dataset_id: string | undefined;
    headers?: CSVHeader[];
    data?: string[][];
    extension?: object;
}

export interface ViewDTO {
    dataset: DatasetDTO;
    fact_table?: DataTableDto;
    current_page: number;
    page_info: PageInfo;
    page_size: number;
    total_pages: number;
    headers: CSVHeader[];
    data: string[][];
    extension?: object;
}

export interface ViewStream {
    success: boolean;
    stream: Readable;
}
