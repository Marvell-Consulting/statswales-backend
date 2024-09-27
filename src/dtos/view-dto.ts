import { Readable } from 'stream';

import { Error } from './error';
import { DatasetDTO } from './dataset-dto';
import { ImportDTO } from './fileimport-dto';

export interface CSVHeader {
    index: number;
    name: string;
}

export interface PageInfo {
    total_records: number | undefined;
    start_record: number | undefined;
    end_record: number | undefined;
}

export interface ViewErrDTO {
    success: boolean;
    status: number;
    errors: Error[];
    dataset_id: string | undefined;
}

export interface ViewDTO {
    success: boolean;
    dataset: DatasetDTO;
    import: ImportDTO;
    current_page: number;
    page_info: PageInfo;
    pages: (string | number)[];
    page_size: number;
    total_pages: number;
    headers: CSVHeader[];
    data: string[][];
}

export interface ViewStream {
    success: boolean;
    stream: Readable;
}
