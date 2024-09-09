import { Readable } from 'stream';

import { Error } from './error';
import { DatasetDTO, ImportDTO } from './dataset-dto';

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
    errors: Error[];
    dataset_id: string | undefined;
}

export interface ViewDTO {
    success: boolean;
    dataset: DatasetDTO;
    import: ImportDTO;
    current_page: number;
    page_info: PageInfo;
    pages: Array<string | number>;
    page_size: number;
    total_pages: number;
    headers: Array<CSVHeader>;
    data: Array<Array<string>>;
}

export interface ViewStream {
    success: boolean;
    stream: Readable;
}
