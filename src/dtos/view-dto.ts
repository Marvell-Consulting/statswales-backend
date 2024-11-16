import { Readable } from 'stream';

import { SourceType } from '../enums/source-type';

import { Error } from './error';
import { DatasetDTO } from './dataset-dto';
import { FileImportDTO } from './file-import-dto';

export interface CSVHeader {
    index: number;
    name: string;
    source_type?: SourceType;
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
}

export interface ViewDTO {
    dataset: DatasetDTO;
    import: FileImportDTO;
    current_page: number;
    page_info: PageInfo;
    page_size: number;
    total_pages: number;
    headers: CSVHeader[];
    data: string[][];
}

export interface ViewStream {
    success: boolean;
    stream: Readable;
}
