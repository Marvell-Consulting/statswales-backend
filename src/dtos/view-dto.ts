/* eslint-disable import/no-cycle */
import { Error } from '../models/error';

import { DatasetDTO } from './dataset-dto';

export interface PageInfo {
    total_records: number | undefined;
    start_record: number | undefined;
    end_record: number | undefined;
}

export interface ViewErrDTO {
    success: boolean;
    errors: Error[];
    dataset_id: string;
}

export interface ViewDTO {
    success: boolean;
    dataset: DatasetDTO;
    current_page: number;
    page_info: PageInfo;
    pages: Array<string | number>;
    page_size: number;
    total_pages: number;
    headers: Array<string> | undefined;
    data: Array<Array<string>>;
}
