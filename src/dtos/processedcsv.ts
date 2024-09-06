import { DatasetDTO } from './dataset-dto';
import { Error } from './error';

export interface PageInfo {
    total_records: number | undefined;
    start_record: number | undefined;
    end_record: number | undefined;
}

export interface ProcessedCSV {
    success: boolean;
    dataset: DatasetDTO | undefined;
    current_page: number | undefined;
    page_info: PageInfo | undefined;
    pages: Array<string | number> | undefined;
    page_size: number | undefined;
    total_pages: number | undefined;
    headers: Array<string> | undefined;
    data: Array<Array<string>> | undefined;
    errors: Array<Error> | undefined;
}
