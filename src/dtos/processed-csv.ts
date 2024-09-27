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
    pages: (string | number)[] | undefined;
    page_size: number | undefined;
    total_pages: number | undefined;
    headers: string[] | undefined;
    data: string[][] | undefined;
    errors: Error[] | undefined;
}
