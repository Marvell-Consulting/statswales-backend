import { DatasetDTO } from './dataset-dto';
import { Error } from './error';
import { PageInfo } from '../interfaces/page-info';

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
