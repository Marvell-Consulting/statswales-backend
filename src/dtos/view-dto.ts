import { FactTableColumnType } from '../enums/fact-table-column-type';
import { Error } from './error';
import { DatasetDTO } from './dataset-dto';
import { DataTableDto } from './data-table-dto';
import { PageInfo } from '../interfaces/page-info';

export interface ColumnHeader {
  index: number;
  name: string;
  source_type?: FactTableColumnType;
  extractor?: Record<string, string>;
}

export interface ViewErrDTO {
  status: number;
  errors: Error[];
  dataset_id: string | undefined;
  headers?: ColumnHeader[];
  data?: string[][];
  extension?: object;
}

export interface ViewDTO {
  dataset: DatasetDTO;
  data_table?: DataTableDto;
  current_page: number;
  page_info: PageInfo;
  page_size: number;
  total_pages: number;
  headers: ColumnHeader[];
  data: string[][] | unknown[][];
  extension?: object;
  note_codes?: string[];
}
