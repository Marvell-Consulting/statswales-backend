import { t } from 'i18next';

import { ColumnHeader, ViewDTO, ViewErrDTO } from '../dtos/view-dto';
import { ErrorMessage } from '../dtos/error';
import { AVAILABLE_LANGUAGES } from '../middleware/translation';
import { Dataset } from '../entities/dataset/dataset';
import { DataTable } from '../entities/dataset/data-table';
import { DatasetDTO } from '../dtos/dataset-dto';
import { DataTableDto } from '../dtos/data-table-dto';

export interface PageInfo {
  total_records: number;
  start_record: number;
  end_record: number;
}

export const viewErrorGenerators = (
  status: number,
  dataset_id: string,
  field: string,
  tag: string,
  extension: object,
  params = {}
): ViewErrDTO => {
  const userMessages: ErrorMessage[] = AVAILABLE_LANGUAGES.map((lang) => {
    return {
      message: t(tag, { ...params, lng: lang }),
      lang
    };
  });
  return {
    status,
    dataset_id,
    errors: [
      {
        field,
        message: { key: tag, params },
        user_message: userMessages
      }
    ],
    extension
  };
};

export const viewGenerator = (
  dataset: Dataset,
  page: number,
  pageInfo: PageInfo,
  size: number,
  totalPages: number,
  headers: ColumnHeader[],
  data: string[][] | unknown[][],
  dataTable?: DataTable
): ViewDTO => {
  return {
    dataset: DatasetDTO.fromDataset(dataset),
    data_table: dataTable ? DataTableDto.fromDataTable(dataTable) : undefined,
    current_page: page,
    page_info: pageInfo,
    page_size: size,
    total_pages: totalPages,
    headers,
    data
  };
};
