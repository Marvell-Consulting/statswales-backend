import { getFileService } from '../utils/get-file-service';
import { Revision } from '../entities/dataset/revision';
import { validateParams } from '../validators/preview-validator';
import { CSVHeader, ViewDTO, ViewErrDTO } from '../dtos/view-dto';
import { FactTableColumnType } from '../enums/fact-table-column-type';
import { DatasetDTO } from '../dtos/dataset-dto';
import { Dataset } from '../entities/dataset/dataset';

export const getTableRowsNoFilter = async (
  datasetId: string,
  revision: Revision,
  lang: string,
  startRow: number,
  endRow: number,
  sortBy?: string
) => {
  const { parquetMetadata, parquetQuery } = await import('hyparquet');
  const fileService = getFileService();
  const buffer = await fileService.loadBuffer(`${revision.id}_${lang}.parquet`, datasetId);
  const arrayBuffer = buffer.buffer.slice(buffer.byteOffset, buffer.byteOffset + buffer.byteLength);

  const asyncBuffer = {
    byteLength: arrayBuffer.byteLength,
    slice: async (start: number | undefined, end: number | undefined) => arrayBuffer.slice(start, end)
  };

  const metadata = parquetMetadata(arrayBuffer);
  const rows = await parquetQuery({ file: asyncBuffer, orderBy: sortBy, rowStart: startRow, rowEnd: endRow });

  return { metadata, rows };
};

export const getTableMetadata = async (datasetId: string, revision: Revision, lang: string) => {
  const { parquetMetadata } = await import('hyparquet');
  const fileService = getFileService();
  const buffer = await fileService.loadBuffer(`${revision.id}_${lang}.parquet`, datasetId);
  const arrayBuffer = buffer.buffer.slice(buffer.byteOffset, buffer.byteOffset + buffer.byteLength);
  return parquetMetadata(arrayBuffer);
};

export const createView = async (
  dataset: Dataset,
  revision: Revision,
  lang: string,
  pageNumber: number,
  pageSize: number,
  sortBy?: string
): Promise<ViewDTO | ViewErrDTO> => {
  const tableMetadata = await getTableMetadata(dataset.id, revision, lang);
  const totalPages = Math.ceil(Number(tableMetadata.num_rows) / pageSize);
  const errors = validateParams(pageNumber, totalPages, pageSize);
  if (errors.length > 0) {
    return {
      status: 400,
      errors,
      dataset_id: dataset.id
    };
  }
  const startRow = (pageNumber - 1) * pageSize;
  const endRow = pageNumber * pageSize;
  const data = await getTableRowsNoFilter(dataset.id, revision, lang, startRow, endRow, sortBy);
  const tableHeaders = Object.keys(data.rows[0]);
  const dataArray = data.rows.map((row) => Object.values(row));
  const headers: CSVHeader[] = [];
  for (let i = 0; i < tableHeaders.length; i++) {
    headers.push({
      index: i - 1,
      name: tableHeaders[i],
      source_type: FactTableColumnType.Unknown
    });
  }
  return {
    dataset: DatasetDTO.fromDataset(dataset),
    current_page: pageNumber,
    page_info: {
      total_records: Number(tableMetadata.num_rows),
      start_record: startRow + 1,
      end_record: endRow < Number(tableMetadata.num_rows) ? endRow : Number(tableMetadata.num_rows)
    },
    page_size: pageSize,
    total_pages: totalPages,
    headers,
    data: dataArray
  };
};
