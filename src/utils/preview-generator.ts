import { Dataset } from '../entities/dataset/dataset';
import { ColumnHeader, ViewDTO } from '../dtos/view-dto';
import { FactTableColumnType } from '../enums/fact-table-column-type';
import { viewGenerator } from './view-error-generators';

export const sampleSize = 5;

export function previewGenerator(
  preview: Record<string, unknown>[],
  totals: { totalLines: number },
  dataset: Dataset,
  sample: boolean
): ViewDTO {
  const tableHeaders = Object.keys(preview[0]);
  const dataArray = preview.map((row: Record<string, unknown>) => Object.values(row));
  const headers: ColumnHeader[] = tableHeaders.map((name, idx) => ({
    index: idx,
    name,
    source_type: FactTableColumnType.Unknown
  });
  const pageInfo = {
    total_records: totals.totalLines,
    start_record: 1,
    end_record: preview.length
  };

  let pageSize = 0;
  if (sample) {
    pageSize = preview.length < sampleSize ? preview.length : sampleSize;
  } else {
    pageSize = preview.length;
  }

  return viewGenerator(dataset, 1, pageInfo, pageSize, 1, headers, dataArray);
}
