import { ColumnHeader } from '../dtos/view-dto';
import { FactTableColumnType } from '../enums/fact-table-column-type';

type ViewTable = {
  headers: ColumnHeader[];
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  data: any[][];
};

export const tableDataToViewTable = (tableData: Record<string, JSON>[]): ViewTable => {
  const tableHeaders = Object.keys(tableData[0]);
  const dataArray = tableData.map((row) => Object.values(row));
  const headers: ColumnHeader[] = tableHeaders.map((header, idx) => ({
    index: idx,
    name: header,
    sourceType: header === 'line_number' ? FactTableColumnType.LineNumber : FactTableColumnType.Unknown
  }));
  return {
    headers,
    data: dataArray
  };
};
