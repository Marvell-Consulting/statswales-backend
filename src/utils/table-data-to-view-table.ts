import { CSVHeader } from '../dtos/view-dto';
import { FactTableColumnType } from '../enums/fact-table-column-type';
import { TableData } from 'duckdb-async';

type ViewTable = {
  headers: CSVHeader[];
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  data: any[][];
};

export const tableDataToViewTable = (tableData: TableData): ViewTable => {
  const tableHeaders = Object.keys(tableData[0]);
  const dataArray = tableData.map((row) => Object.values(row));
  const headers: CSVHeader[] = tableHeaders.map((header, idx) => ({
    index: idx,
    name: header,
    sourceType: header === 'line_number' ? FactTableColumnType.LineNumber : FactTableColumnType.Unknown
  }));
  return {
    headers,
    data: dataArray
  };
};
