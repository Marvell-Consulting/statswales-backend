import { CSVHeader } from '../dtos/view-dto';
import { FactTableColumnType } from '../enums/fact-table-column-type';
import { TableData } from 'duckdb-async';

export const tableDataToViewTable = (tableData: TableData) => {
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
