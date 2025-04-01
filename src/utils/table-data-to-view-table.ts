import { CSVHeader } from '../dtos/view-dto';
import { FactTableColumnType } from '../enums/fact-table-column-type';
import { TableData } from 'duckdb-async';

export const tableDataToViewTable = (tableData: TableData) => {
  const tableHeaders = Object.keys(tableData[0]);
  const dataArray = tableData.map((row) => Object.values(row));
  const headers: CSVHeader[] = [];
  for (let i = 0; i < tableHeaders.length; i++) {
    let sourceType = FactTableColumnType.Unknown;
    if (tableHeaders[i] === 'line_number') {
      sourceType = FactTableColumnType.LineNumber;
    }
    headers.push({
      index: i,
      name: tableHeaders[i],
      source_type: sourceType
    });
  }
  return {
    headers,
    data: dataArray
  };
};
