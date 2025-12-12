export interface FilterValues {
  reference: string;
  description: string;
  children?: FilterValues[];
}

export interface FilterTable {
  columnName: string;
  factTableColumn: string;
  values: FilterValues[];
}
