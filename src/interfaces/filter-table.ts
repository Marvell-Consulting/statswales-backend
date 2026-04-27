export interface FilterValues {
  reference: string;
  description: string;
  count?: number;
  children?: FilterValues[];
}

export interface FilterTable {
  columnName: string;
  factTableColumn: string;
  values: FilterValues[];
}
