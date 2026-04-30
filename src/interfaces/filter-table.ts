export interface FilterValues {
  reference: string;
  description: string;
  count?: string | null;
  children?: FilterValues[];
}

export interface FilterTable {
  columnName: string;
  factTableColumn: string;
  values: FilterValues[];
}
