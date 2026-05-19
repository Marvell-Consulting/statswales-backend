export interface FilterRow {
  reference: string | number;
  language: string;
  fact_table_column: string;
  dimension_name: string;
  description: string;
  hierarchy: string | number | null;
  sort_order?: string | null;
  reference_count?: string | null;
}
