export interface FilterRow {
  reference: string;
  language: string;
  fact_table_column: string;
  dimension_name: string;
  description: string;
  hierarchy: string;
  sort_order?: string | null;
  reference_count?: string | null;
}
