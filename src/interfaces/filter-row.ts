export interface FilterRow {
  reference: string;
  language: string;
  fact_table_column: string;
  dimension_name: string;
  description: string;
  hierarchy: string;
  sort_order?: string | null; // This is actually a bigint
  reference_count?: string | null;
}
