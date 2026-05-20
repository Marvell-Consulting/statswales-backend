export interface FilterRow {
  reference: string | number;
  language: string;
  fact_table_column: string;
  dimension_name: string;
  description: string;
  hierarchy: string | number | null;
  sort_order?: string | null; // Backed by a BIGINT column in the DB, but represented here as a string.
  reference_count?: string | null;
}
