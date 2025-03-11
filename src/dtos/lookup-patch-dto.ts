export interface LookupTablePatchDTO {
  join_column: string;
  description_columns: string[];
  notes_column?: string[];
  sort_column?: string;
  hierarchy?: string;
  language?: string;
}
