export interface LookupTablePatchDTO {
    join_column: string;
    description_columns: string[];
    notes_column: string[] | undefined;
    sort_column: string | undefined;
    hierarchy: string | undefined;
}
