import { ColumnDescriptor } from '../extractors/column-descriptor';

export interface MeasureLookupPatchDTO {
    join_column: string;
    description_columns: string[];
    notes_columns?: string[];
    sort_column?: string;
    measure_type_column: string;
    format_column: string;
    decimal_column: string;
    language_column: string;
}
